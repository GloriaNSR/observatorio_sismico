"""API HTTP de consulta del observatorio sísmico."""

import logging
import math
import os
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse


LOGGER = logging.getLogger("api")
SOURCE_LIVE = "usgs_live"
MAX_LIMIT = 5000
DEFAULT_LIMIT = 1000
HISTORY_START = datetime(2025, 1, 1, tzinfo=timezone.utc)
HISTORY_END = datetime(2026, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class Filtros:
    start: datetime | None
    end: datetime | None
    min_mag: float | None
    country_id: str | None
    mode: str


def _origenes_cors() -> list[str]:
    origenes = {"http://localhost:4200"}
    for variable in ("CORS_ORIGINS", "NETLIFY_ORIGIN"):
        for origen in os.getenv(variable, "").split(","):
            origen = origen.strip().rstrip("/")
            if origen:
                origenes.add(origen)
    return sorted(origenes)


app = FastAPI(title="Observatorio Sísmico API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origenes_cors(),
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["ngrok-skip-browser-warning"],
)


@app.exception_handler(psycopg2.Error)
async def manejar_error_postgres(_request: Request, error: psycopg2.Error):
    LOGGER.error(
        "Error al consultar PostgreSQL",
        exc_info=(type(error), error, error.__traceback__),
    )
    return JSONResponse(
        status_code=503,
        content={"detail": "El almacenamiento del observatorio no está disponible"},
    )


def conectar_postgres():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=int(os.getenv("POSTGRES_CONNECT_TIMEOUT_SECONDS", "5")),
    )


def _parsear_fecha(valor: str | None, nombre: str) -> datetime | None:
    if valor is None:
        return None
    try:
        fecha = datetime.fromisoformat(valor.strip().replace("Z", "+00:00"))
    except ValueError as error:
        raise HTTPException(
            status_code=422,
            detail=f"{nombre} debe ser una fecha ISO 8601 válida",
        ) from error
    if fecha.tzinfo is None or fecha.utcoffset() is None:
        raise HTTPException(
            status_code=422,
            detail=f"{nombre} debe incluir una zona horaria",
        )
    return fecha.astimezone(timezone.utc)


def _validar_filtros(
    start: str | None,
    end: str | None,
    min_mag: float | None,
    country_id: str | None,
    mode: str,
    *,
    ahora: datetime | None = None,
) -> Filtros:
    inicio = _parsear_fecha(start, "start")
    fin = _parsear_fecha(end, "end")
    if inicio is None and fin is None:
        fin = (ahora or datetime.now(timezone.utc)).astimezone(timezone.utc)
        inicio = fin - timedelta(hours=24)
    if inicio is not None and fin is not None and inicio >= fin:
        raise HTTPException(
            status_code=422, detail="start debe ser anterior a end"
        )
    if min_mag is not None and not math.isfinite(min_mag):
        raise HTTPException(status_code=422, detail="min_mag debe ser finito")
    if mode not in {"live", "replay"}:
        raise HTTPException(status_code=422, detail="mode debe ser live o replay")
    if country_id is not None:
        country_id = country_id.strip()
        if not country_id or len(country_id) > 10:
            raise HTTPException(
                status_code=422, detail="country_id debe contener entre 1 y 10 caracteres"
            )
    return Filtros(inicio, fin, min_mag, country_id, mode)


def _where(filtros: Filtros) -> tuple[str, list[Any]]:
    clausulas = ["e.mode = %s"]
    parametros: list[Any] = [filtros.mode]
    if filtros.start is not None:
        clausulas.append("e.event_time >= %s")
        parametros.append(filtros.start)
    if filtros.end is not None:
        clausulas.append("e.event_time < %s")
        parametros.append(filtros.end)
    if filtros.min_mag is not None:
        clausulas.append("e.mag >= %s")
        parametros.append(filtros.min_mag)
    if filtros.country_id == "unassigned":
        clausulas.append("e.country_id IS NULL")
    elif filtros.country_id is not None:
        clausulas.append("e.country_id = %s")
        parametros.append(filtros.country_id)
    return " AND ".join(clausulas), parametros


def _where_geografico(country_id: str | None) -> tuple[str, list[Any]]:
    if country_id == "unassigned":
        return "e.country_id IS NULL", []
    if country_id is not None:
        return "e.country_id = %s", [country_id]
    return "TRUE", []


def _milisegundos(fecha: datetime | None) -> int | None:
    if fecha is None:
        return None
    return int(fecha.timestamp() * 1000)


def _ultima_ingesta(cursor) -> int | None:
    cursor.execute(
        "SELECT last_ingestion_ms FROM estado_ingesta WHERE source = %s",
        (SOURCE_LIVE,),
    )
    fila = cursor.fetchone()
    return fila[0] if fila and fila[0] is not None else None


def _consultar_eventos(cursor, filtros: Filtros, limit: int, offset: int):
    where_sql, parametros = _where(filtros)
    cursor.execute(
        f"SELECT COUNT(*) FROM evento_actual e WHERE {where_sql}",
        parametros,
    )
    total = cursor.fetchone()[0]
    cursor.execute(
        f"""
        SELECT e.event_id, e.time_ms, e.event_time, e.updated_ms,
               e.updated_time, e.mag, e.mag_type, e.place,
               e.longitude, e.latitude, e.depth_km, e.country_id,
               p.country_name
        FROM evento_actual e
        LEFT JOIN pais p ON p.country_id = e.country_id
        WHERE {where_sql}
        ORDER BY e.event_time DESC, e.event_id
        LIMIT %s OFFSET %s
        """,
        [*parametros, limit, offset],
    )
    return total, cursor.fetchall()


def _feature_geojson(fila: tuple[Any, ...]) -> dict[str, Any]:
    (
        event_id,
        time_ms,
        _event_time,
        updated_ms,
        _updated_time,
        mag,
        mag_type,
        place,
        longitude,
        latitude,
        depth_km,
        country_id,
        country_name,
    ) = fila
    coordinates = [longitude, latitude]
    if depth_km is not None:
        coordinates.append(depth_km)
    return {
        "type": "Feature",
        "id": event_id,
        "geometry": {"type": "Point", "coordinates": coordinates},
        "properties": {
            "mag": mag,
            "magType": mag_type,
            "place": place,
            "time": time_ms,
            "updated": updated_ms,
            "depth": depth_km,
            "depth_km": depth_km,
            "country_id": country_id,
            "country_name": country_name,
        },
    }


@app.get("/api/eventos")
def obtener_eventos(
    start: str | None = None,
    end: str | None = None,
    min_mag: float | None = None,
    country_id: str | None = None,
    mode: str = "live",
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    ahora = datetime.now(timezone.utc)
    filtros = _validar_filtros(
        start, end, min_mag, country_id, mode, ahora=ahora
    )
    with closing(conectar_postgres()) as conexion:
        with conexion.cursor() as cursor:
            ultima_ingesta = _ultima_ingesta(cursor)
            total, filas = _consultar_eventos(cursor, filtros, limit, offset)

    count = len(filas)
    next_offset = offset + count if offset + count < total else None
    return {
        "type": "FeatureCollection",
        "metadata": {
            "generated": _milisegundos(ahora),
            "last_ingestion": ultima_ingesta,
            "mode": filtros.mode,
            "count": count,
            "total": total,
            "truncated": next_offset is not None,
            "next_cursor": next_offset,
        },
        "features": [_feature_geojson(fila) for fila in filas],
    }


@app.get("/api/salud")
def obtener_salud() -> dict[str, Any]:
    ahora = datetime.now(timezone.utc)
    umbral = int(os.getenv("INGESTION_STALE_SECONDS", "180"))
    with closing(conectar_postgres()) as conexion:
        with conexion.cursor() as cursor:
            ultima_ingesta = _ultima_ingesta(cursor)
    antiguedad = (
        max(0, int(ahora.timestamp() - ultima_ingesta / 1000))
        if ultima_ingesta is not None
        else None
    )
    return {
        "status": "ok",
        "api": "available",
        "database": "available",
        "last_ingestion": ultima_ingesta,
        "ingestion_age_seconds": antiguedad,
        "ingestion_stale": antiguedad is None or antiguedad > umbral,
    }


def _obtener_distribucion(cursor, where_sql: str, parametros: list[Any]):
    cursor.execute(
        f"""
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE e.mag IS NULL),
               COUNT(*) FILTER (WHERE e.depth_km IS NULL)
        FROM evento_actual e
        WHERE {where_sql}
        """,
        parametros,
    )
    total, magnitudes_nulas, profundidades_nulas = cursor.fetchone()

    cursor.execute(
        f"""
        SELECT width_bucket(e.mag, 0::double precision, 10::double precision, 20),
               COUNT(*)
        FROM evento_actual e
        WHERE {where_sql} AND e.mag IS NOT NULL
        GROUP BY 1
        ORDER BY 1
        """,
        parametros,
    )
    magnitudes = [
        {"range": _etiqueta_bin(row[0], 0, 0.5, 20, "magnitud"), "count": row[1]}
        for row in cursor.fetchall()
    ]

    cursor.execute(
        f"""
        SELECT width_bucket(e.depth_km, 0::double precision, 700::double precision, 14),
               COUNT(*)
        FROM evento_actual e
        WHERE {where_sql} AND e.depth_km IS NOT NULL
        GROUP BY 1
        ORDER BY 1
        """,
        parametros,
    )
    profundidades = [
        {"range": _etiqueta_bin(row[0], 0, 50, 14, "profundidad"), "count": row[1]}
        for row in cursor.fetchall()
    ]
    return (
        total,
        {
            "unit": "magnitud",
            "null_count": magnitudes_nulas,
            "excluded_nulls": magnitudes_nulas,
            "bins": magnitudes,
        },
        {
            "unit": "km",
            "null_count": profundidades_nulas,
            "excluded_nulls": profundidades_nulas,
            "bins": profundidades,
        },
    )


def _etiqueta_bin(
    bucket: int, minimo: float, ancho: float, numero_bins: int, tipo: str
) -> str:
    if bucket == 0:
        return "<0" if tipo == "magnitud" else "<0 km"
    if bucket == numero_bins + 1:
        maximo = minimo + ancho * numero_bins
        return f">={maximo:g}" if tipo == "magnitud" else f">={maximo:g} km"
    inferior = minimo + ancho * (bucket - 1)
    superior = inferior + ancho
    if tipo == "magnitud":
        return f"[{inferior:g}, {superior:g})"
    return f"[{inferior:g}, {superior:g}) km"


def _obtener_comparativa(
    cursor, filtros: Filtros, ahora: datetime
) -> dict[str, Any]:
    geo_sql, geo_params = _where_geografico(filtros.country_id)
    min_mag = max(4.0, filtros.min_mag) if filtros.min_mag is not None else 4.0
    inicio_live = ahora - timedelta(hours=24)
    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM evento_actual e
        WHERE e.mode = 'live'
          AND e.event_time >= %s AND e.event_time < %s
          AND e.mag >= %s AND {geo_sql}
        """,
        [inicio_live, ahora, min_mag, *geo_params],
    )
    live_count = cursor.fetchone()[0]
    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM evento_actual e
        WHERE e.mode = 'replay'
          AND e.event_time >= %s AND e.event_time < %s
          AND e.mag >= %s AND {geo_sql}
        """,
        [HISTORY_START, HISTORY_END, min_mag, *geo_params],
    )
    replay_count = cursor.fetchone()[0]
    return {
        "unit": "eventos",
        "minimum_magnitude": min_mag,
        "geographic_filter": filtros.country_id,
        "live_last_24h": {
            "start": inicio_live.isoformat(),
            "end": ahora.isoformat(),
            "count": live_count,
        },
        "replay_2025": {
            "start": HISTORY_START.isoformat(),
            "end": HISTORY_END.isoformat(),
            "days": 365,
            "count": replay_count,
            "daily_average": replay_count / 365,
        },
        "includes_zero_event_days": True,
    }


@app.get("/api/estadisticas")
def obtener_estadisticas(
    start: str | None = None,
    end: str | None = None,
    min_mag: float | None = None,
    country_id: str | None = None,
    mode: str = "live",
) -> dict[str, Any]:
    ahora = datetime.now(timezone.utc)
    filtros = _validar_filtros(
        start, end, min_mag, country_id, mode, ahora=ahora
    )
    where_sql, parametros = _where(filtros)
    with closing(conectar_postgres()) as conexion:
        with conexion.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT e.country_id,
                       COALESCE(p.country_name, 'Sin país asignado') AS country_name,
                       p.continent,
                       COALESCE(p.subregion, 'Sin país asignado') AS subregion,
                       COUNT(*)
                FROM evento_actual e
                LEFT JOIN pais p ON p.country_id = e.country_id
                WHERE {where_sql}
                GROUP BY e.country_id, p.country_name, p.continent, p.subregion
                ORDER BY COUNT(*) DESC, country_name
                """,
                parametros,
            )
            por_pais = [
                {
                    "country_id": row[0],
                    "country_name": row[1],
                    "continent": row[2],
                    "subregion": row[3],
                    "count": row[4],
                }
                for row in cursor.fetchall()
            ]
            cursor.execute(
                f"""
                SELECT date_trunc('hour', e.event_time AT TIME ZONE 'UTC') AS hour_utc,
                       COUNT(*)
                FROM evento_actual e
                WHERE {where_sql}
                GROUP BY hour_utc
                ORDER BY hour_utc
                """,
                parametros,
            )
            por_hora = [
                {
                    "hour": row[0].replace(tzinfo=timezone.utc).isoformat(),
                    "count": row[1],
                }
                for row in cursor.fetchall()
            ]
            total, magnitudes, profundidades = _obtener_distribucion(
                cursor, where_sql, parametros
            )
            comparativa = _obtener_comparativa(cursor, filtros, ahora)

    return {
        "metadata": {
            "generated": _milisegundos(ahora),
            "mode": filtros.mode,
            "start": filtros.start.isoformat() if filtros.start else None,
            "end": filtros.end.isoformat() if filtros.end else None,
            "min_mag": filtros.min_mag,
            "country_id": filtros.country_id,
            "total": total,
            "complete": True,
        },
        "events_by_country": por_pais,
        "events_by_hour": por_hora,
        "magnitude_distribution": magnitudes,
        "depth_distribution": profundidades,
        "historical_comparison": comparativa,
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=int(os.getenv("API_INTERNAL_PORT", "8000")),
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )
