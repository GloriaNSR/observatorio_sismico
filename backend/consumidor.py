"""Valida eventos Kafka y los persiste en PostgreSQL de forma idempotente."""

import asyncio
import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import geopandas as gpd
import psycopg2
from aiokafka import AIOKafkaConsumer
from shapely.geometry import Point


LOGGER = logging.getLogger("consumidor")
PAISES_ESPERADOS = 177


@dataclass
class IndicePaises:
    geodataframe: gpd.GeoDataFrame
    indice_espacial: Any

    def country_for_point(self, longitude: float, latitude: float) -> str | None:
        punto = Point(longitude, latitude)
        posiciones = self.indice_espacial.query(punto, predicate="within")
        for posicion in posiciones:
            country_id = self.geodataframe.iloc[int(posicion)]["ADM0_A3"]
            if isinstance(country_id, str) and country_id:
                return country_id
        return None


@dataclass(frozen=True)
class EventoValidado:
    event_id: str
    time_ms: int
    updated_ms: int
    event_time: datetime
    updated_time: datetime
    mag: float | None
    mag_type: str | None
    place: str | None
    longitude: float
    latitude: float
    depth_km: float | None
    country_id: str | None
    fecha_id: date
    dia_semana: int


def _es_numero_finito(valor: Any) -> bool:
    return (
        isinstance(valor, (int, float))
        and not isinstance(valor, bool)
        and math.isfinite(valor)
    )


def _valor_numerico_opcional(valor: Any, nombre: str) -> float | None:
    if valor is None:
        return None
    if not _es_numero_finito(valor):
        raise ValueError(f"{nombre} debe ser numérico finito o null")
    return float(valor)


def _datetime_epoch_utc(milisegundos: int, nombre: str) -> datetime:
    try:
        return datetime.fromtimestamp(milisegundos / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as error:
        raise ValueError(f"{nombre} fuera del rango de fecha admitido") from error


def validar_evento(feature: Any, indice_paises: IndicePaises) -> EventoValidado:
    if not isinstance(feature, dict) or feature.get("type") != "Feature":
        raise ValueError("El evento no es un Feature GeoJSON")

    properties = feature.get("properties")
    if not isinstance(properties, dict) or properties.get("type") != "earthquake":
        raise ValueError('properties.type debe ser "earthquake"')

    geometry = feature.get("geometry")
    if not isinstance(geometry, dict) or geometry.get("type") != "Point":
        raise ValueError("La geometría debe ser Point")

    coordinates = geometry.get("coordinates")
    if (
        not isinstance(coordinates, list)
        or len(coordinates) < 2
        or not _es_numero_finito(coordinates[0])
        or not _es_numero_finito(coordinates[1])
    ):
        raise ValueError("Las coordenadas Point deben incluir longitud y latitud válidas")
    longitude, latitude = float(coordinates[0]), float(coordinates[1])
    if not -180 <= longitude <= 180:
        raise ValueError("Longitud fuera del rango WGS84 [-180, 180]")
    if not -90 <= latitude <= 90:
        raise ValueError("Latitud fuera del rango WGS84 [-90, 90]")

    event_id = feature.get("id")
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("El evento no tiene un id válido")

    time_ms = properties.get("time")
    updated_ms = properties.get("updated")
    if isinstance(time_ms, bool) or not isinstance(time_ms, int):
        raise ValueError("properties.time debe ser un entero Unix en milisegundos")
    if isinstance(updated_ms, bool) or not isinstance(updated_ms, int):
        raise ValueError("properties.updated debe ser un entero Unix en milisegundos")
    event_time = _datetime_epoch_utc(time_ms, "properties.time")
    updated_time = _datetime_epoch_utc(updated_ms, "properties.updated")
    fecha_id = event_time.date()

    depth_value = (
        properties["depth"]
        if "depth" in properties
        else coordinates[2] if len(coordinates) > 2 else None
    )
    mag = _valor_numerico_opcional(properties.get("mag"), "properties.mag")
    depth_km = _valor_numerico_opcional(depth_value, "depth")
    mag_type = properties.get("magType")
    place = properties.get("place")
    if mag_type is not None and not isinstance(mag_type, str):
        raise ValueError("properties.magType debe ser texto o null")
    if place is not None and not isinstance(place, str):
        raise ValueError("properties.place debe ser texto o null")

    return EventoValidado(
        event_id=event_id,
        time_ms=time_ms,
        updated_ms=updated_ms,
        event_time=event_time,
        updated_time=updated_time,
        mag=mag,
        mag_type=mag_type,
        place=place,
        longitude=longitude,
        latitude=latitude,
        depth_km=depth_km,
        country_id=indice_paises.country_for_point(longitude, latitude),
        fecha_id=fecha_id,
        dia_semana=fecha_id.isoweekday(),
    )


def cargar_paises(conexion, ruta_geojson: str) -> IndicePaises:
    geodataframe = gpd.read_file(ruta_geojson)
    if "ADM0_A3" not in geodataframe.columns:
        raise ValueError("paises.geojson no contiene la propiedad ADM0_A3")
    if len(geodataframe) != PAISES_ESPERADOS:
        raise ValueError(
            f"paises.geojson debe contener {PAISES_ESPERADOS} entidades; "
            f"contiene {len(geodataframe)}"
        )

    country_ids = geodataframe["ADM0_A3"]
    if country_ids.isna().any() or country_ids.nunique() != PAISES_ESPERADOS:
        raise ValueError("ADM0_A3 debe tener 177 identificadores distintos y no nulos")
    if geodataframe.geometry.isna().any():
        raise ValueError("paises.geojson contiene geometrías nulas")

    if geodataframe.crs is None:
        geodataframe = geodataframe.set_crs("EPSG:4326")
    elif geodataframe.crs.to_epsg() != 4326:
        geodataframe = geodataframe.to_crs("EPSG:4326")

    filas = []
    for _, entidad in geodataframe.iterrows():
        propiedades = entidad
        nombre_es = propiedades.get("NAME_ES")
        admin = propiedades.get("ADMIN")
        nombre = (
            nombre_es
            if isinstance(nombre_es, str) and nombre_es.strip()
            else admin
        )
        if not isinstance(nombre, str) or not nombre.strip():
            raise ValueError(
                f"El país {propiedades['ADM0_A3']} no tiene NAME_ES ni ADMIN"
            )
        filas.append(
            (
                propiedades["ADM0_A3"],
                nombre,
                propiedades.get("CONTINENT"),
                propiedades.get("SUBREGION"),
            )
        )

    with conexion:
        with conexion.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO pais (country_id, country_name, continent, subregion)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (country_id) DO NOTHING
                """,
                filas,
            )

    return IndicePaises(geodataframe, geodataframe.sindex)


def _instante_ingesta(envelope: dict[str, Any]) -> datetime:
    valor = envelope.get("ingested_at")
    if isinstance(valor, str):
        try:
            instante = datetime.fromisoformat(valor.replace("Z", "+00:00"))
            if instante.tzinfo is not None:
                return instante.astimezone(timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _rechazar_constante_json(valor: str) -> None:
    raise ValueError(f"Constante JSON no estándar: {valor}")


def _registrar_cuarentena(
    conexion,
    *,
    event_id: str | None,
    mode: str,
    motivo: str,
    ingested_at: datetime,
    raw_json: Any,
) -> None:
    with conexion:
        with conexion.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO cuarentena (event_id, mode, motivo, ingested_at, raw_json)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                """,
                (
                    event_id,
                    mode,
                    motivo,
                    ingested_at,
                    json.dumps(raw_json, allow_nan=False, separators=(",", ":")),
                ),
            )


def procesar_mensaje(
    conexion,
    indice_paises: IndicePaises,
    valor: bytes,
) -> bool:
    """Persiste el evento o su cuarentena; retorna True si se aceptó."""
    try:
        envelope = json.loads(valor, parse_constant=_rechazar_constante_json)
        if not isinstance(envelope, dict):
            raise ValueError("El sobre Kafka debe ser un objeto JSON")
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as error:
        raw_message = valor.decode("utf-8", errors="replace")
        _registrar_cuarentena(
            conexion,
            event_id=None,
            mode="live",
            motivo=f"JSON de mensaje inválido: {error}",
            ingested_at=datetime.now(timezone.utc),
            raw_json={"raw_message": raw_message},
        )
        return False

    mode = envelope.get("mode")
    motivo_sobre = None
    if not isinstance(mode, str) or mode not in {"live", "replay"}:
        motivo_sobre = "mode debe ser live o replay"
        mode = "live"

    feature = envelope.get("event")
    raw_feature = feature if feature is not None else envelope
    event_id = (
        feature.get("id")
        if isinstance(feature, dict) and isinstance(feature.get("id"), str)
        else None
    )
    ingested_at = _instante_ingesta(envelope)

    try:
        if motivo_sobre:
            raise ValueError(motivo_sobre)
        evento = validar_evento(feature, indice_paises)
    except ValueError as error:
        _registrar_cuarentena(
            conexion,
            event_id=event_id,
            mode=mode,
            motivo=str(error),
            ingested_at=ingested_at,
            raw_json=raw_feature,
        )
        return False

    with conexion:
        with conexion.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO fecha (fecha_id, anio, mes, dia, dia_semana)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (fecha_id) DO NOTHING
                """,
                (
                    evento.fecha_id,
                    evento.fecha_id.year,
                    evento.fecha_id.month,
                    evento.fecha_id.day,
                    evento.dia_semana,
                ),
            )
            cursor.execute(
                """
                INSERT INTO evento_version
                    (event_id, updated_ms, mode, ingested_at, raw_json)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (event_id, updated_ms, mode) DO NOTHING
                """,
                (
                    evento.event_id,
                    evento.updated_ms,
                    mode,
                    ingested_at,
                    json.dumps(feature, allow_nan=False, separators=(",", ":")),
                ),
            )
            cursor.execute(
                """
                INSERT INTO evento_actual (
                    event_id, mode, time_ms, event_time, updated_ms, updated_time,
                    mag, mag_type, place, longitude, latitude, depth_km,
                    country_id, fecha_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id, mode) DO UPDATE SET
                    time_ms = EXCLUDED.time_ms,
                    event_time = EXCLUDED.event_time,
                    updated_ms = EXCLUDED.updated_ms,
                    updated_time = EXCLUDED.updated_time,
                    mag = EXCLUDED.mag,
                    mag_type = EXCLUDED.mag_type,
                    place = EXCLUDED.place,
                    longitude = EXCLUDED.longitude,
                    latitude = EXCLUDED.latitude,
                    depth_km = EXCLUDED.depth_km,
                    country_id = EXCLUDED.country_id,
                    fecha_id = EXCLUDED.fecha_id
                WHERE EXCLUDED.updated_ms > evento_actual.updated_ms
                """,
                (
                    evento.event_id,
                    mode,
                    evento.time_ms,
                    evento.event_time,
                    evento.updated_ms,
                    evento.updated_time,
                    evento.mag,
                    evento.mag_type,
                    evento.place,
                    evento.longitude,
                    evento.latitude,
                    evento.depth_km,
                    evento.country_id,
                    evento.fecha_id,
                ),
            )
    return True


async def procesar_y_confirmar_offset(
    conexion,
    consumer: AIOKafkaConsumer,
    indice_paises: IndicePaises,
    valor: bytes,
) -> bool:
    aceptado = procesar_mensaje(conexion, indice_paises, valor)
    await consumer.commit()
    return aceptado


def conectar_postgres():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
    )


async def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    ruta_geojson = os.getenv("COUNTRIES_GEOJSON_PATH", "/data/paises.geojson")
    conexion = conectar_postgres()
    indice_paises = cargar_paises(conexion, ruta_geojson)
    LOGGER.info(
        "Catálogo espacial preparado: %d países en EPSG:4326",
        len(indice_paises.geodataframe),
    )

    consumer = AIOKafkaConsumer(
        os.getenv("KAFKA_TOPIC", "terremotos"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        group_id=os.getenv("KAFKA_GROUP_ID", "observatorio-consumidor"),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    try:
        await consumer.start()
        async for message in consumer:
            await procesar_y_confirmar_offset(
                conexion, consumer, indice_paises, message.value
            )
    finally:
        await consumer.stop()
        conexion.close()


if __name__ == "__main__":
    asyncio.run(main())
