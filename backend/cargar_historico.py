"""Descarga, archiva y reproduce en Kafka el catálogo sísmico USGS de 2025."""

import argparse
import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from aiokafka import AIOKafkaProducer


LOGGER = logging.getLogger("cargar_historico")
URL_CATALOGO = "https://earthquake.usgs.gov/fdsnws/event/1/query"
ANIO = 2025
UMBRAL_SUBDIVISION = int(os.getenv("USGS_SPLIT_THRESHOLD", "18000"))
LIMITE_USGS = 20000


def _a_utc(valor: datetime) -> datetime:
    if valor.tzinfo is None:
        raise ValueError("Los intervalos deben incluir zona horaria")
    return valor.astimezone(timezone.utc)


def _a_milisegundos(valor: datetime) -> int:
    return int(_a_utc(valor).timestamp() * 1000)


def _desde_milisegundos(valor: int) -> datetime:
    return datetime.fromtimestamp(valor / 1000, tz=timezone.utc)


def subdividir_intervalo(
    inicio: datetime, fin_exclusivo: datetime
) -> tuple[tuple[datetime, datetime], tuple[datetime, datetime]]:
    inicio = _a_utc(inicio)
    fin_exclusivo = _a_utc(fin_exclusivo)
    inicio_ms = _a_milisegundos(inicio)
    fin_ms = _a_milisegundos(fin_exclusivo)
    mitad_ms = inicio_ms + (fin_ms - inicio_ms) // 2
    if mitad_ms <= inicio_ms or mitad_ms >= fin_ms:
        raise ValueError("El intervalo es demasiado pequeño para subdividir")
    mitad = _desde_milisegundos(mitad_ms)
    return (inicio, mitad), (mitad, fin_exclusivo)


def _clave_evento(feature: Any) -> tuple[Any, Any]:
    if not isinstance(feature, dict):
        return "invalid", json.dumps(feature, sort_keys=True, default=str)
    properties = feature.get("properties")
    event_id = feature.get("id")
    updated = properties.get("updated") if isinstance(properties, dict) else None
    if isinstance(event_id, str) and event_id and isinstance(updated, int):
        return event_id, updated
    return "invalid", json.dumps(feature, sort_keys=True, separators=(",", ":"), default=str)


def deduplicar_eventos(features: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Elimina solapes exactos de borde, preservando revisiones distintas."""
    unicos: dict[tuple[Any, Any], dict[str, Any]] = {}
    for feature in features:
        unicos.setdefault(_clave_evento(feature), feature)
    return list(unicos.values())


def ordenar_eventos(features: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def clave(feature: dict[str, Any]) -> tuple[int, int, str]:
        properties = feature.get("properties") or {}
        return (
            properties.get("time", 0),
            properties.get("updated", 0),
            str(feature.get("id", "")),
        )

    return sorted(features, key=clave)


def crear_casos_controlados() -> list[dict[str, Any]]:
    """Construye eventos replay repetibles, aislados del año histórico 2025."""
    original = {
        "type": "Feature",
        "id": "demo-replay-revision-controlada",
        "properties": {
            "type": "earthquake",
            "time": 1767225600000,
            "updated": 1767225601000,
            "mag": 2.5,
            "magType": "ml",
            "place": "Evento sintético de demostración replay",
            "depth": 12.0,
        },
        "geometry": {"type": "Point", "coordinates": [-3.7, 40.4, 12.0]},
    }
    revision = json.loads(json.dumps(original))
    revision["properties"]["updated"] = 1767312001000
    revision["properties"]["mag"] = 2.7
    return [original, json.loads(json.dumps(original)), revision, original]


def _intervalos_mensuales() -> list[tuple[datetime, datetime]]:
    intervalos = []
    for mes in range(1, 13):
        inicio = datetime(ANIO, mes, 1, tzinfo=timezone.utc)
        if mes == 12:
            fin = datetime(ANIO + 1, 1, 1, tzinfo=timezone.utc)
        else:
            fin = datetime(ANIO, mes + 1, 1, tzinfo=timezone.utc)
        intervalos.append((inicio, fin))
    return intervalos


def _nombre_cache(inicio: datetime, fin: datetime) -> str:
    return f"segmento_{_a_milisegundos(inicio)}_{_a_milisegundos(fin)}.geojson"


def _leer_json(ruta: Path) -> dict[str, Any]:
    with ruta.open(encoding="utf-8") as archivo:
        contenido = json.load(archivo)
    if (
        not isinstance(contenido, dict)
        or contenido.get("type") != "FeatureCollection"
        or not isinstance(contenido.get("features"), list)
    ):
        raise ValueError(f"Archivo histórico inválido: {ruta}")
    return contenido


def _guardar_json_atomico(ruta: Path, contenido: Any) -> None:
    temporal = ruta.with_suffix(ruta.suffix + ".tmp")
    with temporal.open("w", encoding="utf-8", newline="\n") as archivo:
        json.dump(contenido, archivo, ensure_ascii=False, separators=(",", ":"))
        archivo.write("\n")
    temporal.replace(ruta)


def _leer_manifest(ruta: Path) -> dict[str, Any]:
    if ruta.exists():
        with ruta.open(encoding="utf-8") as archivo:
            contenido = json.load(archivo)
        if contenido.get("year") != ANIO or not isinstance(
            contenido.get("queries"), list
        ):
            raise ValueError(f"Manifiesto histórico inválido: {ruta}")
        return contenido
    return {"year": ANIO, "source": URL_CATALOGO, "queries": []}


def _actualizar_manifest(
    manifest_path: Path,
    manifest: dict[str, Any],
    registro: dict[str, Any],
) -> None:
    queries = manifest["queries"]
    for indice, existente in enumerate(queries):
        if existente.get("cache_file") == registro["cache_file"]:
            queries[indice] = registro
            break
    else:
        queries.append(registro)
    manifest["last_updated_at"] = datetime.now(timezone.utc).isoformat()
    _guardar_json_atomico(manifest_path, manifest)


def _solicitar_segmento(
    session: requests.Session,
    inicio: datetime,
    fin_exclusivo: datetime,
    directory: Path,
    manifest_path: Path,
    manifest: dict[str, Any],
    *,
    timeout: float,
    max_attempts: int,
) -> tuple[list[dict[str, Any]], int]:
    cache_file = _nombre_cache(inicio, fin_exclusivo)
    cache_path = directory / cache_file
    start_ms = _a_milisegundos(inicio)
    end_ms = _a_milisegundos(fin_exclusivo)
    params = {
        "format": "geojson",
        "starttime": inicio.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "endtime": _desde_milisegundos(end_ms - 1)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
        "minmagnitude": 4,
        "eventtype": "earthquake",
        "orderby": "time-asc",
        "limit": LIMITE_USGS,
    }
    url = URL_CATALOGO
    cached = cache_path.exists()
    if cached:
        payload = _leer_json(cache_path)
        metadata = payload.get("metadata") or {}
        reported_count = metadata.get("count", len(payload["features"]))
        extracted_at = datetime.fromtimestamp(
            cache_path.stat().st_mtime, timezone.utc
        ).isoformat()
    else:
        backoff = float(os.getenv("USGS_RETRY_BACKOFF_SECONDS", "2"))
        max_backoff = float(os.getenv("USGS_RETRY_MAX_SECONDS", "30"))
        for attempt in range(1, max_attempts + 1):
            try:
                response = session.get(url, params=params, timeout=timeout)
                response.raise_for_status()
                payload = response.json()
                if (
                    not isinstance(payload, dict)
                    or payload.get("type") != "FeatureCollection"
                    or not isinstance(payload.get("features"), list)
                ):
                    raise ValueError("USGS no devolvió un FeatureCollection")
                metadata = payload.get("metadata") or {}
                reported_count = metadata.get("count", len(payload["features"]))
                if not isinstance(reported_count, int):
                    raise ValueError("USGS metadata.count no es un entero")
                _guardar_json_atomico(cache_path, payload)
                extracted_at = datetime.now(timezone.utc).isoformat()
                break
            except requests.HTTPError as error:
                status = error.response.status_code if error.response else None
                retryable = status == 429 or (
                    status is not None and 500 <= status <= 599
                )
                if not retryable or attempt == max_attempts:
                    raise
                retry_after = (
                    error.response.headers.get("Retry-After")
                    if error.response
                    else None
                )
                try:
                    delay = float(retry_after) if retry_after is not None else backoff
                except ValueError:
                    delay = backoff
            except (requests.Timeout, requests.ConnectionError):
                if attempt == max_attempts:
                    raise
                delay = backoff
            if attempt < max_attempts:
                delay = min(max(delay, 0), max_backoff)
                LOGGER.warning(
                    "Reintento USGS %d/%d para %s: %.1f s",
                    attempt,
                    max_attempts,
                    cache_file,
                    delay,
                )
                time.sleep(delay)
                backoff = min(max(backoff * 2, 0.1), max_backoff)
        else:
            raise RuntimeError(f"No se pudo descargar {cache_file}")

    registro = {
        "url": url,
        "params": params,
        "start_ms": start_ms,
        "end_exclusive_ms": end_ms,
        "extracted_at": extracted_at,
        "count": reported_count,
        "returned_count": len(payload["features"]),
        "cache_file": cache_file,
        "reused_cache": cached,
    }
    _actualizar_manifest(manifest_path, manifest, registro)

    return payload["features"], reported_count


def validar_cobertura(
    intervalos: list[tuple[datetime, datetime]],
    inicio: datetime,
    fin_exclusivo: datetime,
) -> None:
    ordenados = sorted(
        ((_a_utc(a), _a_utc(b)) for a, b in intervalos), key=lambda par: par[0]
    )
    cursor = _a_utc(inicio)
    for actual_inicio, actual_fin in ordenados:
        if actual_inicio != cursor or actual_fin <= actual_inicio:
            raise ValueError("Los intervalos históricos tienen huecos o solapamientos")
        cursor = actual_fin
    if cursor != _a_utc(fin_exclusivo):
        raise ValueError("La cobertura histórica no incluye todo el año 2025")


def descargar_historico(
    directorio: str | Path,
    *,
    session: requests.Session | None = None,
    timeout: float = 90,
    max_attempts: int = 5,
) -> list[dict[str, Any]]:
    directory = Path(directorio) / str(ANIO)
    directory.mkdir(parents=True, exist_ok=True)
    manifest_path = directory / "manifest.json"
    manifest = _leer_manifest(manifest_path)
    owns_session = session is None
    http = session or requests.Session()
    if owns_session:
        http.headers.update(
            {"User-Agent": "observatorio-sismico/1.0 (educational data ingestion)"}
        )
    inicio_anio = datetime(ANIO, 1, 1, tzinfo=timezone.utc)
    fin_anio = datetime(ANIO + 1, 1, 1, tzinfo=timezone.utc)
    features: list[dict[str, Any]] = []
    intervalos_hoja: list[tuple[datetime, datetime]] = []
    conteo_reportado = 0

    def descargar_intervalo(
        inicio: datetime, fin_exclusivo: datetime
    ) -> list[dict[str, Any]]:
        nonlocal conteo_reportado
        resultado, count = _solicitar_segmento(
            http,
            inicio,
            fin_exclusivo,
            directory,
            manifest_path,
            manifest,
            timeout=timeout,
            max_attempts=max_attempts,
        )
        conteo_reportado += count
        cache = _leer_json(directory / _nombre_cache(inicio, fin_exclusivo))
        if cache.get("metadata", {}).get("count", len(cache["features"])) >= UMBRAL_SUBDIVISION:
            izquierda, derecha = subdividir_intervalo(inicio, fin_exclusivo)
            return descargar_intervalo(*izquierda) + descargar_intervalo(*derecha)
        intervalos_hoja.append((inicio, fin_exclusivo))
        return resultado

    try:
        for inicio, fin_exclusivo in _intervalos_mensuales():
            features.extend(descargar_intervalo(inicio, fin_exclusivo))
    finally:
        if owns_session:
            http.close()

    validar_cobertura(intervalos_hoja, inicio_anio, fin_anio)
    unicos = deduplicar_eventos(features)
    ordenados = ordenar_eventos(unicos)
    resumen = {
        "year": ANIO,
        "source": URL_CATALOGO,
        "query_parameters": {
            "format": "geojson",
            "minmagnitude": 4,
            "eventtype": "earthquake",
            "orderby": "time-asc",
        },
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "query_count": len(manifest["queries"]),
        "reported_count_sum": conteo_reportado,
        "returned_count": len(features),
        "deduplicated_count": len(ordenados),
        "duplicate_count": len(features) - len(ordenados),
        "cache_directory": str(directory),
        "manifest": manifest_path.name,
    }
    manifest["summary"] = resumen
    _guardar_json_atomico(manifest_path, manifest)
    _guardar_json_atomico(directory / "eventos_2025.geojson", {
        "type": "FeatureCollection",
        "metadata": resumen,
        "features": ordenados,
    })
    return ordenados


def _sobre(feature: dict[str, Any], source: str, run_id: str) -> dict[str, Any]:
    return {
        "source": source,
        "ingested_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "mode": "replay",
        "replay_run_id": run_id,
        "event": feature,
    }


async def publicar_eventos_replay(
    features: list[dict[str, Any]],
    *,
    bootstrap_servers: str,
    topic: str,
    speed_factor: float,
    max_events: int | None = None,
    include_controlled_cases: bool = True,
) -> int:
    if speed_factor <= 0:
        raise ValueError("REPLAY_SPEED_FACTOR debe ser mayor que cero")
    ordered = ordenar_eventos(features)
    if max_events is not None:
        ordered = ordered[:max_events]
    sequence = ordered + (crear_casos_controlados() if include_controlled_cases else [])
    run_id = datetime.now(timezone.utc).strftime("replay-%Y%m%dT%H%M%SZ")
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    publicado = 0
    anterior_ms: int | None = None
    try:
        for feature in sequence:
            properties = feature.get("properties") or {}
            event_time = properties.get("time")
            if (
                anterior_ms is not None
                and isinstance(event_time, int)
                and event_time >= anterior_ms
            ):
                delay = min((event_time - anterior_ms) / 1000 / speed_factor, 0.1)
                if delay > 0:
                    await asyncio.sleep(delay)
            if isinstance(event_time, int):
                anterior_ms = event_time
            event_id = feature.get("id")
            if not isinstance(event_id, str) or not event_id:
                raise ValueError("Evento histórico sin id; no se puede publicar en Kafka")
            value = json.dumps(
                _sobre(feature, "usgs_historico_replay", run_id),
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
            confirmacion = await producer.send_and_wait(
                topic, key=event_id.encode("utf-8"), value=value
            )
            if confirmacion is None:
                raise RuntimeError(f"Kafka no confirmó el evento {event_id}")
            publicado += 1
            if publicado % 1000 == 0:
                LOGGER.info("Replay publicado: %d mensajes", publicado)
    finally:
        await producer.stop()
    return publicado


async def ejecutar(args: argparse.Namespace) -> None:
    directory = Path(args.directorio)
    archive_path = directory / str(ANIO) / "eventos_2025.geojson"
    if args.accion != "solo-reproducir" or not archive_path.exists():
        features = descargar_historico(directory, timeout=args.timeout)
    else:
        features = _leer_json(archive_path)["features"]
        LOGGER.info("Reutilizando archivo cacheado: %s", archive_path)

    if args.accion == "solo-descargar":
        LOGGER.info("Descarga terminada: %d eventos únicos", len(features))
        return

    publicados = await publicar_eventos_replay(
        features,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        topic=os.getenv("KAFKA_TOPIC", "terremotos"),
        speed_factor=args.velocidad,
        max_events=args.limite,
        include_controlled_cases=not args.sin_casos_controlados,
    )
    LOGGER.info(
        "Replay terminado: %d mensajes publicados en mode=replay", publicados
    )


def construir_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--accion",
        choices=("todo", "solo-descargar", "solo-reproducir"),
        default="todo",
    )
    parser.add_argument(
        "--directorio",
        default=os.getenv("HISTORICAL_DATA_DIR", "/data/historico"),
    )
    parser.add_argument(
        "--velocidad",
        type=float,
        default=float(os.getenv("REPLAY_SPEED_FACTOR", "1000000")),
        help="Factor de aceleración respecto a las diferencias temporales originales",
    )
    parser.add_argument("--limite", type=int, default=None)
    parser.add_argument("--timeout", type=float, default=90)
    parser.add_argument("--sin-casos-controlados", action="store_true")
    return parser


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(ejecutar(construir_parser().parse_args()))


if __name__ == "__main__":
    main()
