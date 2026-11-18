"""Ingesta eventos USGS en tiempo real y publica revisiones en Kafka.

Cumple el requisito de detectar cambios mediante la pareja (id, updated),
esperar el ACK de Kafka antes de guardar el checkpoint del productor y dejar
que el consumidor confirme PostgreSQL antes de confirmar el offset Kafka.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import psycopg2
import requests
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError


LOGGER = logging.getLogger("productor")
DEFAULT_FEED_URL = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
)
SOURCE = "usgs_live"


def _fecha_utc(valor: datetime) -> str:
    """Convierte una fecha consciente de zona horaria al formato ISO-8601 UTC."""
    return valor.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _milisegundos_epoch(valor: datetime) -> int:
    """Expresa el instante de ingesta como milisegundos Unix para su persistencia."""
    return int(valor.timestamp() * 1000)


def descargar_feed(
    sesion: requests.Session,
    url: str,
    *,
    timeout: float = 15,
    max_intentos: int = 5,
    espera_inicial: float | None = None,
    diagnostico: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Descarga y valida el FeatureCollection; reintenta fallos HTTP/red transitorios."""
    espera = (
        float(os.getenv("USGS_RETRY_BACKOFF_SECONDS", "2"))
        if espera_inicial is None
        else espera_inicial
    )
    espera_maxima = float(os.getenv("USGS_RETRY_MAX_SECONDS", "30"))

    for intento in range(1, max_intentos + 1):
        inicio_intento = time.monotonic()
        try:
            respuesta = sesion.get(url, timeout=timeout)
            if diagnostico is not None:
                diagnostico.update(
                    http_status=respuesta.status_code,
                    response_bytes=len(respuesta.content),
                    latency_ms=round(
                        (time.monotonic() - inicio_intento) * 1000, 3
                    ),
                )
            respuesta.raise_for_status()
            payload = respuesta.json()
            if (
                not isinstance(payload, dict)
                or payload.get("type") != "FeatureCollection"
                or not isinstance(payload.get("features"), list)
            ):
                raise ValueError("La respuesta USGS no es un FeatureCollection válido")
            if diagnostico is not None:
                metadata = payload.get("metadata")
                diagnostico["source_metadata"] = (
                    metadata if isinstance(metadata, dict) else {}
                )
            return payload
        except requests.HTTPError as error:
            codigo = error.response.status_code if error.response is not None else None
            if diagnostico is not None and error.response is not None:
                diagnostico.update(
                    http_status=codigo,
                    response_bytes=len(error.response.content),
                    latency_ms=round(
                        (time.monotonic() - inicio_intento) * 1000, 3
                    ),
                )
            reintentable = codigo == 429 or (
                codigo is not None and 500 <= codigo <= 599
            )
            if not reintentable or intento == max_intentos:
                raise
            encabezado = (
                error.response.headers.get("Retry-After")
                if error.response is not None
                else None
            )
            try:
                retraso = float(encabezado) if encabezado is not None else espera
            except ValueError:
                retraso = espera
            retraso = min(max(retraso, 0), espera_maxima)
            LOGGER.warning(
                "USGS respondió HTTP %s; reintento %d/%d en %.1f s",
                codigo,
                intento,
                max_intentos,
                retraso,
            )
        except (requests.Timeout, requests.ConnectionError) as error:
            if diagnostico is not None:
                diagnostico["latency_ms"] = round(
                    (time.monotonic() - inicio_intento) * 1000, 3
                )
            if intento == max_intentos:
                raise
            retraso = min(espera, espera_maxima)
            LOGGER.warning(
                "Fallo de red al consultar USGS (%s); reintento %d/%d en %.1f s",
                error,
                intento,
                max_intentos,
                retraso,
            )

        time.sleep(retraso)
        espera = min(max(espera * 2, 0.1), espera_maxima)

    raise RuntimeError("Se agotaron los reintentos del feed USGS")


def registrar_consulta_usgs(
    conexion,
    *,
    requested_at: datetime,
    completed_at: datetime,
    url: str,
    diagnostico: dict[str, Any],
    feature_count: int | None,
    error: str | None,
) -> int:
    """Persiste metadatos de extracción y conserva solo los últimos 30 días."""
    status = "error" if error is not None else "ok"
    metadata = diagnostico.get("source_metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}

    with conexion.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO consulta_usgs (
                requested_at, completed_at, source_url, status, http_status,
                response_bytes, latency_ms, feature_count, source_metadata, error
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            RETURNING id
            """,
            (
                requested_at,
                completed_at,
                url,
                status,
                diagnostico.get("http_status"),
                diagnostico.get("response_bytes"),
                diagnostico.get("latency_ms"),
                feature_count,
                json.dumps(metadata, allow_nan=False, separators=(",", ":")),
                error,
            ),
        )
        consulta_id = cursor.fetchone()[0]
        cursor.execute(
            "DELETE FROM consulta_usgs WHERE requested_at < %s - INTERVAL '30 days'",
            (completed_at,),
        )
    conexion.commit()
    return consulta_id


def registrar_eventos_publicados(
    conexion, consulta_id: int, published_count: int
) -> None:
    """Asocia a la consulta el número de eventos que el productor publicó."""
    with conexion.cursor() as cursor:
        cursor.execute(
            "UPDATE consulta_usgs SET published_count = %s WHERE id = %s",
            (published_count, consulta_id),
        )
    conexion.commit()


def validar_feature(feature: Any) -> tuple[str, int]:
    """Exige un identificador y `properties.updated` para controlar cada revisión."""
    if not isinstance(feature, dict):
        raise ValueError("El evento USGS no es un objeto")
    event_id = feature.get("id")
    properties = feature.get("properties")
    updated = properties.get("updated") if isinstance(properties, dict) else None
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("El evento USGS no tiene un id válido")
    if isinstance(updated, bool) or not isinstance(updated, int):
        raise ValueError(f"El evento {event_id} no tiene properties.updated válido")
    return event_id, updated


def leer_ultima_revision(conexion, event_id: str) -> int | None:
    """Lee el último `updated_ms` publicado para decidir si el evento es nuevo."""
    with conexion.cursor() as cursor:
        cursor.execute(
            """
            SELECT last_updated_ms
            FROM estado_productor
            WHERE event_id = %s AND mode = 'live'
            """,
            (event_id,),
        )
        fila = cursor.fetchone()
    return fila[0] if fila else None


def guardar_revision_publicada(conexion, event_id: str, updated_ms: int) -> None:
    """Guarda el checkpoint solo después del ACK de Kafka y solo si avanzó la revisión."""
    with conexion.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO estado_productor (event_id, mode, last_updated_ms)
            VALUES (%s, 'live', %s)
            ON CONFLICT (event_id, mode) DO UPDATE
            SET last_updated_ms = EXCLUDED.last_updated_ms
            WHERE EXCLUDED.last_updated_ms > estado_productor.last_updated_ms
            """,
            (event_id, updated_ms),
        )
    conexion.commit()


def actualizar_ultima_ingesta(conexion, ingested_ms: int) -> None:
    """Actualiza el instante de ingesta que consulta el endpoint de salud."""
    with conexion.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO estado_ingesta (source, last_ingestion_ms, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (source) DO UPDATE
            SET last_ingestion_ms = EXCLUDED.last_ingestion_ms,
                updated_at = NOW()
            """,
            (SOURCE, ingested_ms),
        )
    conexion.commit()


async def procesar_eventos(
    features: list[dict[str, Any]],
    productor: AIOKafkaProducer,
    conexion,
    instante_ingesta: datetime,
) -> int:
    """Publica solo revisiones nuevas y espera ACK antes de avanzar el checkpoint."""
    publicadas = 0
    for feature in features:
        event_id, updated_ms = validar_feature(feature)
        ultima_revision = leer_ultima_revision(conexion, event_id)
        if ultima_revision is not None and updated_ms <= ultima_revision:
            continue

        sobre = {
            "source": SOURCE,
            "ingested_at": _fecha_utc(instante_ingesta),
            "mode": "live",
            "event": feature,
        }
        confirmacion = await productor.send_and_wait(
            os.getenv("KAFKA_TOPIC", "terremotos"),
            key=event_id.encode("utf-8"),
            value=json.dumps(sobre, allow_nan=False, separators=(",", ":")).encode(
                "utf-8"
            ),
        )
        if confirmacion is None:
            raise RuntimeError(f"Kafka no confirmó el evento {event_id}")

        guardar_revision_publicada(conexion, event_id, updated_ms)
        publicadas += 1

    actualizar_ultima_ingesta(conexion, _milisegundos_epoch(instante_ingesta))
    return publicadas


def conectar_postgres():
    """Abre la conexión PostgreSQL usada por los checkpoints del productor."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
    )


async def main() -> None:
    """Ejecuta el ciclo periódico USGS, preservando el último checkpoint ante errores."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    url = os.getenv("USGS_FEED_URL", DEFAULT_FEED_URL)
    intervalo = float(os.getenv("USGS_POLL_INTERVAL_SECONDS", "60"))
    timeout = float(os.getenv("USGS_HTTP_TIMEOUT_SECONDS", "15"))
    max_intentos = int(os.getenv("USGS_MAX_ATTEMPTS", "5"))
    productor = AIOKafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    sesion = requests.Session()
    conexion = None

    try:
        await productor.start()
        while True:
            inicio_ciclo = time.monotonic()
            try:
                if conexion is None or conexion.closed:
                    conexion = conectar_postgres()
                requested_at = datetime.now(timezone.utc)
                diagnostico: dict[str, Any] = {}
                try:
                    payload = descargar_feed(
                        sesion,
                        url,
                        timeout=timeout,
                        max_intentos=max_intentos,
                        diagnostico=diagnostico,
                    )
                except (requests.RequestException, ValueError) as error:
                    registrar_consulta_usgs(
                        conexion,
                        requested_at=requested_at,
                        completed_at=datetime.now(timezone.utc),
                        url=url,
                        diagnostico=diagnostico,
                        feature_count=None,
                        error=str(error),
                    )
                    raise

                instante = datetime.now(timezone.utc)
                consulta_id = registrar_consulta_usgs(
                    conexion,
                    requested_at=requested_at,
                    completed_at=instante,
                    url=url,
                    diagnostico=diagnostico,
                    feature_count=len(payload["features"]),
                    error=None,
                )
                cantidad = await procesar_eventos(
                    payload["features"], productor, conexion, instante
                )
                registrar_eventos_publicados(conexion, consulta_id, cantidad)
                LOGGER.info(
                    "Consulta USGS correcta: HTTP %s, %.1f ms, %s bytes, "
                    "%d eventos leídos, %d publicados",
                    diagnostico.get("http_status"),
                    diagnostico.get("latency_ms", 0),
                    diagnostico.get("response_bytes", 0),
                    len(payload["features"]),
                    cantidad,
                )
            except requests.RequestException:
                LOGGER.exception("No se pudo consultar USGS; se conserva el último estado")
            except ValueError:
                LOGGER.exception("Respuesta/evento USGS inválido; estado sin cambios")
            except KafkaError:
                LOGGER.exception("Kafka no confirmó la publicación; se reintentará")
            except psycopg2.Error:
                LOGGER.exception("Error PostgreSQL; el checkpoint no se actualizó")
                if conexion is not None:
                    conexion.close()
                conexion = None
            await asyncio.sleep(
                max(0, intervalo - (time.monotonic() - inicio_ciclo))
            )
    finally:
        sesion.close()
        if conexion is not None and not conexion.closed:
            conexion.close()
        await productor.stop()


if __name__ == "__main__":
    asyncio.run(main())
