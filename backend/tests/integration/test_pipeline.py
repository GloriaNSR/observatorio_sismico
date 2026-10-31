"""Prueba de integración: Kafka activo, consumidor y PostgreSQL."""

import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition


def _evento(
    event_id: str,
    updated_ms: int,
    *,
    longitude: float = -3.7,
    latitude: float = 40.4,
    tipo: str = "earthquake",
    mag: float | None = -1.2,
    depth: float | None = -3.4,
) -> dict:
    return {
        "type": "Feature",
        "id": event_id,
        "properties": {
            "type": tipo,
            "time": updated_ms - 1000,
            "updated": updated_ms,
            "mag": mag,
            "depth": depth,
            "magType": "ml",
            "place": "evento de prueba",
        },
        "geometry": {
            "type": "Point",
            "coordinates": [longitude, latitude],
        },
    }


def _conexion():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=5,
    )


def test_eventos_kafka_persisten_version_actual_y_cuarentena() -> None:
    base_id = f"fase3-integration-{uuid.uuid4().hex}"
    instante = int(datetime.now(timezone.utc).timestamp() * 1000)
    evento_es = _evento(base_id, instante + 20_000)
    evento_es_antiguo = _evento(base_id, instante + 10_000)
    evento_oceano = _evento(
        f"{base_id}-oceano",
        instante + 30_000,
        longitude=0,
        latitude=0,
        mag=None,
        depth=None,
    )
    evento_tipo_invalido = _evento(
        f"{base_id}-tipo",
        instante + 40_000,
        tipo="quarry",
    )
    evento_coord_invalida = _evento(
        f"{base_id}-coord",
        instante + 50_000,
        longitude=181,
    )
    topic = os.getenv("KAFKA_TOPIC", "terremotos")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    async def publicar() -> dict[int, int]:
        productor = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await productor.start()
        offsets: dict[int, int] = {}
        try:
            for feature in (
                evento_es,
                evento_es_antiguo,
                evento_es,
                evento_oceano,
                evento_tipo_invalido,
                evento_coord_invalida,
            ):
                sobre = {
                    "source": "test_pipeline",
                    "ingested_at": datetime.now(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "mode": "live",
                    "event": feature,
                }
                metadata = await productor.send_and_wait(
                    topic,
                    key=feature["id"].encode(),
                    value=json.dumps(sobre, allow_nan=False).encode(),
                )
                offsets[metadata.partition] = max(
                    offsets.get(metadata.partition, 0), metadata.offset + 1
                )
        finally:
            await productor.stop()
        return offsets

    offsets_esperados = asyncio.run(publicar())

    fecha_limite = time.monotonic() + 40
    conexion = _conexion()
    try:
        filas = None
        while time.monotonic() < fecha_limite:
            with conexion.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT event_id, updated_ms, mag, depth_km, country_id
                    FROM evento_actual
                    WHERE event_id IN (%s, %s)
                    ORDER BY event_id
                    """,
                    (base_id, f"{base_id}-oceano"),
                )
                actuales = cursor.fetchall()
                cursor.execute(
                    """
                    SELECT event_id, COUNT(*)
                    FROM evento_version
                    WHERE event_id IN (%s, %s)
                    GROUP BY event_id
                    """,
                    (base_id, f"{base_id}-oceano"),
                )
                versiones = dict(cursor.fetchall())
                cursor.execute(
                    """
                    SELECT event_id, motivo, raw_json
                    FROM cuarentena
                    WHERE event_id IN (%s, %s)
                    """,
                    (f"{base_id}-tipo", f"{base_id}-coord"),
                )
                cuarentena = cursor.fetchall()
            conexion.rollback()
            if (
                len(actuales) == 2
                and versiones.get(base_id) == 2
                and versiones.get(f"{base_id}-oceano") == 1
                and len(cuarentena) == 2
            ):
                filas = actuales, versiones, cuarentena
                break
            time.sleep(0.5)

        assert filas is not None, "El consumidor no persistió todos los mensajes"
        actuales, versiones, cuarentena = filas
        por_id = {fila[0]: fila for fila in actuales}
        assert por_id[base_id][1] == instante + 20_000
        assert por_id[base_id][2:4] == (-1.2, -3.4)
        assert por_id[base_id][4] == "ESP"
        assert por_id[f"{base_id}-oceano"][2:5] == (None, None, None)
        assert versiones[base_id] == 2
        assert versiones[f"{base_id}-oceano"] == 1
        assert all(raw_json is not None and motivo for _, motivo, raw_json in cuarentena)
        assert {event_id for event_id, _, _ in cuarentena} == {
            f"{base_id}-tipo",
            f"{base_id}-coord",
        }

        async def esperar_offsets_confirmados() -> None:
            group_id = os.getenv("KAFKA_GROUP_ID", "observatorio-consumidor")
            observador = AIOKafkaConsumer(
                bootstrap_servers=bootstrap,
                group_id=group_id,
            )
            await observador.start()
            particiones = [
                TopicPartition(topic, particion)
                for particion in offsets_esperados
            ]
            observador.assign(particiones)
            try:
                limite = time.monotonic() + 20
                while time.monotonic() < limite:
                    confirmados = {
                        particion: await observador.committed(particion)
                        for particion in particiones
                    }
                    if all(
                        confirmados[particion] is not None
                        and confirmados[particion] >= offsets_esperados[particion.partition]
                        for particion in particiones
                    ):
                        return
                    await asyncio.sleep(0.25)
                assert False, (
                    "Kafka no confirmó los offsets de los mensajes persistidos: "
                    f"{confirmados}, esperados {offsets_esperados}"
                )
            finally:
                await observador.stop()

        asyncio.run(esperar_offsets_confirmados())
    finally:
        with conexion:
            with conexion.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM evento_actual WHERE event_id LIKE %s",
                    (f"{base_id}%",),
                )
                cursor.execute(
                    "DELETE FROM evento_version WHERE event_id LIKE %s",
                    (f"{base_id}%",),
                )
                cursor.execute(
                    "DELETE FROM cuarentena WHERE event_id LIKE %s",
                    (f"{base_id}%",),
                )
        conexion.close()
