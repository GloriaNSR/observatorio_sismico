"""Pruebas de aceptación contra los servicios Compose en ejecución."""

import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import urlopen

import psycopg2
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition


API_URL = os.getenv("ACCEPTANCE_API_URL", "http://api:8000")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "terremotos")
KAFKA_GROUP = os.getenv("KAFKA_GROUP_ID", "observatorio-consumidor")


def _conexion():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=5,
    )


def _feature(
    event_id: str,
    updated_ms: int,
    *,
    longitude: float = -3.7038,
    latitude: float = 40.4168,
    magnitude: float | None = 4.3,
    event_type: str = "earthquake",
    time_ms: int | None = None,
) -> dict:
    return {
        "type": "Feature",
        "id": event_id,
        "properties": {
            "type": event_type,
            "time": updated_ms - 1000 if time_ms is None else time_ms,
            "updated": updated_ms,
            "mag": magnitude,
            "depth": 12.5,
            "magType": "ml",
            "place": "prueba de aceptación",
        },
        "geometry": {
            "type": "Point",
            "coordinates": [longitude, latitude, 0],
        },
    }


async def _publicar(eventos: list[tuple[dict, str]]) -> dict[int, int]:
    productor = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await productor.start()
    offsets: dict[int, int] = {}
    try:
        for feature, mode in eventos:
            sobre = {
                "source": "test_acceptance",
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "mode": mode,
                "event": feature,
            }
            metadata = await productor.send_and_wait(
                KAFKA_TOPIC,
                key=feature["id"].encode(),
                value=json.dumps(sobre, allow_nan=False).encode(),
            )
            offsets[metadata.partition] = max(
                offsets.get(metadata.partition, 0), metadata.offset + 1
            )
    finally:
        await productor.stop()
    return offsets


def _api(path: str, parametros: dict | None = None) -> dict:
    query = f"?{urlencode(parametros)}" if parametros else ""
    with urlopen(f"{API_URL}{path}{query}", timeout=15) as respuesta:
        assert respuesta.status == 200
        return json.load(respuesta)


def _esperar_persistencia(
    ids_validos: set[str], ids_cuarentena: set[str], timeout: float = 45
) -> None:
    limite = time.monotonic() + timeout
    conexion = _conexion()
    try:
        while time.monotonic() < limite:
            with conexion.cursor() as cursor:
                cursor.execute(
                    "SELECT event_id FROM evento_actual WHERE event_id = ANY(%s)",
                    (list(ids_validos),),
                )
                actuales = {fila[0] for fila in cursor.fetchall()}
                cursor.execute(
                    "SELECT event_id FROM cuarentena WHERE event_id = ANY(%s)",
                    (list(ids_cuarentena),),
                )
                rechazados = {fila[0] for fila in cursor.fetchall()}
            conexion.rollback()
            if actuales == ids_validos and rechazados == ids_cuarentena:
                return
            time.sleep(0.25)
        pytest.fail(
            "El consumidor no persistió los eventos de aceptación dentro del plazo"
        )
    finally:
        conexion.close()


def _esperar_revision(
    event_id: str, mode: str, updated_ms: int, expected_versions: int, timeout: float = 30
) -> None:
    limite = time.monotonic() + timeout
    conexion = _conexion()
    try:
        while time.monotonic() < limite:
            with conexion.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT updated_ms FROM evento_actual
                    WHERE event_id = %s AND mode = %s
                    """,
                    (event_id, mode),
                )
                actual = cursor.fetchone()
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM evento_version
                    WHERE event_id = %s AND mode = %s
                    """,
                    (event_id, mode),
                )
                versiones = cursor.fetchone()[0]
            conexion.rollback()
            if actual == (updated_ms,) and versiones == expected_versions:
                return
            time.sleep(0.25)
        pytest.fail(f"Las revisiones esperadas de {event_id} no se persistieron")
    finally:
        conexion.close()


def _eliminar_prefijo(prefijo: str) -> None:
    conexion = _conexion()
    try:
        with conexion:
            with conexion.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM evento_actual WHERE event_id LIKE %s",
                    (f"{prefijo}%",),
                )
                cursor.execute(
                    "DELETE FROM evento_version WHERE event_id LIKE %s",
                    (f"{prefijo}%",),
                )
                cursor.execute(
                    "DELETE FROM cuarentena WHERE event_id LIKE %s",
                    (f"{prefijo}%",),
                )
    finally:
        conexion.close()


@pytest.fixture(scope="module")
def flujo_live():
    prefijo = f"acceptance-{uuid.uuid4().hex}-"
    instante = int(time.time() * 1000)
    tiempo_evento = instante - 500
    id_revision = f"{prefijo}revision"
    id_maritimo = f"{prefijo}maritimo"
    id_cantera = f"{prefijo}cantera"
    id_coordenadas = f"{prefijo}coordenadas"
    eventos = [
        (
            _feature(
                id_revision,
                instante,
                longitude=-3.7038,
                latitude=40.4168,
                magnitude=3.2,
                time_ms=tiempo_evento,
            ),
            "live",
        ),
        (
            _feature(
                id_revision,
                instante,
                longitude=-3.7038,
                latitude=40.4168,
                magnitude=3.2,
                time_ms=tiempo_evento,
            ),
            "live",
        ),
        (
            _feature(
                id_revision,
                instante + 3000,
                longitude=2.3522,
                latitude=48.8566,
                magnitude=4.3,
                time_ms=tiempo_evento,
            ),
            "live",
        ),
        (
            _feature(
                id_revision,
                instante + 2000,
                longitude=-3.7038,
                latitude=40.4168,
                magnitude=4.0,
                time_ms=tiempo_evento,
            ),
            "live",
        ),
        (
            _feature(
                id_maritimo,
                instante + 1000,
                longitude=0,
                latitude=0,
                magnitude=None,
            ),
            "live",
        ),
        (
            _feature(
                id_cantera,
                instante + 4000,
                longitude=-96.82033333,
                latitude=34.38883333,
                event_type="quarry blast",
            ),
            "live",
        ),
        (
            _feature(
                id_coordenadas,
                instante + 5000,
                longitude=181,
            ),
            "live",
        ),
    ]
    try:
        offsets = asyncio.run(_publicar(eventos))
        ids_validos = {id_revision, id_maritimo}
        ids_cuarentena = {id_cantera, id_coordenadas}
        _esperar_persistencia(ids_validos, ids_cuarentena)
        _esperar_revision(id_revision, "live", instante + 3000, 3)
        _esperar_revision(id_maritimo, "live", instante + 1000, 1)
        yield {
            "prefix": prefijo,
            "instant": instante,
            "event_time": tiempo_evento,
            "revision_id": id_revision,
            "maritime_id": id_maritimo,
            "quarry_id": id_cantera,
            "coordinates_id": id_coordenadas,
            "offsets": offsets,
        }
    finally:
        _eliminar_prefijo(prefijo)


def test_recibir_evento_nuevo_y_revision_mas_reciente(flujo_live) -> None:
    conexion = _conexion()
    try:
        with conexion.cursor() as cursor:
            cursor.execute(
                """
                SELECT updated_ms, mag, country_id
                FROM evento_actual
                WHERE event_id = %s AND mode = 'live'
                """,
                (flujo_live["revision_id"],),
            )
            actual = cursor.fetchone()
            cursor.execute(
                """
                SELECT COUNT(*) FROM evento_version
                WHERE event_id = %s AND mode = 'live'
                """,
                (flujo_live["revision_id"],),
            )
            versiones = cursor.fetchone()[0]
        assert actual == (flujo_live["instant"] + 3000, 4.3, "FRA")
        assert versiones == 3

        inicio = datetime.fromtimestamp(
            flujo_live["event_time"] / 1000, tz=timezone.utc
        ).isoformat()
        fin = datetime.fromtimestamp(
            (flujo_live["event_time"] + 1) / 1000, tz=timezone.utc
        ).isoformat()
        parametros = {"mode": "live", "start": inicio, "end": fin}
        eventos = _api("/api/eventos", parametros)
        por_id = {feature["id"]: feature for feature in eventos["features"]}
        assert por_id[flujo_live["revision_id"]]["properties"]["country_id"] == "FRA"
        estadisticas = _api("/api/estadisticas", parametros)
        assert estadisticas["metadata"]["total"] == 1
        assert estadisticas["events_by_country"][0]["country_id"] == "FRA"
        bins = {
            item["range"]: item["count"]
            for item in estadisticas["magnitude_distribution"]["bins"]
        }
        assert bins["[4, 4.5)"] == 1
    finally:
        conexion.close()


def test_reenviar_version_y_recibir_revision_antigua_es_idempotente(flujo_live) -> None:
    conexion = _conexion()
    try:
        with conexion.cursor() as cursor:
            cursor.execute(
                """
                SELECT updated_ms FROM evento_actual
                WHERE event_id = %s AND mode = 'live'
                """,
                (flujo_live["revision_id"],),
            )
            assert cursor.fetchone()[0] == flujo_live["instant"] + 3000
            cursor.execute(
                """
                SELECT COUNT(*) FROM evento_version
                WHERE event_id = %s AND mode = 'live'
                """,
                (flujo_live["revision_id"],),
            )
            assert cursor.fetchone()[0] == 3
    finally:
        conexion.close()


def test_magnitud_nula_se_conserva_y_no_cumple_minimo(flujo_live) -> None:
    inicio = datetime.fromtimestamp(
        (flujo_live["instant"] - 2000) / 1000, tz=timezone.utc
    ).isoformat()
    fin = datetime.fromtimestamp(
        (flujo_live["instant"] + 6000) / 1000, tz=timezone.utc
    ).isoformat()
    parametros = {"mode": "live", "start": inicio, "end": fin, "limit": 500}
    eventos = _api("/api/eventos", parametros)
    por_id = {feature["id"]: feature for feature in eventos["features"]}
    assert por_id[flujo_live["maritime_id"]]["properties"]["mag"] is None

    filtrados = _api(
        "/api/eventos",
        {**parametros, "min_mag": "4.0"},
    )
    ids_filtrados = {feature["id"] for feature in filtrados["features"]}
    assert flujo_live["revision_id"] in ids_filtrados
    assert flujo_live["maritime_id"] not in ids_filtrados

    inicio_nulo = datetime.fromtimestamp(
        (flujo_live["instant"] - 1) / 1000, tz=timezone.utc
    ).isoformat()
    fin_nulo = datetime.fromtimestamp(
        (flujo_live["instant"] + 1) / 1000, tz=timezone.utc
    ).isoformat()
    estadisticas = _api(
        "/api/estadisticas",
        {"mode": "live", "start": inicio_nulo, "end": fin_nulo},
    )
    assert estadisticas["metadata"]["total"] == 1
    assert estadisticas["magnitude_distribution"]["null_count"] == 1
    assert sum(
        item["count"] for item in estadisticas["magnitude_distribution"]["bins"]
    ) == 0


def test_cuarentena_rechaza_quarry_blast_y_coordenadas_invalidas(flujo_live) -> None:
    conexion = _conexion()
    try:
        with conexion.cursor() as cursor:
            cursor.execute(
                """
                SELECT event_id, motivo, raw_json
                FROM cuarentena
                WHERE event_id = ANY(%s)
                """,
                ([flujo_live["quarry_id"], flujo_live["coordinates_id"]],),
            )
            cuarentena = {fila[0]: fila[1:] for fila in cursor.fetchall()}
        assert set(cuarentena) == {
            flujo_live["quarry_id"],
            flujo_live["coordinates_id"],
        }
        motivo_cantera, raw_cantera = cuarentena[flujo_live["quarry_id"]]
        assert 'properties.type debe ser "earthquake"' == motivo_cantera
        assert raw_cantera["properties"]["type"] == "quarry blast"
        motivo_coordenadas, raw_coordenadas = cuarentena[
            flujo_live["coordinates_id"]
        ]
        assert "Longitud fuera del rango WGS84" in motivo_coordenadas
        assert raw_coordenadas["geometry"]["coordinates"][0] == 181

        inicio = datetime.fromtimestamp(
            (flujo_live["instant"] - 2000) / 1000, tz=timezone.utc
        ).isoformat()
        fin = datetime.fromtimestamp(
            (flujo_live["instant"] + 6000) / 1000, tz=timezone.utc
        ).isoformat()
        respuesta = _api(
            "/api/eventos",
            {"mode": "live", "start": inicio, "end": fin, "limit": 500},
        )
        ids_expuestos = {feature["id"] for feature in respuesta["features"]}
        assert flujo_live["quarry_id"] not in ids_expuestos
        assert flujo_live["coordinates_id"] not in ids_expuestos
    finally:
        conexion.close()


def test_evento_maritimo_y_catalogo_de_177_paises(flujo_live) -> None:
    inicio = datetime.fromtimestamp(
        (flujo_live["instant"] - 2000) / 1000, tz=timezone.utc
    ).isoformat()
    fin = datetime.fromtimestamp(
        (flujo_live["instant"] + 6000) / 1000, tz=timezone.utc
    ).isoformat()
    respuesta = _api(
        "/api/eventos",
        {"mode": "live", "start": inicio, "end": fin, "limit": 500},
    )
    por_id = {feature["id"]: feature for feature in respuesta["features"]}
    maritimo = por_id[flujo_live["maritime_id"]]["properties"]
    assert maritimo["country_id"] is None
    assert maritimo["country_name"] is None

    conexion = _conexion()
    try:
        with conexion.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM pais")
            assert cursor.fetchone()[0] == 177
    finally:
        conexion.close()

    estadisticas = _api(
        "/api/estadisticas",
        {"mode": "live", "start": inicio, "end": fin},
    )
    grupos = {
        grupo["country_name"]: grupo["count"]
        for grupo in estadisticas["events_by_country"]
    }
    assert grupos.get("Sin país asignado", 0) >= 1
    assert grupos.get("Francia", 0) >= 1


def test_endpoints_salud_eventos_y_estadisticas_responden(flujo_live) -> None:
    salud = _api("/api/salud")
    eventos = _api("/api/eventos", {"mode": "live", "limit": 5})
    estadisticas = _api("/api/estadisticas", {"mode": "live"})
    assert salud["status"] == "ok"
    assert salud["database"] == "available"
    assert eventos["metadata"]["mode"] == "live"
    assert estadisticas["metadata"]["mode"] == "live"
    assert "events_by_country" in estadisticas
    assert "magnitude_distribution" in estadisticas


def test_offset_kafka_se_confirma_tras_persistir_en_postgres(flujo_live) -> None:
    async def esperar_offsets() -> None:
        observador = AIOKafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=KAFKA_GROUP,
        )
        await observador.start()
        particiones = [
            TopicPartition(KAFKA_TOPIC, particion)
            for particion in flujo_live["offsets"]
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
                    and confirmados[particion]
                    >= flujo_live["offsets"][particion.partition]
                    for particion in particiones
                ):
                    return
                await asyncio.sleep(0.25)
            pytest.fail(f"Offsets sin confirmar: {confirmados}")
        finally:
            await observador.stop()

    asyncio.run(esperar_offsets())


def test_replay_queda_aislado_de_live_y_controla_revisiones() -> None:
    prefijo = f"acceptance-replay-{uuid.uuid4().hex}-"
    event_id = f"{prefijo}evento"
    instante = int(time.time() * 1000)
    eventos = [
        (_feature(event_id, instante, magnitude=4.1), "replay"),
        (_feature(event_id, instante, magnitude=4.1), "replay"),
        (_feature(event_id, instante + 2000, magnitude=4.6), "replay"),
    ]
    try:
        asyncio.run(_publicar(eventos))
        _esperar_persistencia({event_id}, set())
        _esperar_revision(event_id, "replay", instante + 2000, 2)

        conexion = _conexion()
        try:
            with conexion.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT updated_ms, mag FROM evento_actual
                    WHERE event_id = %s AND mode = 'replay'
                    """,
                    (event_id,),
                )
                assert cursor.fetchone() == (instante + 2000, 4.6)
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM evento_version
                    WHERE event_id = %s AND mode = 'replay'
                    """,
                    (event_id,),
                )
                assert cursor.fetchone()[0] == 2
        finally:
            conexion.close()

        inicio = datetime.fromtimestamp(
            (instante - 2000) / 1000, tz=timezone.utc
        ).isoformat()
        fin = datetime.fromtimestamp(
            (instante + 4000) / 1000, tz=timezone.utc
        ).isoformat()
        parametros = {"start": inicio, "end": fin, "limit": 500}
        replay = _api("/api/eventos", {**parametros, "mode": "replay"})
        live = _api("/api/eventos", {**parametros, "mode": "live"})
        assert event_id in {feature["id"] for feature in replay["features"]}
        assert event_id not in {feature["id"] for feature in live["features"]}
    finally:
        _eliminar_prefijo(prefijo)
