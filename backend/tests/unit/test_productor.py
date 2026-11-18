import asyncio
import json
from datetime import datetime, timezone

import pytest

import productor


class ProductorKafka:
    def __init__(self, registro: list[tuple]) -> None:
        self.registro = registro
        self.mensajes: list[tuple[bytes, dict]] = []
        self.error: Exception | None = None
        self.confirmacion = object()

    async def send_and_wait(self, topic: str, *, key: bytes, value: bytes):
        if self.error:
            raise self.error
        mensaje = json.loads(value)
        self.mensajes.append((key, mensaje))
        self.registro.append(("ack", mensaje["event"]["properties"]["updated"]))
        return self.confirmacion


def feature(event_id: str, updated: int) -> dict:
    return {
        "type": "Feature",
        "id": event_id,
        "properties": {"updated": updated},
        "geometry": {"type": "Point", "coordinates": [0, 0]},
    }


def test_publica_evento_nuevo_y_guarda_estado_despues_del_ack(monkeypatch) -> None:
    estado: dict[str, int] = {}
    registro: list[tuple] = []
    productor_kafka = ProductorKafka(registro)

    monkeypatch.setattr(
        productor, "leer_ultima_revision",
        lambda _conexion, event_id: estado.get(event_id),
    )

    def guardar(_conexion, event_id, updated):
        registro.append(("estado", updated))
        estado[event_id] = updated

    ingestas: list[int] = []
    monkeypatch.setattr(productor, "guardar_revision_publicada", guardar)
    monkeypatch.setattr(
        productor, "actualizar_ultima_ingesta",
        lambda _conexion, valor: ingestas.append(valor),
    )
    instante = datetime(2026, 10, 31, tzinfo=timezone.utc)

    cantidad = asyncio.run(
        productor.procesar_eventos(
            [feature("us-test", 123)], productor_kafka, object(), instante
        )
    )

    assert cantidad == 1
    assert productor_kafka.mensajes[0][0] == b"us-test"
    assert productor_kafka.mensajes[0][1] == {
        "source": "usgs_live",
        "ingested_at": "2026-10-31T00:00:00Z",
        "mode": "live",
        "event": feature("us-test", 123),
    }
    assert registro == [("ack", 123), ("estado", 123)]
    assert estado == {"us-test": 123}
    assert ingestas == [int(instante.timestamp() * 1000)]


def test_omite_duplicado_y_revision_antigua(monkeypatch) -> None:
    estado = {"duplicado": 200, "antiguo": 300}
    ingestas: list[int] = []
    monkeypatch.setattr(
        productor, "leer_ultima_revision",
        lambda _conexion, event_id: estado.get(event_id),
    )
    monkeypatch.setattr(
        productor, "guardar_revision_publicada",
        lambda *_args: pytest.fail("No debe guardar una revisión ya publicada"),
    )
    monkeypatch.setattr(
        productor, "actualizar_ultima_ingesta",
        lambda _conexion, valor: ingestas.append(valor),
    )
    kafka = ProductorKafka([])

    cantidad = asyncio.run(
        productor.procesar_eventos(
            [feature("duplicado", 200), feature("antiguo", 250)],
            kafka,
            object(),
            datetime(2026, 10, 31, tzinfo=timezone.utc),
        )
    )

    assert cantidad == 0
    assert kafka.mensajes == []
    assert estado == {"duplicado": 200, "antiguo": 300}
    assert len(ingestas) == 1


def test_publica_revision_mas_nueva(monkeypatch) -> None:
    estado = {"us-test": 100}
    monkeypatch.setattr(
        productor, "leer_ultima_revision",
        lambda _conexion, event_id: estado.get(event_id),
    )
    monkeypatch.setattr(
        productor, "guardar_revision_publicada",
        lambda _conexion, event_id, updated: estado.update({event_id: updated}),
    )
    monkeypatch.setattr(productor, "actualizar_ultima_ingesta", lambda *_args: None)

    kafka = ProductorKafka([])
    cantidad = asyncio.run(
        productor.procesar_eventos(
            [feature("us-test", 101)],
            kafka,
            object(),
            datetime(2026, 10, 31, tzinfo=timezone.utc),
        )
    )

    assert cantidad == 1
    assert estado["us-test"] == 101
    assert len(kafka.mensajes) == 1


def test_fallo_de_ack_no_avanza_estado_ni_ingesta(monkeypatch) -> None:
    estado: dict[str, int] = {}
    ingestas: list[int] = []
    monkeypatch.setattr(
        productor, "leer_ultima_revision",
        lambda _conexion, event_id: estado.get(event_id),
    )
    monkeypatch.setattr(
        productor, "guardar_revision_publicada",
        lambda _conexion, event_id, updated: estado.update({event_id: updated}),
    )
    monkeypatch.setattr(
        productor, "actualizar_ultima_ingesta",
        lambda _conexion, valor: ingestas.append(valor),
    )
    kafka = ProductorKafka([])
    kafka.error = RuntimeError("sin confirmación")

    with pytest.raises(RuntimeError, match="sin confirmación"):
        asyncio.run(
            productor.procesar_eventos(
                [feature("us-test", 123)],
                kafka,
                object(),
                datetime(2026, 10, 31, tzinfo=timezone.utc),
            )
        )

    assert estado == {}
    assert ingestas == []


def test_ausencia_de_ack_no_avanza_estado_ni_ingesta(monkeypatch) -> None:
    estado: dict[str, int] = {}
    ingestas: list[int] = []
    monkeypatch.setattr(
        productor, "leer_ultima_revision",
        lambda _conexion, event_id: estado.get(event_id),
    )
    monkeypatch.setattr(
        productor, "guardar_revision_publicada",
        lambda _conexion, event_id, updated: estado.update({event_id: updated}),
    )
    monkeypatch.setattr(
        productor, "actualizar_ultima_ingesta",
        lambda _conexion, valor: ingestas.append(valor),
    )
    kafka = ProductorKafka([])
    kafka.confirmacion = None

    with pytest.raises(RuntimeError, match="no confirmó"):
        asyncio.run(
            productor.procesar_eventos(
                [feature("us-test", 123)],
                kafka,
                object(),
                datetime(2026, 10, 31, tzinfo=timezone.utc),
            )
        )

    assert estado == {}
    assert ingestas == []


@pytest.mark.parametrize("codigo_transitorio", [429, 503])
def test_reintenta_fallos_transitorios_http(monkeypatch, codigo_transitorio) -> None:
    class Respuesta:
        def __init__(self, status_code: int) -> None:
            self.status_code = status_code
            self.headers = {"Retry-After": "0"}

        def raise_for_status(self) -> None:
            import requests

            if self.status_code in {429, 503}:
                raise requests.HTTPError("rate limited", response=self)

        def json(self) -> dict:
            return {"type": "FeatureCollection", "features": []}

    respuestas = iter([Respuesta(codigo_transitorio), Respuesta(200)])

    class Sesion:
        def get(self, *_args, **_kwargs):
            return next(respuestas)

    pausas: list[float] = []
    monkeypatch.setattr(productor.time, "sleep", pausas.append)
    resultado = productor.descargar_feed(
        Sesion(), "https://example.test/feed", max_intentos=2
    )

    assert resultado == {"type": "FeatureCollection", "features": []}
    assert len(pausas) == 1


def test_descarga_registra_metricas_y_metadata_del_feed() -> None:
    class Respuesta:
        status_code = 200
        content = b'{"type":"FeatureCollection","features":[]}'

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "type": "FeatureCollection",
                "metadata": {"count": 0, "title": "USGS"},
                "features": [],
            }

    class Sesion:
        def get(self, *_args, **_kwargs) -> Respuesta:
            return Respuesta()

    diagnostico: dict[str, object] = {}
    payload = productor.descargar_feed(
        Sesion(), "https://example.test/feed", diagnostico=diagnostico
    )

    assert payload["features"] == []
    assert diagnostico["http_status"] == 200
    assert diagnostico["response_bytes"] == len(Respuesta.content)
    assert diagnostico["latency_ms"] >= 0
    assert diagnostico["source_metadata"] == {"count": 0, "title": "USGS"}


def test_registra_consulta_con_metricas_y_retencion_de_30_dias() -> None:
    class Cursor:
        def __init__(self) -> None:
            self.statements: list[tuple[str, tuple]] = []

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def execute(self, statement: str, parameters=()) -> None:
            self.statements.append((statement, parameters))

        def fetchone(self):
            return (42,)

    class Connection:
        def __init__(self) -> None:
            self.cursor_instance = Cursor()
            self.commits = 0

        def cursor(self) -> Cursor:
            return self.cursor_instance

        def commit(self) -> None:
            self.commits += 1

    connection = Connection()
    requested_at = datetime(2026, 11, 18, 8, 0, tzinfo=timezone.utc)
    completed_at = datetime(2026, 11, 18, 8, 0, 1, tzinfo=timezone.utc)

    consulta_id = productor.registrar_consulta_usgs(
        connection,
        requested_at=requested_at,
        completed_at=completed_at,
        url="https://example.test/feed",
        diagnostico={
            "http_status": 200,
            "response_bytes": 2048,
            "latency_ms": 125.5,
            "source_metadata": {"count": 3},
        },
        feature_count=3,
        error=None,
    )

    assert consulta_id == 42
    assert connection.commits == 1
    insert, insert_params = connection.cursor_instance.statements[0]
    assert "INSERT INTO consulta_usgs" in insert
    assert insert_params[:8] == (
        requested_at,
        completed_at,
        "https://example.test/feed",
        "ok",
        200,
        2048,
        125.5,
        3,
    )
    delete, delete_params = connection.cursor_instance.statements[1]
    assert "INTERVAL '30 days'" in delete
    assert delete_params == (completed_at,)
