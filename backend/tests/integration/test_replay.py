import json
from datetime import datetime, timezone

import cargar_historico


def _feature(event_id: str, time_ms: int, updated_ms: int, mag=4.2) -> dict:
    return {
        "type": "Feature",
        "id": event_id,
        "properties": {
            "type": "earthquake",
            "time": time_ms,
            "updated": updated_ms,
            "mag": mag,
        },
        "geometry": {"type": "Point", "coordinates": [0, 0]},
    }


def test_subdivide_intervalo_sin_huecos_ni_solapamientos() -> None:
    inicio = datetime(2025, 1, 1, tzinfo=timezone.utc)
    fin = datetime(2025, 1, 2, tzinfo=timezone.utc)

    izquierda, derecha = cargar_historico.subdividir_intervalo(inicio, fin)

    assert izquierda[0] == inicio
    assert izquierda[1] == derecha[0]
    assert derecha[1] == fin


def test_deduplica_bordes_sin_perder_revisiones_distintas() -> None:
    original = _feature("us-test", 100, 110)
    revision = _feature("us-test", 100, 120, mag=4.3)
    duplicado_borde = _feature("us-test", 100, 110)

    resultado = cargar_historico.deduplicar_eventos(
        [original, revision, duplicado_borde]
    )

    assert [(item["id"], item["properties"]["updated"]) for item in resultado] == [
        ("us-test", 110),
        ("us-test", 120),
    ]


def test_casos_de_replay_incluyen_duplicado_revision_y_version_tardia() -> None:
    secuencia = cargar_historico.crear_casos_controlados()

    assert [feature["properties"]["updated"] for feature in secuencia] == [
        1767225601000,
        1767225601000,
        1767312001000,
        1767225601000,
    ]
    assert secuencia[0] == secuencia[1] == secuencia[3]
    assert secuencia[2]["properties"]["mag"] != secuencia[0]["properties"]["mag"]
    assert secuencia[0]["properties"]["time"] < 1793400000000


def test_orden_replay_por_ocurrencia_y_revision() -> None:
    mas_nuevo = _feature("nuevo", 200, 202)
    mas_antiguo = _feature("antiguo", 100, 101)

    assert [
        feature["id"]
        for feature in cargar_historico.ordenar_eventos([mas_nuevo, mas_antiguo])
    ] == ["antiguo", "nuevo"]


def test_archiva_consultas_reutilizables_y_metadatos(tmp_path) -> None:
    class Respuesta:
        def __init__(self, fecha: str) -> None:
            self.fecha = fecha

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            inicio = datetime.fromisoformat(self.fecha.replace("Z", "+00:00"))
            millis = int(inicio.timestamp() * 1000)
            return {
                "type": "FeatureCollection",
                "metadata": {"count": 1},
                "features": [_feature(self.fecha, millis, millis + 1)],
            }

    class Sesion:
        def __init__(self) -> None:
            self.llamadas: list[dict] = []

        def get(self, url, *, params, timeout):
            assert url == cargar_historico.URL_CATALOGO
            assert timeout > 0
            self.llamadas.append(dict(params))
            return Respuesta(params["starttime"])

    session = Sesion()
    primero = cargar_historico.descargar_historico(tmp_path, session=session)

    assert len(primero) == 12
    assert len(session.llamadas) == 12
    assert all(params["minmagnitude"] == 4 for params in session.llamadas)
    assert all(params["eventtype"] == "earthquake" for params in session.llamadas)

    manifest_path = tmp_path / "2025" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert len(manifest["queries"]) == 12
    assert all(query["url"] == cargar_historico.URL_CATALOGO for query in manifest["queries"])
    assert all(query["extracted_at"] for query in manifest["queries"])
    assert all(query["count"] == 1 for query in manifest["queries"])
    assert (tmp_path / "2025" / "eventos_2025.geojson").exists()

    segunda = cargar_historico.descargar_historico(tmp_path, session=session)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert len(segunda) == 12
    assert len(session.llamadas) == 12
    assert all(query["reused_cache"] for query in manifest["queries"])


def test_subdivide_consultas_que_se_acercan_al_limite(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(cargar_historico, "UMBRAL_SUBDIVISION", 2)

    class Respuesta:
        def __init__(self, parametros: dict) -> None:
            self.parametros = parametros

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            start = datetime.fromisoformat(
                self.parametros["starttime"].replace("Z", "+00:00")
            )
            end_inclusive = datetime.fromisoformat(
                self.parametros["endtime"].replace("Z", "+00:00")
            )
            duracion = (end_inclusive - start).total_seconds()
            cantidad = 2 if duracion > 20 * 86400 else 1
            millis = int(start.timestamp() * 1000)
            features = (
                [_feature(start.isoformat(), millis, millis + 1)]
                if cantidad == 1
                else []
            )
            return {
                "type": "FeatureCollection",
                "metadata": {"count": cantidad},
                "features": features,
            }

    class Sesion:
        def __init__(self) -> None:
            self.llamadas = 0

        def get(self, _url, *, params, timeout):
            self.llamadas += 1
            return Respuesta(params)

    session = Sesion()
    resultado = cargar_historico.descargar_historico(tmp_path, session=session)

    assert session.llamadas == 36
    assert len(resultado) == 24
