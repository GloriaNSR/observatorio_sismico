from datetime import datetime, timezone

import psycopg2
import pytest
from fastapi.testclient import TestClient

import api


class CursorSimulado:
    def __init__(self) -> None:
        self.sentencia = ""
        self.parametros = ()

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, sentencia: str, parametros=()) -> None:
        self.sentencia = sentencia
        self.parametros = parametros

    def fetchone(self):
        if "estado_ingesta" in self.sentencia:
            return (1793452000000,)
        if "FILTER (WHERE e.mag IS NULL)" in self.sentencia:
            return (10, 2, 1)
        if "COUNT(*)" in self.sentencia and "evento_actual" in self.sentencia:
            return (2,)
        if "COUNT" in self.sentencia:
            return (1,)
        raise AssertionError(f"Consulta inesperada: {self.sentencia}")

    def fetchall(self):
        if "SELECT e.event_id" in self.sentencia:
            if "e.country_id = %s" in self.sentencia:
                return [
                    (
                        "us-a",
                        1793450000000,
                        datetime(2026, 10, 31, 12, 0, tzinfo=timezone.utc),
                        1793451000000,
                        datetime(2026, 10, 31, 12, 10, tzinfo=timezone.utc),
                        4.5,
                        "mb",
                        "España",
                        -3.7,
                        40.4,
                        10.0,
                        "ESP",
                        "España",
                    )
                ]
            if "e.country_id IS NULL" in self.sentencia:
                return [
                    (
                        "us-b",
                        1793450100000,
                        datetime(2026, 10, 31, 12, 5, tzinfo=timezone.utc),
                        1793451100000,
                        datetime(2026, 10, 31, 12, 20, tzinfo=timezone.utc),
                        None,
                        None,
                        "Océano",
                        0.0,
                        0.0,
                        None,
                        None,
                        None,
                    )
                ]
            return [
                (
                    "us-a",
                    1793450000000,
                    datetime(2026, 10, 31, 12, 0, tzinfo=timezone.utc),
                    1793451000000,
                    datetime(2026, 10, 31, 12, 10, tzinfo=timezone.utc),
                    4.5,
                    "mb",
                    "España",
                    -3.7,
                    40.4,
                    10.0,
                    "ESP",
                    "España",
                ),
                (
                    "us-b",
                    1793450100000,
                    datetime(2026, 10, 31, 12, 5, tzinfo=timezone.utc),
                    1793451100000,
                    datetime(2026, 10, 31, 12, 20, tzinfo=timezone.utc),
                    None,
                    None,
                    "Océano",
                    0.0,
                    0.0,
                    None,
                    None,
                    None,
                ),
            ]
        if "GROUP BY e.country_id" in self.sentencia:
            return [("ESP", "España", "Europa", "Europa meridional", 1),
                    (None, "Sin país asignado", None, "Sin país asignado", 1)]
        if "GROUP BY hour_utc" in self.sentencia:
            return [(datetime(2026, 10, 31, 12, tzinfo=timezone.utc), 2)]
        if "width_bucket" in self.sentencia:
            return [(9, 1)]
        if "COUNT" in self.sentencia:
            return [(10, 2, 1)]
        raise AssertionError(f"Consulta inesperada: {self.sentencia}")


class ConexionSimulada:
    def __init__(self) -> None:
        self.cursor_actual = CursorSimulado()

    def cursor(self):
        return self.cursor_actual

    def close(self) -> None:
        return None


@pytest.fixture
def cliente(monkeypatch):
    conexion = ConexionSimulada()
    monkeypatch.setattr(api, "conectar_postgres", lambda: conexion)
    with TestClient(api.app) as test_client:
        yield test_client, conexion


def test_eventos_contract_geojson_y_filtros(cliente) -> None:
    test_client, conexion = cliente
    response = test_client.get(
        "/api/eventos",
        params={
            "start": "2026-10-31T12:00:00+02:00",
            "end": "2026-10-31T15:00:00+02:00",
            "min_mag": "4",
            "country_id": "ESP",
            "mode": "replay",
            "limit": 1,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "FeatureCollection"
    assert data["metadata"]["mode"] == "replay"
    assert {
        "generated",
        "last_ingestion",
        "mode",
        "count",
        "truncated",
        "total",
        "next_cursor",
    } <= data["metadata"].keys()
    assert data["metadata"]["count"] == 1
    assert data["metadata"]["truncated"] is True
    assert data["metadata"]["total"] == 2
    assert data["features"][0]["properties"]["mag"] == 4.5
    assert data["features"][0]["properties"]["country_name"] == "España"
    assert data["features"][0]["properties"]["country_id"] == "ESP"
    assert "coordinates" in data["features"][0]["geometry"]

    sql = conexion.cursor_actual.sentencia
    params = conexion.cursor_actual.parametros
    assert "event_time >= %s" in sql
    assert "event_time < %s" in sql
    assert "mag >= %s" in sql
    assert "e.country_id = %s" in sql
    assert "mode = %s" in sql
    assert datetime(2026, 10, 31, 10, 0, tzinfo=timezone.utc) in params


def test_eventos_respuesta_vacia_y_mode_live_por_defecto(cliente, monkeypatch) -> None:
    test_client, conexion = cliente
    cursor = conexion.cursor_actual

    def fetchall_vacio():
        return []

    monkeypatch.setattr(cursor, "fetchall", fetchall_vacio)
    monkeypatch.setattr(cursor, "fetchone", lambda: (0,))
    response = test_client.get("/api/eventos")

    assert response.status_code == 200
    assert response.json()["features"] == []
    assert response.json()["metadata"]["mode"] == "live"
    assert response.json()["metadata"]["truncated"] is False


def test_filtro_unassigned_y_nulos_en_geojson(cliente) -> None:
    test_client, _ = cliente

    response = test_client.get("/api/eventos", params={"country_id": "unassigned"})

    assert response.status_code == 200
    feature = response.json()["features"][0]
    assert feature["properties"]["mag"] is None
    assert feature["properties"]["depth_km"] is None
    assert feature["properties"]["country_id"] is None
    assert feature["geometry"]["coordinates"] == [0.0, 0.0]


def test_filtros_invalidos_devuelven_error_de_cliente(cliente) -> None:
    test_client, _ = cliente

    assert test_client.get("/api/eventos", params={"start": "2025-01-01"}).status_code == 422
    assert test_client.get("/api/eventos", params={"mode": "unknown"}).status_code == 422
    assert test_client.get("/api/eventos", params={"min_mag": "NaN"}).status_code == 422


def test_salud_informa_ultima_ingesta_sin_credenciales(cliente) -> None:
    test_client, _ = cliente

    response = test_client.get("/api/salud")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["last_ingestion"] == 1793452000000
    assert "password" not in response.text.lower()


def test_estadisticas_separan_nulos_y_comparan_365_dias(cliente) -> None:
    test_client, conexion = cliente

    response = test_client.get(
        "/api/estadisticas",
        params={"country_id": "ESP", "min_mag": "2", "mode": "replay"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["events_by_country"][0]["country_name"] == "España"
    assert data["events_by_hour"][0]["count"] == 2
    assert data["magnitude_distribution"]["null_count"] == 2
    assert data["depth_distribution"]["null_count"] == 1
    assert data["historical_comparison"]["replay_2025"]["days"] == 365
    assert data["historical_comparison"]["replay_2025"]["daily_average"] == 2 / 365
    assert "mode = 'replay'" in conexion.cursor_actual.sentencia
    assert "e.country_id = %s" in conexion.cursor_actual.sentencia
    assert "ESP" in conexion.cursor_actual.parametros


def test_cors_preflight_localhost_y_header_ngrok(cliente) -> None:
    test_client, _ = cliente

    response = test_client.options(
        "/api/eventos",
        headers={
            "Origin": "http://localhost:4200",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "ngrok-skip-browser-warning",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://localhost:4200"
    assert "GET" in response.headers["access-control-allow-methods"]
    assert "ngrok-skip-browser-warning" in response.headers["access-control-allow-headers"]


def test_origen_netlify_es_configurable(monkeypatch) -> None:
    monkeypatch.setenv("NETLIFY_ORIGIN", "https://observatorio-ejemplo.netlify.app")
    monkeypatch.setenv("CORS_ORIGINS", "http://localhost:4200")

    assert api._origenes_cors() == [
        "http://localhost:4200",
        "https://observatorio-ejemplo.netlify.app",
    ]


def test_error_postgres_devuelve_503_no_vacio(cliente, monkeypatch) -> None:
    test_client, _ = cliente
    monkeypatch.setattr(
        api,
        "conectar_postgres",
        lambda: (_ for _ in ()).throw(psycopg2.OperationalError("caído")),
    )

    response = test_client.get("/api/eventos")

    assert response.status_code == 503
    assert response.json()["detail"]
