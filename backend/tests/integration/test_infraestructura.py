"""Comprobaciones de Compose y del esquema tras arrancar la infraestructura."""

import json
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]


def ejecutar_compose(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


def obtener_configuracion() -> dict:
    resultado = ejecutar_compose("config", "--format", "json")
    return json.loads(resultado.stdout)


def test_compose_define_servicios_kraft_y_persistencia() -> None:
    configuracion = obtener_configuracion()
    servicios = configuracion["services"]

    assert set(servicios) == {"postgres", "kafka", "productor", "consumidor", "api"}
    assert "zookeeper" not in servicios
    assert servicios["postgres"]["image"] == "postgres:16"
    assert "healthcheck" in servicios["postgres"]
    assert "healthcheck" in servicios["kafka"]
    assert "healthcheck" in servicios["productor"]
    assert "healthcheck" in servicios["consumidor"]
    assert "healthcheck" in servicios["api"]
    assert "postgres_data" in configuracion["volumes"]
    assert "kafka_data" in configuracion["volumes"]

    kafka_env = servicios["kafka"]["environment"]
    assert kafka_env["KAFKA_PROCESS_ROLES"] == "broker,controller"
    assert kafka_env["KAFKA_CONTROLLER_QUORUM_VOTERS"] == "1@kafka:9093"

    api = servicios["api"]
    assert api["ports"][0]["published"] == "8000"
    assert api["ports"][0]["host_ip"] == "127.0.0.1"
    assert servicios["productor"]["build"] == servicios["consumidor"]["build"]
    assert servicios["consumidor"]["build"] == api["build"]
    assert {
        servicios["productor"]["image"],
        servicios["consumidor"]["image"],
        api["image"],
    } == {"observatorio-sismico-backend:local"}
    assert any(
        mount["target"] == "/data/historico"
        and mount["source"].replace("/", "\\").lower().endswith("\\datos\\historico")
        for mount in servicios["productor"]["volumes"]
    )
    assert any(
        mount["target"] == "/data/paises.geojson"
        and mount["read_only"]
        for mount in servicios["consumidor"]["volumes"]
    )


@pytest.fixture(scope="module")
def esquema_postgres() -> tuple[set[str], set[str], set[str]]:
    config = obtener_configuracion()
    postgres_env = config["services"]["postgres"]["environment"]
    usuario = postgres_env["POSTGRES_USER"]
    base = postgres_env["POSTGRES_DB"]
    consulta = """
        SELECT 'table', tablename FROM pg_tables
        WHERE schemaname = 'public'
        UNION ALL
        SELECT 'constraint', c.conname FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
        UNION ALL
        SELECT 'index', indexname FROM pg_indexes
        WHERE schemaname = 'public';
    """
    salida = ejecutar_compose(
        "exec", "-T", "postgres", "psql", "-U", usuario, "-d", base, "-At",
        "-F", "|", "-c", consulta,
    ).stdout
    tablas: set[str] = set()
    restricciones: set[str] = set()
    indices: set[str] = set()
    for linea in salida.splitlines():
        tipo, nombre = linea.split("|", maxsplit=1)
        if tipo == "table":
            tablas.add(nombre)
        elif tipo == "constraint":
            restricciones.add(nombre)
        elif tipo == "index":
            indices.add(nombre)
    return tablas, restricciones, indices


def test_esquema_tiene_tablas_restricciones_e_indices(
    esquema_postgres: tuple[set[str], set[str], set[str]],
) -> None:
    tablas, restricciones, indices = esquema_postgres

    assert {
        "pais",
        "fecha",
        "evento_actual",
        "evento_version",
        "cuarentena",
        "estado_ingesta",
        "estado_productor",
    } <= tablas
    assert "evento_actual_pkey" in restricciones
    assert "evento_actual_country_id_fkey" in restricciones
    assert "evento_actual_fecha_id_fkey" in restricciones
    assert "evento_version_event_id_updated_ms_mode_key" in restricciones
    assert "estado_productor_pkey" in restricciones
    assert {
        "idx_evento_actual_event_time_mode",
        "idx_evento_actual_mag",
        "idx_evento_actual_country_id",
    } <= indices
