import asyncio

import pytest

import consumidor


class IndiceSinPaises:
    def country_for_point(self, _longitude: float, _latitude: float) -> None:
        return None


def feature(**cambios) -> dict:
    evento = {
        "type": "Feature",
        "id": "us-test",
        "properties": {
            "type": "earthquake",
            "time": 1793400000000,
            "updated": 1793400001000,
            "mag": None,
            "depth": None,
        },
        "geometry": {"type": "Point", "coordinates": [0, 0]},
    }
    evento.update(cambios)
    return evento


def test_conserva_nulos_y_valores_negativos() -> None:
    evento_nulo = consumidor.validar_evento(feature(), IndiceSinPaises())
    assert evento_nulo.mag is None
    assert evento_nulo.depth_km is None
    assert evento_nulo.country_id is None

    evento_negativo = consumidor.validar_evento(
        feature(
            properties={
                "type": "earthquake",
                "time": 1793400000000,
                "updated": 1793400001000,
                "mag": -1.2,
                "depth": -3.4,
            }
        ),
        IndiceSinPaises(),
    )
    assert evento_negativo.mag == -1.2
    assert evento_negativo.depth_km == -3.4


@pytest.mark.parametrize(
    ("geometry", "motivo"),
    [
        ({"type": "Polygon", "coordinates": []}, "Point"),
        ({"type": "Point", "coordinates": [180.01, 0]}, "Longitud"),
        ({"type": "Point", "coordinates": [0, -90.01]}, "Latitud"),
    ],
)
def test_rechaza_geometria_o_coordenadas_invalidas(geometry, motivo) -> None:
    with pytest.raises(ValueError, match=motivo):
        consumidor.validar_evento(feature(geometry=geometry), IndiceSinPaises())


def test_offset_se_confirma_despues_de_la_persistencia(monkeypatch) -> None:
    orden: list[str] = []

    def persistir(*_args):
        orden.append("commit_postgres")
        return True

    class KafkaConsumer:
        async def commit(self):
            orden.append("commit_kafka")

    monkeypatch.setattr(consumidor, "procesar_mensaje", persistir)

    resultado = asyncio.run(
        consumidor.procesar_y_confirmar_offset(
            object(), KafkaConsumer(), IndiceSinPaises(), b"{}"
        )
    )

    assert resultado is True
    assert orden == ["commit_postgres", "commit_kafka"]


def test_fallo_de_persistencia_no_confirma_offset(monkeypatch) -> None:
    class KafkaConsumer:
        def __init__(self) -> None:
            self.confirmado = False

        async def commit(self):
            self.confirmado = True

    def persistir(*_args):
        raise RuntimeError("fallo PostgreSQL")

    monkeypatch.setattr(consumidor, "procesar_mensaje", persistir)
    kafka = KafkaConsumer()

    with pytest.raises(RuntimeError, match="fallo PostgreSQL"):
        asyncio.run(
            consumidor.procesar_y_confirmar_offset(
                object(), kafka, IndiceSinPaises(), b"{}"
            )
        )

    assert kafka.confirmado is False
