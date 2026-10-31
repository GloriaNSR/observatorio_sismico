# Contratos de API y mensajería

Este diseño sigue `CONTRATO-API.md` en la raíz del repositorio.

## Filtros de consulta comunes

`GET /api/eventos` y `GET /api/estadisticas` aceptan:

| Parámetro | Tipo | Comportamiento |
|---|---|---|
| `start` | Marca de tiempo ISO 8601 | Límite inferior inclusivo de la hora del evento; interpretar el desfase horario y comparar en UTC |
| `end` | Marca de tiempo ISO 8601 | Límite superior exclusivo de la hora del evento |
| `min_mag` | Número finito | Incluir solo filas con `mag >= min_mag`; las magnitudes nulas no coinciden |
| `country_id` | Cadena | Coincide con `pais.country_id` (`ADM0_A3`); el valor reservado `unassigned` selecciona filas cuyo país es `NULL` en SQL |
| `mode` | `live` o `replay` | Permite indicar un modo concreto; por defecto es `live` para evitar mezclar feeds |

Si no se indica un periodo, `/api/eventos` usa las últimas 24 horas. Todas las vistas y estadísticas utilizan el mismo conjunto de filtros validados. Los filtros inválidos devuelven un error explícito de cliente.

## `GET /api/eventos`

Devuelve HTTP 200 y un `FeatureCollection` GeoJSON; si no hay coincidencias, devuelve correctamente una lista `features` vacía. Se devuelve exactamente una versión actual por evento y modo.

```json
{
  "type": "FeatureCollection",
  "metadata": {
    "generated": 1793113200000,
    "last_ingestion": 1793113140000,
    "mode": "live",
    "count": 1,
    "total": 1,
    "truncated": false,
    "next_cursor": null
  },
  "features": [{
    "type": "Feature",
    "id": "usgs-event-id",
    "geometry": {"type": "Point", "coordinates": [-3.7, 40.4]},
    "properties": {
      "mag": 3.2,
      "magType": "ml",
      "place": "Ejemplo",
      "time": 1793109600000,
      "updated": 1793110000000,
      "depth": null,
      "depth_km": null,
      "country_id": null,
      "country_name": null
    }
  }]
}
```

Las horas se expresan como milisegundos Unix UTC. No serializar NaN ni Infinity. Omitir la tercera coordenada opcional si la profundidad es desconocida; exponer la profundidad nula en `properties`. `count` es el número de features devueltas y `total` es el recuento completo después de aplicar los filtros. Si un límite de página documentado trunca la respuesta, proporcionar el `total` completo y un mecanismo de continuación mediante cursor/offset; las estadísticas se calculan sobre todos los resultados filtrados de la base de datos, nunca solo sobre la página mostrada.

## `GET /api/estadisticas`

Devuelve los metadatos de filtros/periodo, con:

- recuentos por país y subregión, incluida la categoría «Sin país asignado»;
- recuentos en intervalos fijos de una hora UTC, según la hora de ocurrencia;
- distribuciones de magnitud y profundidad con recuentos de nulos informados por separado;
- comparación del recuento live de las últimas 24 horas con `min_mag >= 4` frente al promedio diario replay de 2025 calculado sobre 365 días, respetando los filtros geográficos aplicables. Esta comparación consulta deliberadamente las particiones live y replay por separado, incluso cuando el modo del panel solicitado es live;
- completitud y unidades de cada agregado.

El endpoint devuelve un error HTTP explícito si falla el almacenamiento o la consulta. Un error de base de datos no debe convertirse en una respuesta exitosa vacía o con valores cero.

## `GET /api/salud`

Devuelve la disponibilidad del servicio, la hora de la última ingesta correcta, el estado de antigüedad y cuánto tiempo ha transcurrido. Conservar la última marca de tiempo correcta cuando falle el origen. No incluir credenciales. La frescura se calcula a partir de `estado_ingesta.last_ingestion_ms` y del umbral de antigüedad configurado.

## Envoltorio de eventos Kafka

Cada registro Kafka usa el `id` de USGS como clave. El valor contiene:

```json
{
  "source": "usgs_live",
  "ingested_at": "2026-10-31T11:00:00Z",
  "mode": "live",
  "event": {
    "type": "Feature",
    "id": "usgs-event-id",
    "properties": {"type": "earthquake", "time": 1793109600000, "updated": 1793110000000},
    "geometry": {"type": "Point", "coordinates": [-3.7, 40.4, 12]}
  }
}
```

`mode` debe ser exactamente `live` o `replay`; `event` es el feature GeoJSON original de USGS. El estado del productor solo avanza después de que Kafka confirme la entrega. El consumidor confirma el offset del mensaje únicamente después de que finalice correctamente la transacción PostgreSQL.

## CORS y despliegue

Permitir únicamente el origen Netlify exacto configurado y `http://localhost:4200`, los métodos GET/OPTIONS y la cabecera `ngrok-skip-browser-warning`. Compose publica la API como `127.0.0.1:8000:8000`; solo la API se expone al equipo anfitrión. Dentro del contenedor, la API escucha en `0.0.0.0:8000`.
