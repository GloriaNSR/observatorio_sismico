# Modelo de datos: Observatorio de Actividad Sísmica

El esquema lógico se implementará en PostgreSQL mediante `sql/init.sql`. Las horas almacenadas como `TIMESTAMPTZ` representan instantes UTC; se conservan los milisegundos de ocurrencia/revisión de origen para garantizar compatibilidad y comparación exactas con la API.

## `pais`

Fila de referencia de país obtenida del GeoJSON de Natural Earth proporcionado.

| Columna | Tipo | Restricciones / significado |
|---|---|---|
| `country_id` | `VARCHAR(10)` | Clave primaria; `ADM0_A3` |
| `country_name` | `TEXT` | `NAME_ES`, falling back to `ADMIN` |
| `continent` | `TEXT` | `CONTINENT` |
| `subregion` | `TEXT` | `SUBREGION` |

Cargar el catálogo sin modificar el GeoJSON original. Guardar los datos de país necesarios para filtros y agregaciones de la API. Si un punto no coincide con ningún polígono, tendrá `country_id = NULL` y se mostrará como «Sin país asignado».

## `fecha`

Dimensión de calendario UTC para la fecha de ocurrencia del evento.

| Columna | Tipo | Restricciones / significado |
|---|---|---|
| `fecha_id` | `DATE` | Clave primaria, fecha UTC del evento |
| `anio` | `INTEGER` | Calendar year |
| `mes` | `INTEGER` | 1–12 |
| `dia` | `INTEGER` | 1–31 |
| `dia_semana` | `INTEGER` | Día de la semana ISO: lunes=1 y domingo=7 |

Garantizar que exista una fila de fecha en la misma transacción que la escritura del evento actual correspondiente.

## `evento_actual`

Proyección específica por modo que contiene la revisión aceptada más reciente de cada evento.

| Columna | Tipo | Restricciones / significado |
|---|---|---|
| `event_id` | `TEXT` | Identificador del evento USGS; parte de la clave primaria compuesta |
| `mode` | `VARCHAR(10)` | `live` o `replay`; parte de la clave primaria compuesta |
| `time_ms` | `BIGINT` | Hora de ocurrencia de origen en milisegundos epoch |
| `event_time` | `TIMESTAMPTZ` | Instante de ocurrencia en UTC |
| `updated_ms` | `BIGINT` | Hora de revisión de origen en milisegundos epoch |
| `updated_time` | `TIMESTAMPTZ` | Instante de revisión en UTC |
| `mag` | `DOUBLE PRECISION NULL` | Un nulo sigue siendo desconocido; se conservan valores negativos válidos |
| `mag_type` | `TEXT NULL` | Tipo de magnitud de origen |
| `place` | `TEXT NULL` | Descripción del lugar de origen |
| `longitude` | `DOUBLE PRECISION` | Longitud WGS84, entre -180 y 180 |
| `latitude` | `DOUBLE PRECISION` | Latitud WGS84, entre -90 y 90 |
| `depth_km` | `DOUBLE PRECISION NULL` | Un nulo sigue siendo desconocido; se conservan valores negativos válidos |
| `country_id` | `VARCHAR(10) NULL` | Clave foránea nullable a `pais.country_id` |
| `fecha_id` | `DATE` | Clave foránea a `fecha.fecha_id`, derivada de `event_time` UTC |

Clave primaria: `(event_id, mode)`. Índices requeridos: `(event_time, mode)`, `mag` y `country_id`.
El consumidor utiliza un UPSERT condicionado por `EXCLUDED.updated_ms > evento_actual.updated_ms`; las versiones iguales o más antiguas no modifican la proyección.

## `evento_version`

Historial inmutable de revisiones de eventos y registro de idempotencia.

| Columna | Tipo | Restricciones / significado |
|---|---|---|
| `id` | `SERIAL` | Clave primaria |
| `event_id` | `TEXT` | Identificador del evento USGS |
| `updated_ms` | `BIGINT` | Hora de revisión en milisegundos epoch |
| `mode` | `VARCHAR(10)` | `live` o `replay` |
| `ingested_at` | `TIMESTAMPTZ` | Hora de recepción en UTC |
| `raw_json` | `JSONB` | Feature GeoJSON original |

Restricción de unicidad: `(event_id, updated_ms, mode)`. Guardar una versión válida y actualizar atómicamente la proyección correspondiente. Las entregas duplicadas no crean otra versión.

## `cuarentena`

Registro duradero de features de entrada rechazados antes de incorporarse a la proyección del evento actual.

| Columna | Tipo | Restricciones / significado |
|---|---|---|
| `id` | `SERIAL` | Clave primaria |
| `event_id` | `TEXT NULL` | Identificador de origen, si está disponible |
| `mode` | `VARCHAR(10)` | Modo de ingesta |
| `motivo` | `TEXT` | Motivo explícito de validación |
| `ingested_at` | `TIMESTAMPTZ` | Hora de rechazo en UTC |
| `raw_json` | `JSONB` | Feature original, sin normalización destructiva |

Enviar a cuarentena los tipos distintos de terremoto, las geometrías que no sean Point y las coordenadas inválidas o fuera de rango. La magnitud o profundidad nula o negativa, por sí sola, no es motivo de rechazo.

## `estado_ingesta`

Estado de frescura de las consultas correctas al origen.

| Columna | Tipo | Restricciones / significado |
|---|---|---|
| `source` | `VARCHAR(50)` | Clave primaria; identificador del origen/feed |
| `last_ingestion_ms` | `BIGINT` | Hora de la última consulta correcta en milisegundos epoch |
| `updated_at` | `TIMESTAMPTZ` | Instante UTC de actualización del estado |

No borrar la hora de la última consulta correcta cuando falle una consulta. El endpoint de salud la compara con un umbral de frescura configurado.

## `estado_productor`

Punto de control persistente de publicación para un evento y modo.

| Columna | Tipo | Restricciones / significado |
|---|---|---|
| `event_id` | `TEXT` | Parte de la clave primaria |
| `mode` | `VARCHAR(10)` | Parte de la clave primaria |
| `last_updated_ms` | `BIGINT` | Última revisión confirmada por Kafka |

Actualizar únicamente después de que Kafka confirme la entrega. Para decidir si se publica, usar la condición estricta `updated_ms > last_updated_ms`.

## Relaciones e invariantes de procesamiento

- `evento_actual.country_id` referencia a `pais`; `evento_actual.fecha_id` referencia a `fecha`.
- `evento_actual` contiene una proyección más reciente por `(event_id, mode)`; live y replay nunca se sobrescriben entre sí.
- `evento_version` conserva cada versión aceptada `(event_id, updated_ms, mode)`, aunque llegue después una revisión más antigua.
- Insertar versión/estado actual (o cuarentena) de forma transaccional; confirmar el offset Kafka solo después de confirmar PostgreSQL.
- `start` es inclusivo y `end` exclusivo; los intervalos horarios son ventanas UTC fijas basadas en `event_time`.
- `min_mag` excluye las magnitudes nulas; las magnitudes y profundidades nulas se informan por separado y se excluyen de las distribuciones/medias que dependan de esos valores.
- Conservar magnitudes y profundidades negativas válidas. Para valores ausentes usar `NULL` SQL, no cero.
