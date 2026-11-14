# Evidencias SQL de la captura live

**Instantánea:** 14 de noviembre de 2026, 09:56:54 UTC  
**Base/usuario:** `observatorio` / `observatorio`  
**Alcance de ejecución:** se mantuvo activo únicamente el contenedor `postgres` mediante `docker compose up -d postgres`. Todas las consultas siguientes son de solo lectura; no se publicaron mensajes ni se modificaron filas.

> La instantánea confirma datos live persistidos durante más de 24 horas. No demuestra que el productor sondeara el origen sin interrupción durante todo ese periodo: el esquema guarda revisiones recibidas, no un historial de cada sondeo exitoso. El mayor intervalo entre filas `evento_version` live fue de 4 días, 13 horas y 52 minutos. Puede corresponder a ausencia de nuevas revisiones o a una interrupción; con estos datos no es posible distinguirlo.

## 1. Eventos, versiones e idempotencia

### Conteo live y unicidad de la clave de versión

```sql
SELECT (SELECT count(*) FROM evento_actual WHERE mode='live') AS eventos_unicos_live,
       (SELECT count(*) FROM evento_version WHERE mode='live') AS versiones_live,
       (SELECT count(DISTINCT event_id) FROM evento_version WHERE mode='live') AS eventos_con_version_live,
       (SELECT count(*) FROM evento_version WHERE mode='live')
         - (SELECT count(DISTINCT event_id) FROM evento_version WHERE mode='live')
           AS revisiones_cazadas_live,
       (SELECT count(*) FROM evento_version WHERE mode='live')
         - (SELECT count(DISTINCT (event_id, updated_ms, mode))
            FROM evento_version WHERE mode='live') AS claves_duplicadas;
```

| Eventos únicos live | Versiones live | Eventos con versión | Versiones adicionales (`versiones - eventos`) | Claves duplicadas |
|---:|---:|---:|---:|---:|
| 1.770 | 2.542 | 1.770 | 772 | 0 |

Cada revisión se identifica por `(event_id, updated_ms, mode)`. Las 2.542 filas y las 2.542 claves distintas demuestran que los reenvíos no dejaron versiones duplicadas en esta instantánea.

### Revisiones reales y proyección vigente

```sql
WITH selected(event_id) AS (
    VALUES ('aka2026vqjriv'), ('ci41361583'), ('hv75068057')
),
ranked AS (
    SELECT v.event_id, v.updated_ms,
           v.raw_json->'properties'->>'mag' AS mag_version,
           count(*) OVER (PARTITION BY v.event_id) AS version_count,
           row_number() OVER (PARTITION BY v.event_id ORDER BY v.updated_ms) AS first_rank,
           row_number() OVER (PARTITION BY v.event_id ORDER BY v.updated_ms DESC) AS last_rank
    FROM evento_version v
    JOIN selected s USING (event_id)
    WHERE v.mode='live'
)
SELECT r.event_id, r.version_count, r.updated_ms, r.mag_version,
       a.updated_ms AS actual_updated_ms, a.mag AS actual_mag
FROM ranked r
JOIN evento_actual a ON a.event_id=r.event_id AND a.mode='live'
WHERE r.first_rank=1 OR r.last_rank=1
ORDER BY r.event_id, r.updated_ms;
```

| `event_id` | Versiones | `updated_ms` inicial | Magnitud inicial | `updated_ms` final | Magnitud final | `evento_actual.updated_ms` / `mag` |
|---|---:|---:|---:|---:|---:|---|
| `aka2026vqjriv` | 41 | 1793532696652 | 4.5 | 1793577462689 | 4.3 | 1793577462689 / 4.3 |
| `ci41361583` | 15 | 1794536080876 | 2.39 | 1794606734892 | 2.53 | 1794606734892 / 2.53 |
| `hv75068057` | 14 | 1794003121520 | 3.03 | 1794009956328 | 2.9 | 1794009956328 / 2.9 |

Comprobación de que el UPSERT dejó cada estado actual en la revisión de mayor `updated_ms`:

```sql
SELECT count(*) AS eventos_live,
       count(*) FILTER (WHERE v.max_updated_ms = a.updated_ms) AS actuales_en_maximo,
       count(*) FILTER (
           WHERE v.max_updated_ms <> a.updated_ms OR v.max_updated_ms IS NULL
       ) AS actuales_fuera_del_maximo
FROM evento_actual a
LEFT JOIN (
    SELECT event_id, max(updated_ms) AS max_updated_ms
    FROM evento_version WHERE mode='live'
    GROUP BY event_id
) v USING (event_id)
WHERE a.mode='live';
```

| Eventos live | Actual en máximo `updated_ms` | Actual fuera del máximo |
|---:|---:|---:|
| 1.770 | 1.770 | 0 |

## 2. Cuarentena y calidad

Desglose completo de `cuarentena`, agrupado por motivo y modo:

```sql
SELECT mode, motivo AS reason, count(*) AS filas
FROM cuarentena
GROUP BY mode, motivo
ORDER BY mode, count(*) DESC, motivo;
```

| Modo | Motivo (`reason`) | Filas |
|---|---|---:|
| live | `properties.type debe ser "earthquake"` | 110 |

Desglose adicional de esas entradas por tipo GeoJSON:

```sql
SELECT raw_json->'properties'->>'type' AS event_type, count(*) AS filas
FROM cuarentena
GROUP BY raw_json->'properties'->>'type'
ORDER BY count(*) DESC, event_type;
```

| Tipo recibido | Filas |
|---|---:|
| `other event` | 75 |
| `quarry blast` | 20 |
| `explosion` | 12 |
| `landslide` | 2 |
| `ice quake` | 1 |
| **Total** | **110** |

Ejemplos reales; el JSON original queda persistido en `cuarentena.raw_json`:

```sql
SELECT id, event_id, raw_json->'properties'->>'type' AS event_type,
       raw_json->'properties'->>'place' AS place,
       raw_json->'geometry'->'coordinates' AS coordinates,
       motivo AS reason, ingested_at
FROM cuarentena
WHERE raw_json->'properties'->>'type' IN ('quarry blast', 'other event')
ORDER BY ingested_at DESC, id DESC
LIMIT 4;
```

| `event_id` | Tipo | Lugar | Coordenadas `[lon, lat, depth]` | Motivo | Ingesta UTC |
|---|---|---|---|---|---|
| `mb90212658` | `quarry blast` | 5 km E of Butte, Montana | `[-112.462666666667, 46.0088333333333, -2]` | `properties.type debe ser "earthquake"` | 2026-11-14 09:50:38 |
| `ci41361759` | `quarry blast` | 10 km ESE of Coto De Caza, CA | `[-117.49333, 33.56633, -0.53]` | `properties.type debe ser "earthquake"` | 2026-11-13 21:55:27 |
| `ci41361503` | `quarry blast` | 5 km NE of Bradbury, CA | `[-117.93267, 34.17683, -0.91]` | `properties.type debe ser "earthquake"` | 2026-11-13 17:56:40 |
| `aka2026wmlchi` | `other event` | 64 km ENE of Pedro Bay, Alaska | `[-153.071, 60.052, 0]` | `properties.type debe ser "earthquake"` | 2026-11-14 09:50:38 |

## 3. Rango temporal y checkpoints

Rango de ocurrencia en `evento_actual` y horas UTC distintas con actividad:

```sql
SELECT min(event_time) AS first_event_utc,
       max(event_time) AS last_event_utc,
       round((extract(epoch FROM max(event_time)-min(event_time))/3600)::numeric, 2)
           AS elapsed_hours,
       count(DISTINCT date_trunc('hour', event_time)) AS distinct_active_utc_hours,
       count(*) AS live_event_rows
FROM evento_actual
WHERE mode='live';
```

| Primer `event_time` UTC | Último `event_time` UTC | Rango transcurrido | Horas UTC distintas activas | Filas live |
|---|---|---:|---:|---:|
| 2026-10-30 13:11:46.470 | 2026-11-14 09:26:13.073 | 356,24 h | 195 | 1.770 |

Ventana de recepción de versiones y mayor hueco entre versiones live:

```sql
WITH ordered AS (
    SELECT ingested_at,
           lag(ingested_at) OVER (ORDER BY ingested_at) AS previous_ingested_at
    FROM evento_version WHERE mode='live'
),
gaps AS (
    SELECT ingested_at - previous_ingested_at AS ingestion_gap
    FROM ordered WHERE previous_ingested_at IS NOT NULL
)
SELECT min(v.ingested_at) AS first_version_ingested_utc,
       max(v.ingested_at) AS last_version_ingested_utc,
       round((extract(epoch FROM max(v.ingested_at)-min(v.ingested_at))/3600)::numeric, 2)
           AS span_hours,
       count(DISTINCT date_trunc('hour', v.ingested_at)) AS active_ingestion_hours,
       (SELECT max(ingestion_gap) FROM gaps) AS largest_gap,
       (SELECT count(*) FROM gaps
        WHERE ingestion_gap > interval '24 hours') AS gaps_over_24h
FROM evento_version v
WHERE v.mode='live';
```

| Primera versión recibida UTC | Última versión recibida UTC | Rango | Horas de recepción distintas | Mayor hueco entre versiones | Huecos >24 h |
|---|---|---:|---:|---|---:|
| 2026-10-31 13:11:16 | 2026-11-14 09:50:38 | 332,66 h | 85 | 4 d 13:52:40 | 3 |

Checkpoints existentes por modo:

```sql
SELECT mode, count(*) AS checkpoint_rows,
       min(last_updated_ms) AS min_last_updated_ms,
       max(last_updated_ms) AS max_last_updated_ms
FROM estado_productor
GROUP BY mode
ORDER BY mode;
```

| Modo | Filas de checkpoint | `min_last_updated_ms` | `max_last_updated_ms` |
|---|---:|---:|---:|
| live | 1.828 | 1793366001734 | 1794649737409 |

Total de checkpoints: **1.828**.

## 4. Rendimiento y memoria ya medidos

La memoria ya contenía mediciones de ejecución con las primeras entidades **reales** del GeoJSON histórico 2025, no datos sintéticos. Se conservan aquí para consulta rápida; no se repitieron durante esta extracción SQL.

| Entrada real | Tiempo con índice | Pico Python con índice | Tiempo lineal | Pico Python lineal | RSS máximo con índice / lineal |
|---:|---:|---:|---:|---:|---:|
| 100 | 0,039 s | 0,020 MiB | 0,064 s | 0,036 MiB | 168,61 / 169,04 MiB |
| 1.000 | 0,393 s | 0,095 MiB | 0,641 s | 0,137 MiB | 169,55 / 168,82 MiB |
| 10.000 | 2,428 s | 0,145 MiB | 4,969 s | 0,137 MiB | 169,68 / 169,57 MiB |

El método, equipo, interpretación de `tracemalloc`/RSS y limitaciones están descritos en [memoria.md](./memoria.md), sección 5.
