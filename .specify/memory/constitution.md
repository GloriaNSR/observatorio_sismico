# Constitución del Proyecto: Observatorio de Actividad Sísmica

## 1. Arquitectura e Infraestructura Innegociable
- **5 Servicios Docker Obligatorios:** El backend DEBE ejecutarse en 5 contenedores separados definidos en `compose.yaml`: `productor`, `kafka`, `consumidor`, `postgres` y `api`.
- **Arranque único:** Todo el backend debe levantarse con `docker compose up --build -d`. Prohibido ejecutar el productor o el consumidor fuera de Docker.
- **Imagen Python compartida:** Los servicios `productor`, `consumidor` y `api` compartirán una única imagen Docker reproducible (`backend/Dockerfile`) con comandos de inicio diferentes.
- **Salud y Persistencia:** Configurar `healthcheck` y políticas de reintento (`restart: on-failure` / bucle de reconexión en Python) para esperar a que Kafka y PostgreSQL estén listos. Usar volúmenes persistentes para PostgreSQL y retención limitada en Kafka.
- **Red y Seguridad:** Usar nombres de servicio Docker (`kafka:9092`, `postgres:5432`, `api:8000`), nunca `localhost` entre contenedores. Solo se expone la API (`0.0.0.0:8000`). Prohibido exponer Kafka o PostgreSQL por ngrok. Prohibido subir `.env`, tokens de ngrok o `node_modules` a Git.

## 2. Stack Tecnológico Cerrado
- **Backend y Pipeline:** Python 3.11+, `aiokafka`, `FastAPI`, `Uvicorn`, `psycopg2-binary` (o `asyncpg`/`SQLAlchemy`), `requests`/`httpx`, `geopandas` y `shapely`.
- **Almacenamiento y Broker:** PostgreSQL y Apache Kafka (modo KRaft sin Zookeeper preferiblemente para simplificar contenedores).
- **Frontend:** Proyecto Angular 20.3 + Leaflet 1.9.4 + ngx-leaflet 20.0 proporcionado en `frontend/`. Publicado como SPA en Netlify consumiendo la API vía túnel HTTPS de ngrok.
- **Restricciones:** Prohibido usar Spark, Flink, Kubernetes, Machine Learning o modificar `frontend/public/datos/paises.geojson`.

## 3. Reglas de Extracción e Ingesta (Productor y Histórico)
- **Feed en vivo:** Consultar `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson` cada 60s (configurable por variables de entorno).
- **Control de estado en Productor:** Comparar `(id, updated)` con estado persistente. Solo publicar en Kafka si el evento es nuevo o si `updated` es estrictamente mayor al último publicado. Esperar el ACK de Kafka antes de marcar como publicado.
- **Formato de mensaje Kafka:** Clave = `id` del evento. Valor JSON = `source`, `ingested_at`, `mode` (`live` o `replay`) y el GeoJSON original del evento.
- **Histórico 2025:** Descargar año 2025 completo (`minmagnitude=4`, `eventtype=earthquake`) subdividiendo por meses (sin truncar el límite de 20.000 de USGS) y deduplicando extremos.
- **Aislamiento Replay:** La reproducción histórica acelerada debe usar un topic o etiqueta `mode = 'replay'` y aislarse en base de datos para no contaminar las métricas `live`.

## 4. Reglas de Calidad, Limpieza e Integración Geográfica (Consumidor)
- **Filtro de tipo:** Procesar únicamente `properties.type == "earthquake"`. Cualquier otro tipo o coordenada inválida (fuera de rango `[-180, 180]` lon, `[-90, 90]` lat, o geometría no Point) va a la tabla `cuarentena` con su motivo y JSON original.
- **Tratamiento de Nulos y Valores Extremos:** PROHIBIDO rellenar `mag` o profundidad nulos con `0`. Se conservan como `NULL` en BD y se muestran como `ND` en el frontend, excluyéndolos del denominador en promedios. Magnitudes negativas o profundidades negativas NO se descartan automáticamente.
- **Cruce Espacial (Point-in-Polygon):** Prohibido extraer el país del texto `properties.place`. Usar `GeoPandas`/`Shapely` cruzando `[longitud, latitud]` (WGS84 / EPSG:4326) contra `paises.geojson` usando `ADM0_A3` como clave (`country_id`). Si cae en el mar o fuera de polígonos, guardar `country_id = NULL` y clasificar como `"Sin país asignado"`.

## 5. Modelado de Datos e Idempotencia (PostgreSQL)
- **Escritura Idempotente (At-least-once):** El consumidor debe guardar en `evento_version` (restricción única `(event_id, updated_ms, mode)`) y hacer UPSERT en `evento_actual` SOLO si `EXCLUDED.updated_ms > evento_actual.updated_ms`.
- **Confirmación de Offset:** Confirmar el offset de Kafka únicamente DESPUÉS de hacer `COMMIT` en PostgreSQL. Reenviar la misma versión o una versión más antigua no debe alterar `evento_actual` ni inflar los conteos.
- **Fechas:** Convertir timestamps de milisegundos Unix a UTC (`TIMESTAMPTZ`), separando siempre `event_time` (ocurrencia), `updated_time` (revisión USGS) e `ingested_at` (recepción).

## 6. Contrato de API y Frontend
- **Cumplimiento estricto de `CONTRATO-API.md`:** Implementar `GET /api/eventos` (GeoJSON `FeatureCollection` con `metadata`), `GET /api/salud` y `GET /api/estadisticas`.
- **Coherencia de Filtros y Agregaciones:** Las ventanas horarias UTC, conteos por país, distribuciones y comparativa con la media diaria de 2025 (dividiendo entre 365 días) se calculan desde `evento_actual` aplicando exactamente los mismos filtros (`start`, `end`, `min_mag`, `country_id`).
- **CORS y ngrok:** Habilitar CORS para `http://localhost:4200` y el dominio de Netlify, permitiendo `OPTIONS` y la cabecera `ngrok-skip-browser-warning`.