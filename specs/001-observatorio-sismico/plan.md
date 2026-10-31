# Plan de implementación: Observatorio de Actividad Sísmica

**Rama**: `001-observatorio-sismico` | **Fecha**: 2026-10-31 | **Especificación**: `./spec.md`

**Entrada**: Especificación de la funcionalidad en `/specs/001-observatorio-sismico/spec.md`, junto con la estructura solicitada para backend, base de datos, Compose y Angular.

## Resumen

Implementar el observatorio mediante cinco servicios de Docker Compose: un productor Python consulta USGS y publica en Kafka mensajes live o replay confirmados; un consumidor Python los valida, enriquece geográficamente, versiona y persiste en PostgreSQL; y un servicio FastAPI expone eventos, salud y estadísticas. El frontend existente Angular 20.3/Leaflet incorporará filtros compartidos, paneles estadísticos, frescura del servicio y una tabla accesible de eventos. PostgreSQL mantendrá proyecciones live y replay separadas por `(event_id, mode)`, y las versiones inmutables usarán `(event_id, updated_ms, mode)` para garantizar idempotencia.

## Contexto técnico

**Lenguaje/versión**: Python 3.11 sobre `python:3.11-slim`; Angular 20.3 y TypeScript existentes; Leaflet 1.9.4 y ngx-leaflet 20.0.

**Dependencias principales**: FastAPI, Uvicorn, aiokafka, psycopg2-binary, requests, GeoPandas, Shapely; PostgreSQL 16 y Apache Kafka en modo KRaft.

**Almacenamiento**: Tablas PostgreSQL inicializadas mediante `sql/init.sql`; volumen persistente de PostgreSQL; retención en el tema de Kafka; GeoJSON de Natural Earth montado en modo de solo lectura desde los recursos del frontend.

**Pruebas**: Pruebas unitarias, de integración y de contrato de API con pytest para el backend; comprobaciones del esquema SQL, restricciones e idempotencia; verificaciones de salud y arranque de Docker Compose; compilación de producción de Angular y pruebas específicas del frontend si están configuradas.

**Plataforma objetivo**: Docker en el equipo de desarrollo; API publicada únicamente en `127.0.0.1:8000`; SPA Angular desplegada en Netlify y conectada a la API mediante HTTPS/ngrok.

**Tipo de proyecto**: Flujo de datos y API HTTP en Python, con aplicación Angular de página única.

**Objetivos de rendimiento**: Consultar USGS cada 60 segundos de forma predeterminada; evitar que se solapen las actualizaciones del frontend; mantener la coherencia entre filtros de consulta y agregaciones; procesar el archivo de 2025 en solicitudes mensuales acotadas y subdividirlas si se pudiera alcanzar el límite de resultados de USGS.

**Restricciones**: Exactamente cinco servicios requeridos (`postgres`, `kafka`, `productor`, `consumidor`, `api`); un único Dockerfile/imagen de backend compartido por productor, consumidor y API; no ejecutar productor ni consumidor en el equipo anfitrión; no exponer Kafka ni PostgreSQL; todas las conexiones entre contenedores usan DNS de servicio; conservar nulos y valores negativos válidos; no modificar el GeoJSON de países proporcionado; marcas de tiempo UTC; confirmar offsets de Kafka solo después de confirmar la transacción PostgreSQL.

**Escala/alcance**: Despliegue único para un equipo o aula; flujo live mundial y conjunto histórico 2025 con `minmagnitude=4` y `eventtype=earthquake`; consultas, panel y análisis con aislamiento live/replay.

## Verificación de la constitución

*PUERTA DE CONTROL: Debe aprobarse antes de la investigación de la Fase 0 y volver a comprobarse después del diseño de la Fase 1.*

- **Arquitectura — APROBADA**: `compose.yaml` en la raíz del repositorio define los cinco servicios requeridos. `productor`, `consumidor` y `api` usan `backend/Dockerfile` con comandos distintos. Compose configura comprobaciones de salud, disponibilidad de dependencias, reinicios/reintentos, volumen de PostgreSQL y retención limitada de Kafka.
- **Red y exposición — APROBADA**: Los contenedores usan `postgres:5432`, `kafka:9092` y `api:8000`; solo el puerto de la API se publica en el loopback del equipo como `127.0.0.1:8000:8000`.
- **Tecnologías — APROBADAS**: Python 3.11+, FastAPI, aiokafka, psycopg2-binary, requests, GeoPandas/Shapely, PostgreSQL, Kafka KRaft y Angular/Leaflet proporcionados cumplen la constitución.
- **Ingesta y replay — APROBADOS**: El productor persiste el estado de publicación confirmada; el cargador histórico consulta 2025 en intervalos acotados y publica datos etiquetados como replay; las consultas del consumidor seleccionan siempre el modo solicitado.
- **Calidad y persistencia — APROBADAS**: SQL admite cuarentena, unicidad de versiones inmutables, upserts del evento actual, conservación de nulos y asignación espacial del país mediante `ADM0_A3`. Los offsets Kafka se confirman después de la transacción de base de datos.
- **Frontend/API — APROBADOS**: Eventos, estadísticas y salud usan los endpoints documentados y filtros equivalentes; CORS permite el origen Netlify configurado, el origen local de desarrollo, OPTIONS y la cabecera de advertencia de ngrok.

No hay incumplimientos de la constitución ni decisiones de diseño pendientes que bloqueen la Fase 0. Tras la Fase 1, el diseño sigue superando estas comprobaciones; véanse `research.md` y los contratos de esquema/API.

## Estructura del proyecto

### Documentación de esta funcionalidad

```text
specs/001-observatorio-sismico/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   └── api-and-messaging.md
├── spec.md
└── tasks.md                 # Lo genera /speckit-tasks, no este plan
```

### Código fuente (raíz del repositorio)

```text
compose.yaml
backend/
├── Dockerfile
├── requirements.txt
├── productor.py
├── consumidor.py
├── api.py
└── cargar_historico.py
sql/
└── init.sql
frontend/
├── src/app/
│   ├── app.component.ts
│   └── app.component.html
└── public/datos/
    └── paises.geojson # Referencia de solo lectura; no modificar
CONTRATO-API.md
```

**Decisión sobre la estructura**: Consolidar la aplicación Angular proporcionada en `frontend/` en la raíz del repositorio, y añadir `backend/`, `sql/` y `compose.yaml` en esa misma raíz. Compose montará el GeoJSON de países desde `frontend/public/datos/paises.geojson` en modo de solo lectura en los servicios Python que realizan la asignación espacial. Los documentos de enunciado, API, fuentes y validación también están disponibles en la raíz.

## Seguimiento de complejidad

No se requiere ninguna excepción a la constitución. El despliegue de cinco servicios y las tablas separadas de versiones y estado actual son requisitos de arquitectura, no complejidad opcional.

## Decisiones de diseño y notas operativas

- `evento_actual` usa la clave primaria compuesta solicitada `(event_id, mode)` para que un mismo evento USGS tenga proyecciones live y replay independientes. Su UPSERT solo avanza si el `updated_ms` entrante es mayor.
- `evento_version` tiene una restricción única sobre `(event_id, updated_ms, mode)`. El consumidor inserta la versión y actualiza el estado actual dentro de una transacción PostgreSQL, y luego confirma el offset Kafka.
- `estado_productor` se actualiza únicamente después de que Kafka confirme la publicación. Las publicaciones del cargador histórico usan `mode='replay'`; la deduplicación en el origen y en la base de datos permite reintentar de forma segura.
- Al iniciar el consumidor, cargar idempotentemente el catálogo de países montado en `pais` antes de aceptar mensajes de eventos; mantener el GeoJSON original en modo de solo lectura.
- `fecha.fecha_id` se deriva de `event_time` convertido a UTC; `dia_semana` usa numeración ISO (lunes=1 y domingo=7). Insertar la fila de fecha en la misma transacción que el evento.
- La frescura de salud se deriva de `estado_ingesta.last_ingestion_ms` y de un umbral de antigüedad configurable y documentado; conservar la última marca de tiempo correcta si falla una consulta. No representar un fallo de base de datos/origen como una respuesta vacía exitosa.
- Los índices solicitados de `evento_actual` cubren fecha+modo, magnitud y país. Añadir otros índices solo si los justifican los planes de consulta representativos.
