# Observatorio sísmico

Aplicación para ingerir, validar y visualizar terremotos publicados por el USGS. El flujo combina Kafka y PostgreSQL para procesar eventos en tiempo real y reproducir un histórico, mientras que una API FastAPI entrega los datos a un panel Angular con mapa y estadísticas.

**Panel publicado:** [proyectoobservatoriosismico.netlify.app](https://proyectoobservatoriosismico.netlify.app)

**Documentación principal:** [contrato de la API](./CONTRATO-API.md) · [memoria técnica y resultados](./docs/memoria.md) · [evidencias de aceptación](./docs/evidencias/aceptacion.md)

> El panel publicado necesita que la API y su túnel HTTPS estén disponibles para mostrar datos en vivo. Si no puede conectarse, conserva la última respuesta válida e informa del estado de conexión.

## Qué hace

- Consulta periódicamente el feed GeoJSON del USGS y publica nuevos eventos y revisiones en Kafka.
- Valida la estructura, los tiempos, los valores y las coordenadas; conserva los mensajes inválidos en cuarentena con su motivo y contenido original.
- Asigna países a los eventos mediante un catálogo de 177 países. Los eventos válidos sin país —por ejemplo, los que ocurren en el mar— se conservan.
- Guarda el estado actual y el historial de revisiones en PostgreSQL. Reintentar una versión no crea duplicados y una revisión antigua no reemplaza una más reciente.
- Registra metadatos de cada consulta live al USGS y elimina esos registros al superar 30 días; conserva el GeoJSON original por versión o en cuarentena, no una copia completa repetida por sondeo.
- Expone eventos, salud y estadísticas mediante una API; el panel permite explorar los datos en un mapa y una tabla.
- Permite descargar y reproducir en modo separado (`replay`) el histórico de terremotos de 2025.

## Arquitectura

```text
USGS ──> productor ──> Kafka (terremotos) ──> consumidor ──> PostgreSQL
                                                        └──> cuarentena
                                                        PostgreSQL ──> API FastAPI
                                                                         └──> Angular + Leaflet
```

Compose levanta siete servicios, incluido el frontend estático y el túnel ngrok. Netlify puede publicar el mismo frontend para acceso remoto.

| Servicio | Función |
|---|---|
| `postgres` | PostgreSQL 16: países, eventos actuales, revisiones, cuarentena, auditoría de consultas y estado de ingesta. |
| `kafka` | Kafka 3.9.1 en modo KRaft de un nodo; tema `terremotos`. |
| `productor` | Consulta el feed en tiempo real y ofrece la carga y reproducción del histórico. |
| `consumidor` | Valida eventos, asigna país y persiste o pone en cuarentena cada mensaje. |
| `api` | API FastAPI para eventos, salud y estadísticas. |
| `ngrok` | Expone la API mediante el dominio estable configurado en la cuenta ngrok. |
| `frontend` | Sirve la aplicación Angular compilada con Nginx en `http://localhost:4200`. |

### Estructura del repositorio

| Ruta | Contenido |
|---|---|
| `backend/` | Productor, consumidor, cargador histórico, API y pruebas. |
| `frontend/` | Aplicación Angular, configuración, mapa y datos de demostración. |
| `sql/init.sql` | Esquema y datos iniciales de PostgreSQL. |
| `datos/historico/2025/` | Caché local del histórico; no se incluye en Git. |
| `docs/` | Memoria técnica y evidencias de las pruebas ejecutadas. |
| `CONTRATO-API.md` | Parámetros y formato de las respuestas de la API. |
| `FUENTES-Y-LICENCIAS.md` | Procedencia y licencias de los datos y recursos. |

## Requisitos

- Docker Desktop con Docker Compose v2.
- Node.js `22.12+` (rama 22) o Node.js 24, y npm solo si desarrollas/compilas el frontend fuera de Docker.
- Acceso a Internet para consultar el feed y descargar el histórico cuando no exista una copia local.
- Cuenta ngrok con su authtoken y dominio asignado para el acceso remoto; no es necesario abrir ngrok manualmente.

## Puesta en marcha local

Los comandos siguientes se ejecutan en PowerShell desde la raíz del repositorio.

1. Crea la configuración local y cambia la contraseña de ejemplo. En `.env`, guarda también el authtoken y dominio de ngrok:

   ```powershell
   if (-not (Test-Path .env)) { Copy-Item .env.example .env }
   notepad .env
   ```

   Si ya existe `.env`, no hace falta volver a copiarlo. No publiques ese archivo ni incluyas credenciales en el frontend.
   `NGROK_AUTHTOKEN` debe permanecer solo en `.env`; `NGROK_DOMAIN` es el dominio asignado en la cuenta ngrok.

2. Construye y arranca los servicios:

   ```powershell
   docker compose up --build -d
   docker compose ps
   docker compose config --quiet
   ```

   Espera a que API, PostgreSQL y Kafka estén saludables y a que `ngrok` y `frontend` estén en ejecución. La web local queda disponible en `http://localhost:4200`, la API en `http://127.0.0.1:8000` y, desde fuera, en el dominio ngrok configurado. PostgreSQL y Kafka solo se exponen dentro de la red de Compose.

   Si ya conservas un volumen PostgreSQL de una versión anterior, vuelve a aplicar el SQL idempotente para crear las tablas añadidas:

   ```powershell
   docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -f /docker-entrypoint-initdb.d/002-consulta_usgs.sql'
   ```

3. Comprueba la salud y los datos:

   ```powershell
   curl.exe http://127.0.0.1:8000/api/salud
   curl.exe "http://127.0.0.1:8000/api/eventos?mode=live&limit=5"
   curl.exe http://127.0.0.1:8000/api/estadisticas
   ```

   `/api/salud` debe indicar `status: ok` y `database: available`. El productor consulta el feed aproximadamente cada 60 segundos; los eventos aparecerán cuando el USGS publique datos.

4. Para desarrollo del frontend fuera de Docker, puedes iniciar el servidor Angular:

   ```powershell
   cd frontend
   npm ci
   npm start
   ```

   En el uso habitual no necesitas ejecutar estos comandos: Compose sirve la versión compilada en <http://localhost:4200>. Para compilar manualmente, `npm run build` genera `frontend/dist/observatorio/browser`.

## API

La API escucha en el puerto `8000` y ofrece:

| Ruta | Descripción |
|---|---|
| `GET /api/salud` | Estado de la API, la base de datos y la ingesta. |
| `GET /api/eventos` | Eventos GeoJSON, con filtros temporales, magnitud, país y modo. |
| `GET /api/estadisticas` | Totales, distribución por país y distribución de magnitudes. |

Ejemplo de consulta de reproducción:

```powershell
curl.exe "http://127.0.0.1:8000/api/eventos?mode=replay&start=2025-01-01T00:00:00Z&end=2026-01-01T00:00:00Z&min_mag=4"
```

`start` es inclusivo, `end` exclusivo y `min_mag` no incluye magnitudes nulas. El modo predeterminado es `live`; `country_id=unassigned` selecciona eventos válidos sin país asignado. Consulta el [contrato de la API](./CONTRATO-API.md) para el detalle de parámetros y respuestas.

## Cargar el histórico de 2025

Con los servicios en ejecución, el cargador descarga el histórico desde USGS si no hay una copia local y después lo publica por Kafka en modo `replay`:

```powershell
docker compose exec -T productor python cargar_historico.py
```

Para reproducir una copia ya descargada sin volver a consultar USGS:

```powershell
docker compose exec -T productor python cargar_historico.py --accion solo-reproducir
```

También se puede usar `--accion solo-descargar` para descargar sin publicar, o añadir `--sin-casos-controlados` a la reproducción para omitir los eventos de demostración de duplicados y revisiones. Los datos se guardan en `datos/historico/2025/`, excluidos de Git. La separación entre `live` y `replay` evita mezclar los dos flujos en las consultas.

## Pruebas y estado de validación

Las evidencias registradas incluyen pruebas unitarias, de contrato, integración y aceptación; además, se comprobó la compilación Angular. Con los cinco servicios activos, ejecuta:

```powershell
docker compose run --rm --no-deps consumidor sh -c "pip install -r requirements-test.txt && python -m pytest -q tests/unit tests/contract tests/integration/test_pipeline.py tests/integration/test_replay.py tests/acceptance"
python -m pytest -q backend/tests/integration/test_infraestructura.py
```

Para compilar el frontend:

```powershell
cd frontend
npm ci
npm run build
```

En las evidencias del 6 de noviembre de 2026 constan 37 pruebas del servidor y 2 de infraestructura aprobadas, junto con la compilación Angular correcta. El 16 de noviembre se comprobó visualmente la versión publicada en Netlify y se levantó un túnel ngrok real: `/api/salud`, `/api/eventos` y `/api/estadisticas` respondieron HTTP 200; el preflight `OPTIONS` desde el origen Netlify también respondió 200 con CORS. En el navegador se verificaron datos live y el cambio a replay (18.302 registros del año 2025; 1.000 en la página y estadísticas completas). El 18 de noviembre pasaron 42 pruebas backend y el build Angular; también se verificaron los cinco servicios Compose, el CORS local y el registro de consultas USGS. No se publicó una nueva versión de Netlify. El panel público depende de que el túnel siga activo; si se cierra, deja de recibir datos. Los resultados detallados están en [evidencias de aceptación](./docs/evidencias/aceptacion.md) y [evidencias SQL del 18 de noviembre](./docs/evidencias_24h.md#5-verificación-tras-desplegar-la-auditoría-usgs).

## Evaluación según la rúbrica

El [enunciado oficial, §13](./ENUNCIADO.md#13-evaluación) evalúa cinco criterios. Esta tabla indica dónde está la evidencia existente y qué conviene explicar o mostrar para defender el nivel **sobresaliente**. El mapa por sí solo no sustituye los análisis ni sus resultados.

| Criterio | Evidencias del proyecto | Para defender sobresaliente |
|---|---|---|
| **Extracción** — carga y análisis de tipos | [Memoria, §3: procedencia y carga histórica](./docs/memoria.md#3-procedencia-y-carga-histórica); [manifiesto del histórico](./datos/historico/2025/manifest.json); `backend/productor.py` y `backend/cargar_historico.py`. | Explicar la fuente live y las consultas históricas; distinguir GeoJSON, tipos, unidades y límites; enseñar las consultas mensuales, la deduplicación y cómo se regenera la caché. El histórico registrado contiene 18.302 eventos de 2025 con `minmagnitude=4`. |

| **Calidad** — análisis completo y acciones | [Memoria, §4: calidad y cuarentena](./docs/memoria.md#4-calidad-de-datos-y-cuarentena); [consultas y resultados SQL, §2](./docs/evidencias_24h.md#2-cuarentena-y-calidad); `backend/consumidor.py`; pruebas de aceptación. | Presentar al menos cuatro dimensiones con denominador y resultados antes/después: completitud, unicidad, validez, consistencia de revisiones e integración geográfica. Justificar acciones (conservar nulos, poner tipos/coordenadas inválidos en cuarentena, no descartar eventos sin país), mostrar ejemplos y reconocer límites de la fuente y del periodo. |

| **Modelado** — modelo optimizado | [Memoria, §2: modelo conceptual, lógico y físico](./docs/memoria.md#2-modelo-conceptual-lógico-y-físico); [`sql/init.sql`](./sql/init.sql); [contrato de la API](./CONTRATO-API.md); [pruebas de aceptación, escenarios 1–8 y 11](./docs/evidencias/aceptacion.md#once-escenarios-del-enunciado). | Explicar el grano de cada tabla, las claves y relaciones geográficas, el historial de revisiones, la idempotencia y por qué se usan las restricciones e índices existentes. Aclarar que la medición de almacenamiento a 100 millones es una extrapolación, no una prueba a esa escala. |

| **Análisis** — análisis con insights | [Memoria, §3 y §7](./docs/memoria.md#7-conclusiones-del-análisis); panel: recuentos por país y subregión, actividad por hora, distribuciones de magnitud/profundidad y comparación histórica; [prueba visual y del túnel](./docs/evidencias/aceptacion.md#comprobación-visual-del-panel-y-del-túnel-real). | Relacionar al menos tres resultados con filtros y periodo: comparación de live con la media diaria de 2025 usando el mismo umbral, eventos sin país que siguen siendo válidos, y distribución temporal/por magnitud o profundidad. Explicar qué significan los resultados y sus límites, no solo leer el mapa. |

| **Reflexión** — reflexión excelente y propuestas | [Memoria, §5, §7 y §8](./docs/memoria.md#5-rendimiento-y-complejidad); mediciones de 100, 1.000 y 10.000 eventos; [costes, escalado y mejoras](./docs/memoria.md#complejidad-almacenamiento-y-escalado). | Comparar complejidad, tiempo y memoria; justificar el índice espacial con sus límites; separar mediciones de extrapolaciones; declarar supuestos de coste y proponer mejoras priorizadas (lotes, particiones Kafka, particionado y pruebas de recuperación). |

Para la defensa, lleva las consultas y tablas de la memoria junto con las capturas del [panel live](./docs/evidencias/panel-live-2026-11-18.png) y del [panel replay 2025](./docs/evidencias/panel-replay-2025-2026-11-18.png). Las imágenes muestran el periodo, el recuento y las distribuciones comunicados en la memoria.

## Publicación del panel y acceso remoto

Netlify publica el frontend como una SPA. La configuración registrada usa `frontend` como directorio base, `npm run build` como comando y `dist/observatorio/browser` como directorio de publicación. La URL del panel es [proyectoobservatoriosismico.netlify.app](https://proyectoobservatoriosismico.netlify.app).

El panel usa el dominio ngrok asignado a la cuenta, configurado en `frontend/public/config.json`. Para iniciar juntos la web, la API y el túnel:

```powershell
docker compose up -d
```

Abre <http://localhost:4200> para la web local o la URL de Netlify para la publicada. El túnel reenvía al servicio `api:8000` y no se debe iniciar otro `ngrok http` en Windows. El dominio asignado se reutiliza al reiniciar, pero solo está disponible mientras el ordenador, Docker y la conexión estén activos. No guardes el authtoken en `config.json`: ese archivo se distribuye con el frontend público. Tras cambiar el dominio, actualiza `NGROK_DOMAIN` en `.env` y `apiBaseUrl` en `frontend/public/config.json`, y vuelve a desplegar Netlify.

## Parada

Detén los servicios conservando los datos persistentes:

```powershell
docker compose down
```

No uses `docker compose down -v` salvo que quieras borrar explícitamente los volúmenes de PostgreSQL y Kafka.
