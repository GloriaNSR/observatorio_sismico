# Observatorio sísmico

Flujo de procesamiento de eventos USGS con cinco servicios Docker Compose: PostgreSQL, Kafka, productor, consumidor y API FastAPI. El panel Angular se ejecuta en el equipo anfitrión; no es un sexto contenedor.

## Resumen técnico para la defensa

### Arquitectura

| Contenedor | Responsabilidad |
|---|---|
| `postgres` | PostgreSQL 16: dimensiones, eventos vigentes y revisiones, cuarentena y marcas de ingesta. |
| `kafka` | Kafka 3.9.1 en KRaft de un nodo: canal `terremotos` y offsets del consumidor. |
| `productor` | Consulta USGS cada 60 s, compara `(id, updated)` y publica revisiones nuevas con confirmación de Kafka. También ejecuta la carga histórica. |
| `consumidor` | Valida el GeoJSON, busca el país entre 177 polígonos y guarda el evento o el rechazo en PostgreSQL. |
| `api` | FastAPI: consulta los eventos, el estado del servicio y las estadísticas. |

Angular/Leaflet se sirve desde el equipo local o Netlify, fuera de Compose. La memoria describe el modelo, la transacción de idempotencia y las decisiones de escalado.

### Reglas de calidad y ejemplos de cuarentena

| Regla | Tratamiento |
|---|---|
| GeoJSON `Feature`, tipo `earthquake` y geometría `Point` | La forma incorrecta o los tipos distintos se rechazan con motivo y JSON original en cuarentena. |
| `id`, `time` y `updated` válidos | Se requieren identificador no vacío y tiempos Unix en milisegundos. La pareja `(id, updated)` distingue las revisiones. |
| Coordenadas numéricas finitas dentro de WGS84 | Un punto inválido va a cuarentena; uno válido que no cruza ningún país se conserva con `country_id: null`. |
| Magnitud y profundidad numéricas o nulas | `NULL` se preserva; nunca se convierte en cero. |
| Actualización idempotente | Se inserta la revisión una sola vez y el UPSERT actual solo avanza si aumenta `updated_ms`; PostgreSQL confirma antes del offset Kafka. |

Casos reales consultados en la base de datos: `ok2026vshf` (`quarry blast`) y `uw714132811` (`explosion`) quedaron en cuarentena porque el tipo requerido era `earthquake`. La [memoria](./docs/memoria.md) incluye sus coordenadas, motivos completos y la tabla detallada de reglas.

### Pruebas de aceptación ejecutadas

| # | Escenario | Resultado |
|---:|---|---|
| 1 | Ingesta de un evento nuevo | Aprobado; persistido y visible por API/panel con país asignado. |
| 2 | Reenvío de la misma versión | Aprobado; sin versión duplicada. |
| 3 | Recepción de una revisión posterior | Aprobado; queda vigente el mayor `updated_ms`. |
| 4 | Recepción tardía de revisión antigua | Aprobado; historial conservado, estado actual sin retroceso. |
| 5 | Magnitud nula | Aprobado; `NULL` conservado y excluido por `min_mag`. |
| 6 | Coordenadas inválidas y `quarry blast` | Aprobado; rechazados con motivo y mensaje original en cuarentena. |
| 7 | Evento marítimo | Aprobado; `country_id: null`, visible como «Sin país asignado». |
| 8 | Reinicio del consumidor | Aprobado; offset 19363, lag cero, 18.302 registros replay estables. |
| 9 | Fallo de conexión del panel | Aprobado mediante interceptación local; conservó las 180 filas. No hubo ngrok real. |
| 10 | Recuperación de conexión | Aprobado al retirar la interceptación; no demuestra TLS/CORS de ngrok. |
| 11 | Aislamiento `replay`/`live` | Aprobado; el evento replay no aparece en live y sus revisiones son idempotentes. |

Validación transversal: los tres endpoints devolvieron HTTP 200, las 37 pruebas del servidor y las 2 de infraestructura pasaron, y Angular compiló correctamente.

### Rendimiento, complejidad y coste

Medición sobre eventos reales del histórico 2025; el índice espacial se construyó antes del cronómetro. Los detalles del entorno y las limitaciones de memoria están en la [memoria](./docs/memoria.md).

| Eventos | Tiempo con índice espacial | Memoria Python adicional | RSS máximo del proceso |
|---:|---:|---:|---:|
| 100 | 0,039 s | 0,020 MiB | 168,61 MiB |
| 1.000 | 0,393 s | 0,095 MiB | 169,55 MiB |
| 10.000 | 2,428 s | 0,145 MiB | 169,68 MiB |

Con 177 países, una búsqueda lineal cuesta `O(N·177)`; el índice espacial tiene coste esperado `O(177 log 177 + N log 177)` y memoria `O(177)`. A 10.000 eventos midió 2,05 veces más rápido que la búsqueda lineal. Como referencia no contractual, la memoria estima unos 147 GB para 100 millones de eventos y su historial equivalente; dos copias serían unos 441 GB antes de WAL, espacio libre y revisiones adicionales. El coste monetario en nube depende de proveedor, región, cómputo, transferencia y retención, por lo que no se inventa una cotización.

### Tres conclusiones

1. El índice PIP mejora el tiempo medido con 10.000 eventos de 4,969 s a 2,428 s; para volúmenes mayores se requieren pruebas de carga, lotes y particiones Kafka adicionales.
2. PostgreSQL antes de confirmar el offset, junto con la unicidad `(event_id, updated_ms, mode)`, evitó duplicados durante reenvíos y reinicio; el histórico se mantuvo en 18.302 filas.
3. La ubicación geográfica no es una regla de validez: 14.350 de 18.302 eventos replay (78,4 %) carecen de país pero siguen siendo analizables; eventos no sísmicos sí se ponen en cuarentena con su explicación.

## Requisitos

- Docker Desktop con Docker Compose v2.
- Node.js 22.12 o posterior dentro de la rama 22, o Node 24, y npm.
- Conexión a Internet para descargar el histórico si no existe una copia local.
- ngrok y una cuenta Netlify solo si se va a publicar el panel/API.

## 1. Configuración y arranque desde cero

Desde la raíz del repositorio, crea la configuración local y cambia las contraseñas de ejemplo antes de exponer nada:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
notepad .env
```

Arranca la infraestructura completa con el único comando de inicio:

```powershell
docker compose up --build -d
docker compose ps
docker compose config --quiet
```

Espera a que los cinco servicios indiquen `healthy` (saludable). La API queda enlazada a `127.0.0.1:8000`; PostgreSQL y Kafka solo son accesibles desde la red de Compose. De forma predeterminada, el productor consulta cada 60 segundos el canal de eventos en tiempo real de USGS.

Comprueba la API:

```powershell
curl.exe http://127.0.0.1:8000/api/salud
curl.exe "http://127.0.0.1:8000/api/eventos?mode=live&limit=5"
curl.exe http://127.0.0.1:8000/api/estadisticas
```

`/api/salud` debe devolver `status: ok` y `database: available` (base de datos disponible). Verifica también el catálogo geográfico y los modos de procesamiento guardados:

```powershell
docker compose exec -T postgres psql -U observatorio -d observatorio -c "SELECT COUNT(*) AS paises FROM pais;"
docker compose exec -T postgres psql -U observatorio -d observatorio -c "SELECT mode, COUNT(*) AS eventos FROM evento_actual GROUP BY mode ORDER BY mode;"
docker compose exec -T postgres psql -U observatorio -d observatorio -c "SELECT mode, COUNT(*) AS versiones FROM evento_version GROUP BY mode ORDER BY mode;"
```

El catálogo contiene 177 países. Los archivos históricos se guardan bajo `datos/historico/2025/`, ruta excluida de Git; se descargan desde USGS cuando no hay copia local.

## 2. Cargar y reproducir el histórico de 2025

Para descargar el año completo desde USGS y publicarlo por Kafka en modo de reproducción histórica (`replay`):

```powershell
docker compose exec -T productor python cargar_historico.py
```

El cargador usa consultas mensuales, divide los intervalos que se aproximan al límite de resultados, elimina duplicados en los extremos y conserva el manifiesto y GeoJSON en `datos/historico/2025/`. La ejecución predeterminada descarga y después reproduce el archivo. Si el histórico ya existe y solo quieres volver a reproducirlo:

```powershell
docker compose exec -T productor python cargar_historico.py --accion solo-reproducir
```

Otras opciones:

```powershell
docker compose exec -T productor python cargar_historico.py --accion solo-descargar
docker compose exec -T productor python cargar_historico.py --accion solo-reproducir --sin-casos-controlados
```

El productor de reproducción histórica envía mensajes al tema Kafka existente; el mismo consumidor los valida y guarda con `mode=replay`. No se añade otro servicio y se conserva la hora original del evento. De forma predeterminada se incluyen eventos de prueba para demostrar duplicados y revisiones; `--sin-casos-controlados` los omite.

Consulta los datos reproducidos por separado de los recibidos en tiempo real:

```powershell
curl.exe "http://127.0.0.1:8000/api/eventos?mode=replay&start=2025-01-01T00:00:00Z&end=2026-01-01T00:00:00Z&min_mag=4"
curl.exe "http://127.0.0.1:8000/api/estadisticas?mode=replay&country_id=unassigned"
```

En los filtros de la API, `start` incluye el instante indicado, mientras que `end` lo excluye; ambos aceptan fechas ISO 8601 con zona horaria. `min_mag` excluye las magnitudes nulas. El valor reservado `country_id=unassigned` selecciona los eventos que no caen dentro de ningún polígono.

## 3. Iniciar el frontend Angular

En otra terminal, desde la raíz:

```powershell
cd frontend
npm ci
npm start
```

Abre <http://localhost:4200>. La configuración inicial de [frontend/public/config.json](./frontend/public/config.json) consulta `http://localhost:8000`; la configuración CORS permite solicitudes desde `http://localhost:4200`. Para compilar:

```powershell
npm run build
```

Los archivos generados quedan en `frontend/dist/observatorio/browser`. La tabla y las ayudas emergentes representan los valores ausentes como `ND`; los formatos habituales de ubicación de USGS (distancia y orientación) se muestran en español sin alterar el texto original guardado. Los eventos marítimos conservan `country_id: null` en la API y el panel los muestra como «Sin país asignado».

## 4. Exponer la API con ngrok

La API solo acepta conexiones locales. Instala y configura ngrok siguiendo las instrucciones de su proveedor, guarda el token de acceso en la configuración local de ngrok (nunca en Git ni en la aplicación web) y ejecuta desde el equipo anfitrión:

```powershell
ngrok http 8000
```

La URL pública de API de este despliegue es `https://vertebrae-giggling-hardwired.ngrok-free.dev`. En `frontend/public/config.json`, configura `apiBaseUrl` con esa URL **sin** añadir `/api`:

```json
{
  "mode": "api",
  "apiBaseUrl": "https://vertebrae-giggling-hardwired.ngrok-free.dev",
  "refreshMs": 60000
}
```

Añade el origen exacto del panel a `CORS_ORIGINS` en `.env` (separado por comas si hay más de uno), y reinicia la API:

```powershell
docker compose up -d --force-recreate api
```

El frontend envía siempre la cabecera `ngrok-skip-browser-warning` en sus peticiones a la API; CORS permite GET/OPTIONS y esa cabecera. El middleware admite `http://localhost:4200`, los subdominios HTTPS de `netlify.app` y los orígenes exactos adicionales indicados en `CORS_ORIGINS` o `NETLIFY_ORIGIN`; no usa credenciales. La API y el túnel deben seguir activos mientras se usa el panel. Si se interrumpe la conexión, la interfaz conserva la última lectura válida, muestra el error y cuánto tiempo ha pasado desde esa lectura, y vuelve a consultar automáticamente.

## 5. Desplegar el frontend en Netlify

1. Publica el repositorio sin `.env`, credenciales ni `node_modules`.
2. Conecta el repositorio a Netlify. Configura el campo **Base directory** (directorio base) como `frontend`.
3. Usa `npm run build` como comando de compilación y `dist/observatorio/browser` como directorio que se publicará. [frontend/netlify.toml](./frontend/netlify.toml) ya contiene esos valores y `frontend/public/_redirects` permite recargar las rutas de la aplicación de una sola página (SPA).
4. Configura `frontend/public/config.json` con la URL HTTPS pública de ngrok (sin `/api`) y vuelve a publicar para que el archivo se incluya en la compilación.
5. Si se usa un dominio personalizado, en `.env` define `NETLIFY_ORIGIN` con el origen exacto (sin ruta) y añádelo a `CORS_ORIGINS` junto con `http://localhost:4200`. Los sitios bajo `*.netlify.app` ya están permitidos por el patrón HTTPS del middleware.
6. Aplica la configuración del servidor con `docker compose up -d --force-recreate api` y comprueba el panel y las tres rutas desde el sitio publicado.

No guardes claves ni authtokens en `config.json`: se distribuye públicamente con la aplicación.

## 6. Pruebas y parada segura

Con los cinco servicios activos, las pruebas del servidor y las pruebas de aceptación se pueden ejecutar en un contenedor temporal:

```powershell
docker compose run --rm --no-deps consumidor sh -c "pip install -r requirements-test.txt && python -m pytest -q tests/unit tests/contract tests/integration/test_pipeline.py tests/integration/test_replay.py tests/acceptance"
python -m pytest -q backend/tests/integration/test_infraestructura.py
```

Los escenarios y resultados observados están en [docs/evidencias/aceptacion.md](./docs/evidencias/aceptacion.md); el diseño, mediciones y decisiones están en [docs/memoria.md](./docs/memoria.md).

Para detener los servicios conservando las bases de datos:

```powershell
docker compose down
```

No uses `docker compose down -v` salvo que quieras borrar explícitamente los volúmenes persistentes de PostgreSQL y Kafka.
