# Material del observatorio sísmico

Empieza por `ENUNCIADO.md`. Para una explicación paso a paso, consulta `FAQ-ALUMNADO.md`: contiene preguntas frecuentes, ejemplos de revisiones y duplicados y un itinerario de trabajo. Este paquete aporta el enunciado y la base cartográfica, no una solución completa del proyecto.

## 1. Ejecutar el mapa de demostración

Requisitos: Node.js 22.12 o posterior dentro de la rama 22, o Node 24; npm; acceso a Internet para instalar dependencias y descargar el mapa base.

```bash
cd frontend
npm ci
npm start
```

Abre http://localhost:4200 . Verás fronteras reales de países y **cuatro eventos ficticios**. Comprueba zoom, desplazamiento, selector de capas y fichas al pulsar. Si OpenStreetMap no responde, desactiva esa capa: las fronteras y puntos incluidos siguen siendo locales.

Se fija Angular 20.3, ngx-leaflet 20.0 y Leaflet 1.9.4 como combinación reproducible; no se pretende elegir las últimas versiones. Para un proyecto Angular existente, selecciona la versión de ngx-leaflet compatible con tu Angular en su changelog. No copies este package.json sobre un proyecto ajeno.

## 2. Qué archivos dibujan el mapa

| Archivo | Función |
|---|---|
| `src/app/app.component.ts` | Inicializa capas, carga GeoJSON y actualiza los puntos |
| `src/app/app.component.html` | Contenedor del mapa, estado y leyenda |
| `src/app/app.component.css` | Tamaño y presentación del mapa |
| `src/styles.css` | Estilos globales |
| `public/datos/paises.geojson` | Catálogo completo entregado, sin modificación |
| `public/datos/terremotos-demo.geojson` | Muestra sintética inicial |
| `public/config.json` | Modo demo o API y URL del backend |
| `angular.json` | Carga también el CSS de Leaflet y copia public al build |

`L.geoJSON()` interpreta Polygon/MultiPolygon y el orden de coordenadas automáticamente. Para crear un círculo manualmente se utiliza `[latitud, longitud]`. Las fronteras se dibujan sin relleno sobre OpenStreetMap. Se utiliza la proyección predeterminada del mapa base (Web Mercator): no superpongas un mapamundi equirectangular como si fuera compatible sin reproyectarlo. El catálogo ya contiene la representación cartográfica; no es necesario dibujar sobre una imagen JPG.

El componente sustituye la capa de eventos en cada respuesta: no acumula una copia por refresco. El backend sigue siendo responsable de servir una sola versión actual por evento. Se usan nodos DOM con texto para las fichas, evitando interpretar descripciones como HTML.

## 3. Arrancar los cinco servicios

El productor consulta USGS cada 60 segundos (configurable con `USGS_POLL_INTERVAL_SECONDS`) y publica solo eventos nuevos o revisiones más recientes. El consumidor carga el catálogo de 177 países desde `frontend/public/datos/paises.geojson`, asigna país con el índice espacial y confirma el offset Kafka después del commit de PostgreSQL. FastAPI expone eventos, salud y estadísticas en loopback.

Prepara la configuración local si aún no existe y arranca la aplicación completa:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose up --build -d
docker compose ps
```

Consulta los endpoints:

```powershell
curl.exe "http://127.0.0.1:8000/api/eventos"
curl.exe "http://127.0.0.1:8000/api/salud"
curl.exe "http://127.0.0.1:8000/api/estadisticas"
```

El esquema de PostgreSQL se inicializa desde `sql/init.sql`. PostgreSQL y Kafka conservan sus datos en volúmenes; sus puertos no se publican en el equipo anfitrión. El productor y consumidor usan los nombres DNS internos de Compose `kafka:9092` y `postgres:5432`.

Verifica la ingesta desde PostgreSQL:

```powershell
docker compose exec -T postgres psql -U observatorio -d observatorio -c "SELECT COUNT(*) AS paises FROM pais;"
docker compose exec -T postgres psql -U observatorio -d observatorio -c "SELECT mode, COUNT(*) AS eventos_actuales FROM evento_actual GROUP BY mode;"
docker compose exec -T postgres psql -U observatorio -d observatorio -c "SELECT mode, COUNT(*) AS versiones FROM evento_version GROUP BY mode;"
```

La primera consulta debe devolver 177 países. Las dos últimas muestran los terremotos recibidos del feed USGS; puede ser necesario esperar al primer ciclo si el feed o la red no están disponibles al iniciar.

## 4. Descargar y reproducir el histórico 2025

Ejecuta el cargador dentro del contenedor del productor. Consulta el catálogo USGS FDSN por meses, subdivide intervalos cercanos al límite, conserva un manifiesto de consultas y reutiliza los GeoJSON archivados en `datos/historico/2025/`:

```powershell
docker compose exec -T productor python cargar_historico.py
```

Los eventos se publican por Kafka en `mode=replay` y el consumidor aplica las mismas reglas de validación y geografía que al flujo live. Cada carga incluye además casos deterministas de demostración: duplicado, revisión posterior y versión antigua tardía, con fecha de ocurrencia 2026 para mantener limpia la comparativa de 2025. `--accion solo-descargar` archiva sin publicar; `--accion solo-reproducir` publica desde la caché. Los metadatos y el procedimiento se describen en `docs/memoria.md`.

Los filtros `start` y `end` aceptan ISO 8601 con zona horaria; el inicio es inclusivo y el final exclusivo. `min_mag` excluye magnitudes nulas, `country_id=unassigned` selecciona eventos sin país y `mode` aísla `live` de `replay`:

```powershell
curl.exe "http://127.0.0.1:8000/api/eventos?mode=replay&start=2025-01-01T00:00:00Z&end=2026-01-01T00:00:00Z&min_mag=4"
curl.exe "http://127.0.0.1:8000/api/estadisticas?mode=replay&country_id=ESP&min_mag=4"
```

## 5. Publicar la API mediante ngrok

**Opción A: ngrok instalado en el equipo.** La API se publica únicamente en `127.0.0.1:8000`. Configura ngrok en tu cuenta y ejecuta:
```bash
ngrok http 8000
```

**Opción B: ngrok en otro contenedor de la misma red Compose.** El comando del agente apuntará al nombre del servicio:

```bash
ngrok http http://api:8000
```

No uses `localhost:8000` desde el contenedor de ngrok: apuntaría al propio contenedor. Para esta opción no es necesario publicar el puerto de la API en el host. Proporciona el authtoken al agente mediante una variable de entorno o secreto, nunca en el frontend ni en Git.

**HTTPS sin certificados en FastAPI.** La dirección pública es HTTPS y ngrok proporciona y renueva el certificado. La conexión con el agente también está cifrada; el agente puede comunicarse con la API por HTTP dentro del equipo o de la red Docker. No hace falta configurar certificados en Uvicorn ni añadir un proxy TLS para esta práctica. El plan gratuito tiene cuotas de uso.

Cambia `frontend/public/config.json`:

```json
{
  "mode": "api",
  "apiUrl": "https://TU-DOMINIO-ASIGNADO",
  "refreshMs": 60000,
  "skipNgrokWarning": true
}
```

`apiUrl` no termina en `/api`: el componente añade `/api/eventos`. Usa el dominio asignado realmente a tu cuenta. Recarga la página después de cambiar la configuración. Configura CORS en el backend. Una llamada OPTIONS puede preceder al GET por la cabecera de ngrok; no la bloquees.

La consulta se programa después de terminar la anterior, evitando peticiones solapadas. El timeout es de 15 segundos. Si falla, se conserva la capa anterior y se muestra un aviso. Al destruir el componente se cancela la petición y el temporizador. La interfaz inicial muestra cuándo se leyó y generó la respuesta; añade la última ingesta real de USGS para detectar un productor parado.

## 6. Publicar en Netlify

1. Sube el proyecto a tu repositorio sin node_modules ni secretos.
2. Conecta el repositorio a Netlify.
3. Si el repositorio contiene todo el paquete, establece **Base directory: frontend**. Si subes solo frontend como raíz, deja la base vacía.
4. Comando de build: `npm run build`. Carpeta publicada relativa a esa base: `dist/observatorio/browser`.
5. El `netlify.toml` incluido contiene esos valores; `_redirects` permite recargar rutas SPA.
6. Ajusta `.env`: `NETLIFY_ORIGIN` debe ser el origen exacto (`https://<sitio>.netlify.app`, sin ruta); conserva `CORS_ORIGINS=http://localhost:4200`. Reinicia la API con `docker compose up -d --force-recreate api` y verifica la web desde otro dispositivo.

No requiere SSR. Si integras el componente en una aplicación SSR existente, carga Leaflet solo en navegador; el ejemplo entregado es exclusivamente SPA.

`config.json` se copia al build: cuando cambies el dominio del backend, publica una nueva versión o actualiza el archivo servido por tu procedimiento de despliegue. No guardes claves en ese archivo público.

## 7. Lo que debes desarrollar

La fase actual incluye productor, consumidor, almacenamiento, API, ingesta histórica, integración espacial y agregaciones. El mapa se entrega para que el trabajo se centre en los datos; la interfaz Angular ampliada y las pruebas de aceptación siguen pendientes. La clasificación por país que aparece en el popup la entrega el backend; dibujar fronteras no calcula automáticamente esa clasificación.

En `ENUNCIADO.md` tienes las pruebas y la rúbrica. En `FUENTES-Y-LICENCIAS.md`, procedencia y condiciones. En `VALIDACION.md`, las comprobaciones realizadas sobre el material base.

## 8. Cómo documentar la calidad, sin añadir una aplicación de informes

Incluye un apartado en la memoria o un notebook con tus comprobaciones, resultados y decisiones. Puedes ejecutar consultas SQL sobre PostgreSQL o código Python sobre los datos originales y preparados; no necesitas desarrollar un generador de informes.

Ejemplo: muestra la consulta/código que cuenta magnitudes nulas, su resultado y el porcentaje; explica por qué conservas los nulos y cómo los tratas al calcular medias. Haz lo mismo con duplicados, anomalías y las demás dimensiones que analices. Indica el conjunto de datos, periodo y denominador utilizado y compara antes/después cuando corresponda.

El consumidor valida, transforma y persiste datos; además conserva los rechazos y sus motivos. El alumnado prepara el apartado de calidad a partir de esas evidencias. **No es obligatorio crear `generar_informe_calidad.py`, una tabla específica de métricas/incidencias de calidad ni un panel de calidad en Angular.** El registro duradero de cuarentena solicitado en el enunciado sí debe conservarse, mediante una tabla o una solución equivalente documentada.

Generar automáticamente un Markdown/HTML de calidad o mostrar esas métricas en la web son ampliaciones opcionales. No añaden por sí mismas puntos ni son necesarias para el nivel máximo: se evalúan la calidad del análisis y las acciones justificadas.
