# Memoria del observatorio sísmico

**Corte de evidencias:** 6 de noviembre de 2026.
**Fuente principal:** catálogo de eventos USGS FDSN y canal GeoJSON en tiempo real.

## 1. Arquitectura desplegada

El servidor de la aplicación consta de los cinco servicios requeridos. Angular es una aplicación web de una sola página (SPA), que se desarrolla y publica por separado y no constituye un contenedor adicional.

| Servicio | Tecnología y responsabilidad |
|---|---|
| `postgres` | PostgreSQL 16; dimensiones, eventos actuales y versiones, cuarentena y estado de ingesta. El volumen `postgres_data` conserva la base al recrear contenedores. |
| `kafka` | Apache Kafka 3.9.1 en modo KRaft de un nodo; tema `terremotos`, volumen `kafka_data` y desplazamiento de lectura por grupo de consumidores. |
| `productor` | Python; consulta cada 60 s el canal USGS en tiempo real, publica cambios y revisiones, y ofrece la herramienta de línea de comandos que descarga y reproduce el histórico. |
| `consumidor` | Python, GeoPandas y Shapely; valida los mensajes, asigna país con el índice espacial y persiste o pone en cuarentena. |
| `api` | FastAPI; expone `/api/eventos`, `/api/salud` y `/api/estadisticas`. En local solo publica `127.0.0.1:8000`. |
| Panel (fuera de Compose) | Angular y Leaflet; consulta la API o muestra datos de demostración. Netlify publica la aplicación web; ngrok puede proporcionar acceso HTTPS a la API local. |

El recorrido de tiempo real y el de reproducción histórica comparten el tema, el consumidor y las reglas de persistencia. El campo `mode` (`live`/`replay`) separa claves, filas y consultas; la API usa `live` de forma predeterminada. Durante la medición, el grupo de consumidores tenía una partición Kafka y no presentaba retraso de lectura.

## 2. Modelo conceptual, lógico y físico

**Conceptual.** Un evento sísmico puede tener varias revisiones y se relaciona con cero o un país. Los catálogos de país y fecha dan contexto; los mensajes que no cumplen el contrato se conservan aparte para auditoría. El origen, el modo y los tiempos de ocurrencia, revisión e ingesta permanecen diferenciados.

**Lógico.**

- `pais`: identificador `ADM0_A3`, nombre en español, continente y subregión; 177 filas.
- `fecha`: dimensión diaria UTC para análisis calendario.
- `evento_actual`: estado vigente por `(event_id, mode)`, magnitud y profundidad que admiten valores nulos, coordenadas, país opcional y fecha.
- `evento_version`: historial con GeoJSON original; restricción única `(event_id, updated_ms, mode)`.
- `cuarentena`: modo, identificador si existe, motivo y JSON original rechazado.
- `estado_ingesta` y `estado_productor`: marcas de salud y deduplicación del productor.

**Físico.** PostgreSQL 16 impone claves primarias y foráneas, los valores permitidos `live`/`replay` y los límites geográficos WGS84. Los índices B-tree existentes cubren `(event_time, mode)`, magnitud y país. Kafka conserva el mensaje `{source, ingested_at, mode, event}`; las fechas originales de cada entidad GeoJSON se guardan en UTC.

### Idempotencia y confirmación de offsets

El consumidor valida un mensaje, determina el país y abre una transacción PostgreSQL. Inserta la versión con `ON CONFLICT` y actualiza `evento_actual` solo si `updated_ms` es mayor. Únicamente después de que PostgreSQL confirma la transacción, ejecuta `consumer.commit()` en Kafka para confirmar el desplazamiento del mensaje. Si el proceso se interrumpe antes de confirmar la transacción de la base de datos, Kafka vuelve a entregar el mensaje. Si se interrumpe entre la confirmación PostgreSQL y la de Kafka, la clave única de versión y la operación de inserción o actualización impiden duplicar efectos. Una versión antigua puede conservarse en el historial, pero nunca reemplaza el estado actual más reciente.

La prueba real de reinicio dejó el desplazamiento del grupo `observatorio-consumidor` en `19363` antes y después, sin mensajes pendientes (`lag=0`). La cantidad de eventos reproducidos de 2025 y sus versiones permaneció en `18.302` durante el reinicio.

## 3. Procedencia y carga histórica

El archivo `datos/historico/2025/manifest.json` registra la fuente `https://earthquake.usgs.gov/fdsnws/event/1/query`, 12 consultas mensuales, `format=geojson`, `eventtype=earthquake`, `minmagnitude=4` y orden `time-asc`. La extracción registrada en el manifiesto se realizó el 31-10-2026; las consultas se ejecutaron entre `13:24:25Z` y `13:24:33Z`. Se obtuvieron 18.302 entidades GeoJSON, todas quedaron tras la deduplicación y no se registraron duplicados en los límites de los intervalos. La consulta mensual evita exceder el límite de resultados; el cargador subdivide los intervalos que lo alcanzan.

Ejemplo real de consulta del manifiesto para enero (el resto usa ventanas mensuales equivalentes):

```text
https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2025-01-01T00:00:00.000Z&endtime=2025-01-31T23:59:59.999Z&minmagnitude=4&eventtype=earthquake&orderby=time-asc&limit=20000
```

El archivo anual local contiene 18.302 entidades GeoJSON y ocupa 12.989.310 bytes. En PostgreSQL hay 18.302 eventos de reproducción histórica dentro del intervalo `[2025-01-01, 2026-01-01)`. Los archivos de datos almacenados localmente están excluidos de Git; en una copia del repositorio que no los tenga, el cargador vuelve a descargarlos.

La comparación de la API usa el mismo umbral `min_mag=4` y no aplica filtro geográfico. En la respuesta observada, las últimas 24 horas en tiempo real tenían 13 eventos; la reproducción de 2025 tenía 18.302 eventos en 365 días, con un promedio de 50,142 al día (incluidos los días sin eventos). Son periodos y niveles de agregación distintos —un día frente al promedio diario anual— y no deben interpretarse como tasas idénticas.

Reproducción desde la copia archivada:

```powershell
docker compose exec -T productor python cargar_historico.py --accion solo-reproducir
```

La acción predeterminada `todo` descarga y luego publica los eventos; `--accion solo-descargar` únicamente los guarda. La publicación acelerada conserva la hora original y añade metadatos de reproducción. Los casos de demostración de duplicados y revisiones se pueden omitir con `--sin-casos-controlados`. El consumidor reutiliza la misma instancia.

## 4. Calidad de datos y cuarentena

`backend/consumidor.py:validar_evento` verifica que la entrada sea una entidad GeoJSON Feature de tipo terremoto (`properties.type == "earthquake"`), que tenga geometría de punto (`Point`), y que sus coordenadas, fechas y valores numéricos sean válidos y finitos dentro de los límites WGS84. La magnitud y la profundidad pueden ser nulas. Un punto válido que cae fuera de todos los polígonos sigue siendo válido y conserva `country_id = NULL`. Los mensajes rechazados se guardan con su causa y el JSON original; no se inventan ubicaciones.

| Regla | Validación y tratamiento |
|---|---|
| Forma GeoJSON | La entrada debe ser un objeto `Feature` con `properties` y `geometry` válidos; JSON mal formado o valores no estándar (NaN/Infinity) van a cuarentena. |
| Tipo de evento | Solo se acepta `properties.type = "earthquake"`; otros tipos —incluidos `quarry blast` y `explosion`— se rechazan con el motivo registrado. |
| Geometría y coordenadas | Se exige `Point`, longitud y latitud numéricas finitas dentro de WGS84 (`[-180, 180]`, `[-90, 90]`); coordenadas inválidas se guardan en cuarentena junto al GeoJSON original. |
| Identidad y tiempos | `id` debe ser texto no vacío; `time` y `updated` deben ser enteros Unix en milisegundos convertibles a UTC. |
| Magnitud y profundidad | Se admiten valores numéricos finitos o `null`; el consumidor conserva `NULL`, no lo sustituye por cero. |
| Campos descriptivos | `magType` y `place` deben ser texto o `null`; la descripción original se preserva en base de datos. |
| Asignación geográfica | Un punto válido se cruza con 177 polígonos en EPSG:4326; si no está dentro de un país, se mantiene como evento válido con `country_id = NULL`. |
| Versionado | Una versión se identifica por `(event_id, updated_ms, mode)`; solo una revisión con `updated_ms` superior reemplaza el registro actual del mismo modo. |

Consulta reproducible sobre el histórico 2025:

```sql
SELECT COUNT(*) AS total,
       COUNT(*) FILTER (WHERE mag IS NULL) AS magnitud_nula,
       COUNT(*) FILTER (WHERE depth_km IS NULL) AS profundidad_nula,
       COUNT(*) FILTER (WHERE country_id IS NULL) AS sin_pais
FROM evento_actual
WHERE mode = 'replay'
  AND event_time >= '2025-01-01'
  AND event_time <  '2026-01-01';
```

Resultado observado: `18.302` eventos; `0` magnitudes nulas; `0` profundidades nulas; `14.350` sin país y `3.952` asignados. El porcentaje sin país es `78,4 %` del conjunto (denominador: eventos replay del año, no todos los mensajes del canal). Son eventos válidos, no cuarentena; se mantienen en el total, tabla y estadísticas. La fuente histórica ya filtra magnitud mínima 4, por lo que la ausencia de nulos no se extrapola a otras fuentes de datos o umbrales.

La consulta de mensajes recibidos en tiempo real en `cuarentena`, del 6-11-2026, mostró 9 filas: 8 con `properties.type = quarry blast` (explosión de cantera) y 1 con `explosion` (explosión). Todas se rechazaron porque `properties.type` debía ser `"earthquake"` (terremoto). Ejemplos reales de la base:

| `event_id` | Tipo recibido | Motivo guardado | Coordenadas originales |
|---|---|---|---|
| `ok2026vshf` | `quarry blast` (explosión de cantera) | `properties.type debe ser "earthquake"` (se esperaba un terremoto) | `[-96.82033333, 34.38883333, 0]` |
| `uw714132811` | `explosion` (explosión) | `properties.type debe ser "earthquake"` (se esperaba un terremoto) | `[-121.934333333333, 48.0923333333333, -0.31]` |

El conteo es una instantánea del flujo en tiempo real y puede crecer al continuar la ingesta. Las pruebas de aceptación también enviaron coordenadas inválidas y verificaron el motivo, el mensaje original y la ausencia de esos identificadores en la respuesta de la API. Los eventos marítimos se conservaron con el país nulo; `/api/estadisticas` los agrupa como «Sin país asignado» y el panel muestra esa etiqueta en lugar de descartarlos.

Para presentar ubicaciones, el panel traduce al español las expresiones regulares de distancia y orientación de USGS (por ejemplo, `11 km ENE of Coso Junction, CA` se muestra como «11 km al este-noreste de Coso Junction, CA»). También traduce expresiones conocidas como `Izu Islands, Japan region` a «Islas Izu, región de Japón». Se conservan los topónimos y el texto original en la base/API para no alterar la procedencia.

La API no publica un promedio de magnitud: informa por separado los valores nulos en la distribución y `min_mag` los excluye mediante la comparación SQL. El promedio diario de la comparación histórica corresponde al **recuento de eventos**, no a un promedio de magnitudes. La prueba de magnitud nula verifica el total, `null_count` y que no aumente ningún intervalo numérico.

## 5. Rendimiento y complejidad

### Método y entorno

Se procesaron las primeras 100, 1.000 y 10.000 entidades del GeoJSON histórico real, ejecutando la misma función `validar_evento` que utiliza el consumidor. Se midió el tiempo de ejecución para la validación y el cruce espacial. El caso indexado usa `GeoDataFrame.sindex`/STRtree de GeoPandas; como referencia, se sustituyó la consulta por una búsqueda lineal `contains` en los 177 polígonos. La lectura del archivo, las bibliotecas y la creación del índice se realizaron antes de iniciar el cronómetro. Cada medición se ejecutó en un proceso nuevo.

Equipo: Windows 11 Home, AMD Ryzen 7 5800X y 32 GiB de RAM; Docker Desktop reportó 16 CPU y 15,57 GiB para su máquina virtual. El contenedor no tenía un límite de memoria configurado. Entorno de medición: Python 3.11.18, GeoPandas 1.2.0 y Shapely 2.2.0.

`tracemalloc` mide el máximo temporal de memoria asignada por Python durante el bucle, pero no las asignaciones nativas de GEOS. El RSS máximo se leyó de `/proc/self/status` e incluye el intérprete, las bibliotecas, el GeoJSON completo y el catálogo espacial. Por ello, es una medida del proceso completo, no de la memoria adicional utilizada por cada evento.

| Entrada real | Tiempo con índice | Pico Python con índice | Tiempo búsqueda lineal | Pico Python lineal | RSS máximo del proceso (índice / lineal) |
|---:|---:|---:|---:|---:|---:|
| 100 | 0,039 s | 0,020 MiB | 0,064 s | 0,036 MiB | 168,61 / 169,04 MiB |
| 1.000 | 0,393 s | 0,095 MiB | 0,641 s | 0,137 MiB | 169,55 / 168,82 MiB |
| 10.000 | 2,428 s | 0,145 MiB | 4,969 s | 0,137 MiB | 169,68 / 169,57 MiB |

Con 10.000 entradas, el índice fue `2,05×` más rápido que el recorrido completo. La variación del RSS entre procesos es menor que el consumo inicial de carga; el pico de `tracemalloc` de la tabla representa la memoria adicional durante el bucle. Los tiempos no incluyen la escritura en PostgreSQL, la latencia de Kafka ni la red.

### Complejidad, almacenamiento y escalado

La validación de los campos de cada evento es `O(1)`. Con `N` eventos y `C=177` países, la búsqueda lineal requiere `O(N·C)` operaciones. El índice espacial se construye una sola vez (coste esperado `O(C log C)`) y cada consulta STRtree tiene un coste aproximado de `O(log C + k)` para los candidatos y resultados; en el peor caso sigue siendo `O(C)` si se solapan muchas cajas geográficas. El coste total esperado es `O(C log C + N log C)` y, al ser `C` fijo y pequeño, el crecimiento observado respecto de `N` es esencialmente lineal. La memoria del índice es `O(C)`; el consumidor procesa los mensajes a medida que llegan y no necesita cargar todos los eventos en memoria.

En la medición, `evento_actual` ocupaba 6,24 MB incluidos sus índices, y `evento_version`, 21,69 MB; había 19.032 registros actuales y 19.283 versiones. Al extrapolar esa proporción, se estiman aproximadamente **147 GB para 100 millones de eventos y un historial equivalente**, incluidos los índices actuales de ambas tablas, pero sin espacio libre, registros WAL, copias de seguridad, réplicas ni revisiones adicionales. Es una estimación de almacenamiento, no una prueba con 100 millones de eventos. El archivo GeoJSON de entrada ocupa unos 13 MB por cada 18.302 eventos.

Extrapolar el tiempo indexado de 10.000 eventos a 100 millones equivale a unas 6,75 horas de CPU en serie. Es solo una referencia basada en el bucle Python; no representa un acuerdo de nivel de servicio (SLA) ni incluye inserciones o creación de índices. Para ese volumen conviene procesar los datos en lotes o como flujo continuo, aumentar las particiones Kafka para permitir consumidores en paralelo, particionar PostgreSQL por fecha y modo, ajustar los índices tras medir consultas reales y reservar espacio para WAL y copias de seguridad. La instalación validada tiene una sola partición Kafka; aumentar el número de consumidores sin añadir particiones no genera paralelismo.

Los componentes de la instalación local no tienen coste de licencia. No se asigna un importe mensual para la nube porque no se han definido proveedor, región, periodo de retención ni alta disponibilidad. Para 100 millones de eventos, la estimación es de unos 147 GB de almacenamiento principal; dos copias completas elevarían el volumen facturable a unos 441 GB, sin contar WAL, espacio libre ni revisiones adicionales. El coste mensual se estima como `441 GB × tarifa regional de almacenamiento + instancias PostgreSQL/Kafka + cómputo + transferencia + retención`. Sin elegir proveedor y región no es posible dar una cotización monetaria fiable. Si ya se dispone del equipo, el coste local incremental de licencias es cero.

### Mejoras propuestas

- Aumentar las particiones Kafka y conservar la clave `event_id` para distribuir el procesamiento sin perder el orden de cada evento.
- Particionar `evento_actual` y `evento_version` por fecha y modo; medir los planes de consulta y el proceso `autovacuum` con una carga representativa antes de escalar.
- Procesar los archivos históricos por lotes o como flujo continuo y agrupar las inserciones en PostgreSQL; reservar espacio para revisiones, WAL y copias de seguridad.
- Incorporar pruebas periódicas de recuperación y retención, y verificar ngrok/CORS en cada entorno antes de una defensa pública.

## 6. Verificación del sistema

En la validación final, `docker compose config --quiet` terminó correctamente; los cinco servicios estuvieron saludables; `/api/eventos`, `/api/salud` y `/api/estadisticas` devolvieron HTTP 200; el catálogo contenía 177 países. Pasaron 37 pruebas del servidor: de unidad, contrato, flujo de eventos, reproducción histórica y aceptación. También pasaron 2 pruebas de Compose y del esquema. `npm run build` de Angular finalizó correctamente; la interfaz de compilación de Angular solo mostró el aviso conocido de que Leaflet usa CommonJS.

| # | Prueba de aceptación | Resultado observado |
|---:|---|---|
| 1 | Ingesta de un evento nuevo | Publicado por Kafka, persistido en PostgreSQL, consultable por API y visible en el panel; Francia asignada. |
| 2 | Reenvío de la misma versión | El mensaje repetido no creó una segunda versión. |
| 3 | Recepción de una revisión posterior | `evento_actual` refleja el mayor `updated_ms`; la versión previa queda en el historial. |
| 4 | Recepción tardía de una versión antigua | La versión se conserva en historial, sin hacer retroceder el estado vigente. |
| 5 | Magnitud nula | `mag: null` se conserva y no satisface el filtro `min_mag`; el panel presenta el valor ausente como `ND`. |
| 6 | Coordenadas inválidas y tipo no sísmico | Longitud 181 y un `quarry blast` se rechazaron; la cuarentena conserva motivo y mensaje original. |
| 7 | Evento marítimo | Punto válido sin país conservado con `country_id: null`, visible en estadísticas como «Sin país asignado». |
| 8 | Reinicio del consumidor | Offset 19363 antes y después, lag cero y recuento replay de 18.302 eventos/versiones estable. |
| 9 | Fallo de la conexión del panel | Se conservaron las 180 filas válidas y se mostró el error y antigüedad de lectura. Se interceptaron peticiones localmente; no se probó un túnel ngrok real. |
| 10 | Recuperación de la conexión | Al retirar la interceptación, desapareció la alerta y el panel actualizó los datos. No demuestra TLS/CORS de ngrok. |
| 11 | Aislamiento del modo replay | Un identificador sintético quedó en `mode=replay`, no en `mode=live`; duplicado y revisión procesados idempotentemente. |

Validación transversal: los tres endpoints devolvieron HTTP 200, el catálogo contiene 177 países, pasaron 37 pruebas del servidor y 2 de infraestructura, y Angular compiló correctamente.

El detalle de cada uno de los once escenarios, comandos y límites de la prueba de túnel está en [evidencias/aceptacion.md](./evidencias/aceptacion.md).

## 7. Conclusiones técnicas

1. **El índice espacial reduce el coste de las consultas reales.** En 10.000 entidades de 2025 redujo el tiempo de 4,969 s a 2,428 s (2,05×) y evita la búsqueda lineal, que requeriría 1,77 millones de comprobaciones entre puntos y polígonos. Para 100 millones, el índice debe seguir formando parte del consumidor; aun así, la medición en serie proyecta varias horas de CPU y justifica el procesamiento en paralelo y las inserciones por lotes.
2. **La transacción previa a la confirmación del desplazamiento de Kafka resiste reenvíos y reinicios.** Tras reiniciar el consumidor, el desplazamiento permaneció en 19363 y no hubo mensajes pendientes; los registros y versiones de la reproducción de 2025 permanecieron en 18.302. Las restricciones únicas evitan duplicar efectos si el mensaje se vuelve a entregar entre ambas confirmaciones.
3. **La asignación geográfica no debe utilizarse como criterio de validez.** 14.350 de 18.302 eventos de 2025 (78,4 %) no tienen país asignado, pero siguen siendo válidos y analizables; el sistema los conserva y los muestra como «Sin país asignado». En los datos recibidos en tiempo real, los casos de explosión de cantera (`quarry blast`) y explosión (`explosion`) se envían a cuarentena según su tipo, conservando el mensaje original y la causa.

## 8. Límites conocidos

El histórico de 2025 representa el catálogo USGS con `minmagnitude=4`, no los sismos de menor magnitud. La etiqueta de país depende de la cobertura y los límites del GeoJSON proporcionado; no tener país asignado no significa necesariamente que el evento sea inválido. La prueba de rendimiento abarca la validación y la operación punto en polígono (PIP), pero no la carga en PostgreSQL, las solicitudes simultáneas, el consumo Kafka a gran escala ni la disponibilidad en varios nodos. La publicación mediante ngrok y Netlify depende de credenciales, cuotas y tarifas externas que no forman parte de la medición local.
