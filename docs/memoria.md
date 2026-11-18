# Memoria del observatorio sísmico

**Corte de evidencias:** 18 de noviembre de 2026 (despliegue validado y captura live entre las 09:03:45 y las 09:06:45 UTC; se conservan también instantáneas SQL anteriores identificadas por fecha).
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

### Despliegue y funcionamiento

El entregable web oficial del panel Angular está publicado en Netlify: [https://proyectoobservatoriosismico.netlify.app](https://proyectoobservatoriosismico.netlify.app). Se compila desde `frontend/` con `npm run build` y se publica el directorio `frontend/dist/observatorio/browser`, que incluye la configuración JSON y las redirecciones SPA.

Para el modo API, `frontend/public/config.json` usa `apiBaseUrl=https://vertebrae-giggling-hardwired.ngrok-free.dev` sin añadir `/api`. El túnel HTTPS de ngrok reenvía al servicio local mediante `ngrok http 8000`; por tanto, la API y el túnel deben permanecer activos para servir datos en vivo. CORS permite el origen del panel y la cabecera `ngrok-skip-browser-warning`. Al cambiar la configuración del frontend se debe volver a compilar y publicar en Netlify.

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
- `consulta_usgs`: una fila por sondeo live, con periodo de petición, estado HTTP, bytes, latencia, cantidad de features, metadata de USGS y errores; retención de 30 días.

**Físico.** PostgreSQL 16 impone claves primarias y foráneas, los valores permitidos `live`/`replay` y los límites geográficos WGS84. Los índices B-tree existentes cubren `(event_time, mode)`, magnitud y país; una clave primaria compuesta optimiza la búsqueda del checkpoint por `(event_id, mode)`. `consulta_usgs` se indexa por fecha de petición y retiene 30 días. Kafka conserva el mensaje `{source, ingested_at, mode, event}`; las fechas originales de cada entidad GeoJSON se guardan en UTC.

### Idempotencia y confirmación de offsets

El consumidor valida un mensaje, determina el país y abre una transacción PostgreSQL. Inserta la versión con `ON CONFLICT` y actualiza `evento_actual` solo si `updated_ms` es mayor. Únicamente después de que PostgreSQL confirma la transacción, ejecuta `consumer.commit()` en Kafka para confirmar el desplazamiento del mensaje. Si el proceso se interrumpe antes de confirmar la transacción de la base de datos, Kafka vuelve a entregar el mensaje. Si se interrumpe entre la confirmación PostgreSQL y la de Kafka, la clave única de versión y la operación de inserción o actualización impiden duplicar efectos. Una versión antigua puede conservarse en el historial, pero nunca reemplaza el estado actual más reciente.

La prueba real de reinicio dejó el desplazamiento del grupo `observatorio-consumidor` en `19363` antes y después, sin mensajes pendientes (`lag=0`). La cantidad de eventos reproducidos de 2025 y sus versiones permaneció en `18.302` durante el reinicio.

La instantánea SQL de solo lectura del 14-11-2026 registró **1.770 eventos live únicos**, **2.542 versiones** y **772 versiones adicionales** respecto del primer registro por evento. La restricción `(event_id, updated_ms, mode)` no tenía colisiones: las 2.542 filas correspondían a 2.542 claves distintas. Las 1.770 proyecciones actuales coincidían con el `MAX(updated_ms)` de su historial. Por ejemplo, `ci41361583` pasó de `updated_ms=1794536080876, mag=2.39` a `1794606734892, mag=2.53`; `evento_actual` conservó el segundo valor. Esta comprobación de unicidad y de correspondencia entre historial y estado vigente es evidencia de idempotencia del UPSERT, no una prueba de entrega exactamente una vez. El detalle de consultas y resultados está en [evidencias_24h.md](./evidencias_24h.md).

## 3. Procedencia y carga histórica

El archivo `datos/historico/2025/manifest.json` registra la fuente `https://earthquake.usgs.gov/fdsnws/event/1/query`, 12 consultas mensuales, `format=geojson`, `eventtype=earthquake`, `minmagnitude=4` y orden `time-asc`. La extracción registrada en el manifiesto se realizó el 31-10-2026; las consultas se ejecutaron entre `13:24:25Z` y `13:24:33Z`. Se obtuvieron 18.302 entidades GeoJSON, todas quedaron tras la deduplicación y no se registraron duplicados en los límites de los intervalos. La consulta mensual evita exceder el límite de resultados; el cargador subdivide los intervalos que lo alcanzan.

Ejemplo real de consulta del manifiesto para enero (el resto usa ventanas mensuales equivalentes):

```text
https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2025-01-01T00:00:00.000Z&endtime=2025-01-31T23:59:59.999Z&minmagnitude=4&eventtype=earthquake&orderby=time-asc&limit=20000
```

El archivo anual local contiene 18.302 entidades GeoJSON y ocupa 12.989.310 bytes. En PostgreSQL hay 18.302 eventos de reproducción histórica dentro del intervalo `[2025-01-01, 2026-01-01)`. Los archivos de datos almacenados localmente están excluidos de Git; en una copia del repositorio que no los tenga, el cargador vuelve a descargarlos.

La comparación de la API usa el mismo umbral `min_mag=4` y no aplica filtro geográfico. En la respuesta observada el 6-11-2026, las últimas 24 horas en tiempo real tenían 13 eventos; la reproducción de 2025 tenía 18.302 eventos en 365 días, con un promedio de 50,142 al día (incluidos los días sin eventos). Son periodos y niveles de agregación distintos —un día frente al promedio diario anual— y no deben interpretarse como tasas idénticas.

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
| Asignación geográfica | Un punto válido se cruza con 177 polígonos en EPSG:4326; si no está dentro de un país, se mantiene como evento válido con `country_id = NULL`. `within` excluye los puntos exactamente sobre una frontera; si polígonos se solapan y hay varias coincidencias interiores, se elige determinísticamente el `ADM0_A3` lexicográficamente menor. |
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

La instantánea live de `cuarentena` del 14-11-2026 mostró **110 rechazos**, todos con el motivo `properties.type debe ser "earthquake"`. El desglose completo por `properties.type` fue: 75 `other event`, 20 `quarry blast`, 12 `explosion`, 2 `landslide` y 1 `ice quake`. Son tipos no sísmicos según la regla del consumidor; el JSON original y el motivo se conservan. Por ejemplo, la captura incluyó eventos `quarry blast` además de los registros recientes `other event` de Alaska. La tabla de ejemplos, identificadores y coordenadas se presenta en [evidencias_24h.md](./evidencias_24h.md).

El conteo es una instantánea acumulada de la tabla y puede crecer al continuar la ingesta. Las pruebas de aceptación también enviaron coordenadas inválidas y verificaron el motivo, el mensaje original y la ausencia de esos identificadores en la respuesta de la API. Los eventos marítimos se conservaron con el país nulo; `/api/estadisticas` los agrupa como «Sin país asignado» y el panel muestra esa etiqueta en lugar de descartarlos.

La cobertura de ocurrencia live va de `2026-10-30 13:11:46.470 UTC` a `2026-11-14 09:26:13.073 UTC` (**356,24 horas**, con actividad en 195 horas UTC distintas). Las versiones live tienen `ingested_at` desde `2026-10-31 13:11:16 UTC` hasta `2026-11-14 09:50:38 UTC` (**332,66 horas**). Estos rangos prueban persistencia acumulada durante bastante más de 24 horas, pero no prueban continuidad del proceso: el mayor intervalo entre filas versionadas fue **4 días, 13 horas y 52 minutos**, con tres intervalos superiores a 24 horas. Esos huecos pueden representar ausencia de revisiones recibidas o interrupciones; el esquema no guarda un registro histórico de cada sondeo exitoso. Por tanto, no se afirma una captura ininterrumpida a partir de estos datos. En el momento de consultar, `estado_productor` contenía **1.828 checkpoints live**.

Para presentar ubicaciones, el panel traduce al español las expresiones regulares de distancia y orientación de USGS (por ejemplo, `11 km ENE of Coso Junction, CA` se muestra como «11 km al este-noreste de Coso Junction, CA»). También traduce expresiones conocidas como `Izu Islands, Japan region` a «Islas Izu, región de Japón». Se conservan los topónimos y el texto original en la base/API para no alterar la procedencia.

La API no publica un promedio de magnitud: informa por separado los valores nulos en la distribución y `min_mag` los excluye mediante la comparación SQL. El promedio diario de la comparación histórica corresponde al **recuento de eventos**, no a un promedio de magnitudes. La prueba de magnitud nula verifica el total, `null_count` y que no aumente ningún intervalo numérico.

### Calidad observada en una consulta real del feed

El 18-11-2026 a las `08:52:32Z` se midió una respuesta real del endpoint live: HTTP 200, 152.724 bytes y 85,42 ms de latencia observada para recibir el cuerpo. Es una sola observación, no una latencia media ni un SLA. USGS devolvió 214 features; 211 tenían `properties.type="earthquake"` y tres se excluyeron (dos `quarry blast`, una `explosion`). En las 214 features, `time`, `updated`, `mag`, `magType`, `type` y `place` eran no nulos, la geometría era `Point` y longitud/latitud estaban dentro de WGS84. El contrato interpreta `time` y `updated` como milisegundos Unix, magnitud como valor numérico opcional y las coordenadas como longitud, latitud y profundidad en km.

La respuesta procesada contenía 211 terremotos live dentro de las 24 horas consultadas. En la captura posterior del panel, a las `09:06:05Z`, la API devolvió 212 eventos dentro de sus últimas 24 horas predeterminadas, de los que 32 tenían magnitud `>=4`. A las `09:06:45Z`, PostgreSQL contenía 2.487 eventos live actuales; 37 magnitudes negativas y 36 profundidades negativas se conservaron, sin declararlas inválidas por su signo, y 433 eventos no tenían país asignado. En el histórico equivalente de 2025, con magnitud `>=4`, había 18.302 eventos; ninguno tenía magnitud o profundidad nula y 14.350 (78,4 %) carecían de país.

**Unicidad y consistencia.** La instantánea live contenía 3.637 versiones para 2.485 identificadores, con cero claves duplicadas `(event_id, updated_ms, mode)`. Los 2.485 estados actuales coincidían con la revisión de mayor `updated_ms`. En el original de esas versiones, `time`, `updated` y `place` eran no nulos en 3.637/3.637 casos, mientras que `mag` y `magType` lo eran en 3.636/3.637; los valores opcionales nulos se conservaron. La actualidad se midió por separado del tiempo de respuesta: entre ocurrencia e ingesta, la mediana fue 11.485 s (3 h 11 min), P95 76.801 s (21 h 20 min) y máximo 86.373 s (23 h 59 min). Esto es retraso evento–ingesta observado en versiones, no latencia HTTP de USGS.

**Acciones y limitaciones.** Los tipos que no son `earthquake` se guardan en cuarentena con motivo y GeoJSON original; magnitud/profundidad nulas no se convierten en cero; valores negativos se conservan; y un evento sin asignación espacial permanece analizable como «Sin país asignado». La muestra de feed valida completitud solo para esa respuesta y no demuestra completitud general de USGS. La exactitud absoluta no se puede certificar sin una fuente sísmica independiente; la asignación territorial depende del catálogo Natural Earth simplificado. Las consultas live registran estado HTTP, bytes, latencia, metadata y conteos en `consulta_usgs`, retenidos 30 días; el GeoJSON original se conserva por versión o en cuarentena, no como una copia completa repetida cada minuto. El SQL reproducible y las salidas de esta captura están en [evidencias_24h.md](./evidencias_24h.md).

## 5. Rendimiento y complejidad

### Método y entorno

Se procesaron las primeras 100, 1.000 y 10.000 entidades del GeoJSON histórico real, ejecutando la misma función `validar_evento` que utiliza el consumidor. Se midió el tiempo de ejecución para la validación y el cruce espacial. El caso indexado usa `GeoDataFrame.sindex`/STRtree de GeoPandas; como referencia, se sustituyó la consulta por una búsqueda lineal `contains` en los 177 polígonos. La lectura del archivo, las bibliotecas y la creación del índice se realizaron antes de iniciar el cronómetro. Cada medición se ejecutó en un proceso nuevo.

Las mediciones de 100/1.000/10.000 entradas de la tabla siguiente ya estaban registradas con entidades reales del archivo histórico; no se repitieron durante la consulta de evidencia live para no confundir una prueba de rendimiento con la captura persistida. Las consultas y los resultados de esta instantánea PostgreSQL están en [evidencias_24h.md](./evidencias_24h.md).

Como contexto de volumen —no como benchmark de throughput—, la instantánea live acumulaba 1.770 filas en `evento_actual`, 2.542 en `evento_version` (772 versiones adicionales), 110 rechazos en `cuarentena` y 1.828 checkpoints en `estado_productor`. La recepción de versiones cubre 332,66 horas y contiene intervalos sin nuevas versiones de hasta 4 d 13 h 52 min; no se usa ese rango para inferir eventos por hora ni continuidad del sondeo.

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

La búsqueda del checkpoint de un evento usa la clave B-tree `(event_id, mode)` de `estado_productor`: coste esperado y peor caso `O(log P)` para `P` checkpoints. La inserción de cada revisión usa la restricción única `(event_id, updated_ms, mode)`, también `O(log V)` en `V` versiones, y resuelve reenvíos sin una búsqueda secuencial. `EXPLAIN` sobre la base activa confirmó `Index Scan using estado_productor_pkey`.

La agregación por hora primero limita los hechos al intervalo con `(event_time, mode)` y luego agrupa por `date_trunc` UTC. Para `R` filas del intervalo y `G` horas con actividad, el coste esperado es `O(log E + R + G)` con búsqueda indexada y agregación hash; si PostgreSQL necesita ordenar, el término de agrupación pasa a `O(R log R)` y la memoria del agregado depende de `G`. El plan real para la ventana live de 24 horas utilizó `Index Only Scan using idx_evento_actual_event_time_mode`, seguido de ordenación y `GroupAggregate`. La interfaz completa las horas vacías y limita la gráfica a las 168 ventanas más recientes.

En la medición, `evento_actual` ocupaba 6,24 MB incluidos sus índices, y `evento_version`, 21,69 MB; había 19.032 registros actuales y 19.283 versiones. Al extrapolar esa proporción, se estiman aproximadamente **147 GB para 100 millones de eventos y un historial equivalente**, incluidos los índices actuales de ambas tablas, pero sin espacio libre, registros WAL, copias de seguridad, réplicas ni revisiones adicionales. Es una estimación de almacenamiento, no una prueba con 100 millones de eventos. El archivo GeoJSON de entrada ocupa unos 13 MB por cada 18.302 eventos.

Extrapolar el tiempo indexado de 10.000 eventos a 100 millones equivale a unas 6,75 horas de CPU en serie. Es solo una referencia basada en el bucle Python; no representa un acuerdo de nivel de servicio (SLA) ni incluye inserciones o creación de índices. Para ese volumen conviene procesar los datos en lotes o como flujo continuo, aumentar las particiones Kafka para permitir consumidores en paralelo, particionar PostgreSQL por fecha y modo, ajustar los índices tras medir consultas reales y reservar espacio para WAL y copias de seguridad. La instalación validada tiene una sola partición Kafka; aumentar el número de consumidores sin añadir particiones no genera paralelismo.

Los componentes locales no tienen coste de licencia. Para hacer explícitos los costes omitidos, esta tabla usa un escenario de planificación y no una factura observada:

| Partida | Supuesto reproducible | Estimación |
|---|---|---:|
| Hardware | Equipo existente; valor de reposición supuesto de 1.000 EUR, amortizado en 60 meses | 16,67 EUR/mes imputados; desembolso real desconocido |
| Electricidad, demostración | 100 W medios, 3 h/día, 30 días y 0,20 EUR/kWh | 1,80 EUR/mes |
| Electricidad, 24/7 | Mismos 100 W y 0,20 EUR/kWh, 720 h/mes | 14,40 EUR/mes |
| Almacenamiento local | Base PostgreSQL observada: 38 MB; GeoJSON anual: 12,99 MB; datos en disco existente | Sin coste incremental medido |
| Transferencia USGS | Respuesta de muestra de 152.724 bytes, un sondeo/minuto durante 30 días | 6,60 GB/mes de entrada estimada; varía con el feed |
| Mantenimiento | 2 h/mes imputadas a 15 EUR/h | 30 EUR/mes |
| Desarrollo | Estimación de planificación de 40–80 h a 15 EUR/h; no hay partes horarios | 600–1.200 EUR una vez |

Con esos supuestos, el coste local imputado sería aproximadamente **48,47 EUR/mes** para una demostración de 3 h/día o **61,07 EUR/mes** si el equipo permanece encendido continuamente, más el coste estimado de desarrollo. No incluye tarifas de Internet, sustitución efectiva del equipo, impuestos ni cuotas de terceros; Netlify/ngrok dependen de sus planes y límites vigentes. Los valores de 100 W, 1.000 EUR y 15 EUR/h son hipótesis, no mediciones ni precios declarados por la alumna; deben sustituirse si se dispone de lecturas o costes reales.

No se asigna un importe cloud mensual porque no se han elegido proveedor, región ni retención. Para 100 millones de eventos, la extrapolación existente es de unos 147 GB primarios; dos copias completas elevarían el volumen a unos 441 GB, antes de WAL, espacio libre y revisiones. El coste cotizable sería `441 GB × tarifa regional de almacenamiento + instancias PostgreSQL/Kafka + cómputo + transferencia + retención`. Si se dispone del equipo, la licencia local sigue costando cero; eso no equivale a coste total cero.

### Mejoras propuestas

- Aumentar las particiones Kafka y conservar la clave `event_id` para distribuir el procesamiento sin perder el orden de cada evento.
- Particionar `evento_actual` y `evento_version` por fecha y modo; medir los planes de consulta y el proceso `autovacuum` con una carga representativa antes de escalar.
- Procesar los archivos históricos por lotes o como flujo continuo y agrupar las inserciones en PostgreSQL; reservar espacio para revisiones, WAL y copias de seguridad.
- Incorporar pruebas periódicas de recuperación y retención, y verificar ngrok/CORS en cada entorno antes de una defensa pública.

## 6. Verificación del sistema

En la validación del 18-11-2026, `docker compose config --quiet` terminó correctamente y los cinco servicios estuvieron saludables; `/api/salud` devolvió HTTP 200 y reflejó una ingesta reciente. Pasaron 42 pruebas de unidad, contrato, flujo, replay y aceptación. `npm run build` de Angular finalizó correctamente; Angular mostró el aviso conocido de que Leaflet usa CommonJS. La tabla de auditoría registró cuatro sondeos USGS consecutivos correctos tras desplegar la migración.

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

Validación transversal: pasaron 42 pruebas backend y el build Angular; el catálogo contiene 177 países. La compilación de imágenes Docker y la configuración Compose fueron correctas. El acceso desde `http://localhost:4200` recibió el encabezado CORS permitido y un origen Netlify preview no configurado no recibió `Access-Control-Allow-Origin`.

Como evidencia complementaria del 14-11-2026, la base consultada directamente tenía 1.770 eventos live, 2.542 versiones, cero colisiones de clave de versión, 110 filas en cuarentena y 1.828 checkpoints live. En esta ejecución solo estaba levantado PostgreSQL; no se reiniciaron Kafka, productor, consumidor ni API y no se repitieron las pruebas HTTP. Los resultados detallados y la limitación sobre continuidad están en [evidencias_24h.md](./evidencias_24h.md).

La verificación del 18-11-2026 desplegó la tabla `consulta_usgs` sobre el volumen existente y recreó API, productor y consumidor. La auditoría registró cuatro respuestas HTTP 200 con 214–215 features cada una. Se guardaron capturas del panel live y de replay 2025 en [panel live](./evidencias/panel-live-2026-11-18.png) y [panel replay](./evidencias/panel-replay-2025-2026-11-18.png). La prueba de CORS se hizo contra el API local; no se volvió a probar un túnel ngrok real.

El detalle de cada uno de los once escenarios, comandos y límites de la prueba de túnel está en [evidencias/aceptacion.md](./evidencias/aceptacion.md).

## 7. Conclusiones del análisis

Las cifras siguientes usan el histórico mundial USGS de 2025 con `eventtype=earthquake` y `minmagnitude=4`, salvo la primera, que compara la ventana live indicada con el promedio diario de ese mismo histórico. La fecha de corte de la comparación live fue 18-11-2026; no se aplica filtro geográfico.

1. **La actividad live de magnitud `>=4` fue menor que la media diaria histórica en esa ventana.** Entre `2026-11-17 08:53:37Z` y `2026-11-18 08:53:37Z` se observaron 32 eventos, frente a una media de 50,142 por día en 2025 (18.302/365), un 36,18 % menos. El año incluye los días sin eventos. La ventana reciente puede estar incompleta por retrasos de publicación; esta diferencia no es una predicción ni una medida de riesgo.
2. **La muestra histórica está concentrada en la parte baja del umbral elegido.** 16.173 de 18.302 eventos (88,37 %) tuvieron magnitud en `[4, 5)`: 9.743 en `[4, 4.5)` y 6.430 en `[4.5, 5)`. Es una distribución condicionada a `M>=4`; no describe los terremotos de menor magnitud que el filtro excluye.
3. **La mayoría de las profundidades registradas en el histórico se sitúan en el primer intervalo.** 12.455 de 18.302 eventos (68,05 %) aparecen en `[0, 50)` km. La cifra describe profundidades reportadas por USGS en el conjunto filtrado y no implica por sí sola mayor peligrosidad.
4. **La asignación por país tiene una limitación de cobertura importante.** Solo 3.952 de 18.302 eventos de 2025 recibieron país; 14.350 (78,4 %) quedaron como «Sin país asignado». Entre los asignados, China tuvo 354, Chile 348 e Indonesia 306. Por el alto porcentaje sin asignación y la simplificación de Natural Earth, el ranking no debe interpretarse como distribución completa por país.

Como comprobación de la integración y el rendimiento, el índice espacial procesó 10.000 eventos reales 2,05 veces más rápido que el recorrido lineal (2,428 s frente a 4,969 s). La medición no incluye red, Kafka ni escrituras PostgreSQL, y la extrapolación a 100 millones no es una prueba de escala.

## 8. Límites conocidos

El histórico de 2025 representa el catálogo USGS con `minmagnitude=4`, no los sismos de menor magnitud. La etiqueta de país depende de la cobertura y los límites del GeoJSON proporcionado; no tener país asignado no significa necesariamente que el evento sea inválido. La prueba de rendimiento abarca la validación y la operación punto en polígono (PIP), pero no la carga en PostgreSQL, las solicitudes simultáneas, el consumo Kafka a gran escala ni la disponibilidad en varios nodos. La publicación mediante ngrok y Netlify depende de credenciales, cuotas y tarifas externas que no forman parte de la medición local.
