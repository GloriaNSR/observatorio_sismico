# Tareas: Observatorio de Actividad Sísmica

**Entrada**: Documentos de diseño de `/specs/001-observatorio-sismico/`

**Prerrequisitos**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `contracts/` y `quickstart.md`.

**Organización**: Siete fases incrementales siguiendo el itinerario de `FAQ-ALUMNADO.md`. Las tareas funcionales llevan etiquetas US1–US5 para mantener la trazabilidad con las historias de `spec.md`.

**Pruebas**: Se incluyen pruebas porque el usuario solicitó ejecutar las once pruebas de aceptación del apartado 11 de `ENUNCIADO.md`. También se añaden pruebas automatizadas específicas para productor, consumidor, esquema y API cuando verifican esos flujos.

**Rutas**: La fase 1 consolida la aplicación Angular y los documentos base en la raíz (`frontend/`, `README.md`, `ENUNCIADO.md`, `FAQ-ALUMNADO.md`, `CONTRATO-API.md`, `FUENTES-Y-LICENCIAS.md` y `VALIDACION.md`). Las demás rutas de tareas usan esa estructura raíz.

## Formato

Cada tarea usa el formato obligatorio `- [ ] Tnnn [P?] [USn?] descripción y ruta`. `[P]` identifica trabajo que puede hacerse en paralelo sin editar los mismos archivos ni depender de tareas pendientes; US1–US5 corresponden a las historias de usuario de `spec.md`.

## Fase 1: Setup y estructura

**Objetivo**: Dejar una raíz de proyecto coherente, preservar el frontend suministrado y preparar una imagen Python compartida sin secretos ni datos voluminosos versionados.

- [X] T001 Consolidar la aplicación Angular suministrada como única copia mantenida en `frontend/`, migrando el contenido de `observatorio-sismico-materiales/observatorio-sismico/frontend/` y actualizando las rutas de compilación y recursos en `frontend/angular.json` sin modificar `frontend/public/datos/paises.geojson`.
- [X] T002 [P] Integrar y promover a la raíz los documentos base `README.md`, `ENUNCIADO.md`, `FAQ-ALUMNADO.md`, `CONTRATO-API.md`, `FUENTES-Y-LICENCIAS.md` y `VALIDACION.md`, reconciliando `FAQ-observatorio-sismico .md` y `Guia-entregables-observatorio-sismico.md` sin perder contenido útil.
- [X] T003 [P] Crear `.env.example` en la raíz con nombres, valores no secretos y comentarios para configuración de PostgreSQL, Kafka, USGS, intervalo de consulta, CORS, Netlify, ngrok y reproducción.
- [X] T004 [P] Crear o actualizar `.gitignore` en la raíz para excluir `.env`, `node_modules/`, datos históricos voluminosos descargados, cachés, artefactos de build, logs y volúmenes/datos locales de PostgreSQL y Kafka, preservando los archivos fuente y los GeoJSON suministrados.
- [X] T005 Crear `backend/requirements.txt` con las dependencias de ejecución acordadas: `fastapi`, `uvicorn`, `aiokafka`, `psycopg2-binary`, `requests`, `geopandas` y `shapely`.
- [X] T006 Crear `backend/Dockerfile` basado en `python:3.11-slim`, instalar las dependencias de sistema necesarias para GeoPandas/Shapely y psycopg2, instalar `backend/requirements.txt` y definir una imagen común para los comandos de productor, consumidor y API; crear `backend/requirements-test.txt` con pytest y las dependencias de ejecución para las pruebas.

**Criterio de salida**: Hay una sola aplicación Angular ejecutable desde `frontend/`, los documentos base están en la raíz, el GeoJSON de países permanece intacto y el repositorio no incluye secretos ni datos/volúmenes locales.

## Fase 2: Base de datos y Docker Compose base

**Objetivo**: Crear el esquema durable e iniciar PostgreSQL y Kafka con controles de salud y persistencia antes de implementar el flujo de eventos.

- [X] T007 Crear `sql/init.sql` con todas las columnas y tipos de `data-model.md`: `pais(country_id VARCHAR(10), country_name TEXT, continent TEXT, subregion TEXT)`; `fecha(fecha_id DATE, anio INTEGER, mes INTEGER, dia INTEGER, dia_semana INTEGER)`; `evento_actual(event_id TEXT, mode VARCHAR(10), time_ms BIGINT, event_time TIMESTAMPTZ, updated_ms BIGINT, updated_time TIMESTAMPTZ, mag DOUBLE PRECISION NULL, mag_type TEXT NULL, place TEXT NULL, longitude DOUBLE PRECISION, latitude DOUBLE PRECISION, depth_km DOUBLE PRECISION NULL, country_id VARCHAR(10) NULL, fecha_id DATE)`; `evento_version(id SERIAL, event_id TEXT, updated_ms BIGINT, mode VARCHAR(10), ingested_at TIMESTAMPTZ, raw_json JSONB)`; `cuarentena(id SERIAL, event_id TEXT NULL, mode VARCHAR(10), motivo TEXT, ingested_at TIMESTAMPTZ, raw_json JSONB)`; `estado_ingesta(source VARCHAR(50), last_ingestion_ms BIGINT, updated_at TIMESTAMPTZ)`; `estado_productor(event_id TEXT, mode VARCHAR(10), last_updated_ms BIGINT)`. Aplicar `pais.country_id` como PK (`ADM0_A3`), `fecha.fecha_id` como PK, PK compuesta de evento actual `(event_id, mode)`, UNIQUE de versión `(event_id, updated_ms, mode)`, PK de estado productor `(event_id, mode)`, FK nullable de `evento_actual.country_id` a `pais.country_id` y FK de `fecha_id` a `fecha.fecha_id`.
- [X] T008 Añadir a `sql/init.sql` los índices requeridos `evento_actual(event_time, mode)`, `evento_actual(mag)` y `evento_actual(country_id)`, las restricciones que limiten `mode` a `live`/`replay` y las restricciones de rango de longitud/latitud WGS84; mantener `mag` y `depth_km` nullable y admitir sus valores negativos.
- [X] T009 Crear `compose.yaml` en la raíz con los servicios `postgres` (imagen PostgreSQL 16) y `kafka` (Apache Kafka en modo KRaft, sin Zookeeper), incluyendo nombres DNS internos de servicio y configuración de listeners accesible desde los demás contenedores.
- [X] T010 Configurar en `compose.yaml` los healthchecks de PostgreSQL/Kafka, dependencia de salud cuando corresponda, política de reinicio, volumen persistente de PostgreSQL, almacenamiento persistente de Kafka y retención limitada del topic; montar `sql/init.sql` en `/docker-entrypoint-initdb.d/` y no publicar puertos de PostgreSQL o Kafka al host.
- [X] T011 Añadir una comprobación reproducible del esquema y del Compose en `backend/tests/integration/test_infraestructura.py` que verifique las siete tablas, PK/FK/UNIQUE e índices esperados, KRaft sin Zookeeper y la persistencia definida; documentar el comando para ejecutarla en `README.md`.

**Criterio de salida**: `docker compose config` es válido, PostgreSQL y Kafka llegan a estado saludable, el volumen conserva datos y el esquema inicial se crea automáticamente en una base nueva.

## Fase 3: Pipeline core — productor y consumidor

**Objetivo**: Completar un evento de prueba de extremo a extremo desde USGS/Kafka hasta PostgreSQL, garantizando validación, trazabilidad e idempotencia.

- [X] T012 [P] [US3] Implementar en `backend/productor.py` la consulta periódica configurable del feed `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson`, con timeout, validación de respuesta JSON y reintentos ante errores HTTP/red sin borrar el último estado correcto.
- [X] T013 [US3] Añadir en `backend/productor.py` la comparación persistente de `(id, updated)` con `estado_productor`, publicación solo de eventos nuevos o con `updated` estrictamente mayor, clave Kafka igual a `id`, y sobre de mensaje con `source`, `ingested_at`, `mode='live'` y el Feature GeoJSON original; actualizar el checkpoint únicamente tras ACK de Kafka.
- [X] T014 [P] [US3] Crear `backend/tests/unit/test_productor.py` para verificar eventos nuevos, repetidos, revisiones posteriores, eventos antiguos fuera de orden y que un fallo/ausencia de ACK no avance `estado_productor`.
- [X] T015 [P] [US3] Implementar en `backend/consumidor.py` la carga idempotente del catálogo `frontend/public/datos/paises.geojson` a `pais`, comprobando que contiene las 177 entidades y 177 `ADM0_A3` distintos antes de aceptar eventos; resolver el nombre mediante `NAME_ES` con alternativa `ADMIN` y conservar `CONTINENT` y `SUBREGION`.
- [X] T016 [US3] Implementar en `backend/consumidor.py` la validación del tipo `earthquake`, geometría `Point` y coordenadas WGS84 dentro de longitud `[-180, 180]` y latitud `[-90, 90]`; guardar en `cuarentena` el motivo, modo, fecha de ingesta y JSON original ante cada rechazo, sin descartar por sí solos nulos o valores negativos de magnitud/profundidad.
- [X] T017 [US3] Implementar en `backend/consumidor.py` la asignación espacial punto-en-polígono con GeoPandas/Shapely usando coordenadas `[longitud, latitud]` en EPSG:4326 y clave `ADM0_A3`; no deducir países desde `place` ni asignar el país más cercano, y dejar `country_id = NULL` cuando no haya polígono contenedor.
- [X] T018 [US3] Implementar en `backend/consumidor.py` la conversión de `time`/`updated` de milisegundos Unix a UTC, la derivación de `fecha_id` y `dia_semana` ISO, y la escritura transaccional de `evento_version` y `evento_actual`; el UPSERT de `(event_id, mode)` solo reemplaza el registro actual si `EXCLUDED.updated_ms > evento_actual.updated_ms`.
- [X] T019 [US3] Configurar en `backend/consumidor.py` el commit manual del offset únicamente después del COMMIT PostgreSQL; ante fallo de escritura dejar el mensaje disponible para reintento, y ante mensaje inválido confirmar solo después de persistir su cuarentena.
- [X] T020 Integrar `productor` y `consumidor` en `compose.yaml` usando la imagen única `backend/Dockerfile`, comandos de inicio distintos, `env_file`/variables sin credenciales incorporadas, dependencias de salud Kafka/PostgreSQL, reinicios y rutas internas `kafka:9092`/`postgres:5432`.
- [X] T021 [P] [US3] Crear `backend/tests/integration/test_pipeline.py` para enviar un Feature controlado a Kafka y verificar persistencia de versión/estado actual, país o país nulo, cuarentena y avance del offset después del commit de base de datos.

**Criterio de salida**: Un evento nuevo puede llegar por Kafka y consultarse en PostgreSQL; reintentos y mensajes fuera de orden no duplican ni revierten el estado actual; los rechazos conservan original y motivo.

## Fase 4: Histórico 2025 y modo replay

**Objetivo**: Cargar el histórico completo solicitado, controlar los límites de USGS y demostrar una reproducción acelerada que no contamine el flujo live.

- [X] T022 [P] [US5] Implementar en `backend/cargar_historico.py` la consulta del catálogo USGS FDSN para todo 2025 con `format=geojson`, `minmagnitude=4`, `eventtype=earthquake` y `orderby=time-asc`, dividida inicialmente en intervalos mensuales con límites UTC explícitos.
- [X] T023 [US5] Añadir en `backend/cargar_historico.py` detección y subdivisión recursiva de intervalos que alcancen el máximo de 20.000 resultados, paginación si la fuente la admite, deduplicación de eventos compartidos en los extremos por identidad/versión y validación de cobertura sin huecos para el año completo.
- [X] T024 [US5] Publicar los eventos históricos ordenados por ocurrencia a Kafka desde `backend/cargar_historico.py` con `mode='replay'`, velocidad acelerada configurable, tiempo original intacto y marca/registro de tiempo de reproducción; reutilizar el consumidor existente y no crear un sexto servicio Compose.
- [X] T025 [P] [US5] Crear `backend/tests/integration/test_replay.py` con intervalos y Features sintéticos para probar división de ventanas, duplicados de borde, preservación de revisiones distintas, orden de reproducción e idempotencia al repetir una carga.
- [X] T026 [US5] Registrar en `docs/memoria.md` la fuente histórica, parámetros de las consultas, intervalos procesados, fechas de extracción, cantidades devueltas y procedimiento para repetir la carga; guardar archivos grandes en un volumen/directorio excluido de Git según `.gitignore`.

**Criterio de salida**: El año 2025 puede cargarse sin truncamiento ni duplicar bordes; los mensajes replay pasan por Kafka y consumidor, aparecen únicamente en consultas replay y pueden reproducirse a mayor velocidad.

## Fase 5: API FastAPI

**Objetivo**: Exponer eventos, estadísticas y salud conforme al contrato, con filtros validados y errores explícitos.

- [X] T027 [US2] Implementar en `backend/api.py` conexión a PostgreSQL y validación común de `start`, `end`, `min_mag`, `country_id` y `mode`; usar ISO 8601 con zona horaria, inicio inclusivo, fin exclusivo, `mode='live'` predeterminado y magnitud null excluida al aplicar `min_mag`.
- [X] T028 [US1] Implementar `GET /api/eventos` en `backend/api.py` como GeoJSON `FeatureCollection` con una sola fila actual por evento/modo, periodo predeterminado de últimas 24 horas y metadata `generated`, `last_ingestion`, `mode`, `count` y `truncated`; devolver `features: []` ante una consulta válida sin coincidencias.
- [X] T029 [US5] Completar `GET /api/salud` en `backend/api.py` usando `estado_ingesta.last_ingestion_ms` para informar disponibilidad, última ingesta correcta y antigüedad; conservar la lectura correcta ante fallos de origen y responder explícitamente si falla la base de datos.
- [X] T030 [US4] Implementar en `backend/api.py` los agregados de `GET /api/estadisticas`: conteos por país, subregión y país nulo, ventanas fijas de una hora UTC basadas en `event_time`, distribuciones de magnitud/profundidad y recuentos de valores ausentes.
- [X] T031 [US4] Añadir a `GET /api/estadisticas` en `backend/api.py` la comparación de últimas 24 horas live con magnitud mínima 4 frente al promedio diario del histórico replay 2025 dividido entre 365; aplicar filtros geográficos equivalentes, indicar periodo/unidad/completitud y no calcular sobre una página truncada.
- [X] T032 Configurar en `backend/api.py` CORS limitado a `http://localhost:4200` y al origen Netlify exacto configurado; aceptar GET/OPTIONS y `ngrok-skip-browser-warning`, sin cookies ni credenciales públicas.
- [X] T033 [P] [US2] Crear `backend/tests/contract/test_api.py` para comprobar respuestas GeoJSON, campos requeridos, límites temporales inclusivo/exclusivo, filtros/mode, respuesta vacía, paginación/truncamiento, agregados, nulos, errores explícitos y preflight CORS frente a `CONTRATO-API.md` y `contracts/api-and-messaging.md`.

**Criterio de salida**: Los tres endpoints responden desde PostgreSQL, mantienen los filtros coherentes y nunca presentan una avería como resultado válido vacío o cero.

## Fase 6: Panel Angular y despliegue

**Objetivo**: Completar la experiencia de consulta en la app Angular situada en `frontend/`, sincronizando filtros, mapa, tabla, gráficos y estado operativo.

- [ ] T034 [US1] Actualizar `frontend/public/config.json` y el servicio/configuración Angular para aceptar la URL de API y el intervalo de actualización sin incluir secretos; conservar el modo demo existente claramente identificado como datos ficticios.
- [ ] T035 [US2] Añadir en `frontend/src/app/app.component.ts`, `frontend/src/app/app.component.html` y `frontend/src/app/app.component.css` los filtros de periodo (`start`, `end`), magnitud (`min_mag`), país (`country_id`) y modo (`mode`), compartiendo el mismo estado y parámetros para mapa, tabla y estadísticas.
- [ ] T036 [US1] Añadir en `frontend/src/app/app.component.html` y `frontend/src/app/app.component.ts` una tabla accesible con encabezados, navegación/etiquetas comprensibles, columnas de hora, magnitud, profundidad, lugar y país; mostrar `ND` para magnitud/profundidad null y «Sin país asignado» para país null.
- [ ] T037 [US4] Añadir en `frontend/src/app/app.component.html`, `frontend/src/app/app.component.ts` y `frontend/src/app/app.component.css` paneles/gráficas para recuentos por país/subregión y hora UTC, distribuciones de magnitud/profundidad, valores ausentes y comparación histórica 2025 con periodo, filtros y unidades.
- [ ] T038 [US5] Consultar `GET /api/salud` desde `frontend/src/app/app.component.ts` y mostrar conexión, última lectura, última ingesta y antigüedad; ante timeout/error conservar el último resultado, mostrar aviso y reintentar automáticamente sin solapar peticiones.
- [ ] T039 [US2] Reflejar en `frontend/src/app/app.component.html` y `frontend/src/app/app.component.ts` los filtros inválidos, errores de API y metadata de truncamiento/paginación; no presentar estadísticas globales como completas si la respuesta de eventos es una muestra.
- [ ] T040 [US5] Actualizar `frontend/netlify.toml`, `frontend/public/_redirects` si procede y `README.md` con instrucciones para compilar/publicar la SPA, configurar la URL HTTPS de ngrok, CORS exacto y procedimiento de demostración sin exponer PostgreSQL/Kafka ni credenciales.

**Criterio de salida**: La app permite filtrar el mismo conjunto en mapa, tabla y gráficas, presenta valores ausentes correctamente y diferencia datos actuales, antiguos, truncados, demo y replay.

## Fase 7: Pruebas de aceptación, mediciones y memoria

**Objetivo**: Ejecutar las once pruebas de aceptación del apartado 11 de `ENUNCIADO.md`, medir rendimiento con tres tamaños y documentar resultados y decisiones.

### Once pruebas de aceptación

- [ ] T041 [US1] Añadir a `backend/tests/acceptance/test_acceptance.py` el caso «Recibir un evento nuevo»: verificar una fila actual y visibilidad del evento en API y en las comprobaciones del panel/análisis.
- [ ] T042 [US3] Añadir a `backend/tests/acceptance/test_acceptance.py` el caso «Reenviar la misma versión»: comprobar que no aumenta el número de terremotos ni las filas distintas de `evento_version`.
- [ ] T043 [US3] Añadir a `backend/tests/acceptance/test_acceptance.py` el caso «Recibir una versión posterior»: comprobar que cambian el registro actual y los agregados que dependen de sus campos revisados.
- [ ] T044 [US3] Añadir a `backend/tests/acceptance/test_acceptance.py` el caso «Recibir después una versión antigua»: comprobar que el evento actual conserva la revisión con mayor `updated_ms`.
- [ ] T045 [US1] Añadir a `backend/tests/acceptance/test_acceptance.py` el caso «Magnitud nula»: comprobar que se conserva como null, aparece como «ND» y no cumple `min_mag` ni altera medias.
- [ ] T046 [US3] Añadir a `backend/tests/acceptance/test_acceptance.py` el caso «Coordenadas inválidas»: comprobar cuarentena con motivo y JSON original, y ausencia del punto en API/mapa.
- [ ] T047 [US1] Añadir a `backend/tests/acceptance/test_acceptance.py` el caso «Evento marítimo»: comprobar `country_id = NULL`, etiqueta «Sin país asignado» y permanencia en los análisis.
- [ ] T048 [US3] Añadir a `backend/tests/acceptance/test_acceptance.py` el caso «Reiniciar consumidor»: comprobar que reanuda desde el offset confirmado sin duplicar efectos persistidos.
- [ ] T049 [US5] Documentar en `docs/evidencias/aceptacion.md` el procedimiento «Cerrar el túnel»: desconectar ngrok/API durante una sesión del panel y registrar el aviso, conservación de última lectura y antigüedad.
- [ ] T050 [US5] Documentar en `docs/evidencias/aceptacion.md` el procedimiento «Volver a abrir el túnel»: restablecer el servicio y registrar la recuperación automática de consultas y datos sin intervención manual.
- [ ] T051 [US5] Añadir a `backend/tests/acceptance/test_acceptance.py` el caso «Reproducción histórica»: comprobar publicación/consumo por Kafka en modo replay, orden acelerado, duplicado/revisión controlados y aislamiento completo de las consultas live.
- [ ] T052 Ejecutar las once pruebas del apartado 11 de `ENUNCIADO.md`, registrar comando, resultado PASS/FAIL, fecha, evidencia y limitaciones en `docs/evidencias/aceptacion.md`, y corregir cualquier fallo antes de cerrar la fase.

### Rendimiento, calidad y entrega

- [ ] T053 Medir en `docs/memoria.md` tiempo y memoria con al menos tres tamaños de entrada; identificar equipo, operación, método y resultados, comparar búsqueda secuencial con diccionario o consulta sin/con índice, etiquetar datos sintéticos y razonar el escenario de 100 millones de registros.
- [ ] T054 Documentar en `docs/memoria.md` el análisis de calidad con código/consultas, fuente, periodo, grano/denominador, resultados antes/después, decisiones, limitaciones y acciones justificadas; no crear un generador de informes ni un panel de calidad.
- [ ] T055 Completar `docs/memoria.md` con modelo conceptual/lógico/físico, procedencia, consultas de ingesta, tres conclusiones fundamentadas, comparación equivalente de 2025, costes, complejidad, escalado, limitaciones y mejoras propuestas.
- [ ] T056 Completar `README.md` en la raíz con prerrequisitos, configuración desde `.env.example`, comando único `docker compose up --build -d`, carga del histórico, URL/configuración del frontend, túnel, CORS, validaciones y apagado seguro.
- [ ] T057 Ejecutar el quickstart de `specs/001-observatorio-sismico/quickstart.md`, validar `docker compose config`, salud de los cinco servicios, pruebas backend y `npm run build` en `frontend/`; registrar los resultados finales en `docs/evidencias/aceptacion.md`.

**Criterio de salida**: Las once pruebas del enunciado tienen evidencia; las mediciones cubren tres tamaños; la memoria y el README permiten reproducir y explicar el proyecto.

## Dependencias y orden de ejecución

### Dependencias entre fases

- **Fase 1**: No depende de fases anteriores. T001–T004 editan rutas distintas y pueden organizarse en paralelo; T005 precede a la construcción de la imagen T006.
- **Fase 2**: Depende de la estructura de Fase 1. T007 precede a T008; T009/T010 integran y montan el esquema; T011 valida el conjunto.
- **Fase 3**: Depende de la base de datos y el broker saludables. El productor (T012–T014) y el consumidor (T015–T019) trabajan en archivos distintos y pueden desarrollarse en paralelo después de T007–T010; T020/T021 integran y verifican ambos flujos.
- **Fase 4**: Depende de Kafka, el consumidor y su soporte `mode='replay'` de Fase 3.
- **Fase 5**: Depende del esquema poblado por el consumidor y del conjunto histórico para las comparativas.
- **Fase 6**: Depende de los contratos y endpoints de Fase 5.
- **Fase 7**: Depende del pipeline, API y panel completados; los casos 9 y 10 requieren un túnel real o un sustituto de prueba registrado como tal.

### Historias de usuario: prioridad, comprobación independiente y tareas

| Historia | Prioridad | Criterio de prueba independiente | Tareas principales |
|---|---|---|---|
| US1 — Consultar terremotos actuales | P1 | Con eventos válidos, API/mapa/tabla muestran una sola versión; lista vacía es válida; nulos se muestran como ND y los eventos sin polígono como no asignados. | T027–T028, T034–T036, T041, T045, T047 |
| US2 — Filtrar y comparar eventos | P1 | Filtros individuales/combinados producen el mismo conjunto en mapa, tabla y estadísticas; inicio inclusivo, fin exclusivo, null excluido por magnitud y modo aislado. | T027, T033, T035, T039 |
| US3 — Mantener datos fiables y trazables | P1 | Repetir, revisar y reordenar mensajes conserva versiones únicas y la revisión actual más reciente; inválidos se ponen en cuarentena. | T012–T021, T042–T044, T046, T048 |
| US4 — Analizar patrones y contexto histórico | P2 | Un conjunto conocido produce los conteos geográficos/horarios, distribuciones y comparación histórica esperados, incluidos valores ausentes. | T030–T031, T037, T053–T055 |
| US5 — Supervisar disponibilidad y reproducir el histórico | P2 | Se ve la última ingesta, el panel conserva lectura y se recupera tras una caída; la reproducción Kafka queda separada de live. | T022–T026, T029, T038, T040, T049–T051 |

### Dependencias principales de tareas

```text
T001-T006 → T007-T011 → T012-T021 → T022-T026
                                  └→ T027-T033 → T034-T040 → T041-T057
```

La rama histórica T022–T026 y la API básica T027–T029 comparten el esquema y el consumidor; la comparación estadística T031 requiere el histórico replay cargado. La verificación final T057 requiere todos los componentes integrados.

### Oportunidades de paralelismo

- **Setup**: T001, T002, T003 y T004 son paralelizables al trabajar sobre destinos distintos; T005/T006 son secuenciales.
- **Pipeline**: T012–T014 (productor) puede implementarse a la vez que T015–T019 (consumidor); T021 prueba ambos una vez que estén listos.
- **API y UI**: tras fijar los contratos, trabajo de pruebas de contrato de T033 puede prepararse en paralelo con partes independientes de la interfaz T034; los cambios sucesivos de componente Angular deben integrarse de forma coordinada.
- **Historias**: una vez terminada la infraestructura, el equipo puede asignar productor/consumidor (US3), API/panel de consulta (US1/US2) y estadísticas/replay (US4/US5) a personas distintas, integrando en el orden de fases indicado.
- Las tareas con `[P]` son las oportunidades concretas que no comparten archivos de implementación ni dependen de resultados pendientes.

### Ejemplos de ejecución paralela por historia

- **US1**: una persona puede implementar `backend/api.py` (T028) mientras otra prepara la configuración del frontend (T034), una vez acordado el contrato GeoJSON; integrar después las vistas y la aceptación T041.
- **US2**: preparar pruebas de filtros en `backend/tests/contract/test_api.py` (T033) en paralelo con el diseño de controles/estado compartido en `frontend/src/app/` (T035), y conectarlos al completar T027–T028.
- **US3**: implementar productor y sus pruebas (T012–T014) en paralelo con carga de países y validación/persistencia del consumidor (T015–T019); unir en Compose y probar con T020–T021.
- **US4**: preparar presentación de estadísticas en `frontend/src/app/` (T037) mientras se implementan los agregados API (T030–T031), usando el contrato `contracts/api-and-messaging.md` como interfaz estable.
- **US5**: implementar la consulta de salud en `backend/api.py` (T029) mientras se escribe el cargador replay `backend/cargar_historico.py` (T022–T024); completar aislamiento, interfaz y pruebas tras integrar el consumidor.

## Estrategia de implementación incremental

1. Consolidar estructura y documentación base (Fase 1), después arrancar PostgreSQL/Kafka con esquema durable (Fase 2).
2. Completar el recorrido mínimo de un evento por Kafka, PostgreSQL y API (Fase 3 y endpoint básico de Fase 5); validar la historia US1 antes de añadir visualizaciones completas.
3. Implementar la carga/reproducción histórica aislada (Fase 4), luego completar estadísticas y filtros API (Fase 5).
4. Conectar y finalizar el panel (Fase 6); verificar filtros coherentes, accesibilidad y estados de fallo.
5. Ejecutar las once pruebas requeridas, medir tres tamaños y completar evidencias/memoria (Fase 7).

**MVP sugerido**: US1 (eventos actuales en API, mapa y tabla) apoyado en el esquema, productor/consumidor mínimos, validación de nulos y manejo de lista vacía. No declarar el proyecto finalizado al alcanzar el MVP: las historias P1 de filtros e idempotencia y las P2 de análisis, salud y replay siguen siendo entregables requeridos.
