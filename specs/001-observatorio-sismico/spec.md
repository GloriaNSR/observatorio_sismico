# Feature Specification: Observatorio de Actividad Sísmica

**Feature Branch**: `[001-observatorio-sismico]`

**Created**: 2026-10-30

**Status**: Draft

**Input**: User description: Construir el sistema completo del Observatorio de Actividad Sísmica conforme a `ENUNCIADO.md` y `CONTRATO-API.md`, con ingesta en vivo e histórica, reproducción aislada, validación e integración geográfica, almacenamiento versionado, API de consulta y estadísticas, panel con mapa, filtros y estado operativo, y los escenarios de aceptación indicados.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Consultar terremotos actuales (Priority: P1)

Como persona que consulta la actividad sísmica, quiero ver los terremotos recientes en un mapa y una tabla, para conocer su ubicación, hora, magnitud y país cuando esté disponible.

**Why this priority**: Es el valor central del observatorio y debe funcionar aunque solo se implemente esta experiencia.

**Independent Test**: Con una respuesta válida de eventos, abrir el panel y comprobar que el mapa y la tabla muestran una sola versión de cada evento dentro del periodo predeterminado de 24 horas.

**Acceptance Scenarios**:

1. **Given** que hay eventos válidos de las últimas 24 horas, **When** la persona abre el panel, **Then** ve sus ubicaciones en el mapa y sus datos en la tabla.
2. **Given** que no hay eventos que coincidan con la consulta, **When** se solicita el mapa, **Then** se muestra una colección vacía y no un error.
3. **Given** que la magnitud o la profundidad de un evento es nula, **When** se muestra en la tabla o ficha, **Then** aparece como «ND».
4. **Given** que un evento ocurrió en el mar o fuera de los polígonos, **When** se muestra en el panel y las estadísticas, **Then** se clasifica como «Sin país asignado».

---

### User Story 2 - Filtrar y comparar eventos (Priority: P1)

Como persona investigadora, quiero filtrar eventos por periodo, magnitud mínima, país y modo de datos, para analizar el mismo conjunto de eventos en el mapa, la tabla y las visualizaciones.

**Why this priority**: Los filtros coherentes son necesarios para interpretar resultados y evitar que vistas distintas parezcan contradictorias.

**Independent Test**: Aplicar cada filtro individual y combinado y verificar que las funciones del mapa, la tabla, los recuentos y las distribuciones reflejan el mismo conjunto filtrado.

**Acceptance Scenarios**:

1. **Given** un periodo y filtros válidos, **When** se aplican o modifican, **Then** mapa, tabla y estadísticas se actualizan con los mismos criterios.
2. **Given** un límite de inicio, **When** ocurre un evento exactamente en ese instante, **Then** el evento se incluye; **When** ocurre exactamente en el límite final, **Then** se excluye.
3. **Given** un filtro de magnitud mínima, **When** un evento tiene magnitud nula, **Then** no cumple el filtro ni altera las medias.
4. **Given** datos disponibles en modos live y replay, **When** se elige uno de los modos, **Then** solo se presentan y agregan los registros de ese modo.

---

### User Story 3 - Mantener datos fiables y trazables (Priority: P1)

Como responsable del observatorio, quiero que los eventos se validen, conserven sus revisiones y se procesen de forma idempotente, para que reintentos o mensajes fuera de orden no alteren los resultados.

**Why this priority**: Sin calidad, trazabilidad e idempotencia, tanto el mapa como las estadísticas pueden mostrar datos incorrectos.

**Independent Test**: Ingresar un evento, repetir la misma versión, ingresar una versión posterior y luego una versión antigua; revisar registros actuales e históricos y las estadísticas resultantes.

**Acceptance Scenarios**:

1. **Given** un evento nuevo válido, **When** se procesa, **Then** existe una versión histórica y un registro actual para él.
2. **Given** una versión ya procesada, **When** se recibe de nuevo, **Then** no se duplica ni aumenta los conteos.
3. **Given** una versión posterior con `updated` mayor, **When** se procesa, **Then** se conserva una nueva versión y el evento actual se actualiza.
4. **Given** una versión antigua recibida después de una posterior, **When** se procesa, **Then** el evento actual permanece en su versión más reciente.
5. **Given** una geometría distinta de Point, coordenadas fuera de rango o un tipo distinto de `earthquake`, **When** se procesa, **Then** el original y el motivo se conservan en cuarentena y el evento no aparece en el mapa.
6. **Given** una magnitud o profundidad negativa pero válida, **When** se procesa, **Then** se conserva el valor y no se descarta automáticamente.

---

### User Story 4 - Analizar patrones y contexto histórico (Priority: P2)

Como persona analista, quiero consultar estadísticas por ubicación, hora y distribución, además de comparar la actividad reciente con el año 2025, para contextualizar los eventos observados.

**Why this priority**: Las agregaciones aportan contexto útil, pero dependen de que la consulta básica y la integridad de los datos ya funcionen.

**Independent Test**: Comparar cada agregado con un conjunto conocido de eventos y comprobar periodos, filtros, unidades y tratamiento de valores sin dato.

**Acceptance Scenarios**:

1. **Given** eventos dentro de los filtros, **When** se solicitan estadísticas, **Then** se devuelven recuentos por país y subregión, incluidos los no asignados, y recuentos en ventanas de una hora UTC basadas en la hora de ocurrencia.
2. **Given** eventos con magnitud o profundidad nula, **When** se calculan distribuciones, **Then** los valores nulos se informan aparte y se excluyen de la distribución correspondiente.
3. **Given** eventos válidos de 2025, **When** se consulta la comparación histórica, **Then** el recuento reciente de las últimas 24 horas con magnitud mínima 4 se compara con el promedio diario de 2025 calculado sobre 365 días, respetando los filtros geográficos aplicables.
4. **Given** un fallo de almacenamiento o consulta, **When** se solicitan estadísticas, **Then** el sistema informa un error explícito y no presenta cero como resultado válido.

---

### User Story 5 - Supervisar disponibilidad y reproducir el histórico (Priority: P2)

Como responsable del observatorio, quiero conocer la frescura de los datos y poder reproducir una selección histórica de forma acelerada, para detectar interrupciones y revisar periodos pasados sin mezclarlos con la actividad en vivo.

**Why this priority**: La supervisión permite distinguir falta de actividad sísmica de una interrupción; la reproducción amplía el análisis sin contaminar las métricas en vivo.

**Independent Test**: Consultar el estado y la última ingesta, interrumpir y restablecer la conectividad, y reproducir un periodo histórico verificando su modo y aislamiento.

**Acceptance Scenarios**:

1. **Given** una consulta de origen exitosa, **When** se consulta el estado, **Then** se informa la disponibilidad y el instante de la última ingesta exitosa.
2. **Given** que falla la conexión entre el panel y el servicio de consulta, **When** vence el tiempo de espera, **Then** se conserva la última lectura, se muestra un aviso y su antigüedad, y la actualización se reanuda automáticamente cuando vuelve la conexión.
3. **Given** el histórico del año 2025, **When** se elige un intervalo para reproducción acelerada, **Then** sus eventos quedan identificados como replay y no alteran las consultas ni estadísticas live.
4. **Given** que una publicación no ha sido confirmada, **When** el proceso intenta registrar su progreso, **Then** no marca esa versión como publicada hasta recibir la confirmación.

### Edge Cases

- Los límites temporales se interpretan en ISO 8601; el inicio es inclusivo y el final exclusivo. Las ventanas horarias se definen en UTC.
- Una magnitud o profundidad nula se conserva como desconocida, se muestra como «ND» y se excluye de filtros y promedios que requieran ese valor; nunca se sustituye por cero.
- Los valores negativos de magnitud o profundidad se conservan si el resto del evento es válido.
- Las coordenadas deben estar en orden longitud, latitud y dentro de sus rangos WGS84; una coordenada o geometría inválida se pone en cuarentena.
- Un evento que no intersecta un país se conserva con país desconocido; no se infiere el país a partir del texto de lugar.
- La fuente puede devolver eventos repetidos o revisados en los bordes de periodos históricos; se deben deduplicar sin perder revisiones distintas.
- Una consulta válida sin coincidencias devuelve una lista vacía. Una indisponibilidad de la base o del servicio se distingue explícitamente de una respuesta vacía.
- Si una respuesta se limita por tamaño, se indica que está truncada, se informa el total completo y se ofrece paginación; las estadísticas globales no se calculan sobre una muestra parcial.
- La pérdida de conectividad no elimina la última lectura visible; su antigüedad debe seguir siendo comprensible hasta que una nueva lectura tenga éxito.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: El sistema MUST consultar `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson` cada 60 segundos de forma predeterminada, con intervalo configurable, y registrar metadatos de consulta y la última ingesta exitosa.
- **FR-002**: El sistema MUST publicar solo eventos nuevos o revisiones cuyo `updated` sea posterior a la última versión publicada para ese identificador. La clave lógica de publicación es el identificador del evento y el mensaje conserva `source`, `ingested_at`, `mode` (`live` o `replay`) y el Feature GeoJSON original.
- **FR-003**: El sistema MUST registrar una publicación como completada únicamente después de recibir confirmación de su entrega; ante fallos, debe poder reintentar sin perder el evento ni crear efectos duplicados.
- **FR-004**: El sistema MUST obtener de `https://earthquake.usgs.gov/fdsnws/event/1/query` el conjunto histórico completo del año calendario 2025 con `format=geojson`, `minmagnitude=4`, `eventtype=earthquake` y `orderby=time-asc`; dividirá la consulta por meses para evitar límites de respuesta y deduplicará los bordes sin descartar versiones distintas.
- **FR-005**: El sistema MUST permitir reproducir una selección del histórico a una velocidad superior a la temporal original, identificar cada registro como `replay` y mantenerlo separado de los registros `live` en consultas y agregaciones.
- **FR-006**: El sistema MUST aceptar para procesamiento solo eventos cuyo tipo sea `earthquake`, con geometría Point y coordenadas WGS84 válidas; los rechazados MUST conservar el JSON original y un motivo de rechazo consultable.
- **FR-007**: El sistema MUST preservar valores nulos de magnitud y profundidad como desconocidos, y preservar magnitudes y profundidades negativas válidas.
- **FR-008**: El sistema MUST asignar país mediante la intersección espacial de las coordenadas del evento con los polígonos entregados, utilizando `ADM0_A3` como identificador y `NAME_ES` o `ADMIN`, `CONTINENT` y `SUBREGION` como atributos disponibles. Los eventos sin intersección tendrán país nulo y etiqueta «Sin país asignado»; el país no se inferirá del texto de lugar.
- **FR-009**: El sistema MUST conservar una sola instancia de cada versión identificada por `(event_id, updated_ms, mode)` y mantener para cada evento y modo una versión actual que solo avance cuando el `updated_ms` recibido sea mayor, sin que replay sobrescriba ni contamine live.
- **FR-010**: El sistema MUST confirmar el avance de los mensajes recibidos solo después de que la transacción de persistencia haya terminado correctamente; el reenvío de una versión idéntica o antigua no debe modificar el evento actual ni inflar conteos.
- **FR-011**: El sistema MUST distinguir la hora de ocurrencia, la hora de revisión del origen y la hora de recepción, conservarlas en UTC y exponer los instantes requeridos como milisegundos Unix.
- **FR-012**: `GET /api/eventos` MUST devolver una FeatureCollection GeoJSON con una sola versión actual por evento, geometría válida y metadata `generated`, `last_ingestion`, `mode`, `count` y `truncated`. Sin coincidencias, devolverá `features: []`.
- **FR-013**: La consulta de eventos MUST admitir `start` (ISO 8601 inclusivo), `end` (ISO 8601 exclusivo), `min_mag`, `country_id` y `mode`; por defecto incluirá las últimas 24 horas. Los valores desconocidos no satisfacen un filtro de magnitud mínima.
- **FR-014**: Si la consulta de eventos supera el máximo documentado, el sistema MUST informar que está truncada, devolver el total completo y proporcionar un mecanismo de paginación. Las visualizaciones estadísticas MUST indicar si sus datos están completos y no presentar una muestra truncada como agregado global.
- **FR-015**: Las propiedades públicas de cada evento MUST incluir magnitud, tipo de magnitud, profundidad, lugar, hora, revisión, país y nombre del país. Magnitud, profundidad y país desconocidos se representarán como `null`, nunca como NaN o infinito.
- **FR-016**: `GET /api/salud` MUST exponer el estado del servicio y la fecha de la última ingesta exitosa, sin incluir credenciales.
- **FR-017**: `GET /api/estadisticas` MUST proporcionar recuentos por país y subregión (incluida la categoría sin país), recuentos por ventanas fijas de una hora UTC según ocurrencia, distribuciones de magnitud y profundidad con sus valores nulos excluidos informados, y comparación de las últimas 24 horas con el promedio diario de 2025 sobre 365 días para magnitud mínima 4.
- **FR-018**: Los resultados estadísticos MUST declarar periodo, filtros, unidad y cantidad de valores ausentes, y aplicar criterios equivalentes a los de la consulta de eventos; los errores de almacenamiento o consulta se comunicarán explícitamente, nunca como resultados vacíos o cero.
- **FR-019**: El panel MUST obtener los eventos y el estado del servicio de la configuración pública de la aplicación, permitir filtrar por fechas, magnitud mínima, país o región y modo, y sincronizar esos filtros entre mapa, tabla y gráficos.
- **FR-020**: El panel MUST mostrar recuentos geográficos y horarios, distribuciones de magnitud y profundidad, comparación histórica de 2025 y una tabla consultable con «ND» para valores nulos.
- **FR-021**: El panel MUST mostrar cuándo se realizó la última lectura, la última ingesta de USGS y avisos comprensibles cuando haya errores o datos antiguos. Ante un fallo conservará la última lectura y su antigüedad, y reintentará la actualización automáticamente.
- **FR-022**: El panel MUST reflejar claramente resultados truncados y errores, y no calcular ni presentar estadísticas globales como completas cuando los eventos disponibles sean solo una muestra.
- **FR-023**: La API MUST permitir solicitudes desde `http://localhost:4200` y el origen exacto del sitio publicado en Netlify, aceptar los métodos GET y OPTIONS y permitir la cabecera `ngrok-skip-browser-warning`.

### Key Entities *(include if data involved)*

- **Evento sísmico**: Identidad y geometría de un evento, sus propiedades originales, hora de ocurrencia, país asignado y modo de origen.
- **Versión de evento**: Revisión inmutable de un evento, identificada por evento, instante de revisión y modo; permite reconstruir cambios y deduplicar entregas.
- **Evento actual**: La revisión más reciente aceptada para un evento y modo; se modifica solo ante una revisión posterior.
- **Registro en cuarentena**: Evento original rechazado, motivo y momento del rechazo.
- **Consulta de origen**: Metadatos de una consulta a USGS, resultado y hora de la última consulta exitosa.
- **Reproducción histórica**: Selección de eventos de 2025 emitidos en modo replay, aislada de la actividad en vivo.
- **Estadística**: Agregado de eventos asociados a filtros, periodo, unidad y conteos de valores ausentes.
- **Lectura del panel**: Último conjunto de datos recibido por la interfaz, su hora de generación y antigüedad, conservado mientras se recupera una conexión.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: En pruebas de aceptación, el 100 % de los eventos válidos del feed consultado que sean nuevos o tengan una revisión posterior queda disponible para consulta tras una ingesta exitosa.
- **SC-002**: En pruebas de repetición y desorden, el 100 % de las versiones idénticas queda deduplicado y ninguna versión anterior reemplaza a una versión más reciente.
- **SC-003**: El 100 % de los eventos con tipo, geometría o coordenadas inválidas de los casos de prueba se conserva con su motivo y no aparece en el mapa.
- **SC-004**: En los casos de prueba, el 100 % de los nulos de magnitud o profundidad permanece desconocido, se muestra como «ND» y se excluye de los filtros y cálculos que requieren ese valor.
- **SC-005**: En un conjunto de referencia conocido, el 100 % de los puntos se asigna al país cuyo polígono contiene sus coordenadas, o queda como «Sin país asignado» si no intersecta ninguno.
- **SC-006**: Para todos los filtros probados, mapa, tabla y estadísticas producen resultados coherentes; los límites horarios y de fechas respetan la definición inclusiva/exclusiva especificada.
- **SC-007**: En el 100 % de las pruebas de fallo y recuperación de conexión, la interfaz conserva la última lectura, comunica su antigüedad y vuelve a actualizarse sin intervención manual al restablecerse el servicio.
- **SC-008**: Las estadísticas del conjunto de prueba coinciden exactamente con los recuentos de referencia por país, subregión y hora UTC, e informan correctamente los valores ausentes.
- **SC-009**: En una respuesta limitada, el total y la condición de truncamiento son visibles y ninguna vista presenta estadísticas de muestra como estadísticas globales completas.
- **SC-010**: Una persona usuaria puede localizar un evento por periodo, país o magnitud y entender sus valores desconocidos y el estado de frescura sin asistencia externa.

## Assumptions

- El periodo histórico requerido es el año calendario 2025 completo; se divide en consultas que evitan el límite de resultados de la fuente y cubren sin huecos el inicio y fin del año.
- La comparación histórica utiliza como referencia los eventos válidos de 2025 con magnitud igual o superior a 4, divide el total correspondiente entre 365 días y aplica los filtros geográficos compatibles. La comparación reciente usa las últimas 24 horas y el mismo umbral de magnitud.
- El selector de reproducción permite escoger un intervalo del histórico; la velocidad acelerada puede configurarse sin cambiar el modo ni la identidad de los registros.
- Las ventanas de una hora se presentan en UTC; fechas de entrada con zona explícita se convierten a UTC y las fechas sin zona se consideran inválidas.
- El límite de respuesta de eventos es configurable y documentado; la paginación permite recuperar todos los resultados y el conteo metadata indica el total que coincide con los filtros.
- El usuario del panel consulta información pública y no necesita autenticación para acceder a los datos del observatorio.
- Se utiliza el catálogo de países entregado como referencia geográfica y no se modifica.
- El sistema y sus servicios se desplegarán de acuerdo con las restricciones y la constitución vigentes del proyecto; este documento especifica el comportamiento observable y el contrato que debe verificarse.
