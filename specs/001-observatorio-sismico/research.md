# Investigación: Observatorio de Actividad Sísmica

## Decisión 1: Mantener cuatro scripts especializados como puntos de entrada del backend

**Decisión**: Implementar `backend/productor.py`, `backend/consumidor.py`, `backend/api.py` y `backend/cargar_historico.py`; construir una única imagen Python desde `backend/Dockerfile` y ejecutar el script correspondiente como comando de cada servicio.

**Justificación**: Así se cumple la estructura solicitada y el requisito constitucional de compartir imagen, manteniendo separadas las responsabilidades de consulta/publicación, validación/persistencia, consultas HTTP y carga histórica. Compose, y no scripts ejecutados en el equipo anfitrión, controla el ciclo de vida del productor y el consumidor.

**Alternativas consideradas**:
- Un Dockerfile distinto por servicio Python: se descarta porque la constitución exige una imagen reproducible compartida.
- Un único punto de entrada Python monolítico: se descarta porque acopla ciclos de vida independientes y dificulta identificar la topología requerida de cinco contenedores.

## Decisión 2: Usar PostgreSQL como estado duradero e inicializar el esquema con SQL

**Decisión**: Crear las tablas solicitadas en `sql/init.sql`, montarlo en `/docker-entrypoint-initdb.d/` y conservar el directorio de datos de PostgreSQL en un volumen con nombre de Compose. Registrar el progreso de publicación del productor en `estado_productor`.

**Justificación**: El estado del productor debe sobrevivir a los reinicios, los datos actuales y versionados deben persistir transaccionalmente y el esquema inicial debe poder reproducirse al crear una base de datos nueva.

**Alternativas consideradas**:
- Estado del productor en memoria: se descarta porque un reinicio podría volver a publicar u omitir revisiones.
- Crear el esquema únicamente al arrancar la API: se descarta porque la disponibilidad del productor/consumidor y la propiedad del esquema dependerían del orden de arranque.

## Decisión 3: Modelar el estado actual por evento y modo; conservar cada revisión aceptada

**Decisión**: Usar `(event_id, mode)` como clave primaria de `evento_actual` y `(event_id, updated_ms, mode)` como clave de unicidad de `evento_version`. Almacenar por separado los instantes de ocurrencia, revisión del origen e ingesta; conservar los valores numéricos desconocidos como `NULL` SQL.

**Justificación**: Una fila actual distinta por modo aísla replay de live, mientras que la clave de versión hace idempotente la entrega al menos una vez. La separación de marcas de tiempo y el tratamiento de nulos son requisitos explícitos de la funcionalidad y la constitución.

**Alternativas consideradas**:
- Una única fila actual por evento: se descarta porque replay podría sobrescribir o contaminar el estado live.
- Sustituir magnitud/profundidad nulas por cero: se descarta porque los valores desconocidos no deben afectar filtros o estadísticas como si fueran ceros válidos.

## Decisión 4: Usar la transacción PostgreSQL como límite de confirmación del consumidor

**Decisión**: Validar y enriquecer cada registro, insertar su versión aceptada (o su registro de cuarentena), actualizar el estado actual solo si avanza `updated_ms`, confirmar la transacción PostgreSQL y, únicamente después, confirmar el offset Kafka.

**Justificación**: Esto proporciona procesamiento al menos una vez: un fallo antes de confirmar la base de datos permite reintentar; un fallo después de confirmar la base de datos pero antes de confirmar el offset es seguro gracias a las restricciones únicas y los upserts idempotentes.

**Alternativas consideradas**:
- Confirmar el offset antes de guardar los datos: se descarta porque un fallo de base de datos podría perder un mensaje definitivamente.
- Tratar versiones duplicadas o antiguas como errores: se descarta porque los reintentos normales y las entregas desordenadas no deben cambiar el estado actual ni aumentar los recuentos.

## Decisión 5: Usar el catálogo de países proporcionado mediante geometría espacial y montarlo en modo de solo lectura

**Decisión**: Leer el `paises.geojson` proporcionado desde los recursos existentes del frontend, usar GeoPandas/Shapely con puntos dentro de polígonos WGS84 y persistir el país `ADM0_A3` coincidente junto con sus atributos descriptivos disponibles. Los puntos sin coincidencia quedan sin asignar.

**Justificación**: El catálogo es la referencia proporcionada y no debe modificarse. Montarlo en modo de solo lectura evita copias divergentes o duplicadas; además, está expresamente prohibido deducir el país a partir del texto de `place`.

**Alternativas consideradas**:
- Analizar nombres de países en `place`: se descarta porque lo prohíbe la constitución y no es fiable para eventos mar adentro.
- Asignar el país más cercano: se descarta porque los eventos marítimos deben permanecer sin asignar.

Cargar/actualizar idempotentemente la tabla de referencia `pais` al iniciar el consumidor y antes de procesar registros. Montar el GeoJSON proporcionado en modo de solo lectura para evitar reescribir el catálogo entregado.

## Decisión 6: Separar la extracción histórica de la reproducción acelerada

**Decisión**: `cargar_historico.py` recupera los 12 meses de 2025 con los parámetros USGS FDSN especificados, divide cualquier solicitud que pudiera exceder el límite de resultados, deduplica los límites compartidos entre intervalos sin perder versiones con distinto `updated` y publica datos etiquetados como replay. Las consultas de reproducción usan `mode='replay'`.

**Justificación**: La recuperación histórica y la reproducción tienen objetivos operativos distintos; el etiquetado y el almacenamiento identificado por modo evitan que replay afecte a los paneles y resúmenes live.

**Alternativas consideradas**:
- Una única consulta anual sin gestionar límites: se descarta porque el máximo de respuesta de USGS podría truncar los resultados.
- Insertar directamente filas del archivo sin tratarlas como replay: se descarta porque evitaría el flujo común del consumidor y el aislamiento por modo.

## Decisión 7: Priorizar el contrato de API y compartir el estado de filtros del frontend

**Decisión**: Implementar `GET /api/eventos`, `GET /api/estadisticas` y `GET /api/salud` con los filtros documentados (`start`, `end`, `min_mag`, `country_id`, `mode`). El componente Angular enviará los mismos valores de filtro para eventos y estadísticas, y conservará la última visualización correcta si falla una actualización.

**Justificación**: El mapa, la tabla y las estadísticas deben describir los mismos registros seleccionados. Los estados explícitos de salud, frescura y error permiten distinguir una interrupción de un periodo con poca actividad sísmica.

**Alternativas consideradas**:
- Lógica de filtros independiente en cada panel de la interfaz: se descarta porque puede producir vistas contradictorias.
- Devolver datos vacíos ante un error de API/base de datos: se descarta porque un error no debe mostrarse como un resultado válido de cero eventos.

## Decisiones sobre detalles restantes del esquema

- `fecha_id` es la fecha del calendario UTC de `event_time`; `dia_semana` usa numeración ISO, de lunes=1 a domingo=7.
- `estado_ingesta.last_ingestion_ms` registra la consulta correcta más reciente a USGS; los fallos no la borran. La API calcula si los datos están desactualizados usando esa marca de tiempo y un umbral configurable.
- Inicialmente se usarán solo los índices base solicitados. Antes de añadir índices que aumenten el coste de escritura, se validarán con planes de consulta reales de la API.
