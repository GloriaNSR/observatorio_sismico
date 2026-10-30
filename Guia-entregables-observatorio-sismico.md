# ¿Qué tienes que entregar?
## Proyecto: Observatorio de actividad sísmica

Esta guía explica cómo presentar tu trabajo. Complementa el enunciado y las FAQ: **no añade nuevas tareas ni modifica la rúbrica**.

No tienes que preparar nueve documentos separados. Organiza tu entrega en tres bloques:

| Entrega | Qué permite comprobar |
|---|---|
| **1. Repositorio del proyecto** | Que otra persona puede revisar el código y ejecutar el sistema |
| **2. Memoria o notebook** | Que entiendes los datos, justificas tus decisiones y analizas los resultados |
| **3. Panel y defensa** | Que el sistema funciona y puedes explicar tu trabajo |

Los ejemplos de datos y resultados de esta guía son ficticios. Debes sustituirlos por los resultados reales de tu proyecto.

## 1. Repositorio del proyecto

Entrega el enlace al repositorio y comprueba que el profesor tiene acceso.

### 1.1. Código de los componentes

Incluye el código del productor Python, el consumidor Python, la API FastAPI y el frontend Angular. Añade los procesos de carga histórica y reproducción que hayas implementado.

Distingue qué material de apoyo has reutilizado y qué has desarrollado o modificado. El mapa proporcionado es una base: no sustituye la implementación del procesamiento y del análisis.

### 1.2. Dockerfile(s) y `compose.yaml`

Los cinco servicios obligatorios del backend deben ejecutarse en contenedores Docker:

| Servicio | Responsabilidad |
|---|---|
| Productor | Consultar USGS y publicar novedades en Kafka |
| Kafka | Transportar y conservar los mensajes |
| Consumidor | Validar, transformar, enriquecer y guardar |
| PostgreSQL | Almacenar los datos y el estado necesario |
| API FastAPI | Ofrecer los datos preparados al frontend |

Un Dockerfile explica cómo construir una imagen. El archivo `compose.yaml` define cómo se ejecutan y se conectan los servicios. Los procesos Python pueden compartir una imagen y utilizar comandos de inicio diferentes.

La entrega debe permitir arrancar estos servicios con:

```bash
docker compose up --build -d
```

Documenta los pasos previos, como crear la configuración, inicializar tablas o cargar el histórico. Configura persistencia, comprobaciones de salud y reintentos donde correspondan.

ngrok puede ejecutarse en el equipo o en otro contenedor. Angular se publica en Netlify; no se exige un contenedor de producción para el frontend.

### 1.3. Dependencias y `.env.example`

Incluye los archivos que permitan instalar las versiones de las dependencias utilizadas: por ejemplo, `requirements.txt` o un archivo equivalente en Python, y `package.json` con su archivo de bloqueo en Angular.

Incluye un `.env.example` con las variables que requiere tu implementación. Ejemplo orientativo:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=terremotos.live
POSTGRES_HOST=postgres
POSTGRES_DB=observatorio
POSTGRES_USER=observatorio
POSTGRES_PASSWORD=CAMBIAR_EN_LOCAL
```

Los nombres deben coincidir con tu código y con Compose. No copies el ejemplo sin comprobarlo. Guarda las credenciales reales en tu configuración local y exclúyelas del repositorio.

### 1.4. Instrucciones reproducibles en `README.md`

«Reproducible» significa que otra persona puede ejecutar el proyecto siguiendo tus instrucciones, sin tener que adivinar qué instalar o configurar.

Explica:

- Requisitos y versiones utilizadas.
- Cómo crear la configuración local a partir de `.env.example`.
- Cómo construir y arrancar los contenedores.
- Cómo crear las tablas y realizar la carga histórica.
- Cómo comprobar el estado de los servicios y consultar sus logs.
- Cómo ejecutar el frontend en desarrollo y publicarlo.
- Cómo activar ngrok y configurar la URL de la API.
- Cómo ejecutar las pruebas y la reproducción histórica.
- Cómo detener el sistema y qué datos se conservan al reiniciarlo.

No subas `node_modules`, secretos, volúmenes de bases de datos ni grandes descargas regenerables. Documenta dónde guardas los originales y cómo recuperarlos.

## 2. Memoria o notebook

Puedes utilizar una memoria Markdown o un notebook con explicaciones, código y resultados. Puedes enlazar desde ella los diagramas, SQL, capturas y evidencias guardados en el repositorio.

**No necesitas un documento independiente por cada apartado.** El siguiente índice te permite reunir todas las evidencias en un único lugar.

### 2.1. Objetivo y funcionamiento general

Explica brevemente qué pregunta pretende responder el observatorio y cómo circulan los datos entre los componentes.

Ejemplo de explicación:

> El productor identifica novedades de USGS y las publica en Kafka. El consumidor valida cada evento, lo integra con el catálogo geográfico y lo guarda en PostgreSQL. La API consulta esos datos y el frontend los presenta en el mapa y en los análisis.

Añade las decisiones de alcance: periodo histórico, ámbito geográfico y filtros utilizados.

### 2.2. Diccionario de datos

El diccionario explica el significado de los campos de tu modelo. No es una lista de nombres sin descripción.

Ejemplo:

| Campo | Significado | Tipo o unidad | Tratamiento de ausencias |
|---|---|---|---|
| `event_id` | Identificador del terremoto | Texto | Obligatorio |
| `mag` | Magnitud | Decimal; conservar también su tipo de magnitud | Se admite nulo |
| `depth_km` | Profundidad | Decimal, km | Se admite nulo según las reglas definidas |
| `event_time` | Momento de ocurrencia | Fecha y hora UTC | Obligatorio |
| `updated` | Actualización de la versión en USGS | Fecha y hora UTC o timestamp documentado | Necesario para controlar versiones |
| `country_id` | Identificador de la entidad del catálogo geográfico | Texto | Nulo si no hay asignación |

Describe los campos realmente utilizados. Indica las unidades y las conversiones: por ejemplo, USGS entrega timestamps en milisegundos Unix.

### 2.3. Procedencia y registro de consultas

Explica de dónde procede cada conjunto y cómo lo obtuviste.

Ejemplo de registro:

| Conjunto | Fuente | Periodo y filtros | Extracción |
|---|---|---|---|
| Histórico | Catálogo USGS | Año 2025, magnitud ≥4, tipo earthquake | Fecha de descarga y consultas por meses |
| Recientes | Feed USGS `all_day.geojson` | Últimas 24 horas | Consulta aproximadamente cada 60 segundos |
| Geografía | Natural Earth, copia entregada | Países, escala 1:110.000.000 | Archivo utilizado y procedencia |

Conserva las URLs o parámetros de las consultas históricas, el número de registros obtenido y la fecha de extracción. Para el feed, documenta la URL, frecuencia y filtros aplicados.

El objetivo es poder reconstruir qué datos utilizaste, no pegar en la memoria miles de peticiones repetidas.

### 2.4. Apartado de calidad

Incluye las comprobaciones realizadas, las consultas o el código que las calcula, sus resultados y las decisiones tomadas.

**No se exige generar informes automáticamente, crear un archivo separado de calidad ni añadir un panel de calidad a Angular.** Puedes ejecutar las consultas o el código y documentar los resultados en esta misma memoria o notebook.

Ejemplo ficticio:

| Problema | Antes del tratamiento | Acción | Después |
|---|---|---|---|
| Versiones duplicadas | Cinco repeticiones | No insertar otra vez la misma versión | Ninguna versión duplicada en la tabla |
| Magnitud ausente | Ocho nulos | Conservar nulos; excluirlos de medias e informar del denominador | Ocho nulos identificados |
| Coordenadas inválidas | Dos registros | Conservar original y motivo en cuarentena | No entran en el conjunto válido |

Indica siempre el conjunto y el denominador: mensajes recibidos, versiones distintas o terremotos actuales. Los problemas pueden solaparse; no sumes incidencias como si necesariamente fueran registros diferentes.

Acompaña los resultados con una explicación como:

> No sustituimos la magnitud ausente por cero porque introduciríamos un valor que no proporciona la fuente y alteraríamos las estadísticas.

Considera las dimensiones de calidad del enunciado. Si no puedes verificar la exactitud física de una magnitud, explica esa limitación: comprobar que un valor es numérico no demuestra que sea exacto.

«Después» no significa que todos los valores deban estar completos. Conservar un nulo puede ser correcto. Tampoco debes inventar problemas en los datos reales; identifica por separado los casos sintéticos usados para probar el sistema.

### 2.5. Modelo conceptual, lógico y físico

Son tres formas de explicar la misma base de datos, no tres bases diferentes.

| Nivel | Pregunta que responde | Ejemplo |
|---|---|---|
| Conceptual | ¿Qué entidades existen y cómo se relacionan? | Un terremoto tiene versiones y puede asociarse a un país |
| Lógico | ¿Qué tablas, campos y claves representan esas entidades? | `evento_actual`, `evento_version`, `pais` y `fecha` |
| Físico | ¿Cómo se implementa en PostgreSQL? | Tipos SQL, restricciones, claves foráneas e índices |

Explica qué representa una fila de cada tabla. Distingue un terremoto de sus versiones. Una restricción única sobre el identificador del evento actual y otra sobre la identidad de una versión pueden ayudar a impedir duplicados.

Incluye el SQL de creación de tablas y los índices en el repositorio, y enlázalo desde la memoria. No hace falta repetir todo el SQL en el documento.

Justifica los índices por las consultas que realizan tus programas. Ejemplo:

> Creamos un índice sobre el tiempo de ocurrencia porque la API filtra frecuentemente por intervalos temporales.

No basta con indicar que «los índices mejoran el rendimiento»: explica qué operación ayudan a resolver y aporta la medición que corresponda.

### 2.6. Análisis y al menos tres conclusiones

Presenta resultados e interprétalos. Tres capturas de gráficos sin explicación no equivalen a tres conclusiones.

Ejemplos ficticios:

1. «De los 20 eventos seleccionados, 12 tienen una profundidad inferior a 70 km: el 60 %. No hay profundidades nulas en esta selección».
2. «Cinco de los 20 eventos no tienen país asignado. Se conservan y se representan como categoría independiente».
3. «Con magnitud ≥4 y el mismo ámbito geográfico, el recuento reciente es 14 frente a una media histórica diaria de 10».

Junto a cada conclusión identifica el periodo, ámbito, filtros y denominador. Incluye capturas del panel donde se puedan reconocer esos filtros, o una captura acompañada de una descripción explícita de los mismos.

La comparación histórica debe utilizar filtros equivalentes y periodos comparables. No interpretes una diferencia de recuentos como una predicción de actividad futura.

### 2.7. Complejidad, mediciones, costes y mejoras

Este contenido puede estar en otro apartado de la misma memoria; no exige un segundo documento.

**Complejidad.** Explica operaciones concretas de tu implementación, por ejemplo la búsqueda de versiones por identificador, la integración espacial y la agregación por ventanas.

Ejemplo:

> Buscar un identificador recorriendo una lista cuesta O(N) por búsqueda. Un diccionario permite búsquedas O(1) de media, pero requiere memoria adicional.

**Mediciones.** Mide tiempo y memoria con al menos tres tamaños de entrada, como establece el enunciado. Describe qué operación ejecutas, el equipo, el procedimiento y las unidades.

Puedes organizar los resultados así, rellenando las celdas con tus mediciones:

| Registros procesados | Tiempo | Memoria medida | Observaciones |
|---:|---:|---:|---|
| Tamaño pequeño | … | … | … |
| Tamaño intermedio | … | … | … |
| Tamaño grande | … | … | … |

Si generas datos sintéticos para estudiar volumen, identifícalos y no los mezcles con el análisis de sismicidad real. Razona qué cambiaría con 100 millones de registros.

**Costes.** Identifica recursos del equipo, almacenamiento, electricidad, desarrollo y mantenimiento. Distingue los valores medidos de los supuestos. No necesitas contratar servicios ni elaborar un estudio empresarial. Un servicio gratuito no elimina el tiempo de trabajo ni los recursos locales consumidos.

**Mejoras.** Propón cambios ligados a problemas concretos: optimizar una consulta lenta, reducir duplicación de archivos o repartir el consumo cuando crezca el volumen. Explica en qué situación aplicarías cada mejora.

### 2.8. Evidencias de las pruebas

Presenta las pruebas de aceptación del enunciado con resultado esperado, resultado observado y evidencia. Puedes usar consultas SQL, logs, capturas o una demostración reproducible.

Ejemplo de formato:

| Prueba | Resultado esperado | Resultado observado | Evidencia |
|---|---|---|---|
| Reenviar una versión | No aumenta el número de eventos | Completar tras probar | Consulta o log |
| Recibir una revisión posterior | Se actualizan sus datos sin sumar otro evento | Completar tras probar | Consulta antes/después |
| Recibir después una versión antigua | No sustituye a la más reciente | Completar tras probar | Datos actuales e historial |
| Coordenadas inválidas | Se conserva el rechazo y su motivo | Completar tras probar | Registro de cuarentena |
| Cerrar el túnel | El panel indica que no puede actualizar | Completar tras probar | Captura del aviso |

No marques una prueba como superada porque el comportamiento esté programado: ejecútala y registra lo que ocurre. Explica también cómo se puede repetir. No se exige una herramienta específica de pruebas.

### 2.9. Reproducción histórica

Documenta cómo enviar una selección histórica por Kafka de forma acelerada para demostrar el flujo sin esperar a que aparezcan eventos nuevos.

Explica:

- Qué selección utilizas y cómo la obtuviste.
- Cómo iniciar la reproducción y a qué velocidad envías los eventos.
- Cómo separas los datos de reproducción de los actuales.
- Cómo se identifica el modo «REPRODUCCIÓN» en el panel.
- Qué resultados esperas observar y qué ocurrió al probarlo.

Conserva las fechas originales. Si reproduces eventos de 2025, consulta ese periodo en la vista correspondiente: un filtro de «últimas 24 horas» de 2026 no los mostrará.

El archivo estático de cuatro eventos ficticios del frontend sirve para probar el mapa, pero no sustituye la reproducción a través del pipeline.

## 3. Panel y defensa

### 3.1. URL del panel

Entrega la dirección publicada en Netlify y comprueba que se abre desde otro dispositivo.

El frontend debe utilizar vuestra API, no saltarse el procesamiento consultando directamente USGS. La URL pública de la API será la HTTPS proporcionada por ngrok.

### 3.2. Procedimiento para activar el sistema

Como el backend se ejecuta en tu equipo, incluye un procedimiento breve para ponerlo en marcha durante la defensa:

1. Arrancar Docker Compose y comprobar los servicios.
2. Comprobar que la base contiene los datos necesarios y que funciona la ingesta.
3. Activar ngrok y verificar la URL de la API configurada en Angular.
4. Abrir el panel y comprobar datos, filtros y estado de actualización.
5. Preparar la reproducción histórica para cuando se solicite.

No tienes que mantener el equipo encendido permanentemente. Si lo apagas, el panel de Netlify puede seguir cargando, pero no podrá consultar el backend. Debe mostrar esa situación de forma clara.

### 3.3. Explicación individual

Durante la defensa debes poder explicar el recorrido de un evento desde USGS hasta el panel, indicando qué hace cada componente.

También debes justificar una decisión técnica propia. Por ejemplo:

> Confirmamos el offset después de guardar en PostgreSQL. Si el proceso se interrumpe y Kafka vuelve a entregar el mensaje, las restricciones y la escritura idempotente evitan duplicar el resultado.

No hace falta entregar otro documento para este punto. Es una explicación oral que permite comprobar que comprendes el sistema, también si has trabajado en equipo.

## Organización sugerida de la documentación

Los nombres son orientativos. Puedes adaptarlos si las evidencias se encuentran fácilmente.

| Archivo o carpeta | Contenido |
|---|---|
| `README.md` | Presentación, enlaces y pasos para ejecutar y demostrar |
| `docs/memoria.md` o `notebooks/memoria.ipynb` | Datos, calidad, modelo, análisis, mediciones, costes y decisiones |
| `docs/evidencias/` | Capturas, logs seleccionados y resultados de pruebas |
| `sql/` | Creación de tablas, índices y consultas relevantes |
| Código de cada servicio | Implementación y configuración del proyecto |

En el README indica dónde está cada evidencia. Es preferible una memoria clara con enlaces a varios archivos bien organizados que repetir el mismo contenido en muchos documentos.

## Lista final de comprobación

Antes de entregar, comprueba:

- [ ] El profesor puede acceder al repositorio.
- [ ] El README permite arrancar el sistema desde un entorno limpio con los requisitos indicados.
- [ ] Los cinco servicios obligatorios están definidos en Docker Compose.
- [ ] He incluido dependencias, Dockerfile(s), SQL y `.env.example` sin credenciales reales.
- [ ] He documentado campos, unidades, fuentes, filtros y consultas de extracción.
- [ ] El apartado de calidad contiene código o consultas, resultados y decisiones justificadas.
- [ ] El modelo explica entidades, tablas, claves, tipos e índices.
- [ ] Presento al menos tres conclusiones con resultados y filtros identificables.
- [ ] Incluyo mediciones con tres tamaños, análisis de complejidad, costes y mejoras.
- [ ] He ejecutado las pruebas de aceptación y guardado evidencias.
- [ ] Puedo activar la reproducción histórica sin mezclarla con datos actuales.
- [ ] La URL de Netlify funciona y sé activar el backend y el túnel durante la defensa.
- [ ] Puedo explicar individualmente el flujo completo y una decisión técnica propia.

La evaluación se realiza con la rúbrica del enunciado. **Se valora que el sistema funcione, que las evidencias sean comprobables y que puedas justificar las decisiones; no la cantidad de documentos ni su extensión.**
