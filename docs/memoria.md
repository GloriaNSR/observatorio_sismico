# Memoria del observatorio sísmico

## Ingesta histórica USGS de 2025

La fuente es el catálogo USGS FDSN Event API:
`https://earthquake.usgs.gov/fdsnws/event/1/query`. Se consulta en intervalos
mensuales UTC con `format=geojson`, `minmagnitude=4`, `eventtype=earthquake`,
`orderby=time-asc` y `limit=20000`. Los intervalos que devuelven 18.000
resultados o más se subdividen recursivamente en mitades contiguas; el final de
cada solicitud se expresa como el último milisegundo anterior al límite
exclusivo. Las consultas y reintentos respetan timeout, HTTP 429 y errores 5xx.

Los GeoJSON de cada consulta y el catálogo deduplicado se conservan en
`datos/historico/2025/`, excluido de Git y montado como volumen en el servicio
`productor`. `manifest.json` registra para cada intervalo URL, parámetros,
marcas temporales UTC, fecha de extracción, conteo informado/recibido y si se
reutilizó la caché. Los archivos existentes se vuelven a utilizar, por lo que
ejecutar otra vez el cargador no solicita de nuevo segmentos ya archivados.
Los duplicados exactos se identifican por `(id, updated)`; las revisiones
distintas permanecen en el flujo.

Para descargar y publicar en Kafka:

```powershell
docker compose exec -T productor python cargar_historico.py
```

La publicación usa `mode=replay`, conserva `time`/`updated`, sigue el orden
`(time, updated, id)` y se acelera mediante `REPLAY_SPEED_FACTOR`. El consumidor
aplica la misma validación, cruce espacial y transacción PostgreSQL que al flujo
live. Se incluyen eventos sintéticos deterministas de demostración con
duplicado, revisión posterior y versión antigua tardía; su `event_time` queda
en 2026 para no alterar la media del histórico 2025.

### Evidencia de extracción

La fecha y los conteos de la ejecución efectiva, junto con todas las consultas,
se conservan en `datos/historico/2025/manifest.json`. El campo `summary` del
manifiesto contiene el total informado, el total descargado y el total
deduplicado. La primera extracción se realizó el `2026-10-31T13:24:33Z`. Se
consultaron 12 intervalos mensuales; ninguno alcanzó el umbral de subdivisión
de 18.000. El catálogo devolvió 18.302 features, que produjeron 18.302 claves
únicas `(id, updated)` (0 duplicados exactos). El manifiesto registra los
parámetros y el conteo individual de cada segmento.

La reproducción publicó 18.306 mensajes Kafka: 18.302 features históricos y
cuatro mensajes deterministas de demostración. La restricción de versiones
idempotentes deja 18.304 versiones replay únicas en PostgreSQL: las dos
versiones de demostración son la revisión inicial y la posterior; el duplicado
y la revisión tardía no crean versiones adicionales. El evento sintético tiene
ocurrencia en 2026 y no forma parte del cálculo 2025.

- Archivo fuente: `datos/historico/2025/eventos_2025.geojson`.
- Metadatos de consulta: `datos/historico/2025/manifest.json`.
- Features históricos cargados con magnitud mínima 4: 18.302.
- Conteo actual live/replay y fecha exacta: verificables en PostgreSQL y el
  manifiesto persistente.
- Limitaciones: la API del catálogo puede cambiar con el tiempo; los GeoJSON
  archivados preservan la respuesta obtenida para que una repetición local sea
  reproducible sin otra petición a USGS.
