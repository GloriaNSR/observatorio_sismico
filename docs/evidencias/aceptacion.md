# Evidencias de aceptación

**Fecha de ejecución:** 6 de noviembre de 2026.  
**Entorno:** cinco servicios Compose activos: PostgreSQL 16, Kafka 3.9.1, productor, consumidor y API. El ejecutor de pruebas de pytest fue temporal y no se añadió como servicio permanente.

## Comandos y validación general

```powershell
docker compose up --build -d
docker compose ps
docker compose config --quiet
docker compose run --rm --no-deps consumidor sh -c "pip install -r requirements-test.txt && python -m pytest -q tests/unit tests/contract tests/integration/test_pipeline.py tests/integration/test_replay.py tests/acceptance"
python -m pytest -q backend/tests/integration/test_infraestructura.py
cd frontend
npm run build
```

Resultados observados:

- `docker compose config --quiet`: **APROBADO**.
- `docker compose ps`: los cinco servicios indicaron un estado saludable.
- Pruebas del servidor: **37 aprobadas**, incluidas las ocho nuevas pruebas de aceptación. Pruebas de Compose y del esquema ejecutadas desde el equipo anfitrión: **2 aprobadas**. Sin fallos; se registró un aviso por el uso obsoleto de `starlette.testclient`/`httpx`.
- Angular `npm run build`: **APROBADO**; Angular CLI advierte que Leaflet usa CommonJS, lo que puede limitar algunas optimizaciones.
- `/api/eventos`, `/api/salud` y `/api/estadisticas`: HTTP 200. `/api/salud` confirmó `status=ok` y base de datos disponible. El catálogo consultado en PostgreSQL contiene 177 países.

## Once escenarios del enunciado

| # | Escenario | Resultado y evidencia |
|---:|---|---|
| 1 | Recibir un evento nuevo | **APROBADO.** `test_recibir_evento_nuevo_y_revision_mas_reciente`: se envió un Feature por Kafka; quedó una fila actual en PostgreSQL con Francia asignada y se consultó por la API junto con las estadísticas. El panel cargó las respuestas de tiempo real en el mapa, la tabla y los análisis. |
| 2 | Reenviar la misma versión | **APROBADO.** `test_reenviar_version_y_recibir_revision_antigua_es_idempotente`: cuatro mensajes para el identificador de prueba (incluido un reenvío) produjeron tres versiones con distintos valores de `updated_ms`; el duplicado no creó una versión adicional. |
| 3 | Recibir una versión posterior | **APROBADO.** El registro actual y su magnitud reflejaron la revisión con el mayor `updated_ms`; la versión anterior se conserva en el historial. |
| 4 | Recibir después una versión antigua | **APROBADO.** La revisión tardía quedó en el historial, sin reemplazar el estado actual más reciente. |
| 5 | Magnitud nula | **APROBADO.** La API conserva `mag: null`; una consulta con `min_mag=4.0` no devuelve ese evento. La interfaz muestra los valores ausentes como `ND`. |
| 6 | Coordenadas inválidas | **APROBADO.** La longitud `181` quedó en cuarentena con el motivo WGS84 y el GeoJSON original; ni ese identificador ni el evento de explosión de cantera (`quarry blast`) aparecen en `/api/eventos`. |
| 7 | Evento marítimo | **APROBADO.** Se guardó el punto `[0,0]` con `country_id: null`; las estadísticas lo incluyen bajo «Sin país asignado». El catálogo contiene 177 países. |
| 8 | Reiniciar consumidor | **APROBADO.** Se ejecutó `docker compose restart consumidor`. El desplazamiento del grupo antes y después fue `19363`, sin mensajes pendientes (`lag=0`). La cantidad de eventos y versiones reproducidos de 2025 permaneció en `18.302`/`18.302`; los cinco servicios volvieron a estar saludables. |
| 9 | Cerrar el túnel | **APROBADO con sustituto documentado.** No había un túnel ngrok real configurado. En el navegador Angular se interceptaron y cancelaron las peticiones a la API (`http://localhost:8000/**`) con el panel cargado. La interfaz mostró «Falló la actualización; se conserva en pantalla la última lectura válida», el error de conexión y la antigüedad de la lectura; además, mantuvo las 180 filas existentes. Esto no demuestra TLS, CORS ni la disponibilidad real de ngrok. |
| 10 | Volver a abrir el túnel | **APROBADO con el mismo sustituto.** Se retiró la interceptación y se solicitó una nueva consulta; desapareció la alerta, la API volvió a mostrar «API accesible» y se conservaron las 180 filas. Para repetir la prueba con ngrok real: detener y reiniciar `ngrok http 8000`, actualizar `apiUrl` si cambia el dominio y comprobar CORS. |
| 11 | Reproducción histórica | **APROBADO.** `test_replay_queda_aislado_de_live_y_controla_revisiones`: el duplicado no generó otra versión, la revisión más reciente quedó como actual y el identificador apareció en `mode=replay`, pero no en `mode=live`. También se ejecutó el cargador real con tres entidades guardadas en caché y aceleración `1.000.000`: publicó 3 mensajes, el consumidor terminó sin retraso de lectura y el total replay de 2025 siguió en 18.302 por idempotencia. La prueba `test_orden_replay_por_ocurrencia_y_revision` verifica el orden de publicación. |

Las pruebas nuevas están en `backend/tests/acceptance/test_acceptance.py`. Usan identificadores únicos, esperan la persistencia en Kafka y PostgreSQL y eliminan las filas de prueba al terminar.

## API, ingesta y calidad observadas

- Las tres rutas exigidas de la API devolvieron HTTP 200 con los cinco servicios activos.
- El consumidor procesó eventos en tiempo real del canal USGS. En la consulta de calidad de la base se encontraron mensajes reales en cuarentena: por ejemplo, `ok2026vshf` (`quarry blast`, explosión de cantera) y `uw714132811` (`explosion`); ambos se rechazaron porque `properties.type` debía ser `"earthquake"` (terremoto) y se conservaron sus coordenadas originales.
- La consulta de reproducción de 2025 devolvió 18.302 eventos y 18.302 versiones. En ese conjunto, 14.350 eventos (78,4 %) tienen `country_id` nulo y siguen siendo válidos; las estadísticas los incluyen con la etiqueta «Sin país asignado».
- La memoria con los datos, su procedencia y las consultas de calidad está en [../memoria.md](../memoria.md).

## Procedimiento para una demostración con túnel real

1. Iniciar los cinco servicios y el panel local; comprobar `/api/salud`.
2. Ejecutar `ngrok http 8000`, introducir la URL HTTPS resultante en `frontend/public/config.json` y permitir los orígenes localhost/Netlify en `CORS_ORIGINS`.
3. Con el panel abierto, detener ngrok y cambiar un filtro para provocar una petición. Registrar el aviso, las filas que se conservan y el tiempo transcurrido desde la última lectura.
4. Volver a iniciar ngrok. Si cambia la URL, actualizar `apiUrl` y verificar que el panel se recupera sin reiniciar Angular.

La ejecución registrada de los pasos 3 y 4 utilizó la interceptación de red del navegador como sustituto porque no había un túnel real disponible. Antes de una demostración pública, todavía se deben comprobar la URL HTTPS, CORS y las cuotas de la cuenta ngrok utilizada.
