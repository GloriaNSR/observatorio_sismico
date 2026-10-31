# Guía rápida: Validar el diseño de implementación

## Prerrequisitos

- Docker Engine y Docker Compose
- Node.js 22.12+ (rama 22) o Node 24, y npm
- Acceso a USGS para la ingesta en vivo e histórica
- La aplicación Angular entregada en `frontend/`

## 1. Arrancar los cinco servicios

Desde la raíz del repositorio:

```powershell
docker compose up --build -d
docker compose ps
```

Resultado esperado: `postgres`, `kafka`, `productor`, `consumidor` y `api` en estado saludable/en ejecución; volumen persistente de PostgreSQL; API publicada únicamente en `127.0.0.1:8000`.

## 2. Comprobar salud y consulta de eventos

```powershell
curl.exe -i http://127.0.0.1:8000/api/salud
curl.exe -i "http://127.0.0.1:8000/api/eventos?mode=live"
```

Resultado esperado: el endpoint de salud informa la última ingesta correcta y su frescura; eventos devuelve `FeatureCollection`, `metadata` y solo la ventana predeterminada de 24 horas. Si todavía no hay coincidencias, devuelve `features: []`, no un error.

## 3. Comprobar filtros y estadísticas

```powershell
curl.exe -i "http://127.0.0.1:8000/api/eventos?start=2025-01-01T00:00:00Z&end=2025-02-01T00:00:00Z&min_mag=4&mode=replay"
curl.exe -i "http://127.0.0.1:8000/api/estadisticas?start=2025-01-01T00:00:00Z&end=2025-02-01T00:00:00Z&min_mag=4&mode=replay"
```

Resultado esperado: ambos endpoints aplican los mismos límites temporales y filtros. Las horas y los límites se evalúan en UTC; el inicio es inclusivo y el final exclusivo. Las estadísticas describen todos los resultados filtrados y separan los valores ausentes de los numéricos.

## 4. Ejecutar el frontend suministrado

```powershell
Set-Location frontend
npm ci
npm start
```

Abrir `http://localhost:4200`. Comprobar que los filtros actualizan el mapa, la tabla y las estadísticas usando el mismo conjunto; que la tabla sea accesible y muestre «ND» para magnitud/profundidad nulas; y que el panel refleje la salud y conserve la última lectura cuando falle el backend.

## 5. Revisar aislamiento, reintentos y persistencia

1. Verificar que publicar el mismo `(event_id, updated_ms, mode)` no aumenta las versiones ni los recuentos.
2. Procesar una revisión más nueva y luego una antigua; la proyección `(event_id, mode)` conserva el `updated_ms` más alto.
3. Confirmar que live y replay producen filas distintas y que las consultas filtran por el modo solicitado.
4. Confirmar que una geometría/tipo inválidos quedan en `cuarentena` junto con el JSON original y el motivo.
5. Reiniciar los servicios y verificar que PostgreSQL y el estado de publicación se conservan.
6. Confirmar que el offset Kafka solo avanza después de confirmar la transacción PostgreSQL.

## 6. Validar carga histórica

Ejecutar el comando de `cargar_historico.py` documentado por su implementación para cargar/reproducir el año 2025. Comprobar los doce intervalos mensuales, la subdivisión de intervalos que se acerquen al máximo de USGS, la deduplicación de límites y `mode=replay`. Un reintento no debe crear versiones duplicadas.

## Resultado esperado

Los cinco servicios arrancan mediante un único Compose; la API y la interfaz comparten criterios de filtrado; los modos permanecen aislados; los eventos inválidos son trazables; los nulos no se convierten en cero; y los fallos de consulta se muestran explícitamente mientras se conserva la última lectura correcta.
