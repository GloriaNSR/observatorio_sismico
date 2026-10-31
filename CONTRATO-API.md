# Contrato mínimo de la API

## GET /api/eventos

Devuelve HTTP 200, `Content-Type: application/json` y un GeoJSON `FeatureCollection`. Sin eventos: `features: []`, no un error. Cada evento aparece una sola vez, con su versión actual. Por defecto devuelve terremotos ocurridos en las últimas 24 horas.

```json
{
  "type": "FeatureCollection",
  "metadata": {
    "generated": 1793113200000,
    "last_ingestion": 1793113140000,
    "mode": "live",
    "count": 1,
    "total": 1,
    "truncated": false,
    "next_cursor": null
  },
  "features": [{
    "type": "Feature",
    "id": "ejemplo-ficticio",
    "geometry": {"type": "Point", "coordinates": [-3.7, 40.4, 12]},
    "properties": {
      "mag": 3.2,
      "magType": "ml",
      "place": "Ejemplo ficticio",
      "time": 1793109600000,
      "updated": 1793110000000,
      "country_id": "ESP",
      "country_name": "España"
    }
  }]
}
```

`generated`: generación de esta respuesta; no implica que la ingesta siga activa. `last_ingestion`: última consulta correcta a USGS. Fechas en milisegundos Unix UTC. Coordenadas en orden longitud, latitud, profundidad (km). Usa `null`, nunca NaN/Infinity, para magnitud, profundidad o país desconocidos. Geometría válida obligatoria en este endpoint.

Filtros: `start` (ISO8601 con zona horaria, inclusive), `end` (ISO8601 con zona horaria, exclusive), `min_mag`, `country_id` y `mode` (`live` por defecto o `replay`). `country_id=unassigned` selecciona eventos sin país. Si no se indican fechas, el periodo predeterminado son las últimas 24 horas. La magnitud nula no cumple un filtro `min_mag` y debe contabilizarse por separado.

La API limita la página a 1.000 features por defecto y acepta `limit` (1–5.000) y `offset`. Si trunca, indica `truncated: true`, el `total` completo y `next_cursor` (offset de continuación). `count` es el número de features de esa página. Las estadísticas se calculan sobre todos los resultados filtrados, no solo sobre la página.

## Otros endpoints a implementar

| Ruta | Resultado |
|---|---|
| GET /api/salud | Estado API y fecha de última ingesta; no credenciales |
| GET /api/estadisticas | Conteos por hora UTC, país/subregión y país nulo; distribuciones y comparación histórica |

Las estadísticas contienen `metadata`, `events_by_country` (incluye «Sin país asignado»), `events_by_hour` (ventanas fijas UTC de una hora), `magnitude_distribution`, `depth_distribution` e `historical_comparison`. Las distribuciones informan `null_count`/`excluded_nulls`; los intervalos de magnitud usan pasos de 0,5 y los de profundidad pasos de 50 km. La comparación fija las últimas 24 horas live con magnitud mínima 4 frente al recuento replay de 2025 dividido entre 365, conserva el filtro geográfico solicitado e indica explícitamente periodo, unidad y días con cero eventos. Los filtros de las agregaciones corresponden al mapa. Si la base falla, devuelve un error HTTP 503 explícito; nunca cero eventos como si fuera un resultado válido.

`GET /api/salud` devuelve `status`, disponibilidad de API/base de datos, `last_ingestion` en milisegundos Unix UTC, antigüedad y estado de frescura; no expone credenciales.

## Acceso desde Netlify

Configura CORS en FastAPI para el origen exacto de la web y `http://localhost:4200` durante desarrollo; métodos GET y OPTIONS, y cabecera `ngrok-skip-browser-warning`. Si utilizas `CORSMiddleware`, este middleware atiende el preflight. No necesitas cookies para una API pública de consulta de estos datos.

El encabezado de ngrok evita su página intermedia cuando es aplicable; no autentica al usuario. No incluir secretos en config.json. Añade timeout y manejo de errores en el frontend.

## Transporte y contenedores

La API es un servicio Docker, igual que productor, consumidor, Kafka y PostgreSQL. Uvicorn escucha en `0.0.0.0:8000`. ngrok termina el HTTPS público y gestiona el certificado; la API recibe HTTP local. El frontend utiliza la URL HTTPS del túnel. Desde otro contenedor de la misma red, el agente se conecta a `http://api:8000`; desde el host, al puerto publicado.
