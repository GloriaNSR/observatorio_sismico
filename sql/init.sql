-- Dimensión geográfica del modelo en estrella; country_id es la clave primaria
-- y su unicidad evita duplicar un país al inicializar o recargar el catálogo.
CREATE TABLE IF NOT EXISTS pais (
    country_id VARCHAR(10) PRIMARY KEY,
    country_name TEXT NOT NULL,
    continent TEXT,
    subregion TEXT
);

-- Dimensión diaria UTC para agrupar eventos por calendario; fecha_id es la
-- clave primaria única que relaciona cada hecho con su fecha.
CREATE TABLE IF NOT EXISTS fecha (
    fecha_id DATE PRIMARY KEY,
    anio INTEGER NOT NULL,
    mes INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
    dia INTEGER NOT NULL CHECK (dia BETWEEN 1 AND 31),
    dia_semana INTEGER NOT NULL CHECK (dia_semana BETWEEN 1 AND 7),
    CHECK (anio = EXTRACT(YEAR FROM fecha_id)::INTEGER),
    CHECK (mes = EXTRACT(MONTH FROM fecha_id)::INTEGER),
    CHECK (dia = EXTRACT(DAY FROM fecha_id)::INTEGER)
);

-- Tabla de hechos con el último estado por evento y modo; la clave compuesta
-- (event_id, mode) impide duplicados y aísla live de replay.
CREATE TABLE IF NOT EXISTS evento_actual (
    event_id TEXT NOT NULL,
    mode VARCHAR(10) NOT NULL
        CHECK (mode IN ('live', 'replay')),
    time_ms BIGINT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    updated_ms BIGINT NOT NULL,
    updated_time TIMESTAMPTZ NOT NULL,
    mag DOUBLE PRECISION,
    mag_type TEXT,
    place TEXT,
    longitude DOUBLE PRECISION NOT NULL
        CHECK (longitude BETWEEN -180 AND 180),
    latitude DOUBLE PRECISION NOT NULL
        CHECK (latitude BETWEEN -90 AND 90),
    depth_km DOUBLE PRECISION,
    country_id VARCHAR(10)
        REFERENCES pais(country_id) ON DELETE SET NULL,
    fecha_id DATE NOT NULL
        REFERENCES fecha(fecha_id),
    CONSTRAINT evento_actual_pkey PRIMARY KEY (event_id, mode)
);

-- Historial de revisiones del hecho; la restricción única (event_id, updated_ms,
-- mode) hace idempotente la persistencia de cada versión.
CREATE TABLE IF NOT EXISTS evento_version (
    id SERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    updated_ms BIGINT NOT NULL,
    mode VARCHAR(10) NOT NULL
        CHECK (mode IN ('live', 'replay')),
    ingested_at TIMESTAMPTZ NOT NULL,
    raw_json JSONB NOT NULL,
    CONSTRAINT evento_version_event_id_updated_ms_mode_key
        UNIQUE (event_id, updated_ms, mode)
);

-- Registro de auditoría de mensajes rechazados, con motivo y JSON original.
-- Su id serial identifica cada recepción; no se deduplican rechazos repetidos.
CREATE TABLE IF NOT EXISTS cuarentena (
    id SERIAL PRIMARY KEY,
    event_id TEXT,
    mode VARCHAR(10) NOT NULL
        CHECK (mode IN ('live', 'replay')),
    motivo TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    raw_json JSONB NOT NULL
);

-- Estado de salud por fuente; source es clave primaria y garantiza una marca
-- vigente por cada flujo de ingesta.
CREATE TABLE IF NOT EXISTS estado_ingesta (
    source VARCHAR(50) PRIMARY KEY,
    last_ingestion_ms BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Checkpoint de publicación del productor; la clave compuesta por evento y modo
-- guarda un único updated_ms observado y separa el flujo en vivo del histórico.
CREATE TABLE IF NOT EXISTS estado_productor (
    event_id TEXT NOT NULL,
    mode VARCHAR(10) NOT NULL
        CHECK (mode IN ('live', 'replay')),
    last_updated_ms BIGINT NOT NULL,
    CONSTRAINT estado_productor_pkey PRIMARY KEY (event_id, mode)
);

-- Acelera filtros temporales de hechos, segmentados por modo.
CREATE INDEX IF NOT EXISTS idx_evento_actual_event_time_mode
    ON evento_actual (event_time, mode);

-- Acelera filtros y agregaciones por magnitud; no impone unicidad.
CREATE INDEX IF NOT EXISTS idx_evento_actual_mag
    ON evento_actual (mag);

-- Acelera el cruce analítico por país, incluidos los valores NULL sin asignación.
CREATE INDEX IF NOT EXISTS idx_evento_actual_country_id
    ON evento_actual (country_id);
