-- Auditoría acotada de consultas al feed live; el GeoJSON de cada evento se
-- conserva por versión en evento_version o, si se rechaza, en cuarentena.
CREATE TABLE IF NOT EXISTS consulta_usgs (
    id BIGSERIAL PRIMARY KEY,
    requested_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    source_url TEXT NOT NULL,
    status VARCHAR(10) NOT NULL CHECK (status IN ('ok', 'error')),
    http_status INTEGER,
    response_bytes BIGINT CHECK (response_bytes IS NULL OR response_bytes >= 0),
    latency_ms DOUBLE PRECISION CHECK (latency_ms IS NULL OR latency_ms >= 0),
    feature_count INTEGER CHECK (feature_count IS NULL OR feature_count >= 0),
    published_count INTEGER CHECK (published_count IS NULL OR published_count >= 0),
    source_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_consulta_usgs_requested_at
    ON consulta_usgs (requested_at);
