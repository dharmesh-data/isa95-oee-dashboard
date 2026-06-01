-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── OEE metrics hypertable ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS oee_metrics (
    time            TIMESTAMPTZ NOT NULL,
    line_id         TEXT NOT NULL,
    availability    NUMERIC(5,4),
    performance     NUMERIC(5,4),
    quality         NUMERIC(5,4),
    oee             NUMERIC(5,4),
    total_units     INTEGER,
    good_units      INTEGER,
    PRIMARY KEY (time, line_id)
);

SELECT create_hypertable('oee_metrics', 'time',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_oee_line_time ON oee_metrics (line_id, time DESC);

-- ── Quality stats hypertable ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS quality_stats (
    time            TIMESTAMPTZ NOT NULL,
    line_id         TEXT NOT NULL,
    rolling_cpk     NUMERIC(6,3),
    rolling_mean    NUMERIC(10,4),
    rolling_stddev  NUMERIC(10,4),
    window_batches  INTEGER,
    PRIMARY KEY (time, line_id)
);

SELECT create_hypertable('quality_stats', 'time',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_quality_line_time ON quality_stats (line_id, time DESC);

-- ── Alerts table ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS alerts (
    id              SERIAL PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL,
    equipment_id    TEXT NOT NULL,
    line_id         TEXT NOT NULL,
    alert_type      TEXT NOT NULL,
    severity        TEXT NOT NULL,
    temperature_c   NUMERIC(6,2),
    fault_code      TEXT,
    acknowledged    BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_line_time   ON alerts (line_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_unacked     ON alerts (acknowledged, severity) WHERE acknowledged = FALSE;

-- ── Retention policies (7 days hot storage) ───────────────────────────────────

SELECT add_retention_policy('oee_metrics',   INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('quality_stats', INTERVAL '7 days', if_not_exists => TRUE);

-- ── Continuous aggregate: hourly OEE (speeds up Grafana queries) ──────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS oee_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    line_id,
    AVG(oee)          AS avg_oee,
    MIN(oee)          AS min_oee,
    MAX(oee)          AS max_oee,
    AVG(availability) AS avg_availability,
    AVG(performance)  AS avg_performance,
    AVG(quality)      AS avg_quality
FROM oee_metrics
GROUP BY bucket, line_id;

SELECT add_continuous_aggregate_policy('oee_hourly',
    start_offset      => INTERVAL '3 hours',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists     => TRUE
);
