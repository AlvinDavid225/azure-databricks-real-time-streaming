-- =============================================================
-- P2 Fleet Operations — SQL DDL
-- Run via Notebook 00 or Databricks SQL Editor
-- Catalog: fleet_ops | Schemas: bronze, silver, gold, reference
-- =============================================================

-- ── Catalog & Schemas ─────────────────────────────────────────
CREATE CATALOG IF NOT EXISTS fleet_ops
COMMENT 'P2: Live Fleet Operations Lakehouse';

CREATE SCHEMA IF NOT EXISTS fleet_ops.bronze    COMMENT 'Raw ingestion layer';
CREATE SCHEMA IF NOT EXISTS fleet_ops.silver    COMMENT 'Cleaned and aggregated';
CREATE SCHEMA IF NOT EXISTS fleet_ops.gold      COMMENT 'Business-ready alerts';
CREATE SCHEMA IF NOT EXISTS fleet_ops.reference COMMENT 'Dimension tables';

-- ── Reference: Vehicle Dimension ─────────────────────────────
CREATE TABLE IF NOT EXISTS fleet_ops.reference.vehicle_dimension (
    vehicle_id    STRING  NOT NULL,
    driver_name   STRING,
    route_id      STRING,
    route_name    STRING,
    vehicle_type  STRING,
    max_speed_kmh INT,
    capacity_kg   INT,
    home_depot    STRING
)
USING DELTA
COMMENT 'Static vehicle reference — driver, route, capacity';

-- ── Bronze: Raw Telemetry ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS fleet_ops.bronze.raw_telemetry (
    vehicle_id        STRING,
    event_time        TIMESTAMP,
    lat               DOUBLE,
    lon               DOUBLE,
    speed_kmh         FLOAT,
    fuel_pct          FLOAT,
    engine_temp_c     FLOAT,
    odometer_km       FLOAT,
    event_type        STRING,
    route_id          STRING,
    kafka_partition   INT,
    kafka_offset      BIGINT,
    kafka_enqueue_time TIMESTAMP,
    ingestion_time    TIMESTAMP
)
USING DELTA
PARTITIONED BY (route_id)
COMMENT 'Bronze: raw Event Hub telemetry, checkpointed stream, append-only'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- ── Silver: Windowed Aggregations ────────────────────────────
CREATE TABLE IF NOT EXISTS fleet_ops.silver.vehicle_window_stats (
    vehicle_id          STRING,
    route_id            STRING,
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    avg_speed_kmh       DOUBLE,
    max_speed_kmh       DOUBLE,
    avg_engine_temp_c   DOUBLE,
    max_engine_temp_c   DOUBLE,
    min_fuel_pct        DOUBLE,
    event_count         BIGINT,
    anomaly_event_count BIGINT,
    last_lat            DOUBLE,
    last_lon            DOUBLE,
    -- from stream-static join
    driver_name         STRING,
    route_name          STRING,
    vehicle_type        STRING,
    rated_max_speed     INT,
    home_depot          STRING,
    speed_pct_of_max    DOUBLE,
    ingestion_time      TIMESTAMP
)
USING DELTA
COMMENT 'Silver: 1-minute tumbling window aggregations enriched with vehicle dimension'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ── Gold: Fleet Alerts ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fleet_ops.gold.fleet_alerts (
    alert_id            STRING,
    vehicle_id          STRING  NOT NULL,
    driver_name         STRING,
    route_name          STRING,
    home_depot          STRING,
    alert_type          STRING  COMMENT 'speed_breach|engine_overheat|fuel_critical|multi_anomaly',
    severity            STRING  COMMENT 'WARNING|CRITICAL',
    alert_status        STRING  COMMENT 'OPEN|RESOLVED',
    avg_speed_kmh       DOUBLE,
    max_speed_kmh       DOUBLE,
    rated_max_speed     DOUBLE,
    speed_pct_of_max    DOUBLE,
    avg_engine_temp_c   DOUBLE,
    min_fuel_pct        DOUBLE,
    anomaly_event_count BIGINT,
    last_lat            DOUBLE,
    last_lon            DOUBLE,
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    alert_raised_at     TIMESTAMP,
    last_updated_at     TIMESTAMP,
    resolved_at         TIMESTAMP
)
USING DELTA
COMMENT 'Gold: one row per vehicle, current alert state, Power BI DirectQuery source'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'       = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- ── Dashboard Queries ─────────────────────────────────────────

-- Current fleet status (Power BI main query)
-- SELECT
--     vehicle_id, driver_name, route_name,
--     alert_status, severity, alert_type,
--     ROUND(avg_speed_kmh, 1)       AS avg_speed_kmh,
--     ROUND(rated_max_speed, 0)     AS rated_max_kmh,
--     ROUND(speed_pct_of_max, 1)    AS speed_pct,
--     ROUND(avg_engine_temp_c, 1)   AS engine_temp_c,
--     ROUND(min_fuel_pct, 1)        AS fuel_pct,
--     last_lat, last_lon,
--     alert_raised_at, last_updated_at
-- FROM fleet_ops.gold.fleet_alerts
-- ORDER BY
--     CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,
--     alert_raised_at DESC;

-- SLA compliance by route (last 1 hour)
-- SELECT
--     route_name,
--     COUNT(*) AS total_windows,
--     SUM(CASE WHEN anomaly_event_count > 0 THEN 1 ELSE 0 END) AS anomaly_windows,
--     ROUND(100.0 * SUM(CASE WHEN anomaly_event_count = 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS sla_pct
-- FROM fleet_ops.silver.vehicle_window_stats
-- WHERE window_start >= current_timestamp() - INTERVAL 1 HOUR
-- GROUP BY route_name;

-- Speed distribution (histogram buckets)
-- SELECT
--     CASE
--         WHEN speed_pct_of_max < 60  THEN '0-60%'
--         WHEN speed_pct_of_max < 80  THEN '60-80%'
--         WHEN speed_pct_of_max < 100 THEN '80-100%'
--         WHEN speed_pct_of_max < 115 THEN '100-115%'
--         ELSE '115%+ BREACH'
--     END AS speed_bucket,
--     COUNT(*) AS window_count
-- FROM fleet_ops.silver.vehicle_window_stats
-- WHERE window_start >= current_timestamp() - INTERVAL 1 HOUR
-- GROUP BY 1 ORDER BY 1;
