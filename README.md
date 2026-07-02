# P2: Live Fleet Operations Dashboard
### Azure Event Hub + Databricks Structured Streaming + Delta Lake

> **Business problem:** Ops finds out about delivery delays from customer complaints — not from data.  
> **Solution:** Sub-minute anomaly detection on live GPS telemetry. Ops sees every vehicle. Speed breach or engine anomaly triggers an alert in **under 60 seconds** — before the customer calls.

---

## Architecture

```
Python Producer
(10 vehicles, configurable rate)
        │
        ▼ JSON events (vehicle_id, lat, lon, speed, fuel, engine_temp)
Azure Event Hub
(Kafka-compatible endpoint, partitioned by vehicle_id)
        │
        ▼ readStream, explicit schema, checkpointed every 30s
Bronze Delta Table  ◄── fault-tolerant, append-only, partitioned by route_id
(fleet_ops.bronze.raw_telemetry)
        │
        ▼ withWatermark("event_time", "10 minutes") + window("1 minute")
Silver Delta Table  ◄── windowed agg: avg/max speed, temp, fuel per vehicle per minute
(fleet_ops.silver.vehicle_window_stats)
        │  + stream-static join with vehicle_dimension (driver, route, max_speed)
        ▼ foreachBatch → MERGE INTO
Gold Alerts Table   ◄── one row per vehicle, current alert state (OPEN/RESOLVED)
(fleet_ops.gold.fleet_alerts)
        │
        ▼ DirectQuery
Power BI Dashboard  ◄── fleet map, SLA compliance, anomaly count by route
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Azure Event Hub (Kafka API) |
| Stream Processing | Databricks Structured Streaming (PySpark) |
| Storage | ADLS Gen2 — Bronze, Silver, Gold containers |
| Table Format | Delta Lake (ACID, time travel, CDF) |
| Governance | Unity Catalog (external locations, table grants) |
| Orchestration | Databricks Workflows (notebook tasks) |
| Dashboard | Power BI DirectQuery |
| CI | GitHub Actions |

---

## Business Metric Impact

| Before | After |
|--------|-------|
| 30-minute batch lag | Sub-60-second alert latency |
| Reactive (customer complains) | Proactive (ops intervenes first) |
| No live location visibility | Live fleet map, per-vehicle status |
| SLA breach discovered post-delivery | SLA breach predicted mid-route |

---

## Repository Structure

```
p2-live-fleet-ops/
├── producer/
│   └── telemetry_producer.py       # Python Kafka producer (10 vehicles, anomaly injection)
├── notebooks/
│   ├── 00_setup_unity_catalog.py   # Catalog, schemas, external locations, reference table
│   ├── 01_bronze_ingestion.py      # Event Hub → Bronze Delta (checkpointed stream)
│   ├── 02_silver_windowed_agg.py   # Watermark + tumbling window + stream-static join
│   ├── 03_gold_alerts.py           # Business rules → MERGE INTO Gold alerts table
│   └── 04_checkpoint_recovery_demo.py  # Step-by-step proof of no data loss on restart
├── sql/
│   └── ddl_all_tables.sql          # All DDL: catalog, schemas, tables, dashboard queries
├── data/reference/
│   └── vehicle_reference.csv       # Vehicle dimension (10 vehicles, routes, max speeds)
├── tests/
│   └── test_producer.py            # 12 unit tests for producer logic
├── config/
│   └── .env.template               # Environment variable template
└── .github/workflows/
    └── ci.yml                      # Lint + unit test on push
```

---

## Data Model

### Bronze — `fleet_ops.bronze.raw_telemetry`
Raw events from Event Hub. Append-only. Partitioned by `route_id`.

| Column | Type | Notes |
|--------|------|-------|
| vehicle_id | STRING | Partition key in Kafka |
| event_time | TIMESTAMP | Parsed from producer ISO timestamp |
| lat, lon | DOUBLE | GPS coordinates |
| speed_kmh | FLOAT | Current speed |
| fuel_pct | FLOAT | Fuel level 0–100 |
| engine_temp_c | FLOAT | Engine temperature °C |
| event_type | STRING | normal / speed_breach / engine_overheat / fuel_critical |
| kafka_partition | INT | For gap detection / recovery audit |
| kafka_offset | BIGINT | For gap detection / recovery audit |

### Silver — `fleet_ops.silver.vehicle_window_stats`
1-minute tumbling window aggregates, enriched with vehicle dimension.

| Column | Notes |
|--------|-------|
| window_start / window_end | 1-minute tumbling window boundaries |
| avg_speed_kmh, max_speed_kmh | Speed stats for window |
| avg_engine_temp_c, max_engine_temp_c | Temperature stats |
| min_fuel_pct | Lowest fuel reading in window |
| anomaly_event_count | Count of non-normal events |
| driver_name, route_name, rated_max_speed | From stream-static join |
| speed_pct_of_max | avg_speed / rated_max * 100 |

### Gold — `fleet_ops.gold.fleet_alerts`
One row per vehicle. MERGE INTO upsert on each micro-batch.

| Column | Notes |
|--------|-------|
| alert_type | speed_breach / engine_overheat / fuel_critical / multi_anomaly |
| severity | CRITICAL / WARNING |
| alert_status | OPEN / RESOLVED |
| alert_raised_at | When alert first opened |
| last_updated_at | Last micro-batch update time |

---

## Alert Rules

| Rule | Threshold | Severity |
|------|-----------|----------|
| Engine overheat (critical) | engine_temp_c > 110°C | CRITICAL |
| Speed breach | avg_speed > rated_max × 1.15 | CRITICAL |
| Engine overheat (warning) | engine_temp_c > 95°C | WARNING |
| Fuel critical | fuel_pct < 15% | WARNING |
| Multi-anomaly | ≥ 3 anomaly events in 1-min window | WARNING |

---

## Setup & Run

### Prerequisites
- Azure subscription with Event Hub namespace + ADLS Gen2 account
- Databricks workspace (DBR 13.3 LTS+, Unity Catalog enabled)
- Databricks Secret Scope: `fleet-scope`

### 1. Configure Secrets
```bash
databricks secrets create-scope --scope fleet-scope

databricks secrets put --scope fleet-scope --key eh-namespace
databricks secrets put --scope fleet-scope --key eh-name
databricks secrets put --scope fleet-scope --key eh-connection-string
databricks secrets put --scope fleet-scope --key adls-account
databricks secrets put --scope fleet-scope --key adls-key
```

### 2. Upload Reference Data
```bash
# Upload vehicle_reference.csv to ADLS:
# abfss://bronze@<account>.dfs.core.windows.net/reference/vehicle_reference.csv
azcopy copy data/reference/vehicle_reference.csv \
  "https://<account>.dfs.core.windows.net/bronze/reference/vehicle_reference.csv"
```

### 3. Run Notebooks in Order
```
00_setup_unity_catalog.py    → Creates catalog, schemas, loads reference table
01_bronze_ingestion.py       → Starts Bronze stream (keep running)
02_silver_windowed_agg.py    → Starts Silver stream (keep running)
03_gold_alerts.py            → Starts Gold alert stream (keep running)
```

### 4. Start the Producer
```bash
pip install confluent-kafka python-dotenv
cp config/.env.template config/.env   # fill in your values

# Normal run (50 events/sec, 10 vehicles)
python producer/telemetry_producer.py --rate 50 --vehicles 10

# High-anomaly demo (20% anomaly rate for dashboard demo)
python producer/telemetry_producer.py --rate 50 --anomaly 0.20

# Dry run (no Kafka connection, print to stdout)
python producer/telemetry_producer.py --dry-run --vehicles 2 --duration 10
```

### 5. Checkpoint Recovery Demo
Run `04_checkpoint_recovery_demo.py` step by step — proves zero data loss after stream kill.

---

## Key Design Decisions & Interview Answers

### Why explicit schema instead of inferSchema?
`inferSchema` reads a sample batch before starting the stream — adds latency and fails if the first batch is empty. Explicit schema fails fast at parse time, lets you route bad records to a dead-letter path, and documents the contract.

### Why 10-minute watermark?
GPS devices upload in bursts; network delays can hold events for several minutes. 10 minutes tolerates real-world IoT delays without accumulating unbounded state. Beyond 10 minutes, the event is too stale to affect real-time operations.

### Why append mode for Silver (not update/complete)?
With watermark, Structured Streaming guarantees a window emits exactly once — after the watermark passes the window end. Append mode is correct: no partial results, no full rewrite. Update mode would stream partial aggregates on every trigger.

### Why foreachBatch for Gold?
MERGE INTO (upsert) requires the DeltaTable API, which is only available in batch context. `foreachBatch` gives a regular DataFrame per micro-batch where any Spark/Delta API is available. The function is idempotent: MERGE INTO on the same data produces the same result.

### Why stream-static join (not stream-stream)?
The vehicle dimension changes infrequently (not a stream). Stream-static join broadcasts the static side, avoids managing state for both sides, and is simpler to reason about. For high-frequency dimension changes, use `foreachBatch` and re-read the dimension inside each batch.

### How does checkpoint recovery work?
The checkpoint stores Kafka consumer offsets per partition after each committed micro-batch. On restart, Structured Streaming reads the checkpoint, tells the Kafka consumer to resume from the last committed offset, and Delta's transaction log prevents partial writes — so there are no duplicates and no gaps.

---

## Power BI DirectQuery Setup

1. Open Power BI Desktop → Get Data → Azure → Azure Databricks
2. Connect to your Databricks workspace (Personal Access Token)
3. Select `fleet_ops.gold.fleet_alerts` as the source table
4. Set connection mode to **DirectQuery** (not Import)
5. Build visuals:
   - **Map visual:** lat/lon fields, color by severity
   - **Card:** COUNT of OPEN CRITICAL alerts
   - **Table:** vehicle_id, driver_name, alert_type, speed_pct_of_max, last_updated_at
   - **Bar chart:** anomaly_event_count by route_name (from Silver table)
6. Set auto-refresh: Page refresh → Fixed interval → 30 seconds

---

*Built by Alvin David | Azure / Databricks Data Engineer*  
*Stack: Python · Kafka · Azure Event Hub · Databricks Structured Streaming · Delta Lake · Unity Catalog · Power BI*
