# Azure Databricks Real-Time Fleet Operations Platform
### End-to-End Streaming Pipeline — Azure Event Hubs + Databricks Structured Streaming + Delta Lake

> **Business problem:** Operations finds out about delivery delays from customer complaints — not from data.
> **Solution:** Sub-60-second anomaly detection on live GPS telemetry. Ops sees every vehicle. Speed breach or engine anomaly triggers an alert before the customer calls.

---

## Architecture

![Architecture](screenshots/architecture.png)

## Architecture Components

| Component | Purpose |
|-----------|---------|
| Python Producer | Generates live fleet telemetry for 10 vehicles |
| Azure Event Hubs | Kafka-compatible streaming ingestion endpoint |
| Bronze Delta Lake | Raw append-only event storage with explicit schema |
| Silver Delta Lake | Windowed aggregations and vehicle enrichment |
| Gold Delta Lake | Live alert state — one row per vehicle |
| Databricks AI/BI | Real-time fleet operations dashboard |

---

## Project Metrics

| Metric | Value |
|--------|-------|
| Events Processed | 163,000+ |
| Vehicles Simulated | 10 |
| Event Hub Partitions | 4 |
| Active Streams | 3 (Bronze, Silver, Gold simultaneously) |
| Anomaly Injection Rate | 30% |
| Alert Latency | Under 60 seconds |
| Storage Layers | Bronze, Silver, Gold (Medallion Architecture) |
| Dashboard Refresh | Near real-time (DirectQuery) |

---

## Dashboard

![Dashboard](screenshots/dashboard.png)

**Fleet Operations Mission Control** — 4 KPI cards (Fleet Size, Open Alerts, Critical Alerts, Fleet Health Score), severity donut chart, route risk ranking, top risk vehicles, fuel critical table.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Azure Event Hubs (Kafka API endpoint) |
| Stream Processing | Databricks Structured Streaming (PySpark) |
| Storage | ADLS Gen2 — Bronze, Silver, Gold containers |
| Table Format | Delta Lake (ACID transactions, time travel) |
| Governance | Unity Catalog (3-level namespace: catalog.schema.table) |
| Dashboard | Databricks AI/BI (DirectQuery — no import lag) |

---

## Business Impact

| Before | After |
|--------|-------|
| 30-minute batch lag | Under 60 seconds alert latency |
| Reactive — customer complains first | Proactive — ops intervenes first |
| No live location visibility | Live fleet map, per-vehicle status |
| SLA breach discovered post-delivery | SLA breach predicted mid-route |

---

## Pipeline Screenshots

### Azure Infrastructure
![Azure Resources](screenshots/azure_resources.png)

### Event Hub Namespace
![Event Hub Namespace](screenshots/eventhub_namespace.png)

### Event Hubs — Live Data Flow
![Event Hub Metrics](screenshots/eventhub_metrics.png)

### Producer — 163K Events, Zero Errors
![Producer Running](screenshots/producer_running.png)

### Unity Catalog — Medallion Architecture
![Medallion Catalog](screenshots/medallion_catalog.png)

### Bronze Layer — 163K Events with Severity Distribution
![Bronze Distribution](screenshots/bronze_distribution.png)

### Silver Layer — Windowed Aggregations
![Silver Aggregation](screenshots/silver_aggregation.png)

### Gold Layer — Live Alert State
![Gold Alerts](screenshots/gold_alerts.png)

### 3 Streams Running Simultaneously
![Streams Active](screenshots/streams_active.png)

### ADLS Gen2 — Bronze, Silver, Gold Containers
![ADLS Containers](screenshots/adls_containers.png)

---

## Repository Structure

```
azure-databricks-real-time-streaming/
├── producer/
│   └── telemetry_producer.py        # Python Kafka producer — 10 vehicles, anomaly injection
├── notebooks/
│   ├── 00_setup.ipynb               # Cluster test, ADLS connection, reference table
│   ├── 01_bronze_ingestion.ipynb    # Event Hubs → Bronze Delta Lake (checkpointed stream)
│   ├── 02_silver_windowed_agg.ipynb # Watermark + tumbling window + stream-static join
│   └── 03_gold_alerts.ipynb         # foreachBatch → MERGE INTO Gold alerts table
├── sql/
│   └── ddl_all_tables.sql           # DDL for all Delta tables
├── data/reference/
│   └── vehicle_reference.csv        # 10 vehicles, routes, max speeds
├── config/
│   └── .env.template                # Environment variable template
├── screenshots/                     # Project evidence screenshots
└── README.md
```

---

## Data Model

### Bronze — `raw_telemetry`
Raw events from Event Hubs. Append-only. 163K+ rows.

| Column | Type | Notes |
|--------|------|-------|
| vehicle_id | STRING | Kafka partition key |
| event_time | TIMESTAMP | Parsed from ISO timestamp |
| alert_type | STRING | Derived from event_type (engine_overheat, speed_breach, etc.) |
| severity | STRING | CRITICAL / HIGH / MEDIUM / LOW / NORMAL |
| speed_kmh | FLOAT | Current speed |
| fuel_pct | FLOAT | Fuel level 0-100 |
| engine_temp_c | FLOAT | Engine temperature °C |
| kafka_partition | INT | For gap detection / recovery audit |
| kafka_offset | BIGINT | For checkpoint recovery audit |

### Silver — `vehicle_window_stats`
1-minute tumbling window aggregates enriched with vehicle dimension.

| Column | Notes |
|--------|-------|
| window_start / window_end | 1-minute tumbling window boundaries |
| avg_speed_kmh, max_speed_kmh | Speed stats for window |
| anomaly_event_count | Count of non-normal events in window |
| driver_name, route_name, rated_max_speed | From stream-static join with reference table |
| speed_pct_of_max | avg_speed / rated_max * 100 |

### Gold — `fleet_alerts`
One row per vehicle. MERGE INTO upsert on each micro-batch.

| Column | Notes |
|--------|-------|
| alert_type | engine_overheat / speed_breach / fuel_critical / route_deviation |
| severity | CRITICAL / HIGH / MEDIUM / LOW / NORMAL |
| alert_status | OPEN / RESOLVED |
| alert_raised_at | When alert first opened |
| last_updated_at | Last micro-batch update time |

---

## Alert Rules

| Rule | Severity |
|------|----------|
| engine_temp_c > 110°C | CRITICAL |
| speed > rated_max × 1.15 | MEDIUM |
| fuel_pct < 15% | HIGH |
| route_deviation detected | MEDIUM |
| gps_signal_loss | LOW |

---

## Setup

### Prerequisites
- Azure subscription (Event Hubs Standard, ADLS Gen2, Databricks)
- Databricks workspace (DBR 13.3 LTS, Unity Catalog enabled)
- Python 3.10+

### 1. Configure environment
```bash
cp config/.env.template config/.env
# Fill in your Event Hubs connection string and ADLS storage key
```

### 2. Start the producer
```bash
pip install confluent-kafka python-dotenv
python producer/telemetry_producer.py --rate 10 --vehicles 10 --anomaly 0.30 --duration 1800
```

### 3. Run notebooks in order
```
00_setup.ipynb               → Cluster test, schemas, reference table
01_bronze_ingestion.ipynb    → Start Bronze stream
02_silver_windowed_agg.ipynb → Start Silver stream
03_gold_alerts.ipynb         → Start Gold stream
```

### 4. Verify pipeline
```python
# Check all 3 streams running
for q in spark.streams.active:
    print(f"Stream: {q.id} | Status: {q.status['message']}")

# Check Bronze
spark.sql("""
    SELECT alert_type, severity, COUNT(*) as count
    FROM bronze.raw_telemetry
    GROUP BY alert_type, severity
    ORDER BY count DESC
""").show()

# Check Gold
spark.sql("""
    SELECT vehicle_id, driver_name, severity, alert_type, alert_status
    FROM gold.fleet_alerts
    ORDER BY CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END
""").show()
```

---

## Key Interview Topics Covered

| Topic | Implementation |
|-------|---------------|
| Explicit schema vs inferSchema | Bronze — StructType with severity derivation from event_type |
| Watermark mechanics | Silver — withWatermark("event_time", "1 minute") |
| Tumbling vs sliding window | Silver — F.window("event_time", "1 minute") |
| Stream-static join | Silver — F.broadcast(vehicle_ref) enrichment |
| append vs update vs complete | Silver uses append — windows emit once after watermark |
| foreachBatch + MERGE INTO | Gold — DeltaTable.merge() per micro-batch |
| Checkpoint recovery | Offsets/commits stored in ADLS _checkpoints/ folder |
| Unity Catalog | 3-level namespace: catalog.schema.table |
| End-to-end processing guarantees | Event Hubs provides at-least-once delivery; Delta Lake ensures idempotent writes through checkpointing and MERGE semantics |

---

## Resume Line

> Built a real-time fleet telemetry monitoring platform using Python, Azure Event Hubs (Kafka endpoint), Databricks Structured Streaming, Delta Lake Medallion Architecture, and Databricks AI/BI Dashboard — processing 163K+ streaming events with live anomaly detection and sub-60-second operational alerting.

---

*Stack: Python · Apache Kafka · Azure Event Hubs · Azure ADLS Gen2 · Databricks · PySpark · Delta Lake · Unity Catalog · Databricks AI/BI*
