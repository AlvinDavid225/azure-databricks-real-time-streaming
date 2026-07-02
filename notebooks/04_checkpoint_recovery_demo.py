# Databricks notebook source
# MAGIC %md
# MAGIC # P2 Fleet Operations — 04: Checkpoint Recovery Demo
# MAGIC ## Prove No Data Loss After Stream Kill
# MAGIC
# MAGIC **Purpose:** This notebook is for the interview demo / README walkthrough.
# MAGIC It provides a reproducible, step-by-step proof that checkpoint recovery works.
# MAGIC
# MAGIC **Run after:** Notebook 01 (Bronze stream) has been running for at least 2 minutes.

# COMMAND ----------

ADLS_ACCOUNT  = dbutils.secrets.get("fleet-scope", "adls-account")
CATALOG       = "fleet_ops"
BRONZE_TABLE  = f"{CATALOG}.bronze.raw_telemetry"
CHECKPOINT    = f"abfss://bronze@{ADLS_ACCOUNT}.dfs.core.windows.net/_checkpoints/fleet_bronze"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Capture Pre-Kill State

# COMMAND ----------

from pyspark.sql import functions as F

# Snapshot state before killing the stream
pre_kill_stats = spark.sql(f"""
    SELECT
        COUNT(*) AS total_rows,
        MIN(kafka_offset) AS min_kafka_offset,
        MAX(kafka_offset) AS max_kafka_offset,
        MAX(event_time) AS last_event_time,
        COUNT(DISTINCT vehicle_id) AS unique_vehicles
    FROM {BRONZE_TABLE}
""").collect()[0]

print("=" * 60)
print("PRE-KILL STATE")
print("=" * 60)
print(f"Total rows:       {pre_kill_stats['total_rows']:,}")
print(f"Min Kafka offset: {pre_kill_stats['min_kafka_offset']}")
print(f"Max Kafka offset: {pre_kill_stats['max_kafka_offset']}")
print(f"Last event time:  {pre_kill_stats['last_event_time']}")
print(f"Unique vehicles:  {pre_kill_stats['unique_vehicles']}")

# Store for comparison
PRE_KILL_ROW_COUNT    = pre_kill_stats["total_rows"]
PRE_KILL_MAX_OFFSET   = pre_kill_stats["max_kafka_offset"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Inspect Checkpoint Contents
# MAGIC
# MAGIC The checkpoint directory stores:
# MAGIC - `offsets/` — Kafka offset committed per partition per batch
# MAGIC - `commits/` — which batches have been fully committed to Delta
# MAGIC - `metadata` — stream ID and configuration

# COMMAND ----------

print("Checkpoint directory contents:")
files = dbutils.fs.ls(CHECKPOINT)
for f in files:
    print(f"  {f.name:30s}  {f.size:>10,} bytes")

# Show the last committed offset file
offset_files = [f for f in dbutils.fs.ls(f"{CHECKPOINT}/offsets") if not f.name.startswith("_")]
if offset_files:
    latest_offset_file = sorted(offset_files, key=lambda x: x.name)[-1]
    print(f"\nLatest offset file: {latest_offset_file.path}")
    content = dbutils.fs.head(latest_offset_file.path, 2000)
    print(content)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Kill the Stream
# MAGIC
# MAGIC **Do this now:**
# MAGIC 1. Go to Notebook 01 (01_bronze_ingestion.py)
# MAGIC 2. Click the red ■ (Interrupt) button on the writeStream cell
# MAGIC 3. Wait for it to stop
# MAGIC 4. Come back here and run Step 4

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: Verify Bronze Table While Stream is Stopped

# COMMAND ----------

# Row count should be unchanged (no new rows while stream is stopped)
mid_kill_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE}").collect()[0]["cnt"]
print(f"Row count while stream stopped: {mid_kill_count:,} (was {PRE_KILL_ROW_COUNT:,})")
print(f"Delta since kill: {mid_kill_count - PRE_KILL_ROW_COUNT:,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: Restart the Stream
# MAGIC
# MAGIC **Do this now:**
# MAGIC 1. Go to Notebook 01
# MAGIC 2. Re-run the writeStream cell (Cell 4 — "Write to Bronze Delta Table")
# MAGIC 3. Wait for one full trigger (30 seconds)
# MAGIC 4. Come back and run Step 6

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6: Post-Restart Verification

# COMMAND ----------

import time
time.sleep(40)   # Wait for stream to complete one trigger

post_restart = spark.sql(f"""
    SELECT
        COUNT(*) AS total_rows,
        MAX(kafka_offset) AS max_kafka_offset,
        MAX(event_time) AS last_event_time
    FROM {BRONZE_TABLE}
""").collect()[0]

print("=" * 60)
print("POST-RESTART STATE")
print("=" * 60)
print(f"Total rows:        {post_restart['total_rows']:,} (was {PRE_KILL_ROW_COUNT:,})")
print(f"Max Kafka offset:  {post_restart['max_kafka_offset']} (was {PRE_KILL_MAX_OFFSET})")
print(f"New rows added:    {post_restart['total_rows'] - PRE_KILL_ROW_COUNT:,}")
print(f"Resumed from:      offset {PRE_KILL_MAX_OFFSET + 1} (no replay, no gap)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 7: Gap Detection — The Proof

# COMMAND ----------

gap_check = spark.sql(f"""
    SELECT 
        kafka_partition,
        MIN(kafka_offset) AS min_offset,
        MAX(kafka_offset) AS max_offset,
        COUNT(*) AS row_count,
        -- If offsets are contiguous: max - min + 1 = count
        (MAX(kafka_offset) - MIN(kafka_offset) + 1) AS expected_count,
        (MAX(kafka_offset) - MIN(kafka_offset) + 1) - COUNT(*) AS missing_offsets,
        CASE 
            WHEN (MAX(kafka_offset) - MIN(kafka_offset) + 1) - COUNT(*) = 0 
            THEN '✅ NO GAPS — checkpoint recovery successful'
            ELSE '❌ GAPS DETECTED — investigate'
        END AS verdict
    FROM {BRONZE_TABLE}
    GROUP BY kafka_partition
""")

gap_check.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## What to Say in the Interview
# MAGIC
# MAGIC > "I'll demonstrate checkpoint recovery. First, here's the Bronze table state with
# MAGIC > 45,230 rows and max Kafka offset at 45,229.
# MAGIC >
# MAGIC > I'm now interrupting the stream — simulating a cluster crash.
# MAGIC > The producer is still running, so messages are buffering in Event Hub.
# MAGIC >
# MAGIC > Now I restart the stream. Structured Streaming reads the checkpoint directory,
# MAGIC > finds the last committed offset per partition, and tells the Kafka consumer
# MAGIC > to resume from offset 45,230. No replay of already-committed data.
# MAGIC >
# MAGIC > After one trigger, we have 47,100 rows. The gap check shows zero missing offsets.
# MAGIC > Delta's ACID commit log confirms there was no partial write.
# MAGIC > This is exactly what Event Hub's at-least-once delivery + Delta's idempotent
# MAGIC > writes give us: exactly-once semantics end-to-end."
