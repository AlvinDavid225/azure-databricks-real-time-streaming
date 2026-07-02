# Databricks notebook source
# MAGIC %md
# MAGIC # P2 Fleet Operations — 01: Bronze Layer Ingestion
# MAGIC ## Event Hub → Structured Streaming → Bronze Delta Table
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Reads raw telemetry from Azure Event Hub using the Kafka-compatible endpoint
# MAGIC - Applies an explicit schema (never infer schema in production streaming!)
# MAGIC - Writes checkpointed Delta stream to the Bronze container
# MAGIC - Demonstrates checkpoint recovery (kill → restart → no data loss)
# MAGIC
# MAGIC **Run mode:** Continuous stream (run as a Job in production, interactive for demo)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Cluster Requirements
# MAGIC - DBR 13.3 LTS or higher (includes Spark 3.4+)
# MAGIC - Maven library: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`
# MAGIC   OR use Kafka connector (already bundled in DBR)
# MAGIC - Cluster access mode: **Single User** (required for Unity Catalog)

# COMMAND ----------

# 0a. Configuration — pull secrets from Databricks Secret Scope
# In production: databricks secrets put --scope fleet-scope --key eh-connection-string
# For demo: replace with your values directly (never commit to Git)

EH_NAMESPACE        = dbutils.secrets.get("fleet-scope", "eh-namespace")
EH_NAME             = dbutils.secrets.get("fleet-scope", "eh-name")          # fleet-telemetry
EH_CONNECTION_STR   = dbutils.secrets.get("fleet-scope", "eh-connection-string")

ADLS_ACCOUNT        = dbutils.secrets.get("fleet-scope", "adls-account")
ADLS_KEY            = dbutils.secrets.get("fleet-scope", "adls-key")

# ADLS paths (Unity Catalog external location format)
BRONZE_PATH         = f"abfss://bronze@{ADLS_ACCOUNT}.dfs.core.windows.net/fleet/raw_telemetry"
CHECKPOINT_PATH     = f"abfss://bronze@{ADLS_ACCOUNT}.dfs.core.windows.net/_checkpoints/fleet_bronze"

# Unity Catalog
CATALOG             = "fleet_ops"
BRONZE_SCHEMA       = "bronze"
BRONZE_TABLE        = f"{CATALOG}.{BRONZE_SCHEMA}.raw_telemetry"

# COMMAND ----------

# 0b. Mount ADLS (or use Unity Catalog external location — preferred)
# Skip this block if using Unity Catalog external locations (configured at workspace level)
spark.conf.set(
    f"fs.azure.account.key.{ADLS_ACCOUNT}.dfs.core.windows.net",
    ADLS_KEY
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Define Telemetry Schema
# MAGIC
# MAGIC **Interview point:** Always define explicit schema for streaming sources.
# MAGIC `inferSchema` requires reading a full batch first — unacceptable for real-time.
# MAGIC Schema mismatch at runtime crashes the stream; explicit schema fails fast at parse time
# MAGIC and lets you route bad records to a dead-letter topic.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, FloatType
)

telemetry_schema = StructType([
    StructField("vehicle_id",     StringType(),    nullable=False),
    StructField("timestamp",      StringType(),    nullable=False),  # parse to timestamp below
    StructField("lat",            DoubleType(),    nullable=True),
    StructField("lon",            DoubleType(),    nullable=True),
    StructField("speed_kmh",      FloatType(),     nullable=True),
    StructField("fuel_pct",       FloatType(),     nullable=True),
    StructField("engine_temp_c",  FloatType(),     nullable=True),
    StructField("odometer_km",    FloatType(),     nullable=True),
    StructField("event_type",     StringType(),    nullable=True),
    StructField("route_id",       StringType(),    nullable=True),
])

print("Schema defined:")
telemetry_schema.printTreeString()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Read Stream from Event Hub (Kafka API)
# MAGIC
# MAGIC Event Hub exposes a Kafka-compatible endpoint — same consumer API,
# MAGIC no separate library needed when using DBR 10+.
# MAGIC
# MAGIC **Key Kafka options for Event Hub:**
# MAGIC | Option | Value | Why |
# MAGIC |--------|-------|-----|
# MAGIC | `startingOffsets` | `latest` | Don't replay history on first start |
# MAGIC | `failOnDataLoss` | `false` | Event Hub retention (default 1 day) may cause offset gaps |
# MAGIC | `maxOffsetsPerTrigger` | `10000` | Rate-limit per micro-batch to avoid OOM |

# COMMAND ----------

import json
from pyspark.sql import functions as F

# Kafka SASL config for Event Hub
KAFKA_BOOTSTRAP   = f"{EH_NAMESPACE}.servicebus.windows.net:9093"
SASL_CONFIG       = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="$ConnectionString" password="{EH_CONNECTION_STR}";'
)

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",              KAFKA_BOOTSTRAP)
    .option("kafka.security.protocol",              "SASL_SSL")
    .option("kafka.sasl.mechanism",                 "PLAIN")
    .option("kafka.sasl.jaas.config",               SASL_CONFIG)
    .option("subscribe",                            EH_NAME)
    .option("startingOffsets",                      "latest")
    .option("failOnDataLoss",                       "false")
    .option("maxOffsetsPerTrigger",                 10000)
    .option("kafka.request.timeout.ms",             "60000")
    .option("kafka.session.timeout.ms",             "30000")
    .load()
)

print("Stream schema (raw Kafka envelope):")
raw_stream.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Parse JSON Payload + Enrich with Kafka Metadata

# COMMAND ----------

parsed_stream = (
    raw_stream
    # Decode binary value column → JSON string
    .withColumn("raw_json", F.col("value").cast("string"))
    
    # Parse JSON against explicit schema — bad records produce NULLs (PERMISSIVE mode)
    .withColumn("data", F.from_json(F.col("raw_json"), telemetry_schema))
    
    # Flatten nested struct
    .select(
        "data.*",
        # Kafka metadata columns — useful for debugging & replay
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_enqueue_time"),
    )
    
    # Parse event timestamp string → proper TimestampType
    .withColumn("event_time", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"))
    .drop("timestamp")
    
    # Add ingestion watermark for late-event tracking
    .withColumn("ingestion_time", F.current_timestamp())
    
    # Dead-letter filter: route rows with null vehicle_id (unparseable) to separate path
    # For simplicity here we filter them out — in production write to bronze/_dead_letter/
    .filter(F.col("vehicle_id").isNotNull())
)

print("Parsed stream schema:")
parsed_stream.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Write to Bronze Delta Table
# MAGIC
# MAGIC **Checkpoint design:**
# MAGIC - Checkpoint stores: consumer offsets + partial aggregation state
# MAGIC - Location: ADLS (separate container/path, NOT inside the Delta table path)
# MAGIC - `checkpointInterval` = 30s → worst-case replay on restart = 30 seconds of data
# MAGIC
# MAGIC **Interview point — Checkpoint Recovery Demo:**
# MAGIC 1. Start this cell (stream running)
# MAGIC 2. Interrupt the kernel mid-stream (simulates cluster crash)
# MAGIC 3. Re-run this cell — stream resumes from last checkpoint offset
# MAGIC 4. Verify: no duplicate rows, no gap in offsets

# COMMAND ----------

bronze_write_query = (
    parsed_stream
    .writeStream
    .format("delta")
    .outputMode("append")                           # Bronze is always append
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "false")                 # Explicit schema — reject drift
    # Partition by date for efficient downstream reads + OPTIMIZE
    .partitionBy("route_id")
    .trigger(processingTime="30 seconds")           # Micro-batch every 30s
    # In production: use .trigger(availableNow=True) for triggered batch mode
    .toTable(BRONZE_TABLE)                          # Unity Catalog managed table
)

print(f"Bronze stream started → {BRONZE_TABLE}")
print(f"Checkpoint: {CHECKPOINT_PATH}")
print("Stream is running. Query ID:", bronze_write_query.id)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Monitor Stream Health

# COMMAND ----------

import time

def print_stream_progress(query, max_polls=5):
    """Poll stream progress and print key metrics."""
    for i in range(max_polls):
        time.sleep(35)  # Wait for one trigger
        progress = query.lastProgress
        if progress:
            print(f"\n[Poll {i+1}] Trigger: {progress.get('timestamp', 'N/A')}")
            print(f"  Input rows:        {progress.get('numInputRows', 0):,}")
            print(f"  Rows/sec:          {progress.get('inputRowsPerSecond', 0):.1f}")
            print(f"  Processing time:   {progress.get('durationMs', {}).get('triggerExecution', 0)}ms")
            offsets = progress.get('sources', [{}])[0].get('endOffset', {})
            print(f"  Kafka offsets:     {offsets}")
        else:
            print(f"[Poll {i+1}] No progress yet...")

# Uncomment to run monitoring loop:
# print_stream_progress(bronze_write_query)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Verify Bronze Table

# COMMAND ----------

# Run after stream has processed at least one batch
display(
    spark.sql(f"""
        SELECT 
            route_id,
            COUNT(*) AS event_count,
            MIN(event_time) AS earliest_event,
            MAX(event_time) AS latest_event,
            COUNT(DISTINCT vehicle_id) AS unique_vehicles,
            SUM(CASE WHEN event_type != 'normal' THEN 1 ELSE 0 END) AS anomaly_count
        FROM {BRONZE_TABLE}
        GROUP BY route_id
        ORDER BY event_count DESC
    """)
)

# COMMAND ----------

# Checkpoint recovery verification
# After restarting the stream, run this to confirm no data gap:
spark.sql(f"""
    SELECT 
        kafka_partition,
        MIN(kafka_offset) AS min_offset,
        MAX(kafka_offset) AS max_offset,
        COUNT(*) AS row_count,
        -- Gap detection: max_offset - min_offset + 1 should equal row_count
        (MAX(kafka_offset) - MIN(kafka_offset) + 1) - COUNT(*) AS missing_offsets
    FROM {BRONZE_TABLE}
    GROUP BY kafka_partition
""").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checkpoint Recovery — Step-by-Step Demo
# MAGIC
# MAGIC ```
# MAGIC Step 1: Start stream (Cell 4 above) — note the current kafka_offset
# MAGIC Step 2: Let producer run for 60 seconds
# MAGIC Step 3: Interrupt kernel (red square ■) — simulates cluster failure
# MAGIC Step 4: Re-run Cell 4 — stream reads checkpoint, resumes from last committed offset
# MAGIC Step 5: Run gap detection query above — missing_offsets should be 0
# MAGIC Step 6: In interview: "The checkpoint stores the last committed Kafka offset per partition.
# MAGIC          On restart, Spark reads the checkpoint and resumes exactly where it left off.
# MAGIC          Delta's ACID transactions prevent partial writes — no duplicates on recovery."
# MAGIC ```
