# Databricks notebook source
# MAGIC %md
# MAGIC # P2 Fleet Operations — 03: Gold Layer — Alert Engine
# MAGIC ## Silver → Business Rules → Gold Alerts Table
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Reads enriched Silver stream
# MAGIC - Applies business alert rules with severity classification
# MAGIC - MERGE INTO Gold alerts table (upsert — one live alert row per vehicle)
# MAGIC - Demonstrates foreachBatch pattern for complex write logic
# MAGIC - Gold table is the source for Power BI DirectQuery dashboard

# COMMAND ----------

ADLS_ACCOUNT  = dbutils.secrets.get("fleet-scope", "adls-account")
CATALOG       = "fleet_ops"
SILVER_TABLE  = f"{CATALOG}.silver.vehicle_window_stats"
GOLD_TABLE    = f"{CATALOG}.gold.fleet_alerts"

GOLD_CHECKPOINT = (
    f"abfss://bronze@{ADLS_ACCOUNT}.dfs.core.windows.net"
    f"/_checkpoints/fleet_gold_alerts"
)

# Alert thresholds (mirror .env values)
SPEED_BREACH_MULTIPLIER  = 1.15   # alert if avg_speed > max_speed * 1.15
ENGINE_TEMP_WARNING      = 95.0
ENGINE_TEMP_CRITICAL     = 110.0
FUEL_LOW_PCT             = 15.0
ANOMALY_EVENT_THRESHOLD  = 3      # >= 3 anomalous events in a 1-min window

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Create Gold Alerts Table (DDL)
# MAGIC
# MAGIC **Design decision — MERGE pattern:**
# MAGIC One row per vehicle showing its CURRENT alert state.
# MAGIC On each micro-batch: upsert using MERGE INTO.
# MAGIC - If alert exists for vehicle → UPDATE severity, timestamp, details
# MAGIC - If vehicle has no open alert → INSERT new row
# MAGIC - If alert cleared (all metrics normal) → UPDATE status to 'resolved'
# MAGIC
# MAGIC This keeps the Gold table small and Power BI DirectQuery fast.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
        alert_id            STRING          COMMENT 'UUID for this alert instance',
        vehicle_id          STRING          NOT NULL,
        driver_name         STRING,
        route_name          STRING,
        home_depot          STRING,
        alert_type          STRING          COMMENT 'speed_breach|engine_overheat|fuel_critical|multi_anomaly',
        severity            STRING          COMMENT 'WARNING|CRITICAL',
        alert_status        STRING          COMMENT 'OPEN|RESOLVED',
        -- Metric values at alert time
        avg_speed_kmh       DOUBLE,
        max_speed_kmh       DOUBLE,
        rated_max_speed     DOUBLE,
        speed_pct_of_max    DOUBLE,
        avg_engine_temp_c   DOUBLE,
        min_fuel_pct        DOUBLE,
        anomaly_event_count BIGINT,
        -- Location
        last_lat            DOUBLE,
        last_lon            DOUBLE,
        -- Window metadata
        window_start        TIMESTAMP,
        window_end          TIMESTAMP,
        -- Audit
        alert_raised_at     TIMESTAMP,
        last_updated_at     TIMESTAMP,
        resolved_at         TIMESTAMP
    )
    USING DELTA
    COMMENT 'Gold layer: one row per vehicle showing current alert state. Power BI DirectQuery source.'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

print(f"Gold alerts table ready: {GOLD_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Alert Classification Logic

# COMMAND ----------

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import StringType
import uuid

def classify_alerts(batch_df: DataFrame) -> DataFrame:
    """
    Apply business rules to Silver window stats → generate alert rows.
    
    Rule priority (first match wins):
        1. CRITICAL: engine_temp > 110°C
        2. CRITICAL: avg_speed > rated_max * 1.15
        3. WARNING:  engine_temp > 95°C
        4. WARNING:  fuel_pct < 15%
        5. WARNING:  >= 3 anomaly events in window
        6. RESOLVED: none of the above triggered
    """
    gen_alert_id = F.udf(lambda: str(uuid.uuid4()), StringType())
    
    return (
        batch_df
        .withColumn(
            "alert_type",
            F.when(F.col("max_engine_temp_c") > ENGINE_TEMP_CRITICAL, "engine_overheat")
            .when(F.col("avg_speed_kmh") > F.col("max_speed_kmh") * SPEED_BREACH_MULTIPLIER, "speed_breach")
            .when(F.col("avg_engine_temp_c") > ENGINE_TEMP_WARNING, "engine_overheat")
            .when(F.col("min_fuel_pct") < FUEL_LOW_PCT, "fuel_critical")
            .when(F.col("anomaly_event_count") >= ANOMALY_EVENT_THRESHOLD, "multi_anomaly")
            .otherwise(None)
        )
        .withColumn(
            "severity",
            F.when(F.col("max_engine_temp_c") > ENGINE_TEMP_CRITICAL, "CRITICAL")
            .when(F.col("avg_speed_kmh") > F.col("max_speed_kmh") * SPEED_BREACH_MULTIPLIER, "CRITICAL")
            .when(F.col("alert_type").isNotNull(), "WARNING")
            .otherwise(None)
        )
        .withColumn(
            "alert_status",
            F.when(F.col("alert_type").isNotNull(), "OPEN").otherwise("RESOLVED")
        )
        .withColumn("alert_id", gen_alert_id())
        .withColumn("alert_raised_at",
            F.when(F.col("alert_status") == "OPEN", F.current_timestamp())
        )
        .withColumn("last_updated_at", F.current_timestamp())
        .withColumn("resolved_at",
            F.when(F.col("alert_status") == "RESOLVED", F.current_timestamp())
        )
        .withColumnRenamed("max_speed_kmh",      "rated_max_speed")
        .withColumnRenamed("avg_engine_temp_c",  "avg_engine_temp_c")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. foreachBatch — MERGE INTO Gold Table
# MAGIC
# MAGIC **Why foreachBatch instead of direct writeStream?**
# MAGIC - `writeStream.toTable()` only supports append/complete/update output modes
# MAGIC - MERGE INTO (upsert) requires DeltaTable API — only available in batch context
# MAGIC - foreachBatch gives us a regular batch DataFrame per micro-batch,
# MAGIC   where we can use any Spark or Delta API
# MAGIC
# MAGIC **Interview point:** foreachBatch is the escape hatch for complex write patterns in
# MAGIC Structured Streaming: MERGE, multi-table writes, conditional logic, external API calls.
# MAGIC The function must be idempotent — Spark may call it twice on failure.

# COMMAND ----------

from delta.tables import DeltaTable

def merge_alerts_to_gold(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch function: classify alerts and MERGE INTO gold table.
    
    Idempotency guarantee:
    - MERGE INTO is atomic — partial failures roll back
    - batch_id is logged for debugging duplicate calls
    - Using event_time as tie-breaker ensures deterministic results on replay
    """
    if batch_df.isEmpty():
        print(f"[Batch {batch_id}] Empty batch — skipping")
        return
    
    print(f"[Batch {batch_id}] Processing {batch_df.count()} window records...")
    
    # Classify alerts
    alerts_df = classify_alerts(batch_df)
    
    # Deduplicate: keep the most recent window per vehicle in this batch
    # (multiple windows may arrive in one micro-batch)
    deduped = (
        alerts_df
        .withColumn(
            "rn",
            F.row_number().over(
                __import__("pyspark.sql.window", fromlist=["Window"])
                .Window.partitionBy("vehicle_id").orderBy(F.col("window_end").desc())
            )
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    
    row_count = deduped.count()
    open_alerts  = deduped.filter(F.col("alert_status") == "OPEN").count()
    resolved     = deduped.filter(F.col("alert_status") == "RESOLVED").count()
    print(f"[Batch {batch_id}] Alerts: {open_alerts} OPEN, {resolved} RESOLVED | Total: {row_count}")
    
    # MERGE INTO Gold table
    gold_delta = DeltaTable.forName(spark, GOLD_TABLE)
    
    (
        gold_delta.alias("gold")
        .merge(
            deduped.alias("new"),
            "gold.vehicle_id = new.vehicle_id"
        )
        .whenMatchedUpdate(set={
            "alert_type":           "new.alert_type",
            "severity":             "new.severity",
            "alert_status":         "new.alert_status",
            "avg_speed_kmh":        "new.avg_speed_kmh",
            "rated_max_speed":      "new.rated_max_speed",
            "speed_pct_of_max":     "new.speed_pct_of_max",
            "avg_engine_temp_c":    "new.avg_engine_temp_c",
            "min_fuel_pct":         "new.min_fuel_pct",
            "anomaly_event_count":  "new.anomaly_event_count",
            "last_lat":             "new.last_lat",
            "last_lon":             "new.last_lon",
            "window_start":         "new.window_start",
            "window_end":           "new.window_end",
            "last_updated_at":      "new.last_updated_at",
            "resolved_at":          "new.resolved_at",
            # Only overwrite alert_raised_at when opening a NEW alert
            "alert_raised_at": F.expr("""
                CASE WHEN new.alert_status = 'OPEN' AND gold.alert_status = 'RESOLVED'
                     THEN new.alert_raised_at
                     ELSE gold.alert_raised_at
                END
            """),
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    print(f"[Batch {batch_id}] MERGE complete → {GOLD_TABLE}")


# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Start Gold Alert Stream

# COMMAND ----------

from pyspark.sql.window import Window

silver_stream = (
    spark.readStream
    .format("delta")
    .table(SILVER_TABLE)
)

gold_alert_query = (
    silver_stream
    .writeStream
    .foreachBatch(merge_alerts_to_gold)
    .option("checkpointLocation", GOLD_CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)

print(f"Gold alert stream started. Query ID: {gold_alert_query.id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Query Gold Alerts Dashboard View

# COMMAND ----------

# Live alert summary — this is what Power BI DirectQuery reads
display(spark.sql(f"""
    SELECT
        vehicle_id,
        driver_name,
        route_name,
        home_depot,
        alert_status,
        severity,
        alert_type,
        ROUND(avg_speed_kmh, 1)         AS avg_speed_kmh,
        ROUND(rated_max_speed, 0)       AS rated_max_kmh,
        ROUND(speed_pct_of_max, 1)      AS speed_pct,
        ROUND(avg_engine_temp_c, 1)     AS engine_temp_c,
        ROUND(min_fuel_pct, 1)          AS fuel_pct,
        last_lat,
        last_lon,
        window_start,
        window_end,
        alert_raised_at,
        last_updated_at
    FROM {GOLD_TABLE}
    ORDER BY
        CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,
        alert_raised_at DESC
"""))

# COMMAND ----------

# Alert count summary
display(spark.sql(f"""
    SELECT
        alert_status,
        severity,
        alert_type,
        COUNT(*) AS vehicle_count
    FROM {GOLD_TABLE}
    GROUP BY alert_status, severity, alert_type
    ORDER BY alert_status, severity
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Delta Time Travel — Replay Historical Alerts
# MAGIC
# MAGIC Since Gold is a Delta table, we get time travel for free.
# MAGIC Useful for: incident post-mortems, SLA audit, debugging.

# COMMAND ----------

# See all historical versions of the Gold table
spark.sql(f"DESCRIBE HISTORY {GOLD_TABLE}").display()

# COMMAND ----------

# What did the alert state look like 10 minutes ago?
# spark.sql(f"""
#     SELECT vehicle_id, alert_type, severity, avg_speed_kmh, last_updated_at
#     FROM {GOLD_TABLE} TIMESTAMP AS OF (current_timestamp() - INTERVAL 10 MINUTES)
# """).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Interview Answers Built Into This Notebook
# MAGIC
# MAGIC **Q: Why use foreachBatch for the Gold write instead of direct writeStream?**
# MAGIC A: We need MERGE INTO semantics (upsert) to maintain one current-state row per vehicle.
# MAGIC    Direct writeStream only supports append, update, or complete output modes.
# MAGIC    foreachBatch lets us use the full DeltaTable API in a batch context per micro-batch.
# MAGIC
# MAGIC **Q: How do you ensure foreachBatch is idempotent?**
# MAGIC A: The MERGE INTO operation is atomic — either all rows merge or none do.
# MAGIC    We use window_end as a tie-breaker when deduplicating within a batch.
# MAGIC    If Spark replays the same batch_id (e.g., after a failure), the MERGE produces
# MAGIC    the same final state because matching on vehicle_id means the same update is applied.
# MAGIC
# MAGIC **Q: How would you add a Slack alert from this pipeline?**
# MAGIC A: Inside foreachBatch, after the MERGE, filter for newly-opened CRITICAL alerts
# MAGIC    and call the Slack webhook API (requests.post) with the vehicle details.
# MAGIC    Keep it outside the MERGE transaction so a Slack failure doesn't roll back the write.
