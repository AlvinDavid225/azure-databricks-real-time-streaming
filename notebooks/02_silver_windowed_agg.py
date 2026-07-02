# Databricks notebook source
# MAGIC %md
# MAGIC # P2 Fleet Operations — 02: Silver Layer — Windowed Aggregations
# MAGIC ## Watermark + Tumbling Window + Stream-Static Join
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Reads Bronze Delta stream
# MAGIC - Applies 10-minute watermark for late-event tolerance
# MAGIC - Computes 1-minute tumbling window aggregations (avg/max speed, temp)
# MAGIC - Stream-static join with vehicle reference dimension (driver, route, max_speed)
# MAGIC - Writes enriched Silver table for downstream alerting

# COMMAND ----------

# Configuration (same as Notebook 01)
ADLS_ACCOUNT      = dbutils.secrets.get("fleet-scope", "adls-account")
CATALOG           = "fleet_ops"
BRONZE_TABLE      = f"{CATALOG}.bronze.raw_telemetry"
SILVER_TABLE      = f"{CATALOG}.silver.vehicle_window_stats"
REF_TABLE         = f"{CATALOG}.reference.vehicle_dimension"

SILVER_CHECKPOINT = (
    f"abfss://bronze@{ADLS_ACCOUNT}.dfs.core.windows.net"
    f"/_checkpoints/fleet_silver"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Load Vehicle Reference Dimension (Static)
# MAGIC
# MAGIC **Interview point — Stream-Static Join:**
# MAGIC - The "static" side is a Delta table read as a batch DataFrame (not a stream)
# MAGIC - Spark broadcasts the static side to all executors → no shuffle
# MAGIC - Static side is snapshotted at stream start — updates to the reference table
# MAGIC   require a stream restart to pick up (or use `spark.read` inside `foreachBatch`)
# MAGIC - This is the correct pattern when dimension changes infrequently (vehicles don't change daily)

# COMMAND ----------

from pyspark.sql import functions as F

# Read static dimension — broadcast hint for efficiency
vehicle_ref_df = (
    spark.read
    .table(REF_TABLE)
    .select(
        "vehicle_id",
        "driver_name",
        "route_name",
        "vehicle_type",
        "max_speed_kmh",
        "home_depot",
    )
)

# Cache the reference — it's small (10 vehicles) and read repeatedly
vehicle_ref_cached = vehicle_ref_df.cache()
print(f"Vehicle reference loaded: {vehicle_ref_cached.count()} rows")
vehicle_ref_cached.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Read Bronze as Stream + Apply Watermark
# MAGIC
# MAGIC **Watermark deep-dive (common interview topic):**
# MAGIC
# MAGIC ```
# MAGIC Watermark = max(event_time seen so far) - threshold
# MAGIC
# MAGIC With 10-minute watermark:
# MAGIC   - If latest event seen is at 14:30
# MAGIC   - Watermark = 14:20
# MAGIC   - Any event with event_time < 14:20 is DROPPED (too late)
# MAGIC   - Windows closing before 14:20 are finalized and state is cleared
# MAGIC
# MAGIC Why it matters:
# MAGIC   - Without watermark: Spark keeps ALL window state forever → OOM
# MAGIC   - With watermark: state is bounded, windows are garbage collected
# MAGIC   - 10-minute tolerance handles GPS upload delays, network hiccups
# MAGIC ```

# COMMAND ----------

bronze_stream = (
    spark.readStream
    .format("delta")
    .table(BRONZE_TABLE)
    # Watermark: tolerate events arriving up to 10 minutes late
    .withWatermark("event_time", "10 minutes")
)

print("Bronze stream schema:")
bronze_stream.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Tumbling Window Aggregation
# MAGIC
# MAGIC **1-minute tumbling window** — non-overlapping, fixed-size windows.
# MAGIC Each window = [T, T+1min). A vehicle event at 14:03:45 falls into window [14:03, 14:04).
# MAGIC
# MAGIC **vs Sliding window:** sliding(1 min, 30 sec) → every event appears in 2 windows.
# MAGIC Use sliding for smoothed metrics; tumbling for discrete period summaries.

# COMMAND ----------

windowed_stats = (
    bronze_stream
    .groupBy(
        F.window("event_time", "1 minute"),   # tumbling window: size=1min, slide=1min
        "vehicle_id",
        "route_id",
    )
    .agg(
        F.avg("speed_kmh").alias("avg_speed_kmh"),
        F.max("speed_kmh").alias("max_speed_kmh"),
        F.avg("engine_temp_c").alias("avg_engine_temp_c"),
        F.max("engine_temp_c").alias("max_engine_temp_c"),
        F.min("fuel_pct").alias("min_fuel_pct"),
        F.count("*").alias("event_count"),
        F.sum(
            F.when(F.col("event_type") != "normal", 1).otherwise(0)
        ).alias("anomaly_event_count"),
        F.last("lat").alias("last_lat"),
        F.last("lon").alias("last_lon"),
    )
    # Flatten window struct → window_start, window_end columns
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end",   F.col("window.end"))
    .drop("window")
)

print("Windowed stats schema:")
windowed_stats.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Stream-Static Join — Enrich with Vehicle Dimension
# MAGIC
# MAGIC **How Spark handles stream-static joins:**
# MAGIC - Static side is evaluated once at stream startup
# MAGIC - Each micro-batch of the stream is joined against the cached static DataFrame
# MAGIC - Supported join types: inner, left outer
# MAGIC - NOT supported: right outer, full outer (stream side is unbounded — can't hold all state)

# COMMAND ----------

enriched_stream = (
    windowed_stats
    .join(
        F.broadcast(vehicle_ref_cached),  # broadcast hint — small dimension
        on="vehicle_id",
        how="left",                        # keep all stream rows even if no ref match
    )
    # Derived column: is vehicle over its rated max speed?
    .withColumn(
        "speed_pct_of_max",
        F.round(F.col("avg_speed_kmh") / F.col("max_speed_kmh") * 100, 1)
    )
    .withColumn("ingestion_time", F.current_timestamp())
)

print("Enriched stream schema:")
enriched_stream.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Write Silver Table

# COMMAND ----------

silver_query = (
    enriched_stream
    .writeStream
    .format("delta")
    .outputMode("append")           # append: each completed window emits one row
    # NOTE: outputMode("update") would emit partial results mid-window — not what we want
    # outputMode("complete") would rewrite full result set each trigger — too expensive
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .toTable(SILVER_TABLE)
)

print(f"Silver stream started → {SILVER_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Verify Silver Table

# COMMAND ----------

# After a few minutes, check window completions
display(spark.sql(f"""
    SELECT 
        vehicle_id,
        driver_name,
        route_name,
        window_start,
        window_end,
        ROUND(avg_speed_kmh, 1)  AS avg_speed,
        ROUND(max_speed_kmh, 1)  AS max_speed,
        max_speed_kmh            AS rated_max,
        speed_pct_of_max         AS speed_pct,
        ROUND(avg_engine_temp_c, 1) AS avg_temp,
        ROUND(min_fuel_pct, 1)   AS min_fuel,
        event_count,
        anomaly_event_count
    FROM {SILVER_TABLE}
    ORDER BY window_start DESC, vehicle_id
    LIMIT 50
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Key Interview Answers Built Into This Notebook
# MAGIC
# MAGIC **Q: What happens if a late event arrives after the watermark?**
# MAGIC A: It is silently dropped by Structured Streaming. The window it belonged to has already
# MAGIC    been finalized and its state cleared. This is expected and acceptable — we configured
# MAGIC    10 minutes of tolerance, which covers realistic GPS upload delays.
# MAGIC
# MAGIC **Q: Why append mode and not update mode for windowed aggregations?**
# MAGIC A: With watermark, Structured Streaming guarantees a window will only emit its final
# MAGIC    result ONCE (after the watermark passes the window end). Append mode is correct here.
# MAGIC    Update mode would emit partial results every trigger — correct for non-windowed
# MAGIC    streaming aggregations but not what an ops dashboard needs.
# MAGIC
# MAGIC **Q: How does the stream-static join handle reference data updates?**
# MAGIC A: The static DataFrame is snapshotted at stream startup. If vehicle assignments change,
# MAGIC    the stream must be restarted. For more dynamic dimensions, use foreachBatch to
# MAGIC    re-read the reference table inside each micro-batch — at the cost of extra reads.
