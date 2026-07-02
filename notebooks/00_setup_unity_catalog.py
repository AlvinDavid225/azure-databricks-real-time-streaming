# Databricks notebook source
# MAGIC %md
# MAGIC # P2 Fleet Operations — 00: Unity Catalog Setup
# MAGIC ## Catalog → Schemas → External Locations → Reference Table
# MAGIC
# MAGIC Run this ONCE before any other notebook.
# MAGIC Requires: Unity Catalog enabled workspace + Storage Credential configured for ADLS.

# COMMAND ----------

ADLS_ACCOUNT  = dbutils.secrets.get("fleet-scope", "adls-account")
CATALOG       = "fleet_ops"

# COMMAND ----------
# MAGIC %md ## 1. Create Catalog and Schemas

# COMMAND ----------

# %sql
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} COMMENT 'P2 Live Fleet Operations'")

for schema in ["bronze", "silver", "gold", "reference"]:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}
        COMMENT '{schema.title()} layer — Fleet Operations Lakehouse'
    """)
    print(f"Schema ready: {CATALOG}.{schema}")

# COMMAND ----------
# MAGIC %md ## 2. Create External Locations (run as admin)
# MAGIC
# MAGIC External locations link Unity Catalog to ADLS containers.
# MAGIC Run once by workspace admin. Requires Storage Credential already configured.

# COMMAND ----------

# These SQL statements must be run as a metastore admin
# Uncomment and run manually if you have admin privileges

# spark.sql(f"""
#     CREATE EXTERNAL LOCATION IF NOT EXISTS fleet_bronze
#     URL 'abfss://bronze@{ADLS_ACCOUNT}.dfs.core.windows.net/'
#     WITH (STORAGE CREDENTIAL fleet_adls_credential)
# """)

# spark.sql(f"""
#     CREATE EXTERNAL LOCATION IF NOT EXISTS fleet_gold
#     URL 'abfss://gold@{ADLS_ACCOUNT}.dfs.core.windows.net/'
#     WITH (STORAGE CREDENTIAL fleet_adls_credential)
# """)

print("External locations setup — see comments above for SQL to run as admin")

# COMMAND ----------
# MAGIC %md ## 3. Load Vehicle Reference Dimension

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import *

vehicle_ref_schema = StructType([
    StructField("vehicle_id",    StringType(),  False),
    StructField("driver_name",   StringType(),  True),
    StructField("route_id",      StringType(),  True),
    StructField("route_name",    StringType(),  True),
    StructField("vehicle_type",  StringType(),  True),
    StructField("max_speed_kmh", IntegerType(), True),
    StructField("capacity_kg",   IntegerType(), True),
    StructField("home_depot",    StringType(),  True),
])

# Read from ADLS (after uploading data/reference/vehicle_reference.csv)
vehicle_ref_df = spark.read.csv(
    f"abfss://bronze@{ADLS_ACCOUNT}.dfs.core.windows.net/reference/vehicle_reference.csv",
    header=True,
    schema=vehicle_ref_schema,
)

# Write as managed Delta table in Unity Catalog
(
    vehicle_ref_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.reference.vehicle_dimension")
)

print(f"Reference table loaded: {CATALOG}.reference.vehicle_dimension")
spark.sql(f"SELECT * FROM {CATALOG}.reference.vehicle_dimension").display()

# COMMAND ----------
# MAGIC %md ## 4. Grant Permissions (for shared workspace)

# COMMAND ----------

# spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `data-engineers`")
# spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.bronze TO `data-engineers`")
# spark.sql(f"GRANT SELECT ON TABLE {CATALOG}.gold.fleet_alerts TO `bi-team`")

# COMMAND ----------
# MAGIC %md ## 5. Verify Setup

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
display(spark.sql(f"SHOW TABLES IN {CATALOG}.reference"))
