# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Load SAP source files
# MAGIC Store CSV fields as strings. Each run replaces the current table contents.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------
from pyspark.sql import functions as F
import re

dbutils.widgets.text("table_prefix", "dev_", "Table prefix")
table_prefix = dbutils.widgets.get("table_prefix").strip()
if not re.fullmatch(r"[a-z][a-z0-9_]*_", table_prefix):
    raise ValueError("Use a table prefix such as dev_ or prod_.")

dbutils.widgets.text("catalog", "supplier_performance_dev", "Target catalog")
dbutils.widgets.text("bronze_schema", "dev_bronze", "Target schema")
dbutils.widgets.text(
    "source_path",
    "/Volumes/supplier_performance_dev/dev_landing/dev_source_files",
    "Landing Zone",
)

# COMMAND ----------
catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
source_path = dbutils.widgets.get("source_path").strip().rstrip("/")

if not all([catalog, bronze_schema, source_path]):
    raise ValueError("Set catalog, bronze_schema and source_path.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Source files

# COMMAND ----------
source_files = {
    "lfa1": "LFA1_VENDOR_MASTER.csv",
    "mara": "MARA_MATERIAL_MASTER.csv",
    "ekko": "EKKO_PO_HEADER.csv",
    "ekpo": "EKPO_PO_ITEM.csv",
    "eket": "EKET_SCHEDULE_LINES.csv",
    "likp": "LIKP_DELIVERY_HEADER.csv",
    "lips": "LIPS_DELIVERY_ITEM.csv",
}

available_files = {item.name: item.size for item in dbutils.fs.ls(source_path)}
for file_name in source_files.values():
    if available_files.get(file_name, 0) <= 0:
        raise ValueError(f"Source file is missing or empty: {file_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load and save data

# COMMAND ----------
results = []
for source_table, file_name in source_files.items():
    raw = (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .option("quote", '"')
        .option("escape", '"')
        .csv(f"{source_path}/{file_name}")
    )
    bronze = raw.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_loaded_at"),
    )
    table_name = ".".join(
        f"`{part.replace('`', '``')}`"
        for part in [catalog, bronze_schema, f"{table_prefix}{source_table}"]
    )
    bronze.write.format("delta").mode("overwrite").saveAsTable(table_name)
    results.append((f"{table_prefix}{source_table}", spark.table(table_name).count()))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Results

# COMMAND ----------
display(spark.createDataFrame(results, "table_name STRING, row_count LONG"))
