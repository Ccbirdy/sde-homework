# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Load SAP source files
# MAGIC Read the seven small CSV files from the configured Workspace landing folder.
# MAGIC Store source fields as strings. Preserve duplicate records and leading zeros.
# MAGIC Each run replaces the current table contents. The job identity needs read access to this folder.
# MAGIC Files are parsed in notebook Python before transfer to Spark; this is intended for the small case-study snapshot.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------
import csv
import re
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

dbutils.widgets.text("table_prefix", "dev_", "Table prefix")
table_prefix = dbutils.widgets.get("table_prefix").strip()
if not re.fullmatch(r"[a-z][a-z0-9_]*_", table_prefix):
    raise ValueError("Use a table prefix such as dev_ or prod_.")

dbutils.widgets.text("catalog", "supplier_performance_dev", "Target catalog")
dbutils.widgets.text("bronze_schema", "dev_bronze", "Target schema")
dbutils.widgets.text(
    "source_path",
    "/Workspace/Users/guochengcheng93@gmail.com/landing_zone_case_solution",
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

landing_dir = Path(source_path)
if not source_path.startswith("/Workspace/"):
    raise ValueError("source_path must be an absolute /Workspace/ folder path.")
if not landing_dir.is_dir():
    raise ValueError(f"Landing folder is missing or not readable by the job identity: {source_path}")


def read_source_csv(path):
    """Read a small Workspace CSV locally, preserving strings and leading zeros."""
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError(f"Source file is missing or empty: {path.name}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=";", quotechar='"', doublequote=True, strict=True)
        header = next(reader, None)
        if not header or any(not name for name in header) or len(header) != len(set(header)):
            raise ValueError(f"Invalid or duplicate column names: {path.name}")
        if any(name.startswith("_") for name in header):
            raise ValueError(f"Source columns must not use reserved metadata names: {path.name}")
        rows = []
        for row in reader:
            if not row:
                continue
            if len(row) != len(header):
                raise ValueError(f"Wrong field count in {path.name}, line {reader.line_num}")
            # Match the previous Bronze behaviour: empty CSV fields become null.
            rows.append(tuple(value if value != "" else None for value in row))
    return header, rows


# Validate all seven source files before replacing any Bronze table.
# README.txt and any unrelated files are ignored by the explicit source list.
source_data = {name: read_source_csv(landing_dir / filename)
               for name, filename in source_files.items()}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load and save data

# COMMAND ----------
results = []
for source_table, file_name in source_files.items():
    header, rows = source_data[source_table]
    schema = StructType([StructField(column, StringType(), True) for column in header])
    # Workspace files are read by notebook Python, not by distributed Spark workers.
    raw = spark.createDataFrame(rows, schema=schema)
    bronze = (raw.withColumn("_source_file", F.lit(str(landing_dir / file_name)))
              .withColumn("_loaded_at", F.current_timestamp()))
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
