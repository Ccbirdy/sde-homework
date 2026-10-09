# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: SAP-Dateien laden
# MAGIC CSV-Dateien aus der Landing Zone als Zeichenketten speichern. Jeder Lauf ersetzt den aktuellen Tabelleninhalt.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameter

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Zielkatalog")
dbutils.widgets.text("schema_name", "supplier_performance_dev", "Zielschema")
dbutils.widgets.text(
    "source_path",
    "/Volumes/workspace/supplier_performance_dev/source_files",
    "Landing Zone",
)

# COMMAND ----------
catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
source_path = dbutils.widgets.get("source_path").strip().rstrip("/")

if not all([catalog, schema_name, source_path]):
    raise ValueError("catalog, schema_name und source_path müssen gesetzt sein.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Quelldateien

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
        raise ValueError(f"Quelldatei fehlt oder ist leer: {file_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Daten laden und speichern

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
        for part in [catalog, schema_name, f"bronze_{source_table}"]
    )
    bronze.write.format("delta").mode("overwrite").saveAsTable(table_name)
    results.append((f"bronze_{source_table}", spark.table(table_name).count()))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ergebnis

# COMMAND ----------
display(spark.createDataFrame(results, "table_name STRING, row_count LONG"))
