# Databricks notebook source
# MAGIC %md
# MAGIC # One-time removal of the unused dev Landing Volume
# MAGIC Run after the one-time dev unbind migration and successful deployment.
# MAGIC This notebook is deliberately not part of the recurring processing job.
# MAGIC The active /Workspace Landing folder and its CSV files are not touched.

# COMMAND ----------
catalog = "supplier_performance_dev"
schema = "dev_landing"
volume = "dev_source_files"
full_name = f"{catalog}.{schema}.{volume}"
if spark.sql(f"SHOW SCHEMAS IN {catalog} LIKE '{schema}'").count():
    exists = spark.sql(f"SHOW VOLUMES IN {catalog}.{schema}").filter(f"volume_name = '{volume}'").count()
    if exists:
        entries = dbutils.fs.ls(f"/Volumes/{catalog}/{schema}/{volume}/")
        if entries:
            raise ValueError("Volume is not empty. Stop and review its contents before removal.")
        spark.sql(f"DROP VOLUME {full_name}")
        print(f"Removed empty Volume {full_name}")
    # RESTRICT refuses removal if any other objects are present; never use CASCADE here.
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} RESTRICT")
    print("Removed unused dev Landing schema. Workspace Landing files are unchanged.")
else:
    print("Unused dev Landing schema is already absent.")
