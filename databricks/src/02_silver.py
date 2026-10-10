# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Clean SAP data
# MAGIC Keep valid records and quarantine separately. See DATA_RULES.md.
# MAGIC Dates use DATE; identifiers keep leading zeros as STRING; quantities and amounts use DECIMAL.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------
from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "supplier_performance_dev", "Target catalog")
dbutils.widgets.text("bronze_schema", "dev_bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "dev_silver", "Silver schema")
dbutils.widgets.text("table_prefix", "dev_", "Table prefix")

# COMMAND ----------
catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
silver_schema = dbutils.widgets.get("silver_schema").strip()
table_prefix = dbutils.widgets.get("table_prefix").strip()
if not all([catalog, bronze_schema, silver_schema, table_prefix]):
    raise ValueError("Set catalog, bronze_schema, silver_schema and table_prefix.")


def table_path(name):
    layer, suffix = name.split("_", 1)
    schema_name = {"bronze": bronze_schema, "silver": silver_schema}[layer]
    name = table_prefix + suffix
    return ".".join(f"`{part.replace('`', '``')}`" for part in [catalog, schema_name, name])


# COMMAND ----------
# MAGIC %md
# MAGIC ## Rules

# COMMAND ----------
# Process master data and headers before dependent items.
specs = {
    "lfa1": {"key": ["LIFNR"], "required": ["NAME1"]},
    "mara": {"key": ["MATNR"], "required": ["MEINS"]},
    "ekko": {
        "key": ["MANDT", "EBELN"],
        "required": ["LIFNR", "BEDAT"],
        "dates": ["BEDAT", "AEDAT"],
    },
    "ekpo": {
        "key": ["MANDT", "EBELN", "EBELP"],
        "required": ["MATNR", "MENGE", "MEINS", "WERKS"],
        "numbers": ["MENGE", "NETPR", "NETWR"],
    },
    "eket": {
        "key": ["MANDT", "EBELN", "EBELP", "ETENR"],
        "required": ["EINDT", "MENGE"],
        "dates": ["EINDT"],
        "numbers": ["MENGE", "WEMNG"],
    },
    "likp": {
        "key": ["MANDT", "VBELN"],
        "required": ["LIFNR", "WADAT_IST", "WERKS"],
        "dates": ["WADAT_IST"],
    },
    "lips": {
        "key": ["MANDT", "VBELN", "POSNR"],
        "required": ["EBELN", "EBELP", "MATNR", "LFIMG", "MEINS", "WERKS"],
        "numbers": ["LFIMG"],
    },
}
date_formats = [(r"^\d{4}-\d{2}-\d{2}$", "yyyy-MM-dd"),
                (r"^\d{2}\.\d{2}\.\d{4}$", "dd.MM.yyyy")]
uppercase_fields = {"MEINS", "WAERS", "LAND1", "LOEKZ"}
empty_issues = F.array().cast("array<string>")


def flag(frame, condition, reason):
    issue = F.when(F.coalesce(condition, F.lit(False)), F.array(F.lit(reason)))
    return frame.withColumn("_dq_errors", F.array_union(
        F.col("_dq_errors"), issue.otherwise(empty_issues)
    ))


def usable(frame):
    return frame.filter(F.size("_dq_errors") == 0)


def reference(frame, parent, keys, label, fields=()):
    # Silver parents have at most one row per candidate key.
    lookup = spark.table(table_path(f"silver_{parent}")).select(
        *keys,
        F.lit(True).alias(f"_ref_{label}_found"),
        *[F.col(c).alias(f"_ref_{label}_{c}") for c in fields],
    )
    joined = frame.join(lookup, keys, "left")
    return flag(joined, F.col(f"_ref_{label}_found").isNull(),
                f"UNRESOLVED_REFERENCE:{label}")


def compare(frame, left, right, reason):
    # Missing parents are handled by the reference check.
    return flag(frame, F.col(left).isNotNull() & F.col(right).isNotNull()
                & (F.col(left) != F.col(right)), reason)


# COMMAND ----------
# MAGIC %md
# MAGIC ## Clean data

# COMMAND ----------
quarantine_frames = []
summary_rows = []
for name, spec in specs.items():
    source = spark.table(table_path(f"bronze_{name}"))
    fields = [c for c in source.columns if not c.startswith("_")]
    expected = set(spec["key"] + spec["required"] + spec.get("dates", [])
                   + spec.get("numbers", []))
    if name == "ekpo":
        expected.add("LOEKZ")
    missing_columns = expected - set(fields)
    if missing_columns:
        raise ValueError(f"bronze_{name}: missing columns {sorted(missing_columns)}")

    # Deduplicate identical source fields; preserve lineage.
    frame = source.groupBy(*fields).agg(
        F.count("*").alias("_source_occurrences"),
        F.sort_array(F.collect_set("_source_file")).alias("_source_files"),
        F.min("_loaded_at").alias("_bronze_loaded_at"),
    )
    frame = frame.withColumn("_raw_record", F.to_json(
        F.struct(*[F.col(c) for c in fields]), {"ignoreNullFields": "false"}
    )).withColumn("_dq_errors", empty_issues)
    for column in fields:
        trimmed = F.trim(F.col(column))
        value = F.when(trimmed != "", trimmed)
        frame = frame.withColumn(column, F.upper(value) if column in uppercase_fields else value)

    frame = frame.withColumn("_key_variants", F.count("*").over(Window.partitionBy(*spec["key"])))
    frame = flag(frame, F.col("_key_variants") > 1, "CONFLICTING_KEY")
    for column in dict.fromkeys(spec["key"] + spec["required"]):
        frame = flag(frame, F.col(column).isNull(), f"MISSING:{column}")

    for column in spec.get("dates", []):
        parsed = F.coalesce(*[
            F.when(F.col(column).rlike(pattern),
                   F.try_to_timestamp(F.col(column), F.lit(fmt)).cast("date"))
            for pattern, fmt in date_formats
        ])
        frame = frame.withColumn("_parsed", parsed)
        frame = flag(frame, F.col(column).isNotNull() & F.col("_parsed").isNull(),
                     f"INVALID_DATE:{column}")
        frame = frame.drop(column).withColumnRenamed("_parsed", column)

    for column in spec.get("numbers", []):
        # Accept a decimal dot or comma; no thousands separator.
        frame = frame.withColumn("_parsed", F.when(
            F.col(column).rlike(r"^[+-]?\d+(?:[.,]\d{1,6})?$"),
            F.expr(f"try_cast(replace(`{column}`, ',', '.') AS DECIMAL(38,6))"),
        ))
        frame = flag(frame, F.col(column).isNotNull() & F.col("_parsed").isNull(),
                     f"INVALID_NUMBER:{column}")
        frame = frame.drop(column).withColumnRenamed("_parsed", column)
        frame = flag(frame, F.col(column) < 0, f"NEGATIVE_NUMBER:{column}")
        if column in {"MENGE", "LFIMG"}:
            frame = flag(frame, F.col(column) == 0, f"ZERO_QUANTITY:{column}")

    if name == "ekko":
        frame = reference(frame, "lfa1", ["LIFNR"], "vendor")
    elif name == "ekpo":
        frame = flag(frame, F.col("LOEKZ").isNotNull() & (F.col("LOEKZ") != "L"),
                     "UNKNOWN_DELETE_STATUS")
        frame = frame.withColumn("_is_deleted", F.coalesce(F.col("LOEKZ") == "L", F.lit(False)))
        frame = reference(frame, "ekko", ["MANDT", "EBELN"], "order")
        frame = reference(frame, "mara", ["MATNR"], "material", ["MEINS"])
        frame = compare(frame, "MEINS", "_ref_material_MEINS", "UNIT_MISMATCH:MARA")
    elif name == "eket":
        frame = reference(frame, "ekpo", ["MANDT", "EBELN", "EBELP"], "order_item")
    elif name == "likp":
        frame = reference(frame, "lfa1", ["LIFNR"], "vendor")
    elif name == "lips":
        frame = reference(frame, "ekpo", ["MANDT", "EBELN", "EBELP"], "order_item",
                          ["MATNR", "MEINS", "WERKS"])
        frame = reference(frame, "likp", ["MANDT", "VBELN"], "delivery", ["LIFNR", "WERKS"])
        frame = reference(frame, "ekko", ["MANDT", "EBELN"], "order", ["LIFNR"])
        for column in ["MATNR", "MEINS", "WERKS"]:
            frame = compare(frame, column, f"_ref_order_item_{column}",
                            f"ORDER_ITEM_MISMATCH:{column}")
        frame = compare(frame, "WERKS", "_ref_delivery_WERKS", "DELIVERY_MISMATCH:WERKS")
        frame = compare(frame, "_ref_order_LIFNR", "_ref_delivery_LIFNR", "SUPPLIER_MISMATCH")

    frame = frame.drop(*[c for c in frame.columns if c.startswith("_ref_")])
    frame = frame.withColumn("_processed_at", F.current_timestamp())
    usable(frame).write.format("delta").mode("overwrite").saveAsTable(table_path(f"silver_{name}"))

    rejected = frame.filter(F.size("_dq_errors") > 0)
    quarantine_frames.append(rejected.select(
        F.lit(name).alias("source_table"),
        F.to_json(F.struct(*[F.col(c) for c in spec["key"]]),
                  {"ignoreNullFields": "false"}).alias("candidate_key"),
        "_raw_record", "_dq_errors", "_source_occurrences", "_source_files",
        "_bronze_loaded_at", "_processed_at",
    ))
    counts = frame.agg(
        F.coalesce(F.sum("_source_occurrences"), F.lit(0)).alias("source_rows"),
        F.count("*").alias("distinct_rows"),
        F.coalesce(F.sum(F.when(F.size("_dq_errors") == 0, 1).otherwise(0)), F.lit(0)).alias("silver_rows"),
        F.coalesce(F.sum(F.when(F.size("_dq_errors") > 0, 1).otherwise(0)), F.lit(0)).alias("quarantine_rows"),
    ).first()
    summary_rows.append((name, counts.source_rows, counts.source_rows - counts.distinct_rows,
                         counts.silver_rows, counts.quarantine_rows))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Quarantine and results

# COMMAND ----------
quarantine = reduce(lambda left, right: left.unionByName(right), quarantine_frames)
quarantine.write.format("delta").mode("overwrite").saveAsTable(table_path("silver_quarantine"))
summary = spark.createDataFrame(
    summary_rows,
    "source_table STRING, bronze_rows LONG, duplicate_rows_removed LONG, silver_rows LONG, quarantine_rows LONG",
)
summary.write.format("delta").mode("overwrite").saveAsTable(table_path("silver_load_summary"))
display(summary)
