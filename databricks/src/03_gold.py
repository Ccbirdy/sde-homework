# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Monthly supplier performance
# MAGIC FIFO allocations and schedule-line performance use each scheduled date and its month end.
# MAGIC Supplier monthly rates and rankings give each evaluable schedule line equal weight.
# MAGIC See DATA_RULES.md for the approved allocation rule, exclusions and limitations.
# MAGIC Branch names follow the customer mapping. The delivery-date interpretation remains provisional.

# COMMAND ----------
from datetime import date
import json
import re

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "supplier_performance_dev", "Target catalog")
for key, default in [("bronze_schema", "dev_bronze"), ("silver_schema", "dev_silver"),
                     ("gold_internal_schema", "dev_gold_data"), ("gold_schema", "dev_gold_views"),
                     ("table_prefix", "dev_"), ("branch_groups_json", "{}"), ("central_group", "")]:
    dbutils.widgets.text(key, default, key.replace("_", " ").title())
dbutils.widgets.text("as_of_date", "2026-09-30", "Evaluation cutoff (assumption, yyyy-MM-dd)")
dbutils.widgets.text("min_items", "1", "Minimum evaluated schedule lines for ranking")

catalog = dbutils.widgets.get("catalog").strip()
layer_schemas = {layer: dbutils.widgets.get(layer + "_schema").strip()
                 for layer in ["bronze", "silver", "gold_internal", "gold"]}
table_prefix = dbutils.widgets.get("table_prefix").strip()
branch_groups = json.loads(dbutils.widgets.get("branch_groups_json"))
central_group = dbutils.widgets.get("central_group").strip()
if not isinstance(branch_groups, dict) or any(
    not isinstance(branch, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", branch)
    or not isinstance(group, str) or not group.strip() for branch, group in branch_groups.items()
):
    raise ValueError("branch_groups_json must map branch codes to non-empty account group names.")
if len(set(branch_groups.values())) != len(branch_groups):
    raise ValueError("Use a separate account group for each branch.")
if central_group and central_group in branch_groups.values():
    raise ValueError("The central group must not also be a branch group.")
as_of_date = dbutils.widgets.get("as_of_date").strip()
if date.fromisoformat(as_of_date).isoformat() != as_of_date:
    raise ValueError("as_of_date must use yyyy-MM-dd.")
min_items = int(dbutils.widgets.get("min_items"))
if not catalog or not all(layer_schemas.values()) or not table_prefix or min_items < 1:
    raise ValueError("Set catalog and schemas; min_items must be at least 1.")


def quoted(value):
    return "`" + value.replace("`", "``") + "`"


def sql_literal(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def table_path(name):
    layer, suffix = name.split("_", 1)
    return ".".join(map(quoted, [catalog, layer_schemas[layer], table_prefix + suffix]))


def read(name):
    return spark.table(table_path(name))


# SAP identifiers stay in Bronze/Silver. These Gold aliases express our provisional interpretations.
# WERKS mapping is customer-confirmed; WADAT_IST as delivery date remains provisional.
column_names = {
    "MANDT": "sap_client_id", "EBELN": "purchase_order_id", "EBELP": "purchase_order_item_id",
    "LIFNR": "supplier_id", "NAME1": "supplier_name", "WERKS": "branch_code", "BRANCH_NAME": "branch_name",
    "MATNR": "material_id", "MEINS": "quantity_unit", "BEDAT": "order_date",
    "VBELN": "delivery_id", "POSNR": "delivery_item_id", "WADAT_IST": "actual_delivery_date",
    "LFIMG": "delivered_quantity", "MENGE": "ordered_quantity", "TXZ01": "item_description",
    "NETPR": "net_unit_price", "NETWR": "net_order_value", "WAERS": "currency_code",
    "ETENR": "schedule_line_id", "EINDT": "scheduled_delivery_date", "WEMNG": "recorded_received_quantity",
    "_is_deleted": "is_deleted", "candidate_items": "candidate_schedule_line_count",
    "evaluated_items": "evaluated_schedule_line_count", "excluded_items": "excluded_schedule_line_count",
    "scope": "report_scope", "coverage_rate": "evaluation_coverage_rate",
    "score": "performance_score", "min_items": "minimum_schedule_line_count",
    "ranking_eligible": "is_ranking_eligible", "worst_rank": "worst_supplier_rank",
    "ordered_qty": "ordered_quantity", "scheduled_qty": "scheduled_quantity",
    "schedule_count": "schedule_line_count", "delivered_by_due_qty": "delivered_by_due_quantity",
    "delivered_by_month_end_qty": "delivered_by_month_end_quantity", "shortfall_qty": "shortfall_quantity",
    "first_due_date": "first_scheduled_delivery_date", "final_due_date": "final_scheduled_delivery_date",
    "evaluation_date": "evaluation_month_end", "on_time": "is_on_time", "in_full": "is_in_full",
    "fill_rate": "quantity_fill_rate", "mean_fill_rate": "mean_quantity_fill_rate",
    "by_due_date": "is_by_due_date", "by_month_end": "is_by_month_end", "after_cutoff": "is_after_cutoff",
}
# Source lineage labels are shown in Catalog Explorer descriptions.
# MARA also affects Silver material/unit validation; it is not a separate Gold dataset.
dataset_descriptions = {'purchase_order_items': 'OPERATIONAL DETAIL | Primary source: EKPO (order items). Enriched with EKKO (order '
                         'header) and LFA1 (supplier). Valid Silver records, not a raw CSV copy; deleted '
                         'items remain flagged.',
 'schedule_lines': 'OPERATIONAL DETAIL | Primary source: EKET (schedule lines). Enriched with EKPO, EKKO and '
                   'LFA1 for order, branch and supplier context. One row per valid schedule line.',
 'delivery_items': 'OPERATIONAL DETAIL | Primary source: LIPS (delivery items), joined to LIKP (delivery '
                   'header). Enriched with EKPO, EKKO and LFA1. Valid Silver deliveries without a '
                   'performance-month filter.',
 'delivery_evidence': 'DERIVED EVIDENCE | LIPS + LIKP delivery records linked to order-item performance '
                      'using EKPO, EKKO, EKET and LFA1 context and Silver quarantine. Adds deadline, cutoff '
                      'and evaluation flags; not an EKET copy.',
 'order_item_performance': 'DERIVED ANALYSIS | Bronze EKPO supplies candidate keys. Silver EKPO, EKKO, EKET, '
                           'LIPS, LIKP and LFA1 plus quarantine supply validated context. Calculates '
                           'item-level performance and exclusion reasons.',
 'supplier_monthly': 'DERIVED AGGREGATE | Aggregates FIFO schedule_line_performance with equal line weights by supplier, month and '
                     'company/branch scope. Calculates rates, score, sample size, coverage and ranking '
                     'eligibility.',
 'worst_suppliers': 'DERIVED RANKING | Ranks eligible supplier_monthly results. Up to three lowest-scoring '
                    'suppliers per month and company/branch scope.',
 'quality_summary': 'DERIVED QUALITY SUMMARY | Counts schedule_line_performance evaluation/exclusion reasons '
                    'and unassigned_quarantine records. Reasons can overlap. Central access only.',
 'unassigned_quarantine': 'DERIVED QUALITY DETAIL | Silver quarantine records from EKPO, EKET and LIPS that '
                          'cannot be matched to Bronze EKPO item keys. Includes raw records and validation '
                          'reasons. Central access only.'}
published = []
dataset_descriptions["branch_mapping"] = "REFERENCE DATA | Customer-confirmed WERKS to branch name mapping from Landing via Bronze and Silver."
dataset_descriptions.update({
    "delivery_allocations": "DERIVED ALLOCATION | FIFO by schedule due date and actual delivery date within one order item. Inferred, not source-confirmed links. Cutoff deliveries only; excluded items have no allocations.",
    "schedule_line_performance": "DERIVED ANALYSIS | One Bronze schedule key with valid Silver context where available. FIFO on-time/full-by-month-end results; month follows the individual scheduled date. No supplier weighting chosen.",
    "delivery_allocation_balance": "DERIVED CONTROL | One valid delivery item. Delivered = allocated + unallocated quantity. Distinguishes overdelivery, excluded items and deliveries after cutoff.",
})
for legacy_name in ("order_item_performance", "delivery_evidence"):
    dataset_descriptions[legacy_name] = "LEGACY FINAL-DATE METHOD | " + dataset_descriptions[legacy_name]
branches = read("silver_branch_mapping").select(
    F.col("WERKS").alias("branch_code"), F.col("BRANCH_NAME").alias("branch_name")
)
if read("silver_quarantine").filter(F.col("source_table") == "branch_mapping").limit(1).count():
    raise ValueError("Fix quarantined branch mapping records before publishing Gold.")
if not branches.take(1) or branches.groupBy("branch_code").count().filter("count > 1").limit(1).count():
    raise ValueError("Branch mapping must be nonempty and unique by WERKS.")


def save(frame, name):
    suffix = name.removeprefix("gold_")
    target = ".".join(map(quoted, [catalog, layer_schemas["gold_internal"], table_prefix + suffix]))
    business = frame.select(*[F.col(c).alias(column_names.get(c, c)) for c in frame.columns])
    if "branch_code" in business.columns and "branch_name" not in business.columns:
        # Left join preserves unknown codes and company-wide ALL rows without multiplying quantities.
        business = business.join(branches, "branch_code", "left")
        business = business.withColumn("branch_mapping_status", F.when(
            F.col("branch_code").isNull(), "NOT_APPLICABLE"
        ).when(F.col("branch_name").isNull(), "UNMAPPED").otherwise("MAPPED"))
    (business.withColumn("as_of_date", F.lit(as_of_date).cast("date"))
     .withColumn("processed_at", F.current_timestamp())
     .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target))
    spark.sql(f"COMMENT ON TABLE {target} IS {sql_literal(dataset_descriptions[suffix])}")
    published.append((suffix, target, business.columns))


keys = ["MANDT", "EBELN", "EBELP"]
zero = F.lit(0).cast("decimal(38,6)")
empty_reasons = F.array().cast("array<string>")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Population and quality limits
# MAGIC Excluded order items remain visible. Bronze supplies keys only.
# MAGIC Quarantine supplies exclusion reasons, not quantities for scoring.

# COMMAND ----------
base = read("bronze_ekpo").select(*[
    F.when(F.trim(F.col(c)) != "", F.trim(F.col(c))).alias(c) for c in keys
]).distinct()
items = read("silver_ekpo").select(
    *keys, "MATNR", "WERKS", "MEINS", "_is_deleted",
    F.col("MENGE").alias("ordered_qty"), F.lit(True).alias("_valid_item"),
)
orders = read("silver_ekko").select("MANDT", "EBELN", "LIFNR", "BEDAT")
vendors = read("silver_lfa1").select("LIFNR", "NAME1")
quarantine = read("silver_quarantine")
affected = quarantine.filter(F.col("source_table").isin("ekpo", "eket", "lips")).select(
    "source_table", "_dq_errors", "_raw_record",
    *[F.when(F.trim(F.get_json_object("_raw_record", f"$.{c}")) != "",
             F.trim(F.get_json_object("_raw_record", f"$.{c}"))).alias(c) for c in keys],
)
mapped = affected.join(base, keys, "left_semi")
issues = mapped.groupBy(*keys).agg(
    F.sort_array(F.collect_set("source_table")).alias("quarantined_sources")
)
# Report unassigned schedules and deliveries separately.
unassigned = affected.join(base, keys, "left_anti")

schedules = read("silver_eket").groupBy(*keys).agg(
    F.min("EINDT").alias("first_due_date"),
    F.max("EINDT").alias("final_due_date"),
    F.sum("MENGE").alias("scheduled_qty"),
    F.count("*").alias("schedule_count"),
)
details = (base.join(items, keys, "left")
           .join(orders, ["MANDT", "EBELN"], "left")
           .join(vendors, "LIFNR", "left")
           .join(schedules, keys, "left")
           .join(issues, keys, "left"))
details = (details.withColumn("report_month", F.trunc("final_due_date", "month"))
           .withColumn("evaluation_date", F.last_day("final_due_date")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Delivery evidence and quantities
# MAGIC Aggregate schedules before joining to avoid multiplying quantities.
# MAGIC WADAT_IST and LFIMG are provisional delivery date and quantity fields.

# COMMAND ----------
deliveries = (read("silver_lips").select(
    *keys, "VBELN", "POSNR", "MATNR", "MEINS", "WERKS", "LFIMG"
).join(read("silver_likp").select("MANDT", "VBELN", "WADAT_IST"),
       ["MANDT", "VBELN"], "inner"))
evidence = deliveries.join(details.select(
    *keys, "LIFNR", "report_month", "final_due_date", "evaluation_date", "BEDAT"
), keys, "inner")
evidence = (evidence.withColumn("by_due_date", F.col("WADAT_IST") <= F.col("final_due_date"))
            .withColumn("by_month_end", F.col("WADAT_IST") <= F.col("evaluation_date"))
            .withColumn("after_cutoff", F.col("WADAT_IST") > F.lit(as_of_date).cast("date")))
amounts = evidence.groupBy(*keys).agg(
    F.sum(F.when(F.col("by_due_date") & ~F.col("after_cutoff"), F.col("LFIMG"))
          .otherwise(zero)).alias("delivered_by_due_qty"),
    F.sum(F.when(F.col("by_month_end") & ~F.col("after_cutoff"), F.col("LFIMG"))
          .otherwise(zero)).alias("delivered_by_month_end_qty"),
    F.max((F.col("WADAT_IST") < F.col("BEDAT")).cast("int")).alias("_delivery_before_order"),
)
details = details.join(amounts, keys, "left").withColumn("exclusion_reasons", empty_reasons)


def exclude(frame, condition, reason):
    return frame.withColumn("exclusion_reasons", F.array_union(
        F.col("exclusion_reasons"),
        F.when(F.coalesce(condition, F.lit(False)), F.array(F.lit(reason))).otherwise(empty_reasons),
    ))


for condition, reason in [
    (F.col("_valid_item").isNull(), "INVALID_SILVER_ITEM"),
    (F.col("LIFNR").isNull(), "INVALID_ORDER_HEADER"),
    (F.col("_is_deleted"), "DELETED_ITEM"),
    (F.col("final_due_date").isNull(), "NO_VALID_SCHEDULE"),
    (F.size("quarantined_sources") > 0, "RELATED_QUARANTINE"),
    (F.col("scheduled_qty") != F.col("ordered_qty"), "SCHEDULE_QUANTITY_MISMATCH"),
    (F.col("first_due_date") < F.col("BEDAT"), "SCHEDULE_BEFORE_ORDER"),
    (F.col("_delivery_before_order") == 1, "DELIVERY_BEFORE_ORDER"),
    (F.col("evaluation_date") > F.lit(as_of_date).cast("date"), "MONTH_NOT_CLOSED"),
]:
    details = exclude(details, condition, reason)

details = details.withColumn("is_evaluable", F.size("exclusion_reasons") == 0)
for column in ["delivered_by_due_qty", "delivered_by_month_end_qty"]:
    details = details.withColumn(column, F.coalesce(F.col(column), zero))
details = (details
    .withColumn("on_time", F.when(F.col("is_evaluable"),
                                 F.col("delivered_by_due_qty") >= F.col("ordered_qty")))
    .withColumn("in_full", F.when(F.col("is_evaluable"),
                                 F.col("delivered_by_month_end_qty") >= F.col("ordered_qty")))
    .withColumn("fill_rate", F.when(F.col("is_evaluable"), F.least(
        F.lit(1.0), F.col("delivered_by_month_end_qty") / F.col("ordered_qty"))))
    .withColumn("shortfall_qty", F.when(F.col("is_evaluable"), F.greatest(
        zero, F.col("ordered_qty") - F.col("delivered_by_month_end_qty"))))
    .drop("_valid_item", "_delivery_before_order"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## FIFO allocation and individual schedule performance
# MAGIC Earliest due schedule receives earliest actual delivery quantities first.
# MAGIC Equal dates are ordered by source IDs. Early deliveries may satisfy future schedules.
# MAGIC This infers a relationship; no actual-to-planned linkage was supplied by the customer.

# COMMAND ----------
schedule_keys = keys + ["ETENR"]
# Keep invalid source keys visible; only unique valid Silver rows supply dates/quantities.
schedule_base = read("bronze_eket").select(*[
    F.when(F.trim(F.col(c)) != "", F.trim(F.col(c))).alias(c) for c in schedule_keys
]).distinct()
schedule_valid = read("silver_eket").select(
    *schedule_keys, "EINDT", F.col("MENGE").alias("schedule_quantity"),
    F.lit(True).alias("_valid_schedule"),
)
parent_context = details.select(
    *keys, "LIFNR", "NAME1", "WERKS", "MATNR", "MEINS", "BEDAT",
    F.array_except("exclusion_reasons", F.array(F.lit("MONTH_NOT_CLOSED"))).alias("exclusion_reasons"),
    F.lit(True).alias("_parent_found"),
)
schedule_candidates = (schedule_base.join(schedule_valid, schedule_keys, "left")
                       .join(parent_context, keys, "left")
                       .withColumn("exclusion_reasons", F.coalesce("exclusion_reasons", empty_reasons)))
schedule_candidates = exclude(schedule_candidates, F.col("_parent_found").isNull(), "INVALID_ORDER_ITEM")
schedule_candidates = exclude(schedule_candidates, F.col("_valid_schedule").isNull(), "INVALID_SILVER_SCHEDULE")
schedule_candidates = (schedule_candidates
    .withColumn("is_allocatable", F.size("exclusion_reasons") == 0)
    .withColumn("report_month", F.trunc("EINDT", "month"))
    .withColumn("evaluation_date", F.last_day("EINDT")))
# Allocation includes future schedules to distinguish excess from early fulfilment.
# The performance cutoff excludes future actual deliveries, not future obligations.
schedule_candidates.filter("is_allocatable").select(
    *schedule_keys, "EINDT", "schedule_quantity", "WERKS", "LIFNR", "MATNR", "MEINS"
).createOrReplaceTempView("fifo_schedule_input")
deliveries.filter(F.col("WADAT_IST") <= F.lit(as_of_date).cast("date")).select(
    *keys, "VBELN", "POSNR", "WADAT_IST", "LFIMG"
).createOrReplaceTempView("fifo_delivery_input")

# Intersections of cumulative quantity intervals support both many-to-one and one-to-many.
# Explicit decimal casts preserve six fractional digits during Spark decimal subtraction.
allocation_query = """
WITH schedules_end AS (
  SELECT *, SUM(schedule_quantity) OVER (
    PARTITION BY MANDT, EBELN, EBELP ORDER BY EINDT, ETENR
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS schedule_end
  FROM fifo_schedule_input
), schedules AS (
  SELECT *, COALESCE(LAG(schedule_end) OVER (
    PARTITION BY MANDT, EBELN, EBELP ORDER BY EINDT, ETENR), 0) AS schedule_start
  FROM schedules_end
), deliveries_end AS (
  SELECT *, SUM(LFIMG) OVER (
    PARTITION BY MANDT, EBELN, EBELP ORDER BY WADAT_IST, VBELN, POSNR
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS delivery_end
  FROM fifo_delivery_input
), deliveries AS (
  SELECT *, COALESCE(LAG(delivery_end) OVER (
    PARTITION BY MANDT, EBELN, EBELP ORDER BY WADAT_IST, VBELN, POSNR), 0) AS delivery_start
  FROM deliveries_end
)
SELECT s.MANDT, s.EBELN, s.EBELP, s.ETENR, s.EINDT, s.WERKS, s.LIFNR, s.MATNR, s.MEINS,
       d.VBELN, d.POSNR, d.WADAT_IST,
       CAST(LEAST(s.schedule_end, d.delivery_end) AS DECIMAL(37,6))
       - CAST(GREATEST(s.schedule_start, d.delivery_start) AS DECIMAL(37,6)) AS allocated_quantity
FROM schedules s JOIN deliveries d
  ON s.MANDT = d.MANDT AND s.EBELN = d.EBELN AND s.EBELP = d.EBELP
 AND s.schedule_start < d.delivery_end AND d.delivery_start < s.schedule_end
"""
allocations = (spark.sql(allocation_query)
    .withColumn("report_month", F.trunc("EINDT", "month"))
    .withColumn("allocation_rule", F.lit("FIFO_DUE_DATE_V1"))
    .withColumn("is_inferred", F.lit(True)))
allocated_schedules = allocations.groupBy(*schedule_keys).agg(
    F.sum("allocated_quantity").alias("allocated_quantity"),
    F.sum(F.when(F.col("WADAT_IST") <= F.col("EINDT"), F.col("allocated_quantity"))
          .otherwise(zero)).alias("allocated_by_due_quantity"),
    F.sum(F.when(F.col("WADAT_IST") <= F.last_day("EINDT"), F.col("allocated_quantity"))
          .otherwise(zero)).alias("allocated_by_month_end_quantity"),
    F.max("WADAT_IST").alias("last_allocated_delivery_date"),
)
schedule_performance = schedule_candidates.join(allocated_schedules, schedule_keys, "left")
schedule_performance = exclude(schedule_performance,
    F.col("evaluation_date") > F.lit(as_of_date).cast("date"), "MONTH_NOT_CLOSED")
schedule_performance = schedule_performance.withColumn("is_evaluable", F.size("exclusion_reasons") == 0)
for quantity_column in ("allocated_quantity", "allocated_by_due_quantity", "allocated_by_month_end_quantity"):
    # Unknown/invalid inputs stay null; zero is meaningful only for allocation-eligible records.
    schedule_performance = schedule_performance.withColumn(quantity_column,
        F.when(F.col("is_allocatable"), F.coalesce(F.col(quantity_column), zero)))
schedule_performance = (schedule_performance
    .withColumn("on_time", F.when(F.col("is_evaluable"),
        F.col("allocated_by_due_quantity") >= F.col("schedule_quantity")))
    .withColumn("in_full", F.when(F.col("is_evaluable"),
        F.col("allocated_by_month_end_quantity") >= F.col("schedule_quantity")))
    .withColumn("fill_rate", F.when(F.col("is_evaluable"),
        F.col("allocated_by_month_end_quantity") / F.col("schedule_quantity")))
    .withColumn("shortfall_qty", F.when(F.col("is_evaluable"),
        F.col("schedule_quantity") - F.col("allocated_by_month_end_quantity")))
    .withColumn("completion_date", F.when(F.col("allocated_quantity") >= F.col("schedule_quantity"),
        F.col("last_allocated_delivery_date")))
    .withColumn("allocation_rule", F.lit("FIFO_DUE_DATE_V1"))
    .drop("_valid_schedule", "_parent_found"))

delivery_keys = ["MANDT", "VBELN", "POSNR"]
allocated_deliveries = allocations.groupBy(*delivery_keys).agg(F.sum("allocated_quantity").alias("allocated_quantity"))
allocation_parents = schedule_candidates.filter("is_allocatable").select(*keys).distinct().withColumn("_allocatable", F.lit(True))
allocation_balance = (deliveries.join(allocated_deliveries, delivery_keys, "left")
    .join(allocation_parents, keys, "left")
    .withColumn("allocated_quantity", F.coalesce("allocated_quantity", zero))
    .withColumn("unallocated_quantity", F.col("LFIMG").cast("decimal(37,6)")
                - F.col("allocated_quantity").cast("decimal(37,6)"))
    .withColumn("allocation_status", F.when(F.col("WADAT_IST") > F.lit(as_of_date).cast("date"), "AFTER_CUTOFF")
        .when(F.col("_allocatable").isNull(), "EXCLUDED_OR_NO_VALID_SCHEDULE")
        .when(F.col("unallocated_quantity") > 0, "EXCESS_OVER_SCHEDULES")
        .otherwise("FULLY_ALLOCATED"))
    .drop("_allocatable"))
# Fail before publishing if allocation duplicates or loses quantity through arithmetic.
if (allocations.filter(F.col("allocated_quantity").isNull() | (F.col("allocated_quantity") <= 0)).limit(1).count()
    or allocation_balance.filter(F.col("unallocated_quantity").isNull() | (F.col("unallocated_quantity") < 0)).limit(1).count()
    or schedule_performance.filter(F.col("allocated_quantity") > F.col("schedule_quantity")).limit(1).count()):
    raise ValueError("FIFO quantity conservation failed")

details = details.withColumn("metric_basis", F.lit("LEGACY_ORDER_ITEM_FINAL_DATE"))
schedule_performance = schedule_performance.withColumn("metric_basis", F.lit("FIFO_SCHEDULE_LINE_EQUAL_WEIGHT"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Monthly metrics and ranking
# MAGIC Each evaluable schedule line has equal weight. Do not add quantities across different units.
# MAGIC Score is the mean of the on-time and in-full rates. Lower values are worse.
# MAGIC ALL covers the company; BRANCH calculates each branch separately.

# COMMAND ----------
monthly_base = schedule_performance.filter(
    F.col("LIFNR").isNotNull() & F.col("report_month").isNotNull()
    & (F.col("evaluation_date") <= F.lit(as_of_date).cast("date"))
)
scoped = (monthly_base.withColumn("scope", F.lit("BRANCH"))
          .unionByName(monthly_base.withColumn("WERKS", F.lit(None).cast("string"))
                       .withColumn("scope", F.lit("ALL"))))
scoped.createOrReplaceTempView("supplier_schedule_month_input")
monthly_query = """
SELECT report_month, scope, WERKS, LIFNR, NAME1,
       COUNT(*) AS candidate_items,
       SUM(CAST(is_evaluable AS INT)) AS evaluated_items,
       AVG(CASE WHEN is_evaluable THEN CAST(on_time AS DOUBLE) END) AS on_time_rate,
       AVG(CASE WHEN is_evaluable THEN CAST(in_full AS DOUBLE) END) AS in_full_rate,
       AVG(CASE WHEN is_evaluable THEN fill_rate END) AS mean_fill_rate
FROM supplier_schedule_month_input
GROUP BY report_month, scope, WERKS, LIFNR, NAME1
"""
monthly = spark.sql(monthly_query)
monthly = (monthly.withColumn("excluded_items", F.col("candidate_items") - F.col("evaluated_items"))
           .withColumn("coverage_rate", F.col("evaluated_items") / F.col("candidate_items"))
           .withColumn("score", (F.col("on_time_rate") + F.col("in_full_rate")) / 2)
           .withColumn("ranking_eligible", F.col("evaluated_items") >= min_items)
           .withColumn("min_items", F.lit(min_items))
           .withColumn("metric_basis", F.lit("FIFO_SCHEDULE_LINE_EQUAL_WEIGHT")))
ranking_window = Window.partitionBy("report_month", "scope", "WERKS").orderBy(
    F.col("score").asc(), F.col("on_time_rate").asc(), F.col("in_full_rate").asc(),
    F.col("evaluated_items").desc(), F.col("LIFNR").asc(),
)
ranking = monthly.filter("ranking_eligible").withColumn("worst_rank", F.row_number().over(ranking_window))
worst_three = ranking.filter(F.col("worst_rank") <= 3)



# COMMAND ----------
# MAGIC %md
# MAGIC ## Save results
# MAGIC Outputs replace the previous internal Gold snapshot. Partial writes are possible on failure.
# MAGIC Unassigned deliveries and missing schedules limit conclusions; inspect quality results.

# COMMAND ----------
quality = schedule_performance.select(F.explode(F.when(F.col("is_evaluable"), F.array(F.lit("EVALUATED")))
                                    .otherwise(F.col("exclusion_reasons"))).alias("reason"))
quality = quality.groupBy("reason").count().withColumnRenamed("count", "item_or_record_count")
unknown_quality = unassigned.groupBy("source_table").count().select(
    F.concat(F.lit("UNASSIGNED_QUARANTINE:"), F.col("source_table")).alias("reason"),
    F.col("count").alias("item_or_record_count"),
)
quality = quality.unionByName(unknown_quality)
save(details, "gold_order_item_performance")
save(evidence.join(details.select(*keys, "is_evaluable", "exclusion_reasons"), keys, "left"),
     "gold_delivery_evidence")
save(monthly, "gold_supplier_monthly")
save(worst_three, "gold_worst_suppliers")
save(quality, "gold_quality_summary")
save(unassigned, "gold_unassigned_quarantine")
save(branches, "gold_branch_mapping")
save(allocations, "gold_delivery_allocations")
save(schedule_performance, "gold_schedule_line_performance")
save(allocation_balance, "gold_delivery_allocation_balance")

display(quality.orderBy("reason"))
display(worst_three.filter(F.col("scope") == "ALL").orderBy("report_month", "worst_rank")
        .select(*[F.col(c).alias(column_names.get(c, c)) for c in worst_three.columns]))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Operational data for branch employees
# MAGIC Purchase order items include deleted items with a flag. Delivery items include all valid Silver deliveries.
# MAGIC Schedule lines stay separate to avoid multiplying order and delivery quantities.
# MAGIC These datasets include open and future orders. Performance exclusions do not remove operational records.

# COMMAND ----------
purchase_order_items = (read("silver_ekpo").select(
    *keys, "MATNR", "TXZ01", "WERKS", "MENGE", "MEINS", "NETPR", "NETWR", "_is_deleted"
).join(read("silver_ekko").select("MANDT", "EBELN", "LIFNR", "BEDAT", "WAERS"),
       ["MANDT", "EBELN"], "inner").join(vendors, "LIFNR", "left"))
save(purchase_order_items, "gold_purchase_order_items")
operational_deliveries = (deliveries.join(
    purchase_order_items.select(*keys, "LIFNR", "NAME1", "BEDAT", "TXZ01"), keys, "left"))
save(operational_deliveries, "gold_delivery_items")
schedule_lines = (read("silver_eket").select(
    *keys, "ETENR", "EINDT", F.col("MENGE").alias("scheduled_quantity"), "WEMNG"
).join(purchase_order_items.select(*keys, "LIFNR", "NAME1", "WERKS", "MATNR", "MEINS"),
       keys, "inner"))
save(schedule_lines, "gold_schedule_lines")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Publish protected Gold views
# MAGIC Employees query views as their own identity. Only their account group grants branch access.
# MAGIC ALL rows and unassigned quality records are restricted to the publisher and the optional central group.
# MAGIC Never grant branch users SELECT on internal Gold, Bronze or Silver, or permission to run owner jobs.
# MAGIC Empty group configuration keeps access closed. Administrative owners can still manage the data.

# COMMAND ----------
publisher = spark.sql("SELECT session_user() AS user_name").first().user_name
central_predicate = "session_user() = " + sql_literal(publisher)
if central_group:
    central_predicate += " OR is_account_group_member(" + sql_literal(central_group) + ")"
branch_predicate = " OR ".join(
    "(branch_code = " + sql_literal(branch) + " AND is_account_group_member(" + sql_literal(group) + "))"
    for branch, group in sorted(branch_groups.items())
) or "FALSE"
for suffix, source, columns in published:
    predicate = central_predicate
    if "branch_code" in columns:
        branch_condition = "(" + branch_predicate + ")"
        if "report_scope" in columns:
            branch_condition = "report_scope = 'BRANCH' AND " + branch_condition
        predicate += " OR (" + branch_condition + ")"
    target = table_path("gold_" + suffix)
    projection = ", ".join(map(quoted, columns + ["as_of_date", "processed_at"]))
    description = "PROTECTED VIEW | " + dataset_descriptions[suffix]
    spark.sql(f"CREATE OR REPLACE VIEW {target} COMMENT {sql_literal(description)} "
              f"AS SELECT {projection} FROM {source} WHERE {predicate}")
    readers = set(branch_groups.values()) if "branch_code" in columns else set()
    if central_group:
        readers.add(central_group)
    for group in sorted(readers):
        spark.sql(f"GRANT USE CATALOG ON CATALOG {quoted(catalog)} TO {quoted(group)}")
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA {quoted(catalog)}.{quoted(layer_schemas['gold'])} TO {quoted(group)}")
        spark.sql(f"GRANT SELECT ON VIEW {target} TO {quoted(group)}")
print("Published English Gold views. Configured branch groups:", len(branch_groups))
