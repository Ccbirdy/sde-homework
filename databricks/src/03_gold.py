# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Monthly supplier performance
# MAGIC Evaluate one purchase order item at a time. The final scheduled date determines the month.
# MAGIC On time means fully delivered by that date. In full means fully delivered by month end.
# MAGIC See DATA_RULES.md for assumptions. Individual schedule lines are not scored separately.

# COMMAND ----------
from datetime import date
import json
import re

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "supplier_performance_dev", "Target catalog")
for key, default in [("bronze_schema", "dev_bronze"), ("silver_schema", "dev_silver"),
                     ("gold_internal_schema", "dev_gold_internal"), ("gold_schema", "dev_gold"),
                     ("table_prefix", "dev_"), ("branch_groups_json", "{}"), ("central_group", "")]:
    dbutils.widgets.text(key, default, key.replace("_", " ").title())
dbutils.widgets.text("as_of_date", "2026-09-30", "Evaluation cutoff (assumption, yyyy-MM-dd)")
dbutils.widgets.text("min_items", "1", "Minimum evaluated order items for ranking")

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


def table_path(name):
    layer, suffix = name.split("_", 1)
    return ".".join(map(quoted, [catalog, layer_schemas[layer], table_prefix + suffix]))


def read(name):
    return spark.table(table_path(name))


# SAP identifiers stay in Bronze/Silver. Gold has a stable English business vocabulary.
column_names = {
    "MANDT": "sap_client_id", "EBELN": "purchase_order_id", "EBELP": "purchase_order_item_id",
    "LIFNR": "supplier_id", "NAME1": "supplier_name", "WERKS": "branch_code",
    "MATNR": "material_id", "MEINS": "quantity_unit", "BEDAT": "order_date",
    "VBELN": "delivery_id", "POSNR": "delivery_item_id", "WADAT_IST": "actual_delivery_date",
    "LFIMG": "delivered_quantity", "MENGE": "ordered_quantity", "TXZ01": "item_description",
    "NETPR": "net_unit_price", "NETWR": "net_order_value", "WAERS": "currency_code",
    "ETENR": "schedule_line_id", "EINDT": "scheduled_delivery_date", "WEMNG": "recorded_received_quantity",
    "_is_deleted": "is_deleted", "candidate_items": "candidate_order_item_count",
    "evaluated_items": "evaluated_order_item_count", "excluded_items": "excluded_order_item_count",
    "scope": "report_scope", "coverage_rate": "evaluation_coverage_rate",
    "score": "performance_score", "min_items": "minimum_order_item_count",
    "ranking_eligible": "is_ranking_eligible", "worst_rank": "worst_supplier_rank",
    "ordered_qty": "ordered_quantity", "scheduled_qty": "scheduled_quantity",
    "schedule_count": "schedule_line_count", "delivered_by_due_qty": "delivered_by_due_quantity",
    "delivered_by_month_end_qty": "delivered_by_month_end_quantity", "shortfall_qty": "shortfall_quantity",
    "first_due_date": "first_scheduled_delivery_date", "final_due_date": "final_scheduled_delivery_date",
    "evaluation_date": "evaluation_month_end", "on_time": "is_on_time", "in_full": "is_in_full",
    "fill_rate": "quantity_fill_rate", "mean_fill_rate": "mean_quantity_fill_rate",
    "by_due_date": "is_by_due_date", "by_month_end": "is_by_month_end", "after_cutoff": "is_after_cutoff",
}
published = []


def save(frame, name):
    suffix = name.removeprefix("gold_")
    target = ".".join(map(quoted, [catalog, layer_schemas["gold_internal"], table_prefix + suffix]))
    business = frame.select(*[F.col(c).alias(column_names.get(c, c)) for c in frame.columns])
    (business.withColumn("as_of_date", F.lit(as_of_date).cast("date"))
     .withColumn("processed_at", F.current_timestamp())
     .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target))
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
# MAGIC ## Monthly metrics and ranking
# MAGIC Each evaluable order item has equal weight. Do not add quantities across different units.
# MAGIC Score is the mean of the on-time and in-full rates. Lower values are worse.
# MAGIC ALL covers the company; BRANCH calculates each branch separately.

# COMMAND ----------
monthly_base = details.filter(
    F.col("LIFNR").isNotNull() & F.col("report_month").isNotNull()
    & (F.col("evaluation_date") <= F.lit(as_of_date).cast("date"))
)
scoped = (monthly_base.withColumn("scope", F.lit("BRANCH"))
          .unionByName(monthly_base.withColumn("WERKS", F.lit(None).cast("string"))
                       .withColumn("scope", F.lit("ALL"))))
monthly = scoped.groupBy("report_month", "scope", "WERKS", "LIFNR", "NAME1").agg(
    F.count("*").alias("candidate_items"),
    F.sum(F.col("is_evaluable").cast("long")).alias("evaluated_items"),
    F.avg(F.col("on_time").cast("double")).alias("on_time_rate"),
    F.avg(F.col("in_full").cast("double")).alias("in_full_rate"),
    F.avg("fill_rate").alias("mean_fill_rate"),
)
monthly = (monthly.withColumn("excluded_items", F.col("candidate_items") - F.col("evaluated_items"))
           .withColumn("coverage_rate", F.col("evaluated_items") / F.col("candidate_items"))
           .withColumn("score", (F.col("on_time_rate") + F.col("in_full_rate")) / 2)
           .withColumn("ranking_eligible", F.col("evaluated_items") >= min_items)
           .withColumn("min_items", F.lit(min_items)))
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
quality = details.select(F.explode(F.when(F.col("is_evaluable"), F.array(F.lit("EVALUATED")))
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
def sql_literal(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


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
    spark.sql(f"CREATE OR REPLACE VIEW {target} AS SELECT {projection} FROM {source} WHERE {predicate}")
    readers = set(branch_groups.values()) if "branch_code" in columns else set()
    if central_group:
        readers.add(central_group)
    for group in sorted(readers):
        spark.sql(f"GRANT USE CATALOG ON CATALOG {quoted(catalog)} TO {quoted(group)}")
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA {quoted(catalog)}.{quoted(layer_schemas['gold'])} TO {quoted(group)}")
        spark.sql(f"GRANT SELECT ON VIEW {target} TO {quoted(group)}")
print("Published English Gold views. Configured branch groups:", len(branch_groups))
