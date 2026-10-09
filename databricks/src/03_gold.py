# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Monatliche Lieferantenperformance
# MAGIC Eine Bewertung je Bestellposition. Der letzte Einteilungstermin bestimmt den Monat.
# MAGIC Pünktlich: bis zu diesem Termin vollständig geliefert. Vollständig: bis Monatsende geliefert.
# MAGIC Annahmen und Grenzen stehen in DATA_RULES.md. Einzelne Teiltermine werden nicht bewertet.

# COMMAND ----------
from datetime import date

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Zielkatalog")
dbutils.widgets.text("schema_name", "supplier_performance_dev", "Zielschema")
dbutils.widgets.text("as_of_date", "2026-09-30", "Statistikstichtag (Annahme, yyyy-MM-dd)")
dbutils.widgets.text("min_items", "1", "Mindestens bewertbare Positionen für das Ranking")

catalog = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
as_of_date = dbutils.widgets.get("as_of_date").strip()
if date.fromisoformat(as_of_date).isoformat() != as_of_date:
    raise ValueError("as_of_date muss das Format yyyy-MM-dd haben.")
min_items = int(dbutils.widgets.get("min_items"))
if not catalog or not schema_name or min_items < 1:
    raise ValueError("Katalog und Schema setzen; min_items muss mindestens 1 sein.")


def table_path(name):
    return ".".join(f"`{part.replace('`', '``')}`" for part in [catalog, schema_name, name])


def read(name):
    return spark.table(table_path(name))


def save(frame, name):
    (frame.withColumn("_as_of_date", F.lit(as_of_date).cast("date"))
     .withColumn("_processed_at", F.current_timestamp())
     .write.format("delta").mode("overwrite").saveAsTable(table_path(name)))


keys = ["MANDT", "EBELN", "EBELP"]
zero = F.lit(0).cast("decimal(38,6)")
empty_reasons = F.array().cast("array<string>")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Grundmenge und Qualitätsgrenzen
# MAGIC Auch ausgeschlossene Bestellpositionen bleiben sichtbar. Bronze liefert nur die Schlüssel.
# MAGIC Quarantänedaten liefern Ausschlussgründe, keine Mengen für die Bewertung.

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
# Nicht zuordenbare Einteilungen/Lieferungen werden separat ausgewiesen.
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
# MAGIC ## Lieferbelege und Mengen
# MAGIC Einteilungen werden zuerst je Bestellposition zusammengefasst. So vervielfacht der Join keine Mengen.
# MAGIC WADAT_IST und LFIMG dienen vorläufig als tatsächliches Lieferdatum und Liefermenge.

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
# MAGIC ## Monatskennzahlen und Rangliste
# MAGIC Jede bewertbare Bestellposition hat dasselbe Gewicht. Keine Summe verschiedener Mengeneinheiten.
# MAGIC Score = Mittelwert aus Pünktlichkeitsquote und Vollständigkeitsquote. Niedrige Werte sind schlechter.
# MAGIC ALL umfasst alle Niederlassungen; BRANCH enthält Kennzahlen je WERKS.

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
# MAGIC ## Ergebnisse speichern
# MAGIC Ausgaben ersetzen den bisherigen Gold-Stand. Fehler beim Schreiben können Teilstände hinterlassen.
# MAGIC Unbekannte Lieferungen und fehlende Termine begrenzen die Aussagekraft; siehe Qualitätsübersicht.

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
display(worst_three.filter(F.col("scope") == "ALL").orderBy("report_month", "worst_rank"))
