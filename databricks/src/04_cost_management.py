# Databricks notebook source
# MAGIC %md
# MAGIC # Kostenübersicht und Budget-Simulation
# MAGIC `demo` nutzt erfundene Nutzungsdaten und Preise. `live` liest die Databricks-Abrechnung.
# MAGIC Die Übersicht zeigt Listenpreis-Schätzungen, keine Rechnung und keine CPU-Auslastung.
# MAGIC Sie beendet keine Ressourcen. Auch das Ausführen dieses Notebooks kann Compute-Kosten verursachen.
# MAGIC Für `live`: USE CATALOG auf system, USE SCHEMA auf system.billing und SELECT auf usage/list_prices.

# COMMAND ----------
from datetime import date, timedelta
from decimal import Decimal
from html import escape

dbutils.widgets.dropdown("data_mode", "demo", ["demo", "live"], "Datenquelle")
dbutils.widgets.text("start_date", date.today().replace(day=1).isoformat(), "Von (inklusive)")
dbutils.widgets.text("end_date", date.today().isoformat(), "Bis (inklusive)")
dbutils.widgets.text("workspace_id", "7474656502315766", "Workspace-ID")
dbutils.widgets.text("project", "supplier_performance", "Projekt-Tag (leer: kein Tag-Filter)")
dbutils.widgets.text("job_id", "972797289096916", "Zusätzliche Job-ID ohne Projekt-Tag")
dbutils.widgets.text("currency", "USD", "Währung der Listenpreise")
dbutils.widgets.text("period_budget", "100", "Budget für den gesamten Zeitraum")
dbutils.widgets.text("compute_reduction_pct", "20", "Simulation: weniger Compute-Kosten in %")

params = {key: dbutils.widgets.get(key).strip() for key in [
    "data_mode", "start_date", "end_date", "workspace_id", "project", "job_id", "currency"
]}
start = date.fromisoformat(params["start_date"])
end = date.fromisoformat(params["end_date"])
budget = Decimal(dbutils.widgets.get("period_budget").strip())
reduction = Decimal(dbutils.widgets.get("compute_reduction_pct").strip())
if end < start or (end - start).days > 365:
    raise ValueError("Zeitraum muss zwischen 1 und 366 Tagen liegen.")
if not budget.is_finite() or budget <= 0 or not reduction.is_finite() or not 0 <= reduction <= 100:
    raise ValueError("Budget muss positiv sein; Reduktion muss zwischen 0 und 100 liegen.")
if not params["workspace_id"] or not params["currency"]:
    raise ValueError("Workspace-ID und Währung müssen gesetzt sein.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Abfrage für echte Abrechnungsdaten
# MAGIC `run_as` ist die Ausführungsidentität, nicht zwingend die Person, die Run now angeklickt hat.
# MAGIC SQL-Warehouse-Besitzer stehen getrennt unter `warehouse_owner`; sie sind nicht automatisch Abfragebenutzer.
# MAGIC Korrekturen bleiben enthalten: negative RETRACTION-Mengen gleichen frühere Buchungen aus.
# MAGIC Der Preis muss zum SKU, zur Cloud, Einheit, Währung und zum Nutzungszeitraum passen.

# COMMAND ----------
# Diese SQL-Abfrage kann auch in ein SQL-Notebook oder einen Dashboard-Datensatz kopiert werden.
# Benötigte Parameter: start_date, end_date, workspace_id, project, job_id, currency.
LIVE_SQL = """
WITH selected_usage AS (
  SELECT * FROM system.billing.usage
  WHERE usage_date BETWEEN CAST(:start_date AS DATE) AND CAST(:end_date AS DATE)
    AND workspace_id = :workspace_id
    AND (
      (:project = '' AND :job_id = '')
      OR (:project <> '' AND element_at(custom_tags, 'project') = :project)
      OR (:job_id <> '' AND usage_metadata.job_id = :job_id)
    )
)
SELECT u.record_id, u.usage_date, u.workspace_id, u.sku_name,
       u.billing_origin_product, u.usage_type, u.usage_unit,
       u.usage_quantity, u.record_type,
       u.identity_metadata.run_as AS run_as,
       u.identity_metadata.owned_by AS warehouse_owner,
       u.usage_metadata.job_id AS job_id,
       u.usage_metadata.job_run_id AS job_run_id,
       u.usage_metadata.warehouse_id AS warehouse_id,
       COALESCE(u.usage_metadata.job_id, u.usage_metadata.warehouse_id,
                u.usage_metadata.notebook_id, u.usage_metadata.cluster_id,
                u.usage_metadata.dlt_pipeline_id, u.usage_metadata.endpoint_id,
                u.usage_metadata.app_id, 'UNASSIGNED') AS resource_id,
       element_at(u.custom_tags, 'project') AS project,
       element_at(u.custom_tags, 'environment') AS environment,
       element_at(u.custom_tags, 'cost_center') AS cost_center,
       :currency AS currency,
       CAST(p.pricing.effective_list.default AS DECIMAL(20,10)) AS unit_list_price,
       u.usage_quantity * CAST(p.pricing.effective_list.default AS DECIMAL(20,10))
         AS estimated_list_cost,
       p.pricing.effective_list.default IS NULL AS missing_price,
       'LIVE_LIST_PRICE_ESTIMATE' AS data_source
FROM selected_usage u
LEFT JOIN system.billing.list_prices p
  ON u.account_id = p.account_id AND u.sku_name = p.sku_name AND u.cloud = p.cloud
 AND u.usage_unit = p.usage_unit AND p.currency_code = :currency
 AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_end_time <= p.price_end_time)
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
"""

if params["data_mode"] == "live":
    usage = spark.sql(LIVE_SQL, args={k: v for k, v in params.items() if k != "data_mode"})
    usage.createOrReplaceTempView("cost_usage")
else:
    # Nur Beispieldaten. Preise und Identitäten sind erfunden, keine aktuellen Databricks-Tarife.
    demo_rows = []
    for offset in range((end - start).days + 1):
        day = start + timedelta(days=offset)
        for product, kind, unit, resource, actor, quantity, price in [
            ("JOBS", "COMPUTE_TIME", "DBU", "demo_job", "demo_service_principal", 4 + offset % 3, "0.30"),
            ("SQL", "COMPUTE_TIME", "DBU", "demo_warehouse", None, 2 + offset % 2, "0.50"),
            ("STORAGE", "STORAGE_SPACE", "GB_DAY", "demo_storage", None, 8, "0.02"),
        ]:
            demo_rows.append((
                f"demo_{offset}_{product}", day, params["workspace_id"], product, kind, unit,
                resource, actor, Decimal(quantity), Decimal(price), params["currency"],
                params["project"] or "supplier_performance",
            ))
    demo = spark.createDataFrame(demo_rows, """
      record_id STRING, usage_date DATE, workspace_id STRING, billing_origin_product STRING,
      usage_type STRING, usage_unit STRING, resource_id STRING, run_as STRING,
      usage_quantity DECIMAL(18,6), unit_list_price DECIMAL(20,10), currency STRING, project STRING
    """)
    demo.createOrReplaceTempView("cost_demo_input")
    spark.sql("""
      SELECT *, 'DEMO' AS sku_name, 'ORIGINAL' AS record_type,
        CASE WHEN billing_origin_product = 'SQL' THEN 'demo_warehouse_owner' END AS warehouse_owner,
        CASE WHEN billing_origin_product = 'JOBS' THEN resource_id END AS job_id,
        CASE WHEN billing_origin_product = 'JOBS' THEN concat('demo_run_', CAST(usage_date AS STRING)) END AS job_run_id,
        CASE WHEN billing_origin_product = 'SQL' THEN resource_id END AS warehouse_id,
        'demo' AS environment, 'demo_cost_center' AS cost_center,
        usage_quantity * unit_list_price AS estimated_list_cost,
        false AS missing_price, 'SYNTHETIC_DEMO' AS data_source
      FROM cost_demo_input
    """).createOrReplaceTempView("cost_usage")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Dashboard
# MAGIC Budget gilt für den gewählten Zeitraum. Die Simulation reduziert nur den geschätzten Compute-Anteil.
# MAGIC Fehlende Preise ergeben eine unvollständige Kostensumme, keinen kostenlosen Verbrauch.

# COMMAND ----------
summary = spark.sql("""
  SELECT COUNT(*) AS records, COUNT_IF(missing_price) AS unpriced_records,
         SUM(estimated_list_cost) AS priced_cost,
         SUM(CASE WHEN usage_type = 'COMPUTE_TIME' THEN estimated_list_cost ELSE 0 END) AS compute_cost
  FROM cost_usage
""").first()
daily_rows = spark.sql("""
  SELECT usage_date, SUM(estimated_list_cost) AS cost, COUNT_IF(missing_price) AS unpriced_records
  FROM cost_usage GROUP BY usage_date ORDER BY usage_date
""").collect()
known_cost = Decimal(summary.priced_cost or 0)
compute_cost = Decimal(summary.compute_cost or 0)
scenario_cost = known_cost - compute_cost * reduction / 100
complete = summary.records > 0 and summary.unpriced_records == 0
mode_label = "SIMULATION – erfundene Daten und Preise" if params["data_mode"] == "demo" else "LIVE – Listenpreis-Schätzung"
status = ("Keine Daten" if summary.records == 0 else
          "Unvollständig: Preise fehlen" if not complete else
          "Budget überschritten" if known_cost > budget else "Innerhalb des Budgets")
currency = escape(params["currency"])
max_bar = max([abs(Decimal(r.cost or 0)) for r in daily_rows] + [Decimal(1)])
bars = "".join(
    f'<div style="display:flex;gap:12px;align-items:center;margin:6px 0">'
    f'<span style="width:100px">{r.usage_date}</span>'
    f'<div style="background:{"#dc6868" if Decimal(r.cost or 0) < 0 else "#4278c1"};height:16px;'
    f'width:{float(abs(Decimal(r.cost or 0)) / max_bar * 320):.1f}px"></div>'
    f'<span>{Decimal(r.cost or 0):.2f} {currency}{" *" if r.unpriced_records else ""}</span></div>'
    for r in daily_rows
)
displayHTML(f"""
<div style="font-family:Arial,sans-serif;color:#19324a;padding:24px;background:#f4f7fb;border-radius:12px">
  <div style="font-weight:bold;color:#805500">{mode_label}</div>
  <h2>Kosten und Budget</h2>
  <p>{start} bis {end} · {escape(params['workspace_id'])} · {status}</p>
  <div style="display:flex;gap:32px;flex-wrap:wrap">
    <div>Bepreiste Nutzung<h2>{known_cost:.2f} {currency}</h2></div>
    <div>Zeitraumbudget<h2>{budget:.2f} {currency}</h2></div>
    <div>Szenario: Compute −{reduction}%<h2>{scenario_cost:.2f} {currency}</h2></div>
  </div>
  <p>{summary.records} Abrechnungszeilen · {summary.unpriced_records} ohne passenden Preis.
     Szenario ist eine Rechenannahme, keine zugesicherte Einsparung.</p>
  <h3>Tägliche Kosten</h3>{bars or '<p>Keine Daten im gewählten Bereich.</p>'}
  <p>* Unvollständiger Tag. Negative Werte können Abrechnungskorrekturen sein.</p>
</div>
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ressourcen und Nutzungseinheiten
# MAGIC DBU, Speicher- und andere Einheiten werden getrennt summiert.

# COMMAND ----------
display(spark.sql("""
  SELECT billing_origin_product, resource_id, usage_unit, currency,
         SUM(usage_quantity) AS net_usage,
         SUM(estimated_list_cost) AS estimated_list_cost,
         COUNT_IF(missing_price) AS unpriced_records
  FROM cost_usage GROUP BY billing_origin_product, resource_id, usage_unit, currency
  ORDER BY estimated_list_cost DESC NULLS LAST
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Kosten nach Identität und Kostenstelle
# MAGIC UNASSIGNED bleibt sichtbar. Warehouse-Owner und Run-as sind unterschiedliche Rollen.

# COMMAND ----------
display(spark.sql("""
  SELECT COALESCE(run_as, 'UNASSIGNED') AS run_as,
         warehouse_owner, project, environment, cost_center, currency,
         SUM(estimated_list_cost) AS estimated_list_cost,
         COUNT_IF(missing_price) AS unpriced_records
  FROM cost_usage
  GROUP BY run_as, warehouse_owner, project, environment, cost_center, currency
  ORDER BY estimated_list_cost DESC NULLS LAST
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Kosten je Job-Lauf

# COMMAND ----------
display(spark.sql("""
  SELECT workspace_id, job_id, job_run_id, run_as, currency,
         SUM(estimated_list_cost) AS estimated_list_cost,
         COUNT_IF(missing_price) AS unpriced_records
  FROM cost_usage WHERE job_id IS NOT NULL
  GROUP BY workspace_id, job_id, job_run_id, run_as, currency
  ORDER BY estimated_list_cost DESC NULLS LAST
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Grenzen und Quellen
# MAGIC Abrechnung kann verzögert eintreffen. Leere Ergebnisse beweisen keine Kostenfreiheit.
# MAGIC Listenpreise enthalten keine individuellen Vertragsrabatte; separate Cloud-Rechnungen werden nicht importiert.
# MAGIC Nicht jede Nutzung hat Job-ID oder Benutzerzuordnung. Geteilte Warehouses werden hier nicht je Abfragebenutzer aufgeteilt.
# MAGIC Ein Preiswechsel innerhalb einer Nutzungszeile wird als fehlender Preis ausgewiesen, nicht geschätzt.
# MAGIC Projekt-Tag ODER Job-ID bestimmt den Filter. Für alle Workspace-Ressourcen beide Parameter leeren.
# MAGIC Die Beispiel-Daten simulieren auch Speicher; Live zeigt nur in system.billing.usage vorhandene Positionen.
# MAGIC Quellen: [Billing](https://docs.databricks.com/aws/en/admin/system-tables/billing),
# MAGIC [Preise](https://docs.databricks.com/aws/en/admin/system-tables/pricing),
# MAGIC [Serverless usage policies](https://docs.databricks.com/aws/en/admin/usage/budget-policies).
