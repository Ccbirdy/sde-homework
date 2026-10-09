-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lieferantenperformance
-- MAGIC Monatliche Ergebnisse für den Einkauf. Auswahl nach Monat und Niederlassung.
-- MAGIC Pünktlich: vollständig bis zum letzten Plantermin. Vollständig: vollständig bis Monatsende.
-- MAGIC Score: Mittelwert beider Quoten. Niedrigere Werte sind schlechter.
-- MAGIC Teiltermine werden nicht einzeln bewertet. Fachliche Annahmen: DATA_RULES.md.
-- MAGIC Die Auswertung liest den vorhandenen Gold-Stand und verändert keine Geschäftstabellen.

-- COMMAND ----------
-- MAGIC %python
-- MAGIC from datetime import date
-- MAGIC from html import escape
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import numpy as np
-- MAGIC 
-- MAGIC for key, default, label in [
-- MAGIC     ("catalog", "workspace", "Katalog"),
-- MAGIC     ("schema_name", "supplier_performance_dev", "Schema"),
-- MAGIC     ("report_month", "2026-09-01", "Berichtsmonat (yyyy-MM-01)"),
-- MAGIC     ("branch", "ALL", "Niederlassung: WERKS oder ALL"),
-- MAGIC     ("po_number", "", "Optional: Bestellnummer"),
-- MAGIC     ("po_item", "", "Optional: Position mit führenden Nullen"),
-- MAGIC ]:
-- MAGIC     dbutils.widgets.text(key, default, label)
-- MAGIC 
-- MAGIC selection = {key: dbutils.widgets.get(key).strip() for key in
-- MAGIC              ["catalog", "schema_name", "report_month", "branch"]}
-- MAGIC month = date.fromisoformat(selection["report_month"])
-- MAGIC if month.day != 1:
-- MAGIC     raise ValueError("report_month muss der erste Tag des Monats sein.")
-- MAGIC 
-- MAGIC scope_filter = """((:branch = 'ALL' AND scope = 'ALL')
-- MAGIC  OR (:branch <> 'ALL' AND scope = 'BRANCH' AND WERKS = :branch))"""
-- MAGIC monthly = spark.sql("""
-- MAGIC  SELECT * FROM IDENTIFIER(:catalog || '.' || :schema_name || '.gold_supplier_monthly')
-- MAGIC  WHERE report_month = CAST(:report_month AS DATE) AND """ + scope_filter,
-- MAGIC  args=selection).orderBy("LIFNR").collect()
-- MAGIC worst = spark.sql("""
-- MAGIC  SELECT * FROM IDENTIFIER(:catalog || '.' || :schema_name || '.gold_worst_suppliers')
-- MAGIC  WHERE report_month = CAST(:report_month AS DATE) AND """ + scope_filter,
-- MAGIC  args=selection).orderBy("worst_rank").collect()
-- MAGIC 
-- MAGIC n = sum(r.evaluated_items for r in monthly)
-- MAGIC candidates = sum(r.candidate_items for r in monthly)
-- MAGIC def weighted(field):
-- MAGIC     return sum(float(r[field]) * r.evaluated_items for r in monthly
-- MAGIC                if r[field] is not None and r.evaluated_items > 0) / n if n else None
-- MAGIC 
-- MAGIC def pct(value):
-- MAGIC     return f"{float(value):.1%}" if value is not None else "nicht bewertbar"
-- MAGIC 
-- MAGIC cutoffs = sorted({str(r._as_of_date) for r in monthly})
-- MAGIC card = lambda label, value: f'<div><div>{escape(label)}</div><h2>{escape(str(value))}</h2></div>'
-- MAGIC rows_html = ''.join(
-- MAGIC     f'<tr><td style="padding:8px">{r.worst_rank}</td>'
-- MAGIC     f'<td style="padding:8px">{escape(r.NAME1)} ({escape(r.LIFNR)})</td>'
-- MAGIC     f'<td style="padding:8px">{pct(r.on_time_rate)}</td>'
-- MAGIC     f'<td style="padding:8px">{pct(r.in_full_rate)}</td>'
-- MAGIC     f'<td style="padding:8px">{pct(r.score)}</td>'
-- MAGIC     f'<td style="padding:8px">{r.evaluated_items}</td>'
-- MAGIC     f'<td style="padding:8px">{pct(r.coverage_rate)}</td></tr>' for r in worst
-- MAGIC )
-- MAGIC notice = ('Keine Monatsdaten in dieser Auswahl.' if not monthly else
-- MAGIC           'Keine bewertbaren Positionen.' if not n else
-- MAGIC           'Keine Lieferanten erfüllen die Mindestmenge für das Ranking.' if not worst else
-- MAGIC           'Die Rangliste gilt für die auswertbaren Positionen. Stichprobe und Abdeckung beachten.')
-- MAGIC if len(cutoffs) > 1:
-- MAGIC     notice += ' Unterschiedliche Stichtage im Ergebnis: Datenstand prüfen.'
-- MAGIC displayHTML(f"""
-- MAGIC <div style="font-family:Arial,sans-serif;padding:24px;background:#f4f7fb;color:#17344b">
-- MAGIC <h1>Lieferantenperformance · {month:%Y-%m}</h1>
-- MAGIC <p>Niederlassung: {escape(selection['branch'])} · Stichtag: {escape(', '.join(cutoffs) or 'kein Datenstand')}</p>
-- MAGIC <div style="display:flex;gap:36px;flex-wrap:wrap">
-- MAGIC {card('Bewertete Positionen', n)}{card('Pünktlich', pct(weighted('on_time_rate')))}
-- MAGIC {card('Vollständig', pct(weighted('in_full_rate')))}
-- MAGIC {card('Abdeckung im zuordenbaren Monatsbestand', pct(n / candidates if candidates else None))}
-- MAGIC </div><h2>Bis zu drei schwächste Lieferanten</h2><p>{escape(notice)}</p>
-- MAGIC <table style="border-collapse:collapse;text-align:left"><tr>
-- MAGIC <th>Rang</th><th>Lieferant</th><th>Pünktlich</th><th>Vollständig</th><th>Score</th><th>Positionen</th><th>Abdeckung</th>
-- MAGIC </tr>{rows_html}</table>
-- MAGIC <p>Die Quoten erklären die Bewertung. Ursachen im Betrieb der Lieferanten lassen sich daraus nicht ableiten.
-- MAGIC Nicht zuordenbare Daten fehlen in der Monatsabdeckung. Qualitätsgrenzen stehen am Ende des Notebooks.</p>
-- MAGIC </div>
-- MAGIC """)

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Vergleich der Lieferanten
-- MAGIC Alle bewertbaren Lieferanten der Auswahl. Die Mindestmenge betrifft nur die Rangliste.
-- MAGIC Fehlende KPI-Werte erscheinen nicht als 0 %. Sie bleiben in der Ergebnistabelle sichtbar.

-- COMMAND ----------
-- MAGIC %python
-- MAGIC chart_rows = sorted([r for r in monthly if r.evaluated_items > 0 and r.score is not None],
-- MAGIC                     key=lambda r: (float(r.score), r.LIFNR))
-- MAGIC if chart_rows:
-- MAGIC     positions = np.arange(len(chart_rows))
-- MAGIC     fig, ax = plt.subplots(figsize=(11, max(4, len(chart_rows) * 0.5)))
-- MAGIC     ax.barh(positions - 0.18, [100 * float(r.on_time_rate) for r in chart_rows],
-- MAGIC             height=0.34, color="#3678a8", label="Pünktlich")
-- MAGIC     ax.barh(positions + 0.18, [100 * float(r.in_full_rate) for r in chart_rows],
-- MAGIC             height=0.34, color="#3b947c", label="Vollständig bis Monatsende")
-- MAGIC     ax.set_yticks(positions)
-- MAGIC     ax.set_yticklabels([f"{r.NAME1} · {r.LIFNR} (n={r.evaluated_items})" for r in chart_rows])
-- MAGIC     ax.invert_yaxis()
-- MAGIC     ax.set_xlim(0, 100)
-- MAGIC     ax.set_xlabel("Anteil bewerteter Bestellpositionen (%)")
-- MAGIC     ax.set_title(f"Lieferantenvergleich {month:%Y-%m}")
-- MAGIC     ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=2)
-- MAGIC     ax.grid(axis="x", alpha=0.2)
-- MAGIC     fig.tight_layout()
-- MAGIC     display(fig)
-- MAGIC     plt.close(fig)
-- MAGIC else:
-- MAGIC     print("Keine bewertbaren Lieferanten für das Diagramm.")

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Monatsverlauf
-- MAGIC Gleiche Niederlassung, alle vorhandenen Monate bis zum gewählten Monat.
-- MAGIC Quoten sind nach bewerteten Positionen gewichtet. Zusammensetzung und Abdeckung können sich ändern.

-- COMMAND ----------
-- MAGIC %python
-- MAGIC trend = spark.sql("""
-- MAGIC  SELECT report_month, SUM(evaluated_items) AS evaluated_items,
-- MAGIC         SUM(on_time_rate * evaluated_items) / NULLIF(SUM(evaluated_items), 0) AS on_time_rate,
-- MAGIC         SUM(in_full_rate * evaluated_items) / NULLIF(SUM(evaluated_items), 0) AS in_full_rate,
-- MAGIC         SUM(evaluated_items) / NULLIF(SUM(candidate_items), 0) AS coverage_rate
-- MAGIC  FROM IDENTIFIER(:catalog || '.' || :schema_name || '.gold_supplier_monthly')
-- MAGIC  WHERE report_month <= CAST(:report_month AS DATE) AND """ + scope_filter + """
-- MAGIC  GROUP BY report_month ORDER BY report_month
-- MAGIC """, args=selection).collect()
-- MAGIC if trend:
-- MAGIC     fig, ax = plt.subplots(figsize=(11, 4))
-- MAGIC     for field, label, color in [("on_time_rate", "Pünktlich", "#3678a8"),
-- MAGIC                                 ("in_full_rate", "Vollständig", "#3b947c"),
-- MAGIC                                 ("coverage_rate", "Abdeckung", "#967844")]:
-- MAGIC         ax.plot([r.report_month for r in trend],
-- MAGIC                 [100 * float(r[field]) if r[field] is not None else np.nan for r in trend],
-- MAGIC                 marker="o", label=label, color=color)
-- MAGIC     ax.set_ylim(0, 105)
-- MAGIC     ax.set_ylabel("Anteil (%)")
-- MAGIC     ax.legend()
-- MAGIC     ax.grid(alpha=0.2)
-- MAGIC     fig.autofmt_xdate()
-- MAGIC     fig.tight_layout()
-- MAGIC     display(fig)
-- MAGIC     plt.close(fig)
-- MAGIC else:
-- MAGIC     print("Kein Monatsverlauf vorhanden.")

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Ergebnisdaten und Belege
-- MAGIC Zuerst Rangliste und alle Lieferanten, danach die Bestellpositionen der schwächsten Lieferanten.
-- MAGIC Optional po_number und po_item wählen, um Lieferbelege einzugrenzen.
-- COMMAND ----------
-- Ein leerer gewählter Monat ist sichtbar und nicht automatisch ein Datenfehler.
SELECT COUNT(*) AS supplier_rows_in_selection,
       MIN(_as_of_date) AS first_cutoff, MAX(_as_of_date) AS last_cutoff,
       MAX(_processed_at) AS last_written_at
FROM IDENTIFIER(:catalog || '.' || :schema_name || '.gold_supplier_monthly')
WHERE report_month = CAST(:report_month AS DATE)
  AND ((:branch = 'ALL' AND scope = 'ALL')
       OR (:branch <> 'ALL' AND scope = 'BRANCH' AND WERKS = :branch));

-- COMMAND ----------
SELECT report_month, scope, WERKS, worst_rank, LIFNR, NAME1,
       evaluated_items, excluded_items, coverage_rate,
       on_time_rate, in_full_rate, mean_fill_rate, score
FROM IDENTIFIER(:catalog || '.' || :schema_name || '.gold_worst_suppliers')
WHERE report_month = CAST(:report_month AS DATE)
  AND ((:branch = 'ALL' AND scope = 'ALL')
       OR (:branch <> 'ALL' AND scope = 'BRANCH' AND WERKS = :branch))
ORDER BY worst_rank;

-- COMMAND ----------
-- Alle Lieferanten einschließlich derjenigen ohne ausreichende Stichprobe.
SELECT *
FROM IDENTIFIER(:catalog || '.' || :schema_name || '.gold_supplier_monthly')
WHERE report_month = CAST(:report_month AS DATE)
  AND ((:branch = 'ALL' AND scope = 'ALL')
       OR (:branch <> 'ALL' AND scope = 'BRANCH' AND WERKS = :branch))
ORDER BY score ASC NULLS LAST, LIFNR;

-- COMMAND ----------
-- Bestellpositionen als Belege für die ausgewählte Rangliste.
WITH worst AS (
  SELECT LIFNR
  FROM IDENTIFIER(:catalog || '.' || :schema_name || '.gold_worst_suppliers')
  WHERE report_month = CAST(:report_month AS DATE)
    AND ((:branch = 'ALL' AND scope = 'ALL')
         OR (:branch <> 'ALL' AND scope = 'BRANCH' AND WERKS = :branch))
)
SELECT p.*
FROM IDENTIFIER(:catalog || '.' || :schema_name || '.gold_order_item_performance') p
JOIN worst w ON p.LIFNR = w.LIFNR
WHERE p.report_month = CAST(:report_month AS DATE)
  AND (:branch = 'ALL' OR p.WERKS = :branch)
ORDER BY p.LIFNR, p.MANDT, p.EBELN, p.EBELP;

-- COMMAND ----------
-- Lieferbelege: po_number und po_item optional; leer zeigt alle Belege des gewählten Monats.
SELECT *
FROM IDENTIFIER(:catalog || '.' || :schema_name || '.gold_delivery_evidence')
WHERE report_month = CAST(:report_month AS DATE)
  AND (:po_number = '' OR EBELN = :po_number)
  AND (:po_item = '' OR EBELP = :po_item)
  AND (:branch = 'ALL' OR WERKS = :branch)
ORDER BY MANDT, WADAT_IST, VBELN, POSNR;

-- COMMAND ----------
SELECT *
FROM IDENTIFIER(:catalog || '.' || :schema_name || '.gold_quality_summary')
ORDER BY reason;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Technische Prüfung
-- MAGIC Prüfungen gelten für den gesamten Gold-Stand. PASS bestätigt nur die jeweilige Bedingung.
-- MAGIC Ein grüner Job-Status bedeutet, dass die Abfragen liefen, nicht dass alle Prüfungen PASS zeigen.
-- MAGIC Zuerst einen vollständigen Gold-Lauf abwarten. branch ist ein Anzeigefilter, keine Zugriffskontrolle.
-- COMMAND ----------
-- Strukturprüfung über den gesamten gespeicherten Stand, unabhängig vom Monatsfilter.
WITH items AS (
  SELECT * FROM IDENTIFIER(:catalog || '.' || :schema_name || '.gold_order_item_performance')
), monthly AS (
  SELECT * FROM IDENTIFIER(:catalog || '.' || :schema_name || '.gold_supplier_monthly')
), checks AS (
  SELECT 'GOLD_ITEMS_PRESENT' AS check_name,
         CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS violations FROM items
  UNION ALL
  SELECT 'UNIQUE_ORDER_ITEM_KEY', COUNT(*) FROM (
    SELECT MANDT, EBELN, EBELP FROM items GROUP BY MANDT, EBELN, EBELP HAVING COUNT(*) > 1
  ) duplicates
  UNION ALL
  SELECT 'EVALUATED_ITEMS_HAVE_KPI', COUNT(*) FROM items
  WHERE is_evaluable AND (on_time IS NULL OR in_full IS NULL OR fill_rate IS NULL)
  UNION ALL
  SELECT 'EXCLUDED_ITEMS_HAVE_NO_KPI', COUNT(*) FROM items
  WHERE NOT is_evaluable AND (on_time IS NOT NULL OR in_full IS NOT NULL OR fill_rate IS NOT NULL)
  UNION ALL
  SELECT 'MONTHLY_COUNTS_BALANCE', COUNT(*) FROM monthly
  WHERE candidate_items <> evaluated_items + excluded_items
  UNION ALL
  SELECT 'MONTHLY_RATES_IN_RANGE', COUNT(*) FROM monthly
  WHERE on_time_rate NOT BETWEEN 0 AND 1 OR in_full_rate NOT BETWEEN 0 AND 1
     OR mean_fill_rate NOT BETWEEN 0 AND 1 OR score NOT BETWEEN 0 AND 1
     OR coverage_rate NOT BETWEEN 0 AND 1
  UNION ALL
  SELECT 'EVALUATED_MONTHS_HAVE_RATES', COUNT(*) FROM monthly
  WHERE evaluated_items > 0 AND (on_time_rate IS NULL OR in_full_rate IS NULL OR score IS NULL)
)
SELECT check_name, violations, CASE WHEN violations = 0 THEN 'PASS' ELSE 'CHECK' END AS status
FROM checks ORDER BY check_name;


