-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Supplier performance
-- MAGIC Monthly purchasing results by month and branch.
-- MAGIC On time: fully delivered by the final scheduled date. In full: fully delivered by month end.
-- MAGIC The performance_score is the mean of both rates; lower values are worse.
-- MAGIC Individual schedule lines are not scored separately. See DATA_RULES.md.
-- MAGIC This review reads protected Gold views without changing business data.

-- COMMAND ----------
-- MAGIC %python
-- MAGIC from datetime import date
-- MAGIC from html import escape
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import numpy as np
-- MAGIC 
-- MAGIC for key, default, label in [
-- MAGIC     ("catalog", "supplier_performance_dev", "Catalog"),
-- MAGIC     ("gold_schema", "dev_gold_views", "Gold schema"),
-- MAGIC     ("table_prefix", "dev_", "Table prefix"),
-- MAGIC     ("report_month", "2026-09-01", "Report month (yyyy-MM-01)"),
-- MAGIC     ("branch", "ALL", "Branch code or ALL"),
-- MAGIC     ("po_number", "", "Optional purchase order ID"),
-- MAGIC     ("po_item", "", "Optional order item ID with leading zeros"),
-- MAGIC ]:
-- MAGIC     dbutils.widgets.text(key, default, label)
-- MAGIC 
-- MAGIC selection = {key: dbutils.widgets.get(key).strip() for key in
-- MAGIC              ["catalog", "gold_schema", "table_prefix", "report_month", "branch"]}
-- MAGIC month = date.fromisoformat(selection["report_month"])
-- MAGIC if month.day != 1:
-- MAGIC     raise ValueError("report_month must be the first day of the month.")
-- MAGIC 
-- MAGIC scope_filter = """((:branch = 'ALL' AND report_scope = 'ALL')
-- MAGIC  OR (:branch <> 'ALL' AND report_scope = 'BRANCH' AND branch_code = :branch))"""
-- MAGIC monthly = spark.sql("""
-- MAGIC  SELECT * FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.' || :table_prefix || 'supplier_monthly')
-- MAGIC  WHERE report_month = CAST(:report_month AS DATE) AND """ + scope_filter,
-- MAGIC  args=selection).orderBy("supplier_id").collect()
-- MAGIC worst = spark.sql("""
-- MAGIC  SELECT * FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.' || :table_prefix || 'worst_suppliers')
-- MAGIC  WHERE report_month = CAST(:report_month AS DATE) AND """ + scope_filter,
-- MAGIC  args=selection).orderBy("worst_supplier_rank").collect()
-- MAGIC 
-- MAGIC n = sum(r.evaluated_order_item_count for r in monthly)
-- MAGIC candidates = sum(r.candidate_order_item_count for r in monthly)
-- MAGIC def weighted(field):
-- MAGIC     return sum(float(r[field]) * r.evaluated_order_item_count for r in monthly
-- MAGIC                if r[field] is not None and r.evaluated_order_item_count > 0) / n if n else None
-- MAGIC 
-- MAGIC def pct(value):
-- MAGIC     return f"{float(value):.1%}" if value is not None else "not evaluable"
-- MAGIC 
-- MAGIC cutoffs = sorted({str(r.as_of_date) for r in monthly})
-- MAGIC card = lambda label, value: f'<div><div>{escape(label)}</div><h2>{escape(str(value))}</h2></div>'
-- MAGIC rows_html = ''.join(
-- MAGIC     f'<tr><td style="padding:8px">{r.worst_supplier_rank}</td>'
-- MAGIC     f'<td style="padding:8px">{escape(r.supplier_name)} ({escape(r.supplier_id)})</td>'
-- MAGIC     f'<td style="padding:8px">{pct(r.on_time_rate)}</td>'
-- MAGIC     f'<td style="padding:8px">{pct(r.in_full_rate)}</td>'
-- MAGIC     f'<td style="padding:8px">{pct(r.performance_score)}</td>'
-- MAGIC     f'<td style="padding:8px">{r.evaluated_order_item_count}</td>'
-- MAGIC     f'<td style="padding:8px">{pct(r.evaluation_coverage_rate)}</td></tr>' for r in worst
-- MAGIC )
-- MAGIC notice = ('No monthly data in this selection.' if not monthly else
-- MAGIC           'No evaluable order items.' if not n else
-- MAGIC           'No suppliers meet the minimum sample size for ranking.' if not worst else
-- MAGIC           'Ranking covers evaluable order items. Check sample size and coverage.')
-- MAGIC if len(cutoffs) > 1:
-- MAGIC     notice += ' Mixed cutoff dates: check the data snapshot.'
-- MAGIC displayHTML(f"""
-- MAGIC <div style="font-family:Arial,sans-serif;padding:24px;background:#f4f7fb;color:#17344b">
-- MAGIC <h1>Supplier performance · {month:%Y-%m}</h1>
-- MAGIC <p>Branch: {escape(selection['branch'])} · Cutoff: {escape(', '.join(cutoffs) or 'no snapshot')}</p>
-- MAGIC <div style="display:flex;gap:36px;flex-wrap:wrap">
-- MAGIC {card('Evaluated order items', n)}{card('On time', pct(weighted('on_time_rate')))}
-- MAGIC {card('In full', pct(weighted('in_full_rate')))}
-- MAGIC {card('Coverage of attributable monthly items', pct(n / candidates if candidates else None))}
-- MAGIC </div><h2>Up to three worst-performing suppliers</h2><p>{escape(notice)}</p>
-- MAGIC <table style="border-collapse:collapse;text-align:left"><tr>
-- MAGIC <th>Rank</th><th>Supplier</th><th>On time</th><th>In full</th><th>Score</th><th>Order items</th><th>Coverage</th>
-- MAGIC </tr>{rows_html}</table>
-- MAGIC <p>Rates explain the performance_score. They do not establish operational causes at the supplier.
-- MAGIC Unassigned data is outside monthly coverage. See quality limitations below.</p>
-- MAGIC </div>
-- MAGIC """)

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Supplier comparison
-- MAGIC Show all evaluable suppliers. The minimum sample size affects ranking only.
-- MAGIC Missing KPI values are not displayed as zero.

-- COMMAND ----------
-- MAGIC %python
-- MAGIC chart_rows = sorted([r for r in monthly if r.evaluated_order_item_count > 0 and r.performance_score is not None],
-- MAGIC                     key=lambda r: (float(r.performance_score), r.supplier_id))
-- MAGIC if chart_rows:
-- MAGIC     positions = np.arange(len(chart_rows))
-- MAGIC     fig, ax = plt.subplots(figsize=(11, max(4, len(chart_rows) * 0.5)))
-- MAGIC     ax.barh(positions - 0.18, [100 * float(r.on_time_rate) for r in chart_rows],
-- MAGIC             height=0.34, color="#3678a8", label="On time")
-- MAGIC     ax.barh(positions + 0.18, [100 * float(r.in_full_rate) for r in chart_rows],
-- MAGIC             height=0.34, color="#3b947c", label="In full by month end")
-- MAGIC     ax.set_yticks(positions)
-- MAGIC     ax.set_yticklabels([f"{r.supplier_name} · {r.supplier_id} (n={r.evaluated_order_item_count})" for r in chart_rows])
-- MAGIC     ax.invert_yaxis()
-- MAGIC     ax.set_xlim(0, 100)
-- MAGIC     ax.set_xlabel("Share of evaluated order items (%)")
-- MAGIC     ax.set_title(f"Supplier comparison {month:%Y-%m}")
-- MAGIC     ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=2)
-- MAGIC     ax.grid(axis="x", alpha=0.2)
-- MAGIC     fig.tight_layout()
-- MAGIC     display(fig)
-- MAGIC     plt.close(fig)
-- MAGIC else:
-- MAGIC     print("No evaluable suppliers for the chart.")

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Monthly trend
-- MAGIC Same branch, all available months up to the selected month.
-- MAGIC Rates are weighted by evaluated order item counts. Composition and coverage may change.

-- COMMAND ----------
-- MAGIC %python
-- MAGIC trend = spark.sql("""
-- MAGIC  SELECT report_month, SUM(evaluated_order_item_count) AS evaluated_order_item_count,
-- MAGIC         SUM(on_time_rate * evaluated_order_item_count) / NULLIF(SUM(evaluated_order_item_count), 0) AS on_time_rate,
-- MAGIC         SUM(in_full_rate * evaluated_order_item_count) / NULLIF(SUM(evaluated_order_item_count), 0) AS in_full_rate,
-- MAGIC         SUM(evaluated_order_item_count) / NULLIF(SUM(candidate_order_item_count), 0) AS evaluation_coverage_rate
-- MAGIC  FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.' || :table_prefix || 'supplier_monthly')
-- MAGIC  WHERE report_month <= CAST(:report_month AS DATE) AND """ + scope_filter + """
-- MAGIC  GROUP BY report_month ORDER BY report_month
-- MAGIC """, args=selection).collect()
-- MAGIC if trend:
-- MAGIC     fig, ax = plt.subplots(figsize=(11, 4))
-- MAGIC     for field, label, color in [("on_time_rate", "On time", "#3678a8"),
-- MAGIC                                 ("in_full_rate", "In full", "#3b947c"),
-- MAGIC                                 ("evaluation_coverage_rate", "Coverage", "#967844")]:
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
-- MAGIC     print("No monthly trend available.")

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Results and supporting records
-- MAGIC Show ranking and suppliers, then supporting purchase order items.
-- MAGIC Use optional po_number and po_item filters for delivery evidence.
-- COMMAND ----------
-- An empty selected month is visible and is not automatically a data error.
SELECT COUNT(*) AS supplier_rows_in_selection,
       MIN(as_of_date) AS first_cutoff, MAX(as_of_date) AS last_cutoff,
       MAX(processed_at) AS last_written_at
FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.' || :table_prefix || 'supplier_monthly')
WHERE report_month = CAST(:report_month AS DATE)
  AND ((:branch = 'ALL' AND report_scope = 'ALL')
       OR (:branch <> 'ALL' AND report_scope = 'BRANCH' AND branch_code = :branch));

-- COMMAND ----------
SELECT report_month, report_scope, branch_code, worst_supplier_rank, supplier_id, supplier_name,
       evaluated_order_item_count, excluded_order_item_count, evaluation_coverage_rate,
       on_time_rate, in_full_rate, mean_quantity_fill_rate, performance_score
FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.' || :table_prefix || 'worst_suppliers')
WHERE report_month = CAST(:report_month AS DATE)
  AND ((:branch = 'ALL' AND report_scope = 'ALL')
       OR (:branch <> 'ALL' AND report_scope = 'BRANCH' AND branch_code = :branch))
ORDER BY worst_supplier_rank;

-- COMMAND ----------
-- Include suppliers below the minimum sample size.
SELECT *
FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.' || :table_prefix || 'supplier_monthly')
WHERE report_month = CAST(:report_month AS DATE)
  AND ((:branch = 'ALL' AND report_scope = 'ALL')
       OR (:branch <> 'ALL' AND report_scope = 'BRANCH' AND branch_code = :branch))
ORDER BY performance_score ASC NULLS LAST, supplier_id;

-- COMMAND ----------
-- Order item evidence for the selected ranking.
WITH worst AS (
  SELECT supplier_id
  FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.' || :table_prefix || 'worst_suppliers')
  WHERE report_month = CAST(:report_month AS DATE)
    AND ((:branch = 'ALL' AND report_scope = 'ALL')
         OR (:branch <> 'ALL' AND report_scope = 'BRANCH' AND branch_code = :branch))
)
SELECT p.*
FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.' || :table_prefix || 'order_item_performance') p
JOIN worst w ON p.supplier_id = w.supplier_id
WHERE p.report_month = CAST(:report_month AS DATE)
  AND (:branch = 'ALL' OR p.branch_code = :branch)
ORDER BY p.supplier_id, p.sap_client_id, p.purchase_order_id, p.purchase_order_item_id;

-- COMMAND ----------
-- Optional order filters; empty values show all delivery evidence for the selected month.
SELECT *
FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.' || :table_prefix || 'delivery_evidence')
WHERE report_month = CAST(:report_month AS DATE)
  AND (:po_number = '' OR purchase_order_id = :po_number)
  AND (:po_item = '' OR purchase_order_item_id = :po_item)
  AND (:branch = 'ALL' OR branch_code = :branch)
ORDER BY sap_client_id, actual_delivery_date, delivery_id, delivery_item_id;

-- COMMAND ----------
SELECT *
FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.' || :table_prefix || 'quality_summary')
ORDER BY reason;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Technical checks
-- MAGIC Checks cover the visible Gold data. PASS confirms only the named condition.
-- MAGIC A successful job does not mean that every check returned PASS.
-- MAGIC Wait for a complete Gold run. The branch widget is a display filter; view predicates enforce access.
-- COMMAND ----------
-- Check all visible data, independent of the selected month.
WITH items AS (
  SELECT * FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.' || :table_prefix || 'order_item_performance')
), monthly AS (
  SELECT * FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.' || :table_prefix || 'supplier_monthly')
), checks AS (
  SELECT 'GOLD_ITEMS_PRESENT' AS check_name,
         CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS violations FROM items
  UNION ALL
  SELECT 'UNIQUE_ORDER_ITEM_KEY', COUNT(*) FROM (
    SELECT sap_client_id, purchase_order_id, purchase_order_item_id FROM items GROUP BY sap_client_id, purchase_order_id, purchase_order_item_id HAVING COUNT(*) > 1
  ) duplicates
  UNION ALL
  SELECT 'EVALUATED_ITEMS_HAVE_KPI', COUNT(*) FROM items
  WHERE is_evaluable AND (is_on_time IS NULL OR is_in_full IS NULL OR quantity_fill_rate IS NULL)
  UNION ALL
  SELECT 'EXCLUDED_ITEMS_HAVE_NO_KPI', COUNT(*) FROM items
  WHERE NOT is_evaluable AND (is_on_time IS NOT NULL OR is_in_full IS NOT NULL OR quantity_fill_rate IS NOT NULL)
  UNION ALL
  SELECT 'MONTHLY_COUNTS_BALANCE', COUNT(*) FROM monthly
  WHERE candidate_order_item_count <> evaluated_order_item_count + excluded_order_item_count
  UNION ALL
  SELECT 'MONTHLY_RATES_IN_RANGE', COUNT(*) FROM monthly
  WHERE on_time_rate NOT BETWEEN 0 AND 1 OR in_full_rate NOT BETWEEN 0 AND 1
     OR mean_quantity_fill_rate NOT BETWEEN 0 AND 1 OR performance_score NOT BETWEEN 0 AND 1
     OR evaluation_coverage_rate NOT BETWEEN 0 AND 1
  UNION ALL
  SELECT 'EVALUATED_MONTHS_HAVE_RATES', COUNT(*) FROM monthly
  WHERE evaluated_order_item_count > 0 AND (on_time_rate IS NULL OR in_full_rate IS NULL OR performance_score IS NULL)
)
SELECT check_name, violations, CASE WHEN violations = 0 THEN 'PASS' ELSE 'CHECK' END AS status
FROM checks ORDER BY check_name;



-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Operational purchase orders and deliveries
-- MAGIC These datasets include open orders and future schedules. The report month does not filter them.
-- MAGIC Branch access is enforced by the Gold views. Order ID filters are optional.

-- COMMAND ----------
SELECT * FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.' || :table_prefix || 'purchase_order_items')
WHERE (:branch = 'ALL' OR branch_code = :branch)
  AND (:po_number = '' OR purchase_order_id = :po_number)
  AND (:po_item = '' OR purchase_order_item_id = :po_item)
ORDER BY branch_code, purchase_order_id, purchase_order_item_id;

-- COMMAND ----------
SELECT * FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.' || :table_prefix || 'delivery_items')
WHERE (:branch = 'ALL' OR branch_code = :branch)
  AND (:po_number = '' OR purchase_order_id = :po_number)
  AND (:po_item = '' OR purchase_order_item_id = :po_item)
ORDER BY branch_code, actual_delivery_date, delivery_id, delivery_item_id;

-- COMMAND ----------
SELECT * FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.' || :table_prefix || 'schedule_lines')
WHERE (:branch = 'ALL' OR branch_code = :branch)
  AND (:po_number = '' OR purchase_order_id = :po_number)
  AND (:po_item = '' OR purchase_order_item_id = :po_item)
ORDER BY branch_code, purchase_order_id, purchase_order_item_id, schedule_line_id;
