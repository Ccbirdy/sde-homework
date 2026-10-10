# Supplier performance

A reproducible Databricks case solution that turns seven SAP-style CSV extracts into monthly supplier performance, branch reports and traceable purchasing/delivery records. Business interpretations and limitations are recorded in [DATA_RULES.md](DATA_RULES.md); implementation does not imply customer confirmation.

## Data flow and responsibilities

```text
Workspace CSV landing folder
  -> dev_bronze      source strings and ingestion metadata
  -> dev_silver      typed, validated records + quarantine + reconciliation
  -> dev_gold_data   physical Delta tables for results and operational data
  -> dev_gold_views  protected views queried by business users
```

All four schemas are in `supplier_performance_dev`. Prod uses `supplier_performance_prod` and the `prod_` prefix. Gold data stores the actual records; Gold views read those records and apply access predicates without making a second copy. Processing identities and administrators maintain the tables. Employees query only the views.

The active landing folder is `/Workspace/Users/guochengcheng93@gmail.com/landing_zone_case_solution`. The Bundle also retains an unused `dev_landing.dev_source_files` Volume; it is not an ingestion input. Configure `source_path` in [databricks.yml](databricks/databricks.yml) to use another Workspace folder. Dev and prod currently share the demonstration input.

| Code | Responsibility |
|---|---|
| [00_dataquality.ipynb](databricks/src/00_dataquality.ipynb) | Explore source quality and sample-size thresholds; the final sections distinguish local candidate counts from actual Gold evaluated counts. |
| [01_bronze.py](databricks/src/01_bronze.py) | Parse all seven CSV files before writing source strings and lineage. |
| [02_silver.py](databricks/src/02_silver.py) | Type and validate fields, remove exact duplicates, quarantine conflicting/invalid records, reconcile row counts. |
| [03_gold.py](databricks/src/03_gold.py) | Calculate metrics, publish operational tables and create protected views with group grants. |
| [05_result_queries.sql](databricks/src/05_result_queries.sql) | Present KPI cards, rankings, trends, evidence, quality checks and operational records. Includes Python plotting cells. |
| [04_cost_management.py](databricks/src/04_cost_management.py) | Optional interactive cost demonstration or live billing analysis, separate from the processing jobs. |

## First-time setup

Requirements: a Databricks workspace with serverless compute and Default Storage, a deployment identity with catalog creation privileges and access to the source folder, and GitHub Actions configured with repository variable `DATABRICKS_HOST` and secret `DATABRICKS_TOKEN`. Use the same identity for bootstrap and processing, or arrange the required catalog privileges explicitly. This repository pins Databricks CLI 1.20.0 in Actions.

1. Upload these original files to the landing folder: `LFA1_VENDOR_MASTER.csv`, `MARA_MATERIAL_MASTER.csv`, `EKKO_PO_HEADER.csv`, `EKPO_PO_ITEM.csv`, `EKET_SCHEDULE_LINES.csv`, `LIKP_DELIVERY_HEADER.csv`, `LIPS_DELIVERY_ITEM.csv`. Other files are ignored. Files use semicolons and UTF-8, optionally with a BOM.
2. In GitHub Actions run **Deploy Databricks bootstrap**, selecting `dev` and the code branch to deploy.
3. In Databricks Jobs & Pipelines run **dev_supplier_performance_initialize_catalog** and wait for success. This runs `CREATE CATALOG IF NOT EXISTS` from the [bootstrap SQL](databricks-bootstrap/src/dev_create_catalog.sql).
4. Run **Deploy Databricks** for `dev`, then run **dev_supplier_performance** in Databricks to populate the layers.
5. Run **Run Databricks review**, selecting a month with results and either `ALL` or an exact branch code. Inspect the report and its PASS/CHECK results.

Bootstrap is needed once per environment. Its SQL is safe to repeat and preserves an existing catalog. A separate Bundle is necessary because the main Bundle needs the catalog when deploying schemas, before a processing job can execute. For prod, select `prod` and run the corresponding `prod_` jobs.

The workflow files must exist on GitHub's default branch for their manual **Run workflow** buttons to appear. The branch selector then chooses which version to deploy. Deployment uploads code and configures resources; it does not execute the processing notebooks or upload CSV files. The processing schedule is paused by default.

## Subsequent runs

Push the updated code, run **Deploy Databricks** for the intended target, run the processing job, then run the review. The review Action uses the already deployed job: selecting a different Git branch does not deploy its notebooks. Clear stale manual job parameter overrides when changing configured paths.

The main parameters are `source_path`, `as_of_date` (default `2026-09-30`), `min_items` (default `1`), `branch_groups_json` and `central_group`. Set persistent access settings in the target variables in [databricks.yml](databricks/databricks.yml). Each processing run replaces the snapshot; there is no incremental history or cross-table transaction. A failed run can leave partial results. Resolve the failure and rerun before presenting the report.

CLI alternative, from the repository root after configuring a Databricks authentication profile:

```powershell
# Once per environment
Set-Location databricks-bootstrap
databricks bundle validate -t dev -p supplier-performance
databricks bundle deploy -t dev -p supplier-performance
databricks bundle run initialize_catalog -t dev -p supplier-performance

# Deploy and process
Set-Location ../databricks
databricks bundle validate -t dev -p supplier-performance
databricks bundle deploy -t dev -p supplier-performance
databricks bundle run supplier_performance -t dev -p supplier-performance
databricks bundle run supplier_performance_review -t dev -p supplier-performance
```

The Workspace CSV parser holds the small input snapshot in notebook memory before creating Spark DataFrames. It is a case-solution ingestion method, not a large-file ingestion design. Repository CSV fixtures support local exploration; the pipeline reads the uploaded Workspace files.

## Where to find and present the conclusions

Start with **dev_supplier_performance_review** and choose `report_month` and `branch`. Its output is the presentation entry point. The business results are views under `supplier_performance_dev.dev_gold_views`, all prefixed `dev_`:

| Presentation step | View suffix | What it answers |
|---|---|---|
| 1. Monthly overview | `supplier_monthly` | How did each supplier perform? Show evaluated sample size, coverage, on-time/in-full rates and score together. |
| 2. Suppliers needing review | `worst_suppliers` | Which up to three eligible suppliers scored lowest in this month and scope? |
| 3. Explain a result | `order_item_performance`, `delivery_evidence` | Which items and deliveries support the result, and which items were excluded? |
| 4. Operational detail | `purchase_order_items`, `schedule_lines`, `delivery_items` | What was ordered, scheduled and delivered in the user's branch? These include valid open/future records beyond the performance month. |
| 5. Qualification of conclusions | `quality_summary`, `unassigned_quarantine` | What data-quality limitations affect interpretation? These views are central-only. |

Example, using the querying user's own identity:

```sql
SELECT report_month, branch_code, supplier_id, supplier_name,
       evaluated_order_item_count, evaluation_coverage_rate,
       on_time_rate, in_full_rate, performance_score
FROM supplier_performance_dev.dev_gold_views.dev_supplier_monthly
WHERE report_month = DATE '2026-09-01'
ORDER BY performance_score, supplier_id;
```

The score is the mean of the on-time and in-full rates. Each evaluated order item has equal weight; amounts and quantities do not weight the ranking. On time means complete by the final scheduled date, while in full means complete by reporting month end. Lower scores are worse. Rates are stored between 0 and 1.

The minimum sample remains **1 evaluated item per supplier/month/scope**. Small samples can produce extreme scores. The local source preview found no eligible candidate populations at thresholds 10 or 20; actual Gold evaluated counts are the basis for choosing the threshold. Do not interpret the weakest-three output as a reliable long-term supplier judgement without reviewing sample size, exclusions and the unconfirmed delivery-date semantics. The report computes the current conclusion from the loaded snapshot; this README does not hard-code a winning or losing supplier.

## How branch isolation works

Implementation: the **Publish protected Gold views** section at the end of [03_gold.py](databricks/src/03_gold.py). The notebook generates a `WHERE` predicate using `is_account_group_member()` for the querying identity. For monthly aggregates, branch users additionally require `report_scope = 'BRANCH'`, so they cannot see company totals. Operational views apply the same branch membership check. This follows [Databricks dynamic-view access control](https://docs.databricks.com/aws/en/views/dynamic).

Example configuration under the appropriate target's `variables` (group names and codes are illustrative, not provisioned or approved):

```yaml
branch_groups_json: '{"2100":"supplier_branch_2100","2200":"supplier_branch_2200"}'
central_group: 'supplier_central'
```

Create the account groups and assign approved employees first, configure the exact source branch codes, then deploy and rerun Gold through the processing job. By default the mapping is `{}` and the central group is empty, so no employee access is granted. A user belonging to several configured branch groups can see all of those branches. Group membership must reflect approved access.

Conceptually, the branch part of a monthly-view predicate is:

```sql
report_scope = 'BRANCH' AND (
  (branch_code = '2100' AND is_account_group_member('supplier_branch_2100'))
  OR (branch_code = '2200' AND is_account_group_member('supplier_branch_2200'))
)
```

The full generated predicate also permits the publishing identity and the optional central group to read all rows. Quality summaries and unassigned quarantine are limited to those central readers. The notebook grants configured groups `USE CATALOG`, `USE SCHEMA` on Gold views and `SELECT` on their applicable views.

Employees must not have direct or inherited `SELECT` on Gold data, Bronze or Silver, broad catalog `SELECT`/`ALL PRIVILEGES`, or ownership/management rights allowing them to change the security boundary. Administrators and owners are privileged. Employees must query with their own identities; do not give them access to owner-identity processing/review jobs or their global outputs. A report branch widget is only a display filter, not an access control.

Before employee release, verify group membership and inherited grants, then test using two real branch identities: each must see its own branch, no other branch or ALL aggregate, and be denied direct Gold-data reads. Also test a user with no authorised group. The implemented predicates do not constitute a completed real-user access test.

## Business rules and optional cost analysis

[DATA_RULES.md](DATA_RULES.md) contains the field interpretations, candidate keys, quarantine rules, metric/exclusion definitions and dataset grains. `WERKS` as branch and `WADAT_IST` as actual delivery date remain unconfirmed interpretations. Readable aliases do not confirm these meanings.

The cost notebook supports `demo` (synthetic usage/prices) and `live` (billing system tables requiring separate access). Live estimates use time-matched list prices, exclude contract discounts and separate cloud invoices, and report missing prices. Its budget covers the selected date period; reduction scenarios are hypothetical. Notebook execution itself can incur compute costs. Job resource tags identify project, environment and cost centre; no serverless usage policy is bound. A shared run identity does not identify the person who started each run.
