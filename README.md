# Supplier performance on Databricks

This project was developed for the Senior Data Engineer technical assessment and discussion with Data Insight Consulting. It uses the fictional Nordbau Handels GmbH scenario: a building-materials supplier with branches in Hamburg, Bremen, Hannover and Rostock.

The objective is to turn seven SAP ECC CSV extracts and a customer-provided branch mapping into a repeatable monthly evaluation: identify the three weakest suppliers, explain their results through delivery evidence, and prepare access so purchasing employees see only their own branch. The solution is packaged as a Databricks Asset Bundle with `dev` and `prod` targets.

This document describes the demonstration scope, architecture and answers to the assessment requirements. [DATA_RULES.md](DATA_RULES.md) records the data decisions, customer clarifications, assumptions and limitations. Customer-confirmed facts are distinguished from rules chosen for this case.

## Setup

The seven source CSV files and `BRANCH_MAPPING.csv` have been uploaded to the Workspace Landing Zone:

```text
/Workspace/Users/guochengcheng93@gmail.com/landing_zone_case_solution
```

The SAP extracts contain supplier and material master data, purchase orders, planned delivery schedules and delivery records. The branch mapping associates WERKS 2000 with Hamburg, 2100 with Bremen, 2200 with Hannover and 2300 with Rostock. Local originals remain in `../DIC_Arbeitsprobe_Testdaten_SAP_MM/`; the solution repository does not duplicate them.

Unity Catalog holds the catalog, schema, table and view definitions and their permissions. Bronze, Silver and physical Gold results are stored as managed Delta tables. Source-file and load-time metadata are retained with the ingested records.

The demonstration uses an uploaded snapshot. A production ingestion task could retrieve data from an approved SAP interface, API or source export into managed storage, then trigger the same processing stages through a scheduled Job. That source connection is outside this demonstration. The current processing Job has a configured daily schedule, paused for the demonstration; its results are evaluated by month.

## Data flow and responsibilities

```text
Workspace CSV Landing Zone
  -> Bronze       source strings and ingestion metadata
  -> Silver       typed, validated records + quarantine + reconciliation
  -> Gold data    physical Delta results and operational datasets
  -> Gold views   protected views for business consumption
```

| Layer | Responsibility |
|---|---|
| Bronze | Preserve source values, leading zeros and duplicates, with file and load metadata. |
| Silver | Parse dates and numbers, resolve eligible EKKO conflicts, validate references and units, and retain rejected or superseded records separately. |
| Gold data | Allocate deliveries to plans, calculate performance and retain traceable operational details. |
| Gold views | Present readable business columns and apply branch access predicates to the querying identity. |

The Job runs Bronze, Silver and Gold in dependency order. Each run replaces the current snapshot. Incremental ingestion, historical snapshots and a transaction across all output tables are outside the current scope.

## 1. Development and production separation

Databricks Free Edition allows one workspace and one metastore per account. The demonstration therefore configures both targets in the same workspace, with separate catalogs, schemas and Jobs. [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).

| Target | Catalog | Schemas | Object prefix |
|---|---|---|---|
| `dev` | `supplier_performance_dev` | `dev_bronze`, `dev_silver`, `dev_gold_data`, `dev_gold_views` | `dev_` |
| `prod` | `supplier_performance_prod` | Corresponding `prod_` schemas | `prod_` |

The Asset Bundle defines the code, Jobs and environment configuration in version control. The `prod` target represents the deployment configuration; it does not create a second workspace or establish that production has been deployed. Both targets currently reference the demonstration Landing folder.

For a real customer, the proposed design uses separate development and production workspaces, source locations and deployment identities, with catalog permissions and workspace bindings where applicable. This separates operational changes and data access more clearly than the demonstration configuration.

## 2. Data preparation and quality

Quality rules cover required fields, key conflicts, date and number formats, parent references, quantities, units and consistency between orders and deliveries. Invalid records remain traceable in Silver quarantine with explicit reasons.

Identical duplicates are collapsed. For EKKO, a unique latest AEDAT selects the current record when all conflicting variants have usable dates; older variants remain in `superseded_records`. Using AEDAT for version ordering is a documented assumption for this synthetic dataset. Unresolved conflicts are quarantined.

Bronze row counts reconcile with removed duplicates, valid Silver rows, quarantine and superseded records. Quality exclusions are reported separately from supplier failures, so unusable evidence does not automatically become a late or missing delivery.

## 3. Supplier performance and the three weakest suppliers

The business result is published in **`supplier_performance_dev.dev_gold_views.dev_worst_suppliers`**. It contains up to three eligible suppliers with the lowest score for each month, for the whole company and separately for each branch. Physical results are stored in `dev_gold_data`.

The evaluation unit is a planned delivery line. Within one purchase-order item, actual deliveries are allocated to the earliest scheduled delivery first (FIFO). One delivery can satisfy several plans, and several deliveries can satisfy one plan. The allocation is inferred because the extract contains no direct link between actual and planned deliveries.

| Measure | Definition |
|---|---|
| Reporting month | Month of the individual planned delivery date. Only closed months through the evaluation cutoff are scored. |
| On-time rate | Share of evaluable plans whose full quantity was allocated by their scheduled date. |
| In-full rate | Share of evaluable plans whose full quantity was allocated by the end of their scheduled month. |
| Performance score | Mean of the on-time and in-full rates, with 50% weight each. Lower scores are worse. |
| Weighting | Every evaluable plan has one vote, independent of quantity or value. Orders with more plans therefore influence the result more. |
| Coverage | Evaluable plans divided by candidate plans attributable to the supplier and month. It does not measure completeness of the whole export. |

The minimum sample defaults to one evaluable plan per supplier, month and scope and is configurable. Results include sample size and coverage. Fewer than three eligible suppliers produce fewer than three ranked results. Company-wide rates are calculated from the plans across branches, not by averaging branch percentages.

The supporting records explain whether a low score comes from missed deadlines or quantities still missing at month end. They do not establish the supplier's internal operational cause. The [result review](databricks/src/05_result_queries.sql) presents rankings, comparisons, trends and evidence from the same Gold views.

### Gold datasets

The names below are suffixes; dev views use `supplier_performance_dev.dev_gold_views.dev_<suffix>` and corresponding physical tables use `dev_gold_data`.

| Dataset | Purpose and source relationship |
|---|---|
| `worst_suppliers` | Up to three lowest-scoring eligible suppliers per month and reporting scope. |
| `supplier_monthly` | Supplier rates, score, plan counts, coverage and ranking eligibility. Derived from schedule-line performance. |
| `schedule_line_performance` | Per-plan FIFO evaluation, scheduled month, allocated quantities, KPI results and exclusion reasons. |
| `delivery_allocations` | Trace from each EKET plan to LIPS/LIKP delivery evidence, with allocated quantity and the inferred allocation rule. |
| `delivery_allocation_balance` | Delivered, allocated and unallocated quantities; distinguishes excess, excluded records and deliveries after cutoff. |
| `purchase_order_items` | EKPO order items enriched with EKKO headers and LFA1 supplier context. |
| `schedule_lines` | Valid EKET plans enriched with order, supplier, material and branch context. |
| `delivery_items` | Valid LIPS/LIKP deliveries enriched with purchasing and supplier context. |
| `branch_mapping` | Customer-provided branch codes and names from the Landing reference file. |
| `quality_summary` | Schedule evaluation/exclusion counts and unassigned quarantine counts; central access only. |
| `unassigned_quarantine` | EKPO/EKET/LIPS quarantine records that cannot be matched to an original order-item key; central access only. |
| `order_item_performance`, `delivery_evidence` | Auxiliary comparison using the previous final-order-date method. Explicitly labelled legacy; not used in the current supplier ranking. |

Operational datasets also include valid open and future records. MARA supports material and unit validation in Silver and is not published as a separate Gold dataset.

## 4. Branch access control

Business users access protected views in `dev_gold_views`. The physical backing tables are in `dev_gold_data`. The [Gold implementation](databricks/src/03_gold.py) generates view predicates that compare each row's `branch_code` with the querying user's configured account-group membership through `is_account_group_member()`.

For monthly results, branch users can see only their matching branch and `report_scope = 'BRANCH'`. Company-wide `ALL` results and unassigned quality records are reserved for the publishing identity and the optional central group. A report's branch selector changes the display; the view predicate enforces access.

This implements row filtering through **dynamic views and group membership**. Databricks native ABAC uses governed tags and centralized policies; those policies are not configured in this demonstration. The branch attribute and group mapping express the required access rule without claiming a native ABAC deployment. [Dynamic views](https://docs.databricks.com/aws/en/views/dynamic), [Unity Catalog ABAC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/core-concepts).

The supplied configuration has no employee-group mapping and grants no branch access by default. The access design requires approved group membership and no direct or inherited access to backing tables, Bronze or Silver. Real-user isolation has not been demonstrated. Production validation would check separate branch identities and a user without a branch group; administrators and object owners remain privileged.

## 5. Decisions, assumptions and scope

[DATA_RULES.md](DATA_RULES.md) is the detailed decision record. It covers customer answers, source-field interpretations, key handling, FIFO allocation, equal plan weighting, metric definitions, exclusions and limitations.

The main boundaries are the uploaded snapshot, the assumed evaluation cutoff, use of WADAT_IST as the evaluation date, and inferred delivery allocation. Missing usable evidence alone does not prove non-delivery. The design can be extended with invoice and complaint data linked to purchasing and delivery identifiers, as envisaged in the scenario; those sources are not part of this implementation.

An additional [cost notebook](databricks/src/04_cost_management.py) offers a clearly labelled simulation and a separate mode for billing-system queries. It is outside the core supplier assessment and does not represent a verified invoice or enforce spending limits.

## Repository guide

| Location | Contents |
|---|---|
| `databricks/src/00_dataquality.ipynb` | Initial exploration of the local SAP extracts. |
| `databricks/src/01_bronze.py` to `03_gold.py` | Ingestion, validation, allocation, metrics and protected views. |
| `databricks/src/05_result_queries.sql` | Business result review and supporting checks. |
| `databricks/src/04_cost_management.py` | Optional cost analysis. |
| `databricks/databricks.yml`, `databricks/resources/` | Asset Bundle targets, schemas and Jobs. |
| `tests/` | Focused allocation and equal-weight aggregation tests. |
| `DATA_RULES.md` | Public data decisions and assumptions. |
