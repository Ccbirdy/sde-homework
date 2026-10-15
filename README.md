# Supplier performance on Databricks

This project demonstrates data management and supplier performance analysis on Databricks. It simulates a project for Nordbau Handels GmbH, a fictional building-materials company with branches in Hamburg, Bremen, Hannover and Rostock.

**Project goals from the case brief**

- Build an automated, traceable Databricks solution for monthly supplier performance analysis from seven SAP ECC extracts.
- Define data-quality rules, performance metrics and a justified evaluation granularity to assess punctual and complete deliveries.
- Identify the three weakest suppliers and explain their results through purchasing and delivery evidence.
- Prepare the data model for purchasers to access only their own branch and for later integration of invoices and complaints.
- Deliver code, Jobs and configuration as a Databricks Asset Bundle with `dev` and `prod` targets, and document decisions and assumptions.

**Implementation checklist**

Checked items describe implemented code and configuration; operational limitations are stated alongside them.

- [x] Prepare the seven extracts and customer branch mapping through Bronze, Silver and Gold, including quality checks and quarantine.
- [x] Calculate monthly on-time and in-full performance with equal weight per planned delivery line, and retain FIFO allocation evidence.
- [x] Provide up to three weakest eligible suppliers per month, company-wide and per branch, with supporting delivery records.
- [x] Define a processing Job with dependent tasks and a schedule. The current schedule runs daily and is paused for the demonstration; results are monthly.
- [x] Implement branch predicates in protected views. Employee-group configuration and validation with separate user identities remain outstanding.
- [x] Package code, Jobs and configuration with `dev` and `prod` targets. A separate production deployment has not been demonstrated.
- [x] Document assumptions, metric definitions and limitations in [DATA_RULES.md](DATA_RULES.md); retain purchasing and delivery identifiers for future invoice and complaint integration. Those additional sources are outside the current implementation.

**Documentation**

- This README: demonstration scope, architecture and solution overview.
- [DATA_RULES.md](DATA_RULES.md): customer clarifications, data decisions, assumptions and limitations.

## Setup

The seven source [CSV files](data/) and `BRANCH_MAPPING.csv` have been uploaded as Workspace Files to storage managed by Databricks. In this demonstration, the project uses a folder named `landing_zone_case_solution` in the user's Workspace area as its Landing Zone. This is a project folder, not a predefined Databricks landing directory; the source location is configurable through the Bundle's `source_path` variable.

| Source file | Table / reference | Contents |
|---|---|---|
| `LFA1_VENDOR_MASTER.csv` | LFA1 | Supplier identifiers, names and master data. |
| `MARA_MATERIAL_MASTER.csv` | MARA/MAKT | Material identifiers, descriptions and base units. |
| `EKKO_PO_HEADER.csv` | EKKO | Purchase-order headers, including supplier and order date. |
| `EKPO_PO_ITEM.csv` | EKPO | Ordered materials, quantities, prices and receiving plants. |
| `EKET_SCHEDULE_LINES.csv` | EKET | Planned delivery dates and quantities for each order item. |
| `LIKP_DELIVERY_HEADER.csv` | LIKP | Delivery headers, including supplier and goods-movement date. |
| `LIPS_DELIVERY_ITEM.csv` | LIPS | Delivered materials, quantities and purchase-order references. |
| `BRANCH_MAPPING.csv` | Customer reference | WERKS plant code → branch name: 2000 Hamburg, 2100 Bremen, 2200 Hannover, 2300 Rostock. |

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

The Job runs Bronze, Silver and Gold in dependency order. Each run replaces the current snapshot. Incremental ingestion, historical snapshots and coordinated publication across output tables are not implemented yet and could be added in a future extension.

## 1. Development and production separation

Databricks Free Edition allows one workspace and one metastore per account. The demonstration therefore configures both targets in the same workspace, with separate catalogs, schemas and Jobs. [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).

| Target | Catalog | Schemas | Object prefix |
|---|---|---|---|
| `dev` | `supplier_performance_dev` | `dev_bronze`, `dev_silver`, `dev_gold_data`, `dev_gold_views` | `dev_` |
| `prod` | `supplier_performance_prod` | Corresponding `prod_` schemas | `prod_` |

An Asset Bundle keeps processing code, Jobs and configuration together. Targets reuse the same processing logic and table definitions while supplying environment-specific settings. With separate workspaces, this allows a version to be tested in dev and deployed to prod for downstream use. [Bundle overview](https://docs.databricks.com/aws/en/dev-tools/bundles/).

For a production setup, targets can select different workspaces, data locations, deployment identities and compute settings. RBAC permissions and ABAC policies can also be tailored to each environment through the appropriate governance configuration. Compute capacity can be matched to each environment's workload and cost needs.

In this demonstration, dev and prod share one Free Edition workspace and the same uploaded input. The prod target is configured; this does not mean a separate production environment has already been deployed. The implemented branch controls are described below.

## 2. Data preparation and quality

- An initial quality review identifies issues in the source files.
- Bronze, Silver and Gold process the data according to the responsibilities above.
- Records with unresolved quality issues are retained in quarantine, with reasons for review and discussion.
- Data-quality exclusions remain separate from supplier delivery failures.

Detailed rules and decisions are documented in [DATA_RULES.md](DATA_RULES.md).

## 3. Supplier performance and the three weakest suppliers

The business result is published in **`supplier_performance_dev.dev_gold_views.dev_worst_suppliers`**. It contains up to three eligible suppliers with the lowest score for each month, for the whole company and separately for each branch. The Unity Catalog hierarchy is:

| Level | Business access | Physical result |
|---|---|---|
| Catalog | `supplier_performance_dev` | `supplier_performance_dev` |
| Schema | `dev_gold_views` | `dev_gold_data` |
| Object | `dev_worst_suppliers` — protected view | `dev_worst_suppliers` — Delta table |

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

## Repository guide

| Location | Contents |
|---|---|
| `databricks/src/00_dataquality.ipynb` | Initial exploration of the local SAP extracts. |
| `databricks/src/01_bronze.py` to `03_gold.py` | Ingestion, validation, allocation, metrics and protected views. |
| `databricks/src/05_result_queries.sql` | Business result review and supporting checks. |
| `databricks/databricks.yml`, `databricks/resources/` | Asset Bundle targets, schemas and Jobs. |
| `tests/` | Focused allocation and equal-weight aggregation tests. |
| `DATA_RULES.md` | Public data decisions and assumptions. |
