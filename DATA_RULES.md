# Data rules, interpretations and decisions

Updated: 2026-10-11. Customer replies confirm SAP-ECC semantics for EKKO/EKPO/EKET/LIKP/LIPS, the four branch names, and latest-change-date precedence. EKKO.AEDAT as a version proxy is separately authorised by the project owner for this synthetic case. Other source questions remain open. The project owner requested English Gold column names and a separate project catalog. Those are design decisions; the business meanings below remain provisional.

## Architecture

For dev, the catalog is `supplier_performance_dev`; schemas are `dev_bronze`, `dev_silver`, `dev_gold_data` and `dev_gold_views`. Object names use `dev_`. Prod follows the same pattern with `prod`.

| Layer | Purpose |
|---|---|
| Landing | Seven original SAP CSV files plus BRANCH_MAPPING.csv manually uploaded to `/Workspace/Users/guochengcheng93@gmail.com/landing_zone_case_solution`. Local originals are kept in `../DIC_Arbeitsprobe_Testdaten_SAP_MM/`; no duplicate source folder is maintained in this repository. |
| Bronze | Source fields as strings, plus source file and load time. Keep source duplicates. |
| Silver | Typed, validated records, quarantine and load reconciliation. Keep SAP field names for traceability. |
| Gold data | Physical Delta results and operational datasets, with English column names. Restricted to the processing identity and administrators. |
| Gold views | Protected views for business consumption. Example: `supplier_performance_dev.dev_gold_views.dev_supplier_monthly`. |

## Our interpretations and unconfirmed assumptions

This register distinguishes source-field interpretations, chosen metric definitions, and requested technical design. Customer confirmations and project-owner decisions are labelled separately below.

| Date | Topic | Basis | Our interpretation or decision | Status and follow-up |
|---|---|---|---|---|
| 2026-10-11 | `WERKS` to `branch_code` | Customer reply supplied four branches. | 2000 Hamburg; 2100 Bremen; 2200 Hannover; 2300 Rostock. Store in BRANCH_MAPPING.csv, validate in Silver and left-join in Gold. | **Customer-confirmed mapping.** Employee/group membership remains separate and unconfirmed. |
| 2026-10-11 | `LIFNR` to `supplier_id`; `NAME1` to `supplier_name` | Supplier master and header joins in this dataset. | Preserve the source identifier as text and use its master name. Identical names do not merge different IDs. | **Our field interpretation.** Confirm identifier scope and master-data semantics. |
| 2026-10-11 | Order and delivery aliases | Source table names and existing joins. | EBELN/EBELP become purchase_order_id/purchase_order_item_id; VBELN/POSNR become delivery_id/delivery_item_id. MANDT remains part of the key as sap_client_id. | **Our field interpretation.** These are readable aliases, not proof of source-system semantics. |
| 2026-10-11 | `candidate_schedule_line_count` | Implemented monthly population in Gold. | Count distinct schedule keys attributable to a supplier and a closed scheduled month, including plans excluded from scoring. No month attribution means outside this denominator. | **Our metric definition**, not a raw source field or a count of all exported items. |
| 2026-10-11 | `evaluated_schedule_line_count` | Gold exclusion rules. | Candidate schedule lines with no performance exclusion reason. | **Our metric definition.** Confirm the exclusion policy with the business. |
| 2026-10-11 | `excluded_schedule_line_count` | Difference between candidate and evaluated counts. | candidate_schedule_line_count minus evaluated_schedule_line_count. | **Our metric definition.** It excludes records that cannot be attributed to a supplier/month in the first place. |
| 2026-10-11 | `actual_delivery_date` from WADAT_IST | Existing provisional delivery logic. | Use this date for the demonstration. | **Unconfirmed and material.** It may describe goods issue rather than arrival at the customer. The English alias does not resolve this uncertainty. |
| 2026-10-11 | `delivered_quantity` from LFIMG | Existing delivery-item logic. | Use LFIMG as the delivered quantity, with matching units. | **Our unconfirmed interpretation.** Confirm receipt, reversals and completeness of the extract. |
| 2026-10-11 | `net_unit_price`, `net_order_value`, `currency_code` | NETPR/NETWR on EKPO and WAERS on EKKO. | Display source amounts and currency without conversion. | **Our field interpretation.** Price-unit factors are not supplied; do not reconstruct NETWR as quantity times NETPR or use NETPR for weighted metrics without clarification. |
| 2026-10-11 | Gold operational datasets | Project owner's request to include purchasing and delivery basics. | Publish valid Silver order items, schedule lines and delivery items alongside performance results. Keep deleted orders flagged and future/open records visible. | **Requested design; our implementation choice.** These are valid Silver records, not the entire raw export. Quarantined records remain in Silver. |
| 2026-10-11 | Catalog and readable names | Explicit project-owner request. | Use project/environment catalog, layer schemas and English Gold aliases. | **Confirmed technical requirement from the project owner**, not a confirmation of business meanings. |
| 2026-10-11 | Dev/prod environment isolation | Project owner confirmed use of Free Edition, which allows one workspace per account. | Configure separate dev/prod catalogs in the same workspace for this demonstration. A prod target does not create a second workspace. | **Documented platform constraint.** Prefer separate workspaces for production, with separate deployment identities and source locations, plus catalog permissions and workspace-catalog bindings where applicable. See the README environment-isolation section. |
| 2026-10-11 | Branch access groups | No approved employee-to-branch mapping has been supplied. | Default to an empty group mapping and no employee grants. A configured group only reads its matching branch rows. | **Security design.** Employee/group membership and branch ownership still require real information. |

## Bronze decisions

- Preserve strings and leading zeros. Disable schema inference. Empty CSV fields may load as null.
- Treat the seven SAP input files plus the branch reference as a complete snapshot and overwrite tables on every run. Snapshot completeness is an assumption; incremental exports and multi-extract history are not implemented.
- Keep all source duplicates. Silver decides how duplicates and conflicting keys are handled.

## Silver rules

DATE types and string identifiers were project-owner decisions on 2026-10-09. Other business interpretations remain provisional.

| Topic | Processing | Limitation |
|---|---|---|
| Text | Trim outer whitespace; blank becomes null. Uppercase MEINS, WAERS, LAND1 and LOEKZ. | Preserve leading zeros. `_raw_record` keeps original values. |
| Dates | Parse yyyy-MM-dd and dd.MM.yyyy as DATE. | Other or invalid nonblank dates go to quarantine. DATE has no stored display format. |
| Numbers | Decimal dot or comma; DECIMAL(38,6). | Assume no thousands separators. Mixed separators, overflow and more than six decimal places are rejected. |
| Amounts and quantities | Reject negative numbers; MENGE and LFIMG must be positive. | Do not assume negative values are returns. Missing optional prices and WEMNG remain null. |
| Exact duplicates | Collapse identical original source fields, independent of load time/file path. | `_source_occurrences` and `_source_files` preserve multiplicity and lineage. Bronze is unchanged. |
| Conflicting keys | For EKKO, select the unique latest AEDAT when all variants have valid dates. Archive older variants in superseded_records. Otherwise quarantine conflicting variants. | AEDAT is an authorised demo proxy, not a verified SAP change timestamp. Same-date ties and missing/invalid dates stay unresolved. Never select an older record merely because the newest fails validation. Other tables have no authorised version-date field. |
| References | Join only unique valid Silver parents. | UNRESOLVED_REFERENCE means no usable parent, which can be absent or quarantined. |
| Units | Compare EKPO.MEINS with MARA.MEINS and LIPS.MEINS with EKPO.MEINS. | Without conversion factors, quarantine mismatches even when different units could be legitimate. |
| Material and branch | Match LIPS material and WERKS to EKPO, and LIPS.WERKS to LIKP. | Use the customer-supplied branch reference for Gold labels; unknown codes stay visible as UNMAPPED. |
| Supplier | Validate EKKO/LIKP against LFA1; match supplier between order and delivery. | Assume LFA1/MARA apply to all MANDT values because those master files lack MANDT. |
| Deletion flag | Blank LOEKZ means active; L means deleted; other flags are rejected. | Deleted items remain flagged in Silver and are excluded from performance scoring. |

Candidate keys: LFA1: LIFNR; MARA: MATNR; EKKO: MANDT/EBELN; EKPO: MANDT/EBELN/EBELP; EKET: MANDT/EBELN/EBELP/ETENR; LIKP: MANDT/VBELN; LIPS: MANDT/VBELN/POSNR. The customer refers the five transactional tables to SAP-ECC standard keys and relationships. LFA1/MARA export keys remain dataset interpretations.

Required fields beyond keys: LFA1.NAME1; MARA.MEINS; EKKO.LIFNR/BEDAT; EKPO.MATNR/MENGE/MEINS/WERKS; EKET.EINDT/MENGE; LIKP.LIFNR/WADAT_IST/WERKS; LIPS.EBELN/EBELP/MATNR/LFIMG/MEINS/WERKS. See `specs` in the Silver notebook. Requiring WADAT_IST makes the demonstration evaluable; it does not verify its business meaning.

Silver outputs include seven valid SAP tables, `dev_branch_mapping`, `dev_quarantine`, `dev_superseded_records` and `dev_load_summary`. Reconciliation is Bronze rows = removed duplicate rows + Silver rows + quarantine rows + superseded rows. A record can have several error reasons. All outputs are replaced separately; a failed run can leave a partial snapshot. Gold runs only after Silver succeeds.

## Gold supplier ranking: equal schedule-line weights (approved 2026-10-12)

| Topic | Rule | Limitation |
|---|---|---|
| Cutoff | `as_of_date`, initially 2026-09-30; include only closed months. | Demo assumption, not a confirmed extract cutoff. Input must cover deliveries through that date. |
| Grain and month | One MANDT/EBELN/EBELP/ETENR plan; its own EKET.EINDT determines report_month. | Invalid schedules without a usable date cannot be assigned a month. |
| Ordered quantity | EKPO.MENGE must equal the sum of EKET.MENGE. | Exclude mismatches; no automatic correction or tolerance. |
| Deliveries | Use FIFO-allocated LFIMG quantities by the relevant date, using WADAT_IST. | Do not add WEMNG as extra delivery quantity. Delivery-date meaning remains open. |
| Partial/over-delivery | FIFO allocates each delivered quantity at most once; plan allocation cannot exceed its quantity. | Unallocated excess is reported separately. |
| On time | Full schedule quantity allocated by its own scheduled date, inclusive. | Zero-day tolerance. A late complete delivery is still late. |
| In full | Full schedule quantity allocated by its own reporting month end, inclusive. | Later deliveries do not change earlier monthly scores. |
| Exclusions | Invalid/deleted items, missing schedules, quantity mismatch, schedules/deliveries before order date, or attributable EKPO/EKET/LIPS quarantine. | Invalid LIKP records already quarantine dependent LIPS records in Silver. Excluded items have null KPI values. |
| No delivery row | Use quantity zero for an otherwise evaluable item without attributable quarantine. | Valid only under the complete-export assumption. Unassigned quarantine is additional uncertainty. |
| Monthly rates | Proportion of evaluated schedule lines on time/in full; also mean schedule fill rate. | One vote per plan, independent of quantity, value or number of plans on the parent item; more plans mean more votes. |
| Score and ranking | Mean of on_time_rate and in_full_rate, ascending. | Ties: lower on-time rate, lower in-full rate, more evaluated schedule lines, then supplier ID. The final ID ordering is deterministic, not a performance difference. |
| Minimum sample | `min_items` remains the parameter name, but now means minimum evaluated schedule lines (default 1) per supplier, month and scope. | Small samples have limited reliability. Fewer than three eligible suppliers yield fewer places. The local preview found no candidates at thresholds 10 or 20; Gold counts remain the decision basis. |
| Reporting scope | ALL is recalculated across branches; BRANCH is recalculated per WERKS. | Never filter a company aggregate and claim it is a branch aggregate. |

Rates range from 0 to 1. `evaluation_coverage_rate` = evaluated_schedule_line_count / candidate_schedule_line_count. It is not completeness of the whole export; order items with no schedule and schedules without month/supplier attribution are not in this denominator. All distinct Bronze item keys remain in internal item performance, including keys without month attribution. Excluded items with partial schedules can have provisional month attribution. Exclusion reasons can overlap.

## Approved FIFO schedule evaluation (2026-10-11)

Origin: project-owner approval of allocation and per-schedule evaluation. The customer asks us to choose and document a rule; the customer did not prescribe FIFO.

- Partition by MANDT/EBELN/EBELP. Silver validates material, branch and matching units before Gold allocation. There is no cross-item or cross-unit matching.
- Sort plans by EINDT then ETENR. Sort actual deliveries by WADAT_IST then VBELN/POSNR. Same-date ID ordering is deterministic, not evidence of real within-day sequence.
- Allocate quantities to the oldest outstanding plan. A delivery may satisfy several plans; several deliveries may satisfy one plan. Early deliveries may fulfil future plans. Each quantity is used at most once. Unmatched excess is retained separately.
- Allocate only actual deliveries on/before as_of_date, including late deliveries after a plan's month end. Such late quantities remain evidence but do not improve earlier monthly results. Future plans may receive early quantities, but open months have null KPIs.
- `schedule_line_performance` has one distinct Bronze schedule key. Valid Silver schedules provide dates/quantities. Invalid schedules and parents remain excluded with reasons; unrecoverable dates imply no month attribution. No missing quantity on an excluded record is presented as verified zero.
- Each plan's EINDT determines report_month. On time means its full schedule quantity is allocated by EINDT inclusive. In full means the full quantity is allocated by that month's end inclusive. No grace period. Fill rate and shortfall also use that month end.
- Parent quality exclusions still apply, including deleted items, schedule/order quantity mismatch, related quarantine and dates before order date. The parent's final-month closure exclusion is removed and recalculated for each plan, so a future plan does not automatically exclude an earlier closed month.
- `delivery_allocation_balance` preserves all valid delivery items and separates AFTER_CUTOFF, EXCLUDED_OR_NO_VALID_SCHEDULE, EXCESS_OVER_SCHEDULES and FULLY_ALLOCATED. Unallocated quantity on excluded items is not labelled overdelivery.
- Limitations: inferred links, not physical delivery intent; final snapshot cannot reconstruct plan changes; WADAT_IST is a proxy for this evaluation; missing delivery implies zero only under the complete-export assumption. WEMNG is never added to LFIMG.
- Superseded on 2026-10-12: the owner approved equal schedule-line weights. `supplier_monthly` and `worst_suppliers` now use FIFO schedule results, labelled FIFO_SCHEDULE_LINE_EQUAL_WEIGHT. Only `order_item_performance` and `delivery_evidence` retain legacy final-date logic as auxiliary comparison evidence; they do not feed supplier ranking.
- Local verification executes the actual allocation SQL using SQLite window functions: eight cases including 50 random quantity conservation trials. SQLite is not Spark decimal validation. Production code additionally checks quantity bounds before publishing; full Databricks execution remains pending.

## Gold datasets

All names below are prefixed with `dev_` in dev and published in `dev_gold_views`; physical backing tables use the same names in `dev_gold_data`.

| Name suffix | Grain and contents |
|---|---|
| supplier_monthly | FIFO supplier/month/scope/branch rates with equal schedule-line weights, explicit schedule counts and metric_basis. |
| worst_suppliers | Up to three eligible suppliers per month/scope/branch. |
| order_item_performance | Auxiliary legacy final-date result per candidate order item; not used in supplier ranking. |
| delivery_evidence | Valid delivery items linked to performance, including later deliveries and temporal flags. |
| purchase_order_items | Valid Silver purchase order items with supplier, branch, order date, material, quantity, unit, source amounts/currency and deletion flag. No performance cutoff filter. |
| delivery_items | Valid Silver delivery items with linked purchasing/supplier context. No performance cutoff filter. |
| delivery_allocations | Inferred FIFO intersections: order item, schedule ID, delivery ID/item, dates, allocated quantity and rule. |
| schedule_line_performance | One Bronze schedule key; per-plan month, quantities, on-time/in-full/fill-rate results and exclusions. |
| delivery_allocation_balance | One valid delivery item; allocated/unallocated quantity and status, including post-cutoff records. |
| schedule_lines | Valid Silver schedules linked to their order item, supplier and branch. Kept separate to prevent fan-out. |
| quality_summary | Counts of schedule keys by evaluation/exclusion reason, plus unassigned quarantine record counts. Central access only. |
| unassigned_quarantine | Unassignable EKPO/EKET/LIPS quarantine with raw records and reasons. Central access only. |

`as_of_date` records the evaluation cutoff, not a filter on the operational datasets. `processed_at` records write time. Writes are not a cross-table transaction; multiple extracts are not historised. The business-facing views enforce branch access but do not turn the provisional business rules into confirmed facts.

## Decision record discipline

For each change record date, rule, observed evidence, decision, origin (`our interpretation`, `project-owner decision`, `open customer question`, or `customer-confirmed`) and impact. Record a customer confirmation only when an actual answer exists. Review quarantine before revising exclusions. Missing usable delivery evidence alone does not prove non-delivery.


## Landing Zone decision (2026-10-11)

Origin: explicit project-owner instruction. Use the uploaded Workspace folder as Bronze input. This is a confirmed technical path choice, not confirmation of business semantics or export completeness. The input parser reads semicolon-delimited UTF-8 (optional BOM), accepts quoted fields, keeps strings and duplicates, and maps empty fields to null. Malformed rows fail before Bronze writes. The seven SAP CSV names are unchanged; BRANCH_MAPPING.csv is also required. Other files are ignored. The uploaded files are the active input; repository fixtures are for local exploration.

## Customer reply and approved implementation (2026-10-11)

- The branch reference is stored locally at `../DIC_Arbeitsprobe_Testdaten_SAP_MM/BRANCH_MAPPING.csv` and uploaded to the Workspace Landing folder by the project owner. Bronze reads it from the configured source_path.
- Gold publishes `branch_mapping` and adds `branch_name` plus `branch_mapping_status` to branch-bearing datasets. Left joins preserve unmapped codes and ALL scope. Invalid reference rows stop Gold before publication. This name mapping grants no employee permissions.
- Local EKKO evidence: 216 rows; 184 distinct valid AEDAT dates, 2025-01-09 to 2026-06-30; no missing/invalid AEDAT. Six conflicting keys each differ only in EKGRP and AEDAT. Each has a unique latest date. These observations support the owner's authorised synthetic-case proxy, not a claim about production SAP semantics.
- Superseded records retain raw content, provenance, old/selected dates and the rule LATEST_AEDAT_DEMO_PROXY. They are not quality failures and do not invalidate current parents. Current-state selection cannot reconstruct the state known at historical month end.
- Customer states no actual-to-planned-delivery linkage or fixed allocation rule exists. The project owner subsequently approved FIFO and schedule-level evaluation; these are now implemented as detailed above. Supplier weighting was subsequently approved on 2026-10-12: equal schedule-line weights.


## Equal schedule-line weighting decision (2026-10-12)

Origin: explicit project-owner agreement that every promised delivery has one vote. Nine on-time plans and one late plan produce 90% on-time, even if nine plans belong to one order item. Supplier monthly score remains the arithmetic mean of on-time and in-full rates (50% each). Excluded plans have null KPIs and are outside the rate denominator. ALL scope is recomputed from individual plans, not an average of branch percentages. Sample threshold, coverage, report cards, trend weights and ranking evidence now consistently use schedule lines. Public count columns changed to candidate_schedule_line_count, evaluated_schedule_line_count, excluded_schedule_line_count and minimum_schedule_line_count. Run the full Gold job and deploy the updated review notebook together; consumers using old order-count names must be updated.
