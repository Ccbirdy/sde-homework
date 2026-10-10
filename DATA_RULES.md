# Data rules, interpretations and decisions

Updated: 2026-10-11. Questions about source semantics remain open. Implementation does not imply customer confirmation. The project owner requested English Gold column names and a separate project catalog. Those are design decisions; the business meanings below remain provisional.

## Architecture

For dev, the catalog is `supplier_performance_dev`; schemas are `dev_landing`, `dev_bronze`, `dev_silver`, `dev_gold_data` and `dev_gold_views`. Object names use `dev_`. Prod follows the same pattern with `prod`.

| Layer | Purpose |
|---|---|
| Landing | Seven original CSV files manually uploaded to `/Workspace/Users/guochengcheng93@gmail.com/landing_zone_case_solution`. The previously created landing Volume is retained but unused. |
| Bronze | Source fields as strings, plus source file and load time. Keep source duplicates. |
| Silver | Typed, validated records, quarantine and load reconciliation. Keep SAP field names for traceability. |
| Gold data | Physical Delta results and operational datasets, with English column names. Restricted to the processing identity and administrators. |
| Gold views | Protected views for business consumption. Example: `supplier_performance_dev.dev_gold_views.dev_supplier_monthly`. |

## Our interpretations and unconfirmed assumptions

This register distinguishes source-field interpretations, chosen metric definitions, and requested technical design. No entry below is a customer-confirmed business rule.

| Date | Topic | Basis | Our interpretation or decision | Status and follow-up |
|---|---|---|---|---|
| 2026-10-11 | `WERKS` to `branch_code` | Source field appears on order and delivery items; task requires branch reporting. | Treat each WERKS code as the reporting and access branch. Do not invent city names. | **Our unconfirmed interpretation.** Confirm whether a plant/site corresponds one-to-one to a branch and obtain the authorised employee mapping before granting access. |
| 2026-10-11 | `LIFNR` to `supplier_id`; `NAME1` to `supplier_name` | Supplier master and header joins in this dataset. | Preserve the source identifier as text and use its master name. Identical names do not merge different IDs. | **Our field interpretation.** Confirm identifier scope and master-data semantics. |
| 2026-10-11 | Order and delivery aliases | Source table names and existing joins. | EBELN/EBELP become purchase_order_id/purchase_order_item_id; VBELN/POSNR become delivery_id/delivery_item_id. MANDT remains part of the key as sap_client_id. | **Our field interpretation.** These are readable aliases, not proof of source-system semantics. |
| 2026-10-11 | `candidate_order_item_count` | Implemented monthly population in Gold. | Count distinct order items attributable to a supplier and a closed reporting month, including items later excluded from scoring. | **Our metric definition**, not a raw source field or a count of all exported items. |
| 2026-10-11 | `evaluated_order_item_count` | Gold exclusion rules. | Candidate items with no performance exclusion reason. | **Our metric definition.** Confirm the exclusion policy with the business. |
| 2026-10-11 | `excluded_order_item_count` | Difference between candidate and evaluated counts. | candidate_order_item_count minus evaluated_order_item_count. | **Our metric definition.** It excludes records that cannot be attributed to a supplier/month in the first place. |
| 2026-10-11 | `actual_delivery_date` from WADAT_IST | Existing provisional delivery logic. | Use this date for the demonstration. | **Unconfirmed and material.** It may describe goods issue rather than arrival at the customer. The English alias does not resolve this uncertainty. |
| 2026-10-11 | `delivered_quantity` from LFIMG | Existing delivery-item logic. | Use LFIMG as the delivered quantity, with matching units. | **Our unconfirmed interpretation.** Confirm receipt, reversals and completeness of the extract. |
| 2026-10-11 | `net_unit_price`, `net_order_value`, `currency_code` | NETPR/NETWR on EKPO and WAERS on EKKO. | Display source amounts and currency without conversion. | **Our field interpretation.** Price-unit factors are not supplied; do not reconstruct NETWR as quantity times NETPR or use NETPR for weighted metrics without clarification. |
| 2026-10-11 | Gold operational datasets | Project owner's request to include purchasing and delivery basics. | Publish valid Silver order items, schedule lines and delivery items alongside performance results. Keep deleted orders flagged and future/open records visible. | **Requested design; our implementation choice.** These are valid Silver records, not the entire raw export. Quarantined records remain in Silver. |
| 2026-10-11 | Catalog and readable names | Explicit project-owner request. | Use project/environment catalog, layer schemas and English Gold aliases. | **Confirmed technical requirement from the project owner**, not a confirmation of business meanings. |
| 2026-10-11 | Branch access groups | No approved employee-to-branch mapping has been supplied. | Default to an empty group mapping and no employee grants. A configured group only reads its matching branch rows. | **Security design.** Employee/group membership and branch ownership still require real information. |

## Bronze decisions

- Preserve strings and leading zeros. Disable schema inference. Empty CSV fields may load as null.
- Treat the seven input files as a complete snapshot and overwrite tables on every run. Snapshot completeness is an assumption; incremental exports and multi-extract history are not implemented.
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
| Conflicting keys | Quarantine all variants after text normalisation. | Do not choose an arbitrary latest record. Pure formatting variants can also conflict. |
| References | Join only unique valid Silver parents. | UNRESOLVED_REFERENCE means no usable parent, which can be absent or quarantined. |
| Units | Compare EKPO.MEINS with MARA.MEINS and LIPS.MEINS with EKPO.MEINS. | Without conversion factors, quarantine mismatches even when different units could be legitimate. |
| Material and branch | Match LIPS material and WERKS to EKPO, and LIPS.WERKS to LIKP. | No invented mapping of WERKS to cities. |
| Supplier | Validate EKKO/LIKP against LFA1; match supplier between order and delivery. | Assume LFA1/MARA apply to all MANDT values because those master files lack MANDT. |
| Deletion flag | Blank LOEKZ means active; L means deleted; other flags are rejected. | Deleted items remain flagged in Silver and are excluded from performance scoring. |

Candidate keys: LFA1: LIFNR; MARA: MATNR; EKKO: MANDT/EBELN; EKPO: MANDT/EBELN/EBELP; EKET: MANDT/EBELN/EBELP/ETENR; LIKP: MANDT/VBELN; LIPS: MANDT/VBELN/POSNR. These keys are not customer-confirmed.

Required fields beyond keys: LFA1.NAME1; MARA.MEINS; EKKO.LIFNR/BEDAT; EKPO.MATNR/MENGE/MEINS/WERKS; EKET.EINDT/MENGE; LIKP.LIFNR/WADAT_IST/WERKS; LIPS.EBELN/EBELP/MATNR/LFIMG/MEINS/WERKS. See `specs` in the Silver notebook. Requiring WADAT_IST makes the demonstration evaluable; it does not verify its business meaning.

Silver outputs include seven valid source tables, `dev_quarantine` and `dev_load_summary`. Reconciliation is Bronze rows = removed duplicate rows + Silver rows + quarantine rows. A record can have several error reasons. All outputs are replaced separately; a failed run can leave a partial snapshot. Gold runs only after Silver succeeds.

## Gold performance rules

| Topic | Rule | Limitation |
|---|---|---|
| Cutoff | `as_of_date`, initially 2026-09-30; include only closed months. | Demo assumption, not a confirmed extract cutoff. Input must cover deliveries through that date. |
| Grain and month | One MANDT/EBELN/EBELP item; final valid EKET.EINDT determines report_month. | Earlier schedule deadlines are not scored separately. |
| Ordered quantity | EKPO.MENGE must equal the sum of EKET.MENGE. | Exclude mismatches; no automatic correction or tolerance. |
| Deliveries | Sum valid LFIMG by the relevant date, using WADAT_IST. | Do not add WEMNG as extra delivery quantity. Delivery-date meaning remains open. |
| Partial/over-delivery | Aggregate before joins; over-delivery counts as complete; fill rate capped at 1. | No allocation to individual schedule lines. |
| On time | Full ordered quantity delivered by the final scheduled date, inclusive. | Zero-day tolerance. A late complete delivery is still late. |
| In full | Full ordered quantity delivered by reporting month end, inclusive. | Later deliveries do not change earlier monthly scores. |
| Exclusions | Invalid/deleted items, missing schedules, quantity mismatch, schedules/deliveries before order date, or attributable EKPO/EKET/LIPS quarantine. | Invalid LIKP records already quarantine dependent LIPS records in Silver. Excluded items have null KPI values. |
| No delivery row | Use quantity zero for an otherwise evaluable item without attributable quarantine. | Valid only under the complete-export assumption. Unassigned quarantine is additional uncertainty. |
| Monthly rates | Proportion of evaluated items on time/in full; also mean capped quantity fill rate. | Equal item weights; no cross-unit quantity sum and no monetary weighting. |
| Score and ranking | Mean of on_time_rate and in_full_rate, ascending. | Ties: lower on-time rate, lower in-full rate, more evaluated items, then supplier ID. The final ID ordering is deterministic, not a performance difference. |
| Minimum sample | `min_items` remains 1 per supplier, month and scope. | Small samples have limited reliability. Fewer than three eligible suppliers yield fewer places. The local preview found no candidates at thresholds 10 or 20; Gold counts remain the decision basis. |
| Reporting scope | ALL is recalculated across branches; BRANCH is recalculated per WERKS. | Never filter a company aggregate and claim it is a branch aggregate. |

Rates range from 0 to 1. `evaluation_coverage_rate` = evaluated_order_item_count / candidate_order_item_count. It is not completeness of the whole export. All distinct Bronze item keys remain in internal item performance, including keys without month attribution. Excluded items with partial schedules can have provisional month attribution. Exclusion reasons can overlap.

## Gold datasets

All names below are prefixed with `dev_` in dev and published in `dev_gold_views`; physical backing tables use the same names in `dev_gold_data`.

| Name suffix | Grain and contents |
|---|---|
| supplier_monthly | Supplier/month/scope/branch rates, score, item counts, coverage and ranking eligibility. |
| worst_suppliers | Up to three eligible suppliers per month/scope/branch. |
| order_item_performance | One candidate order item, quantities, dates, score flags and exclusion reasons. |
| delivery_evidence | Valid delivery items linked to performance, including later deliveries and temporal flags. |
| purchase_order_items | Valid Silver purchase order items with supplier, branch, order date, material, quantity, unit, source amounts/currency and deletion flag. No performance cutoff filter. |
| delivery_items | Valid Silver delivery items with linked purchasing/supplier context. No performance cutoff filter. |
| schedule_lines | Valid Silver schedules linked to their order item, supplier and branch. Kept separate to prevent fan-out. |
| quality_summary | Counts by evaluation/exclusion reason, plus unassigned quarantine counts. Central access only. |
| unassigned_quarantine | Unassignable EKPO/EKET/LIPS quarantine with raw records and reasons. Central access only. |

`as_of_date` records the evaluation cutoff, not a filter on the operational datasets. `processed_at` records write time. Writes are not a cross-table transaction; multiple extracts are not historised. The business-facing views enforce branch access but do not turn the provisional business rules into confirmed facts.

## Decision record discipline

For each change record date, rule, observed evidence, decision, origin (`our interpretation`, `project-owner decision`, `open customer question`, or `customer-confirmed`) and impact. Record a customer confirmation only when an actual answer exists. Review quarantine before revising exclusions. Missing usable delivery evidence alone does not prove non-delivery.


## Landing Zone decision (2026-10-11)

Origin: explicit project-owner instruction. Use the uploaded Workspace folder as Bronze input. This is a confirmed technical path choice, not confirmation of business semantics or export completeness. The input parser reads semicolon-delimited UTF-8 (optional BOM), accepts quoted fields, keeps strings and duplicates, and maps empty fields to null. Malformed rows fail before Bronze writes. The seven expected CSV names are unchanged; other files are ignored. The uploaded files are the active input; repository fixtures are for local exploration.
