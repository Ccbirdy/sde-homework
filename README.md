# Supplier Delivery Performance on Databricks

[中文](README.zh-CN.md)

## Project overview

This project uses SAP export data to assess supplier delivery performance for a fictional building materials distributor with four branches. It aims to replace manual tracking with a repeatable monthly analysis.

The key business question is: **Which three suppliers perform worst, and why?** Results will show delivery timeliness, quantity fulfilment, sample sizes, and supporting order details.

**Current status:** Project structure and initial local data checks are complete. The Databricks pipeline, metrics, and deployment are not implemented yet. No supplier ranking or Databricks run has been validated.

## Data

The input contains seven synthetic CSV exports from SAP ECC MM. Files use UTF-8 encoding and semicolon separators. Row counts below include duplicate records and exclude headers.

| Export | Content | Rows |
|---|---|---:|
| LFA1 | Supplier master | 15 |
| MARA/MAKT | Material master and descriptions | 24 |
| EKKO | Purchase order headers | 216 |
| EKPO | Purchase order items | 468 |
| EKET | Purchase order schedule lines | 820 |
| LIKP | Delivery headers | 957 |
| LIPS | Delivery items | 963 |

Source files remain unchanged at `../DIC_Arbeitsprobe_Testdaten_SAP_MM/` in the local setup. Initial checks found duplicate candidate keys in EKKO, EKPO, EKET, and LIPS. Their causes and treatment are still under review.

## Implementation checklist

Checked items are completed at the stated scope. Unchecked items are planned work. Configuration and successful execution are tracked separately.

### Data preparation and quality

- [x] Create the project structure.
- [x] Read all seven CSV files locally and inspect headers and samples.
- [x] Count source rows and check duplicate candidate keys locally.
- [ ] Validate relationships, missing values, dates, quantities, and units.
- [ ] Define and implement rules for duplicates and invalid records.

### Databricks Medallion architecture

- [ ] Bronze: ingest source records and preserve source information.
- [ ] Silver: apply types, quality rules, and validated relationships.
- [ ] Gold: build monthly supplier and branch metrics with traceable details.
- [ ] Verify that reruns do not duplicate results.
- [ ] Run and validate the pipeline in Databricks.

### Metrics and business results

- [ ] Define timeliness, quantity fulfilment, reporting periods, and partial-delivery rules.
- [ ] Define ranking criteria and show sample sizes.
- [ ] Identify the three weakest suppliers and explain each result with order details.
- [ ] Check calculations against selected source examples and document limitations.

### Databricks Asset Bundle (DAB) and scheduling

- [ ] Package code, Jobs, and configuration in an Asset Bundle.
- [ ] Configure separate `dev` and `prod` targets.
- [ ] Configure task dependencies and a monthly schedule with reporting parameters.
- [ ] Validate the Bundle configuration.
- [ ] Deploy and run in an available Databricks environment; record the target and evidence.

### Branch access

- [ ] Preserve branch identifiers in the analytical model.
- [ ] Document user-to-branch access rules and the enforcement approach.
- [ ] Clearly distinguish the access design from any tested access controls.

### Documentation and presentation

- [x] Add English and Chinese project READMEs.
- [ ] Document final assumptions, decisions, results, and next steps.
- [ ] Add instructions for the implemented run and deployment process.
- [ ] Prepare a concise walkthrough of the solution and evidence.
- [ ] Optional: build a Power BI report after the required engineering work.

## Project structure

```text
databricks/
  src/          Ingestion, transformation, and metrics
  resources/    Job configuration
tests/          Data logic and result checks
outputs/        Analysis results and validation evidence
```

These directories are placeholders for implementation. Run and deployment instructions will be added when executable code and configuration are available.
