# Deployment, access and costs

Updated: 2026-10-11. The revised code uses a dedicated project catalog and English notebooks. README files are intentionally unchanged at the project owner's request; this document describes the new structure.

## Catalog layout

```text
supplier_performance_dev
  dev_landing       dev_source_files (unused legacy Volume; retained for safe deployment)
  dev_bronze        dev_lfa1, dev_mara, dev_ekko, dev_ekpo, dev_eket, dev_likp, dev_lips
  dev_silver        corresponding typed tables, dev_quarantine, dev_load_summary
  dev_gold_internal physical results and operational records; no employee access
  dev_gold          protected English business views
```

Example: `supplier_performance_dev.dev_gold.dev_supplier_monthly`. Prod uses `supplier_performance_prod.prod_gold.prod_supplier_monthly`. Layer schemas avoid mixing source records with reports. Internal Gold is required so users cannot bypass view predicates by reading the underlying tables.

The main Bundle references an existing catalog through `var.catalog`. A separate `databricks-bootstrap` Bundle deploys a serverless SQL notebook job without catalog dependencies. Run it once before the main deployment; see BOOTSTRAP.md. It uses `CREATE CATALOG IF NOT EXISTS` without a storage path. Official documentation supports this for serverless workspaces using Default Storage; runtime success must still be checked in this workspace. The rejected REST catalog creation path is not reused. Schemas and the source Volume retain `prevent_destroy: true`. The same identity should run bootstrap and main deployment; otherwise provide the main deployer with the required catalog privileges.

## Deploy and run

1. Follow BOOTSTRAP.md to deploy and run catalog initialisation before the first main deployment. Push the reviewed repository changes, including both Bundles, `data/source`, scripts and workflows. The repository root is `case_solution`; Actions use `databricks` as their bundle directory.
2. Run **Deploy Databricks** for dev. It first checks that the catalog exists, then validates and deploys schemas, the Volume, jobs and notebooks, The original CSV files are uploaded manually to the Workspace landing folder described below. Deployment does not run the processing job.
3. Run **dev_supplier_performance** in Databricks. Tasks run Bronze, then Silver, then Gold. The daily 06:00 Europe/Berlin schedule stays paused.
4. Run **Run Databricks review** or the review notebook. Choose a month with data and a branch code or ALL. Review PASS/CHECK output as well as job success.

The review Action runs the already deployed notebook version. The selected Git branch only supplies Bundle configuration; deploy code changes first. Deployment and review Actions share a concurrency group. Cancelling a GitHub runner does not prove that the Databricks run has stopped. The review workflow must exist on the default Git branch to be selectable manually.

Dev and prod use the same workspace with separate catalogs and jobs. Source files are synthetic project fixtures, not employee or customer production data. Original source values, including German product names, are not translated.

## Branch access

`branch_groups_json` is a JSON object mapping exact source WERKS codes to **existing account group names**, for example `{"2100":"supplier_branch_2100"}`. This is an example, not a provisioned group or an approved employee assignment. `central_group` optionally names an existing central purchasing group. Both default to empty. No branch-specific groups were present during the remote inspection on 2026-10-11.

The Gold notebook creates dynamic views and grants only USE CATALOG, USE SCHEMA on Gold, and SELECT on the relevant views to configured groups. The publisher identity and optional central group can read all rows. Branch groups can read only matching `branch_code` rows; supplier aggregates also require `report_scope = 'BRANCH'`. Global quality/unassigned records remain central-only. An empty mapping provides no branch access. Changing a group mapping updates view predicates; users from removed groups can no longer see branch rows even if an old USE privilege remains.

Do not grant branch employees SELECT on Bronze, Silver, internal Gold, or broad inherited SELECT/ALL PRIVILEGES at catalog level. Do not make branch users owners of views or grant them MANAGE/CREATE privileges. Inspect inherited grants when adding real groups. Users with administrative or ownership privileges can manage/bypass this application boundary; ordinary employee isolation does not restrict administrators.

Employees must query Gold using their own identities. Do not grant them access to run owner-identity jobs or view their global outputs. The review job is an administrative presentation tool, not an employee access portal. A notebook branch widget is only a display filter. Groups and memberships must be verified with two different branch identities before describing employee isolation as operationally tested. No employee memberships are invented.

The WERKS-to-branch interpretation is explicitly unconfirmed; see DATA_RULES.md. A separate Platinum layer is not necessary for these protected Gold views.

Sources: [Dynamic views](https://docs.databricks.com/aws/en/views/dynamic), [Unity Catalog privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges), [Bundle catalog resources](https://docs.databricks.com/aws/en/dev-tools/bundles/resources#catalogs).

## Migration from the legacy schema

The old location is `workspace.supplier_performance_dev`. Before removal, compare remote source files with the repository's seven checksummed copies. Preserve a remote object inventory and Bundle state backup outside tracked source. Detach the old schema and Volume from Bundle state before removing them; keep existing job bindings so Actions updates the jobs instead of duplicating them. Remove only explicitly inventoried project tables and the source Volume, then remove the empty project schema without cascading into other schemas. Never delete the `workspace` catalog or another project's data.

The new deployment uploads source files from the repository, so the removed Volume is not required to rebuild results. Existing result data is derived from the source snapshot; it will be regenerated after deployment and a successful processing run. Remote cleanup and validation evidence are recorded in MIGRATION.md when completed. A successful Bundle validation alone is not a runtime test.

## Result review

`05_result_queries.sql` contains Python cells for widgets and plots. It queries the English Gold views for KPI cards, worst suppliers, supplier comparison, monthly trend, item evidence and delivery evidence. Technical checks cover visible records and return PASS/CHECK; job success alone does not imply every check passed.

The final section exposes operational purchase order items, deliveries and schedules. Those queries respect branch and optional order filters but do not apply the performance report-month filter, so open/future items remain discoverable. Changing this behaviour requires an explicit business decision. Metrics are read from Gold; the report does not implement a second scoring model.

## Cost attribution

Jobs carry `project=supplier_performance`, `environment=dev/prod`, and `cost_center=supplier_analytics`. These resource tags do not replace a serverless usage policy. A permitted administrator must create a policy with the required billing tags and bind its ID through job `budget_policy_id`. No policy ID is fabricated or bound by this change.

The execution identity is recorded in `system.billing.usage.identity_metadata.run_as`; it may differ from the person starting a job. A shared service principal does not identify individual initiators. Initiator attribution would require audit/job-run data; a user-entered widget is not reliable evidence.

Source: [Serverless usage policies](https://docs.databricks.com/aws/en/admin/usage/budget-policies).

## Cost notebook

`04_cost_management.py` is an interactive notebook, not a published AI/BI dashboard. `LIVE_SQL` can be reused as a dashboard dataset. It stays outside processing/review jobs.

- `data_mode=demo`: synthetic usage and prices, no billing read permission required. Notebook execution can still incur compute cost.
- `data_mode=live`: reads system.billing.usage and system.billing.list_prices; needs USE CATALOG, USE SCHEMA and SELECT privileges.
- `start_date` and `end_date`: inclusive, at most 366 days. `period_budget` applies to that full period and currency.
- `workspace_id`: limits the workspace. `project` OR `job_id` selects usage; leave both empty to include the entire selected workspace. Clear an obsolete job ID after any job replacement.
- `compute_reduction_pct`: hypothetical reduction of compute costs, not a resource shutdown or guaranteed saving.

The estimate uses time-matched list prices and keeps negative corrections. Missing prices are counted; quantities with different units are not combined. Contract discounts and separate cloud invoices are excluded. Warehouse ownership and run-as identity are distinct. No billing rows does not prove no cost. Billing and identity information require access controls separate from supplier reports.


## Active Workspace Landing Zone (2026-10-11)

The project owner uploaded the seven original CSV files to `/Workspace/Users/guochengcheng93@gmail.com/landing_zone_case_solution` and selected this folder as the active Landing Zone. The Bundle `source_path` variable and the processing job parameter now use that absolute path. Dev and prod currently share this read-only demonstration input; override source_path per target if separate inputs are needed.

Bronze uses Python's CSV reader in the notebook process, then creates a Spark DataFrame with explicit STRING columns. This avoids relying on distributed Spark workers to access Workspace files. It preserves leading zeros, duplicate rows and whitespace; empty fields become null as before. All seven files are parsed before any Bronze table is replaced. README.txt is ignored. The input is a small snapshot held in memory, not a scalable large-file ingestion design. The job's execution identity must have read access to the folder.

The deploy Action no longer checks or uploads repository CSV files. Therefore the Windows/Git line-ending checksum mismatch does not block deployment. The optional local checksum script and archived fixtures remain; their existing byte-level mismatch across checkouts is not silently redefined or claimed fixed. The previously deployed landing schema/Volume are retained and unused to avoid an unrelated destructive deployment; no new file copy is made there.

After committing and pushing, rerun Deploy Databricks on the updated Git branch. Then run dev_supplier_performance. Existing manual source_path overrides must be cleared or changed to the Workspace path.
