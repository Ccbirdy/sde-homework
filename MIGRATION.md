# Migration record

Date: 2026-10-11. Requested change: move supplier performance from the shared workspace catalog into a dedicated project catalog with separate layer schemas.

## Completed remote cleanup

- Inventoried exactly 22 managed tables in `workspace.supplier_performance_dev`, plus its `source_files` Volume.
- Downloaded all seven remote CSV files and verified each SHA-256 checksum against the tracked `data/source` copies. All matched.
- Backed up the object inventory and remote Bundle state to local ignored `.databricks/migration/remote_backup`.
- Unbound only `resources.volumes.source_files` and `resources.schemas.project_schema` from the existing dev Bundle state. Existing job bindings remain.
- Checked that the existing project jobs had no active runs and that table IDs matched the inventory before deletion.
- Deleted the 22 inventoried project tables and the verified legacy Volume. Removed the empty legacy schema without force/cascade.
- Preserved the `workspace` catalog, other schemas/projects, and both existing project jobs.

Removed table groups: seven bronze source tables; seven silver source tables; silver_quarantine and silver_load_summary; gold_order_item_performance, gold_delivery_evidence, gold_supplier_monthly, gold_worst_suppliers, gold_quality_summary and gold_unassigned_quarantine.

## Failed Action and resolution

The first Action attempt validated and uploaded files but stopped because deleting the old schema/Volume required confirmation in a non-interactive console. No `--auto-approve` was added. After backup, unbinding and explicit cleanup, `bundle plan -t dev` returned:

```text
create catalogs.project_catalog
update jobs.supplier_performance
update jobs.supplier_performance_review
create schemas.bronze_schema
create schemas.gold_internal_schema
create schemas.gold_schema
create schemas.landing_schema
create schemas.silver_schema
create volumes.landing_files

Plan: 7 to add, 2 to change, 0 to delete
```

The old destructive deployment prompt is resolved. The project owner will rerun Deploy Databricks. The workflow deploys resources and uploads verified source CSVs; a subsequent processing run is required to populate new tables/views.

## Validation scope

Local Python notebooks and Python cells in the SQL notebook were syntax-checked. The local data-quality notebook executed successfully against the source data, including sample-size checks. Source checksums passed. Both dev and prod Bundles validated successfully. The dev plan contains no deletion. Gold path routing and SQL literal escaping checks passed. All non-README project Markdown prose was checked for the English-language conversion.

The revised Bronze/Silver/Gold pipeline has not yet been executed end-to-end on Databricks. Successful configuration validation does not prove runtime success. Employee branch isolation still requires real account-group mappings and tests with employee identities; none were invented.


## Default Storage catalog creation fix

The next Action attempt failed at catalog creation with HTTP 400: "Please use the UI to create a catalog with Default Storage." No catalog was created by that request. Configuration validation had not tested this workspace-specific creation restriction.

Removed the Bundle catalog resource and the copied storage_root setting. All resources now reference the existing catalog through var.catalog. The workflow checks that the selected catalog exists before deployment. The project owner chose to create supplier_performance_dev manually in the Databricks UI with Default Storage; catalog creation is pending that step. Prod needs the corresponding supplier_performance_prod catalog before its first deployment.

The revised dev configuration validated successfully. The plan is now 6 to add, 2 to change, 0 to delete: five schemas and one Volume, plus updates to the two existing jobs. No catalog creation or deletion is planned. Commit and push this revised configuration before rerunning the Action.


## Reproducible SQL bootstrap (current approach)

The project owner subsequently requested a reproducible SQL initialisation instead of manual UI creation. BOOTSTRAP.md supersedes the manual-UI next step above. A separate bootstrap Bundle deploys only a serverless SQL notebook job. It has no catalog/schema/Volume resources, avoiding a deployment dependency cycle. The user runs that job before the main Bundle deployment. No SQL has been executed as part of preparing these files.
