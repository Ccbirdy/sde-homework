# Reproducible catalog initialisation

## Why there are two Bundles

The main Bundle creates schemas and a Volume, which require a catalog to exist during deployment. Adding catalog SQL as the first task of its processing job would be too late: resource deployment happens before job execution.

The separate `databricks-bootstrap` Bundle creates only a serverless notebook job. Its deployment does not depend on any project catalog. Run the job to initialise the catalog, then deploy the main Bundle. Both Bundles have separate names and state paths.

## GitHub Actions and manual execution

1. Commit and push the repository changes. The new `.github/workflows/deploy-bootstrap.yml` must also be on the repository's default branch for GitHub to expose the manual Run workflow button.
2. In Actions, select **Deploy Databricks bootstrap**, choose the branch containing these changes, select `dev`, and run it. This uploads the SQL notebook and creates a job; it does not execute SQL.
3. In Databricks **Jobs & Pipelines**, open **dev_supplier_performance_initialize_catalog** and click **Run now**. Wait for success. The second notebook cell displays catalog metadata. Confirm that `supplier_performance_dev` appears in Catalog Explorer.
4. Run the existing **Deploy Databricks** Action for `dev`. It checks catalog availability, deploys layer schemas, the Volume and main jobs, then uploads the checksummed source CSV files.
5. Run **dev_supplier_performance** to populate Bronze, Silver and Gold. Deployment alone does not process data.

For prod, select prod in both Actions and run **prod_supplier_performance_initialize_catalog**. It creates only `supplier_performance_prod`. The dev and prod SQL notebooks use explicit catalog names to avoid accidentally initialising the other environment.

## CLI alternative

Run from the repository root (`case_solution`):

```powershell
Set-Location databricks-bootstrap
databricks bundle validate -t dev -p supplier-performance
databricks bundle deploy -t dev -p supplier-performance
# Execute only when ready; this runs SQL rather than merely deploying files.
databricks bundle run initialize_catalog -t dev -p supplier-performance
Set-Location ../databricks
databricks bundle validate -t dev -p supplier-performance
databricks bundle deploy -t dev -p supplier-performance
```

The CLI main deployment does not perform the GitHub workflow's source upload step. Use the main Action for the complete deploy-and-upload flow, or upload all seven files from `data/source` to `/Volumes/supplier_performance_dev/dev_landing/dev_source_files` before running Bronze.

## Requirements and repeatability

- A serverless workspace supporting Default Storage and a run identity with CREATE CATALOG. Use the same account as the main deployer, or explicitly arrange the required catalog access.
- Existing GitHub DATABRICKS_HOST and DATABRICKS_TOKEN configuration is reused; no new secret is required.
- No storage_root, MANAGED LOCATION, DROP or CREATE OR REPLACE is used. Repeated SQL execution keeps an existing catalog and its data. It does not change an existing catalog's storage or permissions.
- The bootstrap job has no schedule. Running it consumes serverless compute; deploying it does not run it.
- A green validation/deployment result is not evidence that SQL execution succeeded. Check the job result before the main deployment.

Source: [Databricks Default Storage documentation](https://docs.databricks.com/aws/en/storage/default-storage).
