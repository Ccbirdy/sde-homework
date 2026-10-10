-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Initialise the supplier performance prod catalog
-- MAGIC Run this notebook through the bootstrap job on serverless compute.
-- MAGIC The workspace must support Default Storage and the execution identity needs CREATE CATALOG.
-- MAGIC No managed storage path is supplied. The catalog uses the workspace's Default Storage.
-- MAGIC IF NOT EXISTS allows repeated runs without replacing an existing catalog or its data.
-- MAGIC An existing catalog keeps its original settings; this does not migrate its storage.
-- MAGIC The main Bundle creates schemas, the source Volume and processing jobs afterwards.
-- MAGIC Reference: [Default Storage](https://docs.databricks.com/aws/en/storage/default-storage).

-- COMMAND ----------
CREATE CATALOG IF NOT EXISTS supplier_performance_prod
COMMENT 'Supplier performance prod catalog';

-- COMMAND ----------
-- Verify that the catalog exists and display its metadata.
DESCRIBE CATALOG EXTENDED supplier_performance_prod;
