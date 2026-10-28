# Setup guide

This guide recreates the environment used by the project. Resource names match the notebooks; if you choose different names, update the `abfss://` paths and the catalog name in the notebooks.

## Prerequisites

- An Azure subscription with rights to create resources and assign roles
- Permission to create a Databricks workspace and administer a Unity Catalog metastore
- This repository cloned locally

## 1. Provision Azure infrastructure

1. Create the resource group `Databricks_RG_ETE`.
2. Create the storage account `databrickslakestorageete` with **Hierarchical namespace** enabled (ADLS Gen2).
3. Create five containers: `source`, `bronze`, `silver`, `gold`, `metastore`.
4. Create an **Access Connector for Azure Databricks** named `databricks_ete_connector`.
5. On the storage account, assign the connector's managed identity the **Storage Blob Data Contributor** role.
6. Deploy the Azure Databricks workspace `databricks_project` (Premium tier, required for Unity Catalog).

## 2. Configure Unity Catalog

1. Create external locations for the containers using the access connector, for example:

   ```text
   abfss://metastore@databrickslakestorageete.dfs.core.windows.net/
   ```

2. Create the catalog `databricks_catalog` and the schemas `bronze`, `silver` and `gold`.

## 3. Load sample data

Upload the files from `data/sample` into the `source` container, one folder per dataset:

```text
source/
├── orders/      orders_first.parquet
├── customers/   customer_first.parquet
├── products/    products_first.parquet
└── regions/     regions.parquet
```

Keep the `*_second.parquet` files aside; you will add them later to demonstrate incremental loads.

`Silver_Regions` reads the table `databricks_catalog.bronze.regions`, so register the regions file as that table (for example, with a `CREATE TABLE ... USING PARQUET LOCATION ...` or the Catalog Explorer upload).

## 4. Import the notebooks

Either import the folders under `notebooks/` into your workspace, for example under
`/Workspace/Users/<your-email>/Databricks ETL Project/`, or import `exports/databricks-etl-project.dbc`.

## 5. Create the DLT pipeline

1. Create a pipeline named `Gold_Products` on serverless compute.
2. Set the source code to the `Gold_Products` notebook.
3. Set the default catalog to `databricks_catalog` and the target schema to `gold`.

## 6. Create the workflow

Create a job named `End-To-End-Pipeline` (serverless) with these tasks:

| Task | Type | Depends on |
| --- | --- | --- |
| `parameters` | Notebook `parameters` | none |
| `Bronze_Autoloader` | *For each* over `parameters` output `output_datasets`, running notebook `Bronze_Layer` with input `file_name` | `parameters` |
| `Silver_Customers`, `Silver_Orders`, `Silver_Products`, `Silver_Regions` | Notebooks | `Bronze_Autoloader` |
| `Gold_DimCustomers` | Notebook `Gold_Customers` (parameter `init_load_flag`) | the three Silver data tasks |
| `Gold_DimProducts` | Pipeline `Gold_Products` | the three Silver data tasks |
| `Gold_Fact_Orders` | Notebook `Gold_Orders` | both Gold dimension tasks |

Set `init_load_flag` to `1` for the very first run and `0` afterwards.

## 7. Run and verify

1. Run the job.
2. In a SQL warehouse, check the results:

   ```sql
   SELECT * FROM databricks_catalog.gold.factorders LIMIT 10;
   SELECT * FROM databricks_catalog.gold.dimproducts ORDER BY product_id;
   ```

3. Upload the `*_second.parquet` files to the matching `source` folders, set `init_load_flag` to `0`, and run again to see incremental ingestion, SCD Type 1 updates and SCD Type 2 history.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `403` or authorization errors on `abfss://` paths | Role assignment missing or not yet propagated for the access connector |
| Table not found in Silver or Gold | Schema or catalog name differs from `databricks_catalog.<schema>` |
| `Gold_Customers` fails on the first run | `init_load_flag` was left at `0` before the dimension table existed |
| DLT pipeline fails on expectations | A product row has a null `product_id`, which is configured to fail the update |
