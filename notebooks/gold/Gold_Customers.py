# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading From Source**

# COMMAND ----------

df = spark.sql("SELECT * FROM databricks_catalog.silver.customers_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC **Removing duplicates**

# COMMAND ----------

df = df.dropDuplicates(subset=["customer_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Create DimCustomers (first run only)**
# MAGIC
# MAGIC `DimCustomerKey` is a Delta **identity column**: Delta generates it on insert,
# MAGIC so there is no `max(DimCustomerKey)` lookup, no offset and no `init_load_flag`.
# MAGIC
# MAGIC The business columns are taken from the Silver table, so the table definition
# MAGIC always matches the source schema.

# COMMAND ----------

TARGET = "databricks_catalog.gold.dimcustomers"
LOCATION = "abfss://gold@databrickslakestorageete.dfs.core.windows.net/DimCustomers/"

business_cols = df.columns  # customer_id plus every attribute coming from Silver

if not spark.catalog.tableExists(TARGET):
    cols_ddl = ",\n  ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
    spark.sql(f"""
        CREATE TABLE {TARGET} (
          DimCustomerKey BIGINT GENERATED ALWAYS AS IDENTITY,
          {cols_ddl},
          create_date TIMESTAMP,
          update_date TIMESTAMP
        )
        USING DELTA
        LOCATION '{LOCATION}'
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SCD Type - 1 (MERGE on the business key)**
# MAGIC
# MAGIC - Match on `customer_id` (not on the surrogate key).
# MAGIC - Matched: overwrite the attributes and refresh `update_date`; keep `DimCustomerKey` and `create_date`.
# MAGIC - Not matched: insert with explicit columns and leave `DimCustomerKey` out so Delta generates it.

# COMMAND ----------

attr_cols = [c for c in business_cols if c != "customer_id"]

update_set = {c: f"src.`{c}`" for c in attr_cols}
update_set["update_date"] = "current_timestamp()"

insert_values = {c: f"src.`{c}`" for c in business_cols}
insert_values["create_date"] = "current_timestamp()"
insert_values["update_date"] = "current_timestamp()"

(DeltaTable.forName(spark, TARGET).alias("trg")
    .merge(df.alias("src"), "trg.customer_id = src.customer_id")
    .whenMatchedUpdate(set=update_set)
    .whenNotMatchedInsert(values=insert_values)
    .execute())

# COMMAND ----------

spark.sql(f"SELECT * FROM {TARGET} ORDER BY DimCustomerKey").display()
