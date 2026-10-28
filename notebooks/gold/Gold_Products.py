# Databricks notebook source
# MAGIC %md
# MAGIC # **DLT Pipeline**

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming Table**

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

# 1) Stage: stream من الـ silver + data quality
@dp.table(name="DimProducts_stage")
@dp.expect_or_fail("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_product_name", "product_name IS NOT NULL")
@dp.expect_or_drop("valid_price", "price > 0")
def DimProducts_stage():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")   # يتجاهل الـ Overwrite في الـ silver
        .table("databricks_catalog.silver.products_silver")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming View**

# COMMAND ----------

# 2. Streaming view: stable source for the CDC flow (not stored or published)
@dp.temporary_view
def DimProducts_view():
    return spark.readStream.table("DimProducts_stage")

# COMMAND ----------

# MAGIC %md
# MAGIC **DimProducts**

# COMMAND ----------

# 2) Target: streaming table فاضي
dp.create_streaming_table("DimProducts")

# COMMAND ----------

# 4. Auto CDC flow: apply changes from the view into the target as SCD2
dp.create_auto_cdc_flow(
    target="DimProducts",
    source="DimProducts_view",
    keys=["product_id"],
    sequence_by="update_ts",   # must be a timestamp/version column, NOT product_id
    stored_as_scd_type=2,
)