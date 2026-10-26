# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@databrickslakestorageete.dfs.core.windows.net/products/")

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Creating Functions to be retain in the Catalog to be resued anywhere**

# COMMAND ----------

df.createOrReplaceTempView("df_products_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select category, round(sum(price), 2) as total_price
# MAGIC from df_products_temp
# MAGIC group by category
# MAGIC order by total_price desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_catalog.bronze.discount_func(p_price DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN round((p_price * 0.9), 2)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_id, price, databricks_catalog.bronze.discount_func(price) as discounted_price
# MAGIC FROM df_products_temp

# COMMAND ----------

## using function we created in python

## using expr
df = df.withColumn("discounted_price", expr("databricks_catalog.bronze.discount_func(price)"))

# COMMAND ----------

## using call_function

df.withColumn(
    "discounted_price",
    F.call_function("databricks_catalog.bronze.discount_func", F.col("price"))
).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_catalog.bronze.upper_func(col_to_convert STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC AS
# MAGIC $$
# MAGIC     return col_to_convert.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC select brand, databricks_catalog.bronze.upper_func(brand) as brand_upper
# MAGIC from df_products_temp

# COMMAND ----------

df = df.withColumn("update_ts", F.current_timestamp())

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta")\
        .mode("overwrite")\
        .option("path", "abfss://silver@databrickslakestorageete.dfs.core.windows.net/products/")\
        .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.products_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databrickslakestorageete.dfs.core.windows.net/products/'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_catalog.silver.products_silver

# COMMAND ----------

