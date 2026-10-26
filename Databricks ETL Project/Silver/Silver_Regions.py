# Databricks notebook source
# MAGIC %md
# MAGIC ### **Read delta table (regions)**

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL

# COMMAND ----------

df = spark.sql("SELECT * FROM databricks_catalog.bronze.regions")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark

# COMMAND ----------

df = spark.read.table("databricks_catalog.bronze.regions")
df.display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

df.write.format("delta")\
        .mode("overwrite")\
        .save("abfss://silver@databrickslakestorageete.dfs.core.windows.net/regions/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.regions_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databrickslakestorageete.dfs.core.windows.net/regions/'

# COMMAND ----------

