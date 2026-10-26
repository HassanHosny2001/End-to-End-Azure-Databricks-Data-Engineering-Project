# Databricks notebook source
# MAGIC %md
# MAGIC ### **Dynamic Capabilities**

# COMMAND ----------

# DBTITLE 1,Cell 2
dbutils.widgets.text("file_name", "")

# COMMAND ----------

file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df_order_bronze = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", f"abfss://bronze@databrickslakestorageete.dfs.core.windows.net/checkpoint_{file_name}/")\
    .load(f"abfss://source@databrickslakestorageete.dfs.core.windows.net/{file_name}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df_order_bronze.writeStream.format("parquet")\
    .outputMode("append")\
    .option("checkpointLocation", f"abfss://bronze@databrickslakestorageete.dfs.core.windows.net/checkpoint_{file_name}/")\
    .option("path", f"abfss://bronze@databrickslakestorageete.dfs.core.windows.net/{file_name}/")\
    .trigger(once=True)\
    .start()

# COMMAND ----------

