# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@databrickslakestorageete.dfs.core.windows.net/customers/")

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Split the domains from the email**

# COMMAND ----------

df = df.withColumn("domain", F.split(F.col("email"), "@").getItem(1))

# COMMAND ----------

(df.groupBy("domain")
   .agg(F.count("*").alias("total_customers"))
   .orderBy(F.col("total_customers").desc())
   .display())

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Split the Customers based on the domain of the email (gmail.com, hotmail.com or yahoo.com)**

# COMMAND ----------

df_gmail =  df.filter(F.col("domain") == "gmail.com")

df_hotmail =  df.filter(F.col("domain") == "hotmail.com")

df_yahoo =  df.filter(F.col("domain") == "yahoo.com")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Make the full name from concat the first_name and the last_name**

# COMMAND ----------

df = df.withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
df = df.drop("first_name", "last_name")

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databrickslakestorageete.dfs.core.windows.net/customers/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.customers_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databrickslakestorageete.dfs.core.windows.net/customers/'

# COMMAND ----------

