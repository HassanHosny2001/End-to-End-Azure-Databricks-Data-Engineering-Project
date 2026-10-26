# Databricks notebook source
# MAGIC %md
# MAGIC # **FACT ORDERS**

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Reading**

# COMMAND ----------

df = spark.sql("select * from databricks_catalog.silver.orders_silver")

# COMMAND ----------

df.display()

# COMMAND ----------

df_dimcust = spark.sql("select DimCustomerKey, customer_id as dim_customer_id from databricks_catalog.gold.dimcustomers")
df_dimprod = spark.sql("select product_id as DimProductKey, product_id as dim_product_id from databricks_catalog.gold.dimproducts")

# COMMAND ----------

df.limit(2).display()

# COMMAND ----------

df_dimprod.limit(2).display()

# COMMAND ----------

df_fact = df.join(df_dimcust, df['customer_id'] == df_dimcust['dim_customer_id'], how='left')\
            .join(df_dimprod, df['product_id'] == df_dimprod['dim_product_id'], how='left')

df_fact_new = df_fact.drop("dim_customer_id", "customer_id", "dim_product_id", "product_id")

# COMMAND ----------

df_fact_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Upsert on Fact Table**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_catalog.gold.FactOrders"):
    print("yes table is there!!")
else:
    print("table not there !!")

# COMMAND ----------

if spark.catalog.tableExists("databricks_catalog.gold.FactOrders"):
    dlt_obj = DeltaTable.forName(spark, "databricks_catalog.gold.FactOrders")

    dlt_obj.alias('trg')\
        .merge(df_fact_new.alias('src'), "trg.order_id = src.order_id AND trg.DimCustomerKey = src.DimCustomerKey AND trg.DimProductKey = src.DimProductKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_fact_new.write.mode("overwrite")\
        .format("delta")\
        .option("path", "abfss://gold@databrickslakestorageete.dfs.core.windows.net/FactOrders/")\
        .saveAsTable("databricks_catalog.gold.FactOrders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_catalog.gold.factorders

# COMMAND ----------

 