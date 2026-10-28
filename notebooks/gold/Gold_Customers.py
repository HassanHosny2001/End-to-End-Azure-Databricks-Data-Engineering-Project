# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


# COMMAND ----------

dbutils.widgets.text("init_load_flag", "0")
init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading From Source**

# COMMAND ----------

df = spark.sql("SELECT * FROM databricks_catalog.silver.customers_silver")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Checking for duplicates**

# COMMAND ----------

df_dups = spark.sql("""
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY customer_id
                   ORDER BY full_name
               ) AS rn
        FROM databricks_catalog.silver.customers_silver
    )
    WHERE rn > 1
""")

# df_dups.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Removing duplicates**

# COMMAND ----------

df = df.dropDuplicates(subset=['customer_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC # **Dividing New vs Old Records**

# COMMAND ----------

if init_load_flag == 0:
    # Incremental load: the dimension already has data, so read it
    df_old = spark.sql('''select DimCustomerKey, customer_id, create_date, update_date
                       from databricks_catalog.gold.dimcustomers''')
else:
    df_old = spark.sql("""
        select cast(0 as int)          as DimCustomerKey,
               customer_id,
               current_timestamp()     as create_date,
               current_timestamp()     as update_date
        from databricks_catalog.silver.customers_silver
        where 1=0
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC - Incremental run (init_load_flag == 0): df_old holds the existing customers, so the next steps can compare the new data with the old and decide who is new and who changed.
# MAGIC - First run (init_load_flag is not 0): there are no old records. df_old is empty, but it still has the right columns.

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Renaming Columns of df_old**

# COMMAND ----------

df_old = (df_old
    .withColumnRenamed("DimCustomerKey", "old_DimCustomerKey")
    .withColumnRenamed("customer_id", "old_customer_id")
    .withColumnRenamed("create_date", "old_create_date")
    .withColumnRenamed("update_date", "old_update_date")
)

# COMMAND ----------

df_old.columns

# COMMAND ----------

# MAGIC %md
# MAGIC **Applying Join with the Old Records**

# COMMAND ----------

df_join = df.join(df_old, df['customer_id'] == df_old['old_customer_id'], 'left')

# COMMAND ----------

df_join.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC - df is the incoming data (today’s customers from the source).
# MAGIC - df_old is the existing dimension table (customers already in DimCustomers, with their surrogate keys and dates).
# MAGIC - A left join keeps every row from df and attaches the matching old record, if there is one.

# COMMAND ----------

# MAGIC %md
# MAGIC **Seperating New vs Old Records**

# COMMAND ----------

df_new = df_join.filter(col("old_DimCustomerKey").isNull()).select(*df.columns)

df_old = df_join.filter(col("old_DimCustomerKey").isNotNull())

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing df_old**

# COMMAND ----------

df_old.display()

# COMMAND ----------

df_old = (df_old
    .withColumn("DimCustomerKey", col("old_DimCustomerKey"))
    .withColumn("create_date", col("old_create_date"))
    .withColumn("update_date", current_timestamp())
    .drop("old_DimCustomerKey", "old_customer_id", "old_create_date", "old_update_date")
)

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing df_new**

# COMMAND ----------

# max_sk = df_old.agg(max('DimCustomerKey')).collect()[0][0] or 0

df_new = ( df_new
          .withColumn('create_date', current_timestamp())
          .withColumn('update_date', current_timestamp())
          )

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Add a surrogate key - From 1**

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", monotonically_increasing_id() + lit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC **Adding Max Surrogate Key**

# COMMAND ----------

if init_load_flag == 1:
    max_sk = 0
else:
    df_max_sk = spark.sql("select max(DimCustomerKey) as max_sk from databricks_catalog.gold.DimCustomers")

    #Converting df_max_sk to max_sk variable
    max_sk = df_max_sk.collect()[0]['max_sk']

# COMMAND ----------

print(df_new.columns)
print(df_old.columns)

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", col("DimCustomerKey") + lit(max_sk))

# COMMAND ----------

# MAGIC %md
# MAGIC **Union of df_old and df_new**

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **SCD Type - 1**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_catalog.gold.dimcustomers"):
    dlt_obj = DeltaTable.forPath(spark, "abfss://gold@databrickslakestorageete.dfs.core.windows.net/DimCustomers/")

    dlt_obj.alias("trg").merge(df_final.alias("src"), "trg.DimCustomerKey = src.DimCustomerKey")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
else:
    df_final.write.mode("overwrite")\
        .format("delta")\
        .option("path", "abfss://gold@databrickslakestorageete.dfs.core.windows.net/DimCustomers/")\
        .saveAsTable("databricks_catalog.gold.dimcustomers")

# COMMAND ----------

