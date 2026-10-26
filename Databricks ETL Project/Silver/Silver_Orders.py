# Databricks notebook source
from pyspark.sql.functions import col, year, month, dayofmonth, dayofweek, weekofyear
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df_orders = spark.read.format("parquet")\
            .load("abfss://bronze@databrickslakestorageete.dfs.core.windows.net/orders/")

# COMMAND ----------

df_orders.display()

# COMMAND ----------

df_orders.printSchema()

# COMMAND ----------

df_orders.select(
    year(df_orders["order_date"]).alias("year"),
    month(df_orders["order_date"]).alias("month"),
    dayofmonth(df_orders["order_date"]).alias("day"),
    dayofweek(df_orders["order_date"]).alias("weekday"),
    weekofyear(df_orders["order_date"]).alias("week_number")
).display()

# COMMAND ----------

df_orders = df_orders.withColumn("year", year(col('order_date')))

# COMMAND ----------

df_orders = df_orders.drop('_rescued_data')

# COMMAND ----------

df_ranked = df_orders.withColumn("ranked_amount", F.dense_rank().over(Window.partitionBy("year").orderBy(F.col("total_amount").desc())))

# COMMAND ----------

# MAGIC %md
# MAGIC #### In SQL, RANK() and DENSE_RANK() are window functions used to assign sequential integers to rows based on a specified order, but they handle ties (equal values) differently.  RANK() skips subsequent numbers after a tie (e.g., 1, 2, 2, 4), whereas DENSE_RANK() produces consecutive ranks with no gaps (e.g., 1, 2, 2, 3). 

# COMMAND ----------

df_ranked.display()

# COMMAND ----------

df_row_number = df_ranked.withColumn("row_number", F.row_number().over(Window.partitionBy("year").orderBy(F.col("total_amount").desc())))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Classes - OOP**

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


class WindowFunctions:

    def __init__(self, df):
        self.df = df

    # ---------- Ranking ----------
    def row_number(self, partition_col, order_col, name="row_number"):
        w = Window.partitionBy(partition_col).orderBy(F.col(order_col).desc())
        return self.df.withColumn(name, F.row_number().over(w))

    def rank(self, partition_col, order_col, name="rank"):
        w = Window.partitionBy(partition_col).orderBy(F.col(order_col).desc())
        return self.df.withColumn(name, F.rank().over(w))

    def dense_rank(self, partition_col, order_col, name="dense_rank"):
        w = Window.partitionBy(partition_col).orderBy(F.col(order_col).desc())
        return self.df.withColumn(name, F.dense_rank().over(w))

    def top_n(self, partition_col, order_col, n):
        w = Window.partitionBy(partition_col).orderBy(F.col(order_col).desc())
        return (self.df.withColumn("_rn", F.dense_rank().over(w))
                       .filter(F.col("_rn") <= n)
                       .drop("_rn"))

    # ---------- lag / lead ----------
    def lag(self, partition_col, order_col, value_col, offset=1, name="lag"):
        w = Window.partitionBy(partition_col).orderBy(F.col(order_col).asc())
        return self.df.withColumn(name, F.lag(value_col, offset).over(w))

    def lead(self, partition_col, order_col, value_col, offset=1, name="lead"):
        w = Window.partitionBy(partition_col).orderBy(F.col(order_col).asc())
        return self.df.withColumn(name, F.lead(value_col, offset).over(w))

    def diff_from_previous(self, partition_col, order_col, value_col, name="diff"):
        w = Window.partitionBy(partition_col).orderBy(F.col(order_col).asc())
        return self.df.withColumn(name, F.col(value_col) - F.lag(value_col, 1).over(w))

    # ---------- Running ----------
    def running_total(self, partition_col, order_col, value_col, name="running_total"):
        w = (Window.partitionBy(partition_col)
                   .orderBy(F.col(order_col).asc())
                   .rowsBetween(Window.unboundedPreceding, Window.currentRow))
        return self.df.withColumn(name, F.sum(value_col).over(w))

    def moving_avg(self, partition_col, order_col, value_col, window_size=3, name="moving_avg"):
        w = (Window.partitionBy(partition_col)
                   .orderBy(F.col(order_col).asc())
                   .rowsBetween(-(window_size - 1), Window.currentRow))
        return self.df.withColumn(name, F.avg(value_col).over(w))

# COMMAND ----------

wf = WindowFunctions(df_orders)

wf.dense_rank("year", "total_amount").display()
#wf.top_n("year", "total_amount", 3).display()
#wf.running_total("year", "order_date", "total_amount").display()
#wf.pct_of_total("year", "total_amount").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df_orders.display()

# COMMAND ----------

df_orders.write.format("delta").mode("overwrite").save("abfss://silver@databrickslakestorageete.dfs.core.windows.net/orders/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.orders_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databrickslakestorageete.dfs.core.windows.net/orders/'

# COMMAND ----------

