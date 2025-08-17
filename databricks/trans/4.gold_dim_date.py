# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crete Flag Parameter

# COMMAND ----------

# incremental flag with default value 0
dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')

# COMMAND ----------

print(incremental_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch Relative Columns

# COMMAND ----------

df_src = spark.sql(
    """
    SELECT *
    FROM PARQUET.`abfss://silver@adlscarsalesp.dfs.core.windows.net/carsales`
    """
)

# COMMAND ----------

df_src.display()

# COMMAND ----------

df_src = spark.sql(
    """
    SELECT Date_ID
    FROM PARQUET.`abfss://silver@adlscarsalesp.dfs.core.windows.net/carsales`
    """
)

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_branch sink - Initial  Incremental
# MAGIC #### (Just bring the schema if table does not exist)

# COMMAND ----------

if not spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    df_sink = spark.sql(
        """
        SELECT 1 as dim_date_key, Date_ID
        FROM PARQUET.`abfss://silver@adlscarsalesp.dfs.core.windows.net/carsales`
        WHERE 1=0
        """
    )
else:
    df_sink = spark.sql(
        """
        SELECT dim_date_key, Date_ID
        FROM cars_catalog.gold.dim_date
        """
    )

# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter new records & old records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.Date_ID == df_src.Date_ID, 'left').select(df_src.Date_ID, df_sink.dim_date_key)

# COMMAND ----------

df_filter.display()

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_date_key.isNotNull())

# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter.dim_date_key.isNull()).select('Date_ID')

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ** Fetch the max Surrogate Key from existing table

# COMMAND ----------

# we will use the incremental flag
if(incremental_flag == '0'):
    max_value = 1
else:
    if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
        max_value_df = spark.sql("SELECT max(dim_date_key) FROM cars_catalog.gold.dim_date")
        max_value = max_value_df.collect()[0][0]

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim'):
    max_value_df = spark.sql("SELECT max(dim_date_key) FROM cars_catalog..gold.dim_date")
    max_value = max_value_df.collect()[0][0]
else:
    max_value = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ** Create Surrogate key column & ADD the max surrogate key**

# COMMAND ----------

from pyspark.sql import functions as F
df_filter_new = df_filter_new.withColumn('dim_date_key', max_value + F.monotonically_increasing_id())


# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Slowly Changing Dimension (SCD) TYPE - 1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    delta_table = DeltaTable.forPath(spark, "abfss://gold@adlscarsalesp.dfs.core.windows.net/dim_date")

    delta_table.alias("target").merge(df_final.alias("source"),"target.dim_date_key = source.dim_date_key")\
     .whenMatchedUpdateAll()\
     .whenNotMatchedInsertAll()\
     .execute()

else:
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path", "abfss://gold@adlscarsalesp.dfs.core.windows.net/dim_date")\
        .saveAsTable("cars_catalog.gold.dim_date")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cars_catalog.gold.dim_date