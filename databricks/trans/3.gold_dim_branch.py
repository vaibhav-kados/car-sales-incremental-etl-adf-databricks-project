# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text('incremental_flag', '0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')
print(incremental_flag)

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



# COMMAND ----------

df_src = spark.sql(
    """
    SELECT Branch_ID, Branch_Name
    FROM PARQUET.`abfss://silver@adlscarsalesp.dfs.core.windows.net/carsales`
    """
)
df_src.display()


# COMMAND ----------

if not spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    df_sink = spark.sql(
        '''
        SELECT 1 as dim_branch_key, Branch_ID, Branch_Name 
        FROM PARQUET.`abfss://silver@adlscarsalesp.dfs.core.windows.net/carsales`
        WHERE 1=0
        '''
    )
else:
    df_sink = spark.sql(
        '''
        SELECT dim_branch_key, Branch_ID, Branch_Name 
        FROM cars_catalog.gold.dim_branch
        '''
    )



# COMMAND ----------

df_sink.display()

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.Branch_ID == df_sink.Branch_ID, 'left').select(df_src.Branch_ID, df_src.Branch_Name, df_sink.dim_branch_key)


# COMMAND ----------

df_filter.display()

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_branch_key.isNotNull())

# COMMAND ----------

df_filter_old.display()

# COMMAND ----------


df_filter_new = df_filter.filter(df_filter.dim_branch_key.isNull()).select('Branch_ID', 'Branch_Name')


# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

if (incremental_flag == '0'): 
    max_value = 1
else:
    if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
        max_value_df = spark.sql("SELECT max(dim_branch_key) FROM cars_catalog.gold.dim_branch")
        max_value = max_value_df.collect()[0][0]

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    max_value_df = spark.sql("SELECT max(dim_branch_key) FROM cars_catalog.gold.dim_branch")
    max_value = max_value_df.collect()[0][0]
else:
    max_value = 1

# COMMAND ----------


from pyspark.sql import functions as F
df_filer_new = df_filter_new.withColumn('dim_branch_key', max_value + F.monotonically_increasing_id())

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_branch_key', max_value + F.monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# Incremental RUN 
if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    delta_table = DeltaTable.forPath(spark, "abfss://gold@adlscarsalesp.dfs.core.windows.net/dim_branch")
    # update when the value exists
    # insert when new value 
    delta_table.alias("target").merge(df_final.alias("source"), "target.dim_branch_key = source.dim_branch_key")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

# Initial RUN 
else: # no table exists
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path", "abfss://gold@adlscarsalesp.dfs.core.windows.net/dim_branch")\
        .saveAsTable("cars_catalog.gold.dim_branch")