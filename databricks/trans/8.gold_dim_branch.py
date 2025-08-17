# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating Flag Parameter

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

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

# MAGIC %md 
# MAGIC ### dim_branch Sink -  Initial & Incremental 
# MAGIC #### (just bring the schema if tabel does not exist)

# COMMAND ----------

# this query is never true, so it will not return any data but it will only return the SCHEMA of the dimension table that we want to create
# this condition is only for initial laod
# if the table does not exist, I want to get the schema
# if the table exists (incremental load), bring all the data (which will be used in the left join)

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

# MAGIC %md
# MAGIC ## Filtering new records & Old records

# COMMAND ----------

df_filter = df_src.join(df_sink,df_src.Branch_ID == df_sink.Branch_ID, 'left').select(df_src.Branch_ID,df_src.Branch_Name,df_sink.dim_branch_key)

# COMMAND ----------

df_filter.display()

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_branch_key.isNotNull())

# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter.dim_branch_key.isNull()).select('Branch_ID','Branch_Name')

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

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit

w = Window.partitionBy(lit(1)).orderBy("Branch_ID")

df_filter_new = df_filter_new.withColumn(
    "dim_branch_key",
    (row_number().over(w) + max_value).cast(IntegerType())
)



# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Slowly Changing Dimension (SCD) TYPE - 1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

from pyspark.sql.types import LongType

df_final = df_final.withColumn("dim_branch_key", df_final["dim_branch_key"].cast(LongType()))

# COMMAND ----------

# Incremental Run

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

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cars_catalog.gold.dim_branch