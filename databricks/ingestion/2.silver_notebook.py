# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Data Reading

# COMMAND ----------

df = spark.read.format("parquet")\
    .option('inferSchema', True)\
    .load('abfss://bronze@adlscarsalesp.dfs.core.windows.net/raw_data')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df = df.withColumn('model_category', F.split(df['Model_Id'],'-')[0])

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('revenue_per_unit', df['Revenue']/df['Units_Sold'])

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import sum as F_sum

df.groupBy('Year','Branch_Name').agg(
    F_sum('Units_Sold').alias('Total_Units_Sold')
).sort('Year','Total_Units_Sold',ascending=[True,False]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Data Writing

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
    .option('path','abfss://silver@adlscarsalesp.dfs.core.windows.net/carsales')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM PARQUET.`abfss://silver@adlscarsalesp.dfs.core.windows.net/carsales`

# COMMAND ----------

# MAGIC %md
# MAGIC