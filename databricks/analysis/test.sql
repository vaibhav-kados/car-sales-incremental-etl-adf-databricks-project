-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.widgets.text("incremental_flag",'0')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC param_value = dbutils.widgets.get("incremental_flag")
-- MAGIC print(f"param_value = {param_value}")

-- COMMAND ----------

SELECT count(*) FROM cars_catalog.gold.fact_sales

-- COMMAND ----------

SELECT count(*) FROM cars_catalog.gold.dim_branch

-- COMMAND ----------

SELECT count(*) FROM cars_catalog.gold.dim_dealer

-- COMMAND ----------

