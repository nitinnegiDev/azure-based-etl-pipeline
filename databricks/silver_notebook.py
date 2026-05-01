# Databricks notebook source
# MAGIC %md
# MAGIC ## Reading data

# COMMAND ----------

# DBTITLE 1,create dataframe
df = spark.read.format("parquet")\
    .option('inferSchema', True)\
    .load('abfss://bronze@adlscardataprojectdev.dfs.core.windows.net/raw_data')

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations

# COMMAND ----------

# DBTITLE 1,imports
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,transform model_id column
df = df.withColumn('car_brand', split(col('model_id'), '-')[0])\
    .withColumn('model_version', get(split(col('model_id'), '-'),1))

display(df)

# COMMAND ----------

# DBTITLE 1,adding date column
df = df.withColumn('date', concat_ws("-", col('year'), col('month'), col('day')))
df.show()

# COMMAND ----------

# DBTITLE 1,KPI revenue per unit
df = df.withColumn('rev_per_unit', col('revenue')/col('units_sold'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## AD_HOC
# MAGIC

# COMMAND ----------

# DBTITLE 1,units sold by each branch every year
df.groupBy(col('year'), col('branch_name'))\
    .agg(sum(col('units_sold')).alias('units_sold_per_year'))\
    .sort(col('year'), col('units_sold_per_year'), col('branch_name'), ascending=[1, 0, 1])\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing data to silver container

# COMMAND ----------

# DBTITLE 1,write data to silver container
df.write.format('parquet')\
  .mode('overwrite')\
  .option('path', 'abfss://silver@adlscardataprojectdev.dfs.core.windows.net/car_sales')\
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying silver data

# COMMAND ----------

# DBTITLE 1,silver data query
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM parquet.`abfss://silver@adlscardataprojectdev.dfs.core.windows.net/car_sales`

# COMMAND ----------

