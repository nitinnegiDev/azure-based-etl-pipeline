# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# DBTITLE 1,imports for spark
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create flag for initial vs incremental run

# COMMAND ----------

# DBTITLE 1,creating widget for flag
dbutils.widgets.text('incremental_flag', '0')

# COMMAND ----------

# DBTITLE 1,retrieving flag value from widget
incremental_flag = dbutils.widgets.get('incremental_flag')
print(type(incremental_flag))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create dimention model

# COMMAND ----------

# DBTITLE 1,check silver data
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM parquet.`abfss://silver@adlscardataprojectdev.dfs.core.windows.net/car_sales`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting dimention table

# COMMAND ----------

# DBTITLE 1,selecting distinct data for dimention table
df_src = spark.sql("""
    SELECT DISTINCT dealer_id, dealer_name
    FROM parquet.`abfss://silver@adlscardataprojectdev.dfs.core.windows.net/car_sales`;
""")

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create sink dimention model for initial or incremental load

# COMMAND ----------

# DBTITLE 1,create (first run) or get dimention table (incremental runs)
if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
  df_sink = spark.sql("""
    SELECT dim_dealer_key, dealer_id, dealer_name
    FROM cars_catalog.gold.dim_dealer;
  """)
else:
  df_sink = spark.sql("""
  SELECT 1 as dim_dealer_key, dealer_id, dealer_name
  FROM parquet.`abfss://silver@adlscardataprojectdev.dfs.core.windows.net/car_sales`
  WHERE 1=0;
""")

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtering new records to insert in dimention table

# COMMAND ----------

# DBTITLE 1,insert new records in dimention table
df_joined = df_src.join(df_sink, df_src['dealer_id'] == df_sink['dealer_id'], 'left')\
    .select(df_src['dealer_id'], df_src['dealer_name'], df_sink['dim_dealer_key'])
df_joined.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtering old and new records from joined table

# COMMAND ----------

# DBTITLE 1,filtering old/exiting records
df_filter_existing = df_joined.filter(col('dim_dealer_key').isNotNull())
df_filter_existing.display()

# COMMAND ----------

# DBTITLE 1,filtering new records
df_filter_new = df_joined.filter(col('dim_dealer_key').isNull())\
    .select(col('dealer_id'), col('dealer_name'))
df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create surrogate key for new records

# COMMAND ----------

# DBTITLE 1,find max diimention/surrogate key from dimention table
if incremental_flag == '0':
  max_dim_dealer_key = 0
else:
  df_max_dim_dealer_key = spark.sql("""
    SELECT MAX(dim_dealer_key)
    FROM cars_catalog.gold.dim_dealer;
  """)
  max_dim_dealer_key = df_max_dim_dealer_key.collect()[0][0]

print(max_dim_dealer_key)

# COMMAND ----------

# DBTITLE 1,insert surrogate keys to new records
# adding 1 because montonically_increasing_id() function starts from 0
df_filter_new = df_filter_new.withColumn('dim_dealer_key', max_dim_dealer_key + monotonically_increasing_id() + 1)
df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create final dimention model dataframe

# COMMAND ----------

# DBTITLE 1,union new and old df to get final df
df_final = df_filter_new.union(df_filter_existing)
df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Slowly Changing Dimentions Type-1 / Upsert (Update + Insert)

# COMMAND ----------

# DBTITLE 1,import DelataTable
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,upsert data to dimention table
if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
  # incremental data i.e. upsert data
  delta_table = DeltaTable.forPath(spark, 'abfss://gold@adlscardataprojectdev.dfs.core.windows.net/dim_dealer')

  delta_table.alias("trg").merge(df_final.alias("src"), "trg.dim_dealer_key = src.dim_dealer_key")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
  
else:
  # table doesnt exist i.e. inital run
  df_final.write.format('delta')\
    .mode('overwrite')\
    .option('path', 'abfss://gold@adlscardataprojectdev.dfs.core.windows.net/dim_dealer')\
    .saveAsTable('cars_catalog.gold.dim_dealer')

# COMMAND ----------

# DBTITLE 1,Querying dimention model
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM cars_catalog.gold.dim_dealer;

# COMMAND ----------

