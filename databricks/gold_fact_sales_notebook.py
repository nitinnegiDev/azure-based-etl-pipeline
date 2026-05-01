# Databricks notebook source
# MAGIC %md
# MAGIC # Create fact table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading data (silver and gold/dimention tables)

# COMMAND ----------

# DBTITLE 1,reading silver data
df_silver = spark.sql("""
    SELECT *
    FROM parquet.`abfss://silver@adlscardataprojectdev.dfs.core.windows.net/car_sales`;
""")

df_silver.display()

# COMMAND ----------

# DBTITLE 1,reading gold tables or dimention tables
df_dim_model = spark.sql("""
  SELECT *
  FROM cars_catalog.gold.dim_model
""")

df_dim_branch = spark.sql("""
  SELECT *
  FROM cars_catalog.gold.dim_branch
""")

df_dim_dealer = spark.sql("""
  SELECT *
  FROM cars_catalog.gold.dim_dealer
""")

df_dim_date = spark.sql("""
  SELECT *
  FROM cars_catalog.gold.dim_date
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Associate surrogate key from dimention tables to fact table (sales table)

# COMMAND ----------

# DBTITLE 1,creating fact table
df_fact = df_silver.join(df_dim_model, df_silver['model_id'] == df_dim_model['model_id'], 'left')\
  .join(df_dim_branch, df_silver['branch_id'] == df_dim_branch['branch_id'], 'left')\
  .join(df_dim_dealer, df_silver['dealer_id'] == df_dim_dealer['dealer_id'], 'left')\
  .join(df_dim_date, df_silver['date_id'] == df_dim_date['date_id'], 'left')\
  .select(df_silver['revenue'], df_silver['units_sold'], df_silver['rev_per_unit'], 
          df_dim_model['dim_model_key'], 
          df_dim_branch['dim_branch_key'], 
          df_dim_dealer['dim_dealer_key'], 
          df_dim_date['dim_date_key']
          )
  
df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing fact table

# COMMAND ----------

# DBTITLE 1,import DeltaTable
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,saving sales/fact table
if spark.catalog.tableExists('fact_sales'):
  delta_table = DeltaTable.forName(spark, 'cars_catalog.gold.fact_sales')

  delta_table.alias('trg').merge(df_fact.alias('src'),
                                  'trg.dim_model_key == src.dim_model_key' 
                                  and 'trg.dim_branch_key == src.dim_branch_key'
                                  and 'trg.dim_dealer_key == src.dim_dealer_key'
                                  and 'trg.dim_date_key == src.dim_date_key'
                                )\
                          .whenMatchedUpdateAll()\
                          .whenNotMatchedInserrtAll()\
                          .execute()
else:
  df_fact.write.format('delta')\
    .mode('overwrite')\
    .option('path', 'abfss://gold@adlscardataprojectdev.dfs.core.windows.net/fact_sales')\
    .saveAsTable('cars_catalog.gold.fact_sales')

# COMMAND ----------

# DBTITLE 1,testing table
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM cars_catalog.gold.fact_sales

# COMMAND ----------

