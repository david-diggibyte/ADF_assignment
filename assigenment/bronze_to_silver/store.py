# Databricks notebook source
# MAGIC %run "/Workspace/Users/smilingdavid001@gmail.com/assigenment/bronze_to_silver/utils"

# COMMAND ----------

from pyspark.sql.functions import split, to_date

# COMMAND ----------

raw_store_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/store/20240107_sales_store.csv', header=True, inferSchema=True)

# COMMAND ----------

renamed_store_df = toSnakeCase(raw_store_df)

# COMMAND ----------

store_category_df = renamed_store_df.withColumn("domain", split('email_address', '@')[1])\
    .withColumn("store_category", split('domain', '\.')[0]).drop('domain')

# COMMAND ----------

formated_date_df = store_category_df.withColumn('created_at', to_date('created_at', 'dd-MM-yyyy'))\
    .withColumn('updated_at', to_date('updated_at', 'dd-MM-yyyy'))

# COMMAND ----------

writeTo = f'dbfs:/mnt/silver/sales_view/store'
write_delta_upsert(formated_date_df, writeTo)

# COMMAND ----------

