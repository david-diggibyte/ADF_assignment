# Databricks notebook source
# MAGIC %run "/Workspace/Users/smilingdavid001@gmail.com/assigenment/bronze_to_silver/utils"

# COMMAND ----------

raw_sales_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/sales/20240107_sales_data.csv', header=True, inferSchema=True)

# COMMAND ----------

renamed_sales_df = toSnakeCase(raw_sales_df)

# COMMAND ----------

writeTo = f'dbfs:/mnt/silver/sales_view/customer_sales'
write_delta_upsert(renamed_sales_df, writeTo)

# COMMAND ----------

