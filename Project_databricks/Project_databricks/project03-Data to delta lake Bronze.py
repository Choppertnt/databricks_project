# Databricks notebook source
input_csv_path = "s3a://project-data-landing-chopper/customer_infomation_1.csv"
output_s3_bucket = "s3a://project-data-bronze-chopper/customer_infomation/"

df = spark.read.csv(input_csv_path, header=True, inferSchema=True)
new_columns = [col.replace(" ", "_") for col in df.columns]
df = df.toDF(*new_columns)
df.write.saveAsTable('bronze.customer_infomation', format='delta', mode='overwrite', path=output_s3_bucket)

# COMMAND ----------

input_csv_path = "s3a://project-data-landing-chopper/data_sales_1.csv"
output_s3_bucket = "s3a://project-data-bronze-chopper/data_sales/"

df = spark.read.csv(input_csv_path, header=True, inferSchema=True)
new_columns = [col.replace(" ", "_") for col in df.columns]
df = df.toDF(*new_columns)
df.write.saveAsTable('bronze.data_sales', format='delta', mode='overwrite', path=output_s3_bucket)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze.data_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze.customer_infomation

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM bronze.data_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM bronze.customer_infomation