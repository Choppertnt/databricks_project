# Databricks notebook source
from pyspark.sql.functions import col, to_date, from_unixtime,date_format

# COMMAND ----------

customer_bronze = spark.sql("SELECT * FROM bronze.customer_infomation")
display(customer_bronze)

# COMMAND ----------

customer_bronze.toPandas().info()

# COMMAND ----------

customer_bronze.filter(col("customername").isNull() | col("gender").isNull() | col("age").isNull()).display()
customer_silver = customer_bronze.dropna()
customer_silver.display()

# COMMAND ----------

datasales_bronze = spark.sql("SELECT * FROM bronze.data_sales")
display(datasales_bronze)

# COMMAND ----------

datasales_bronze.toPandas().info()

# COMMAND ----------

datasales_bronze.filter(col("createtime").isNull() | col("id").isNull() | col("product").isNull() | col("quan").isNull() \
                        | col("shop").isNull() |  col("platform").isNull() \
                        ).display()
datasales_silver = datasales_bronze.dropna()
datasales_silver.display()

# COMMAND ----------

datasales_silver.write.saveAsTable('silver.cleaned_datasales', format='delta', mode='overwrite', path='s3a://project-data-silver-chopper/datasales')

# COMMAND ----------

customer_silver.write.saveAsTable('silver.cleaned_customers', format='delta', mode='overwrite', path='s3a://project-data-silver-chopper/customers')