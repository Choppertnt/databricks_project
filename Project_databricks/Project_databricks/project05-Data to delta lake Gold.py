# Databricks notebook source
datasales_silver = spark.sql("SELECT * FROM silver.cleaned_datasales")

customer_silver = spark.sql("SELECT * FROM silver.cleaned_customers")

datasales_of_customer = datasales_silver.join(customer_silver, on=["id"], how="inner")

display(datasales_of_customer)

# COMMAND ----------

datasales_of_customer.write.saveAsTable('gold.datasales_of_customer', format='delta', mode='overwrite', path='s3a://project-data-gold-chopper/datasales_of_customer')