# Databricks notebook source
s3_access_key = dbutils.secrets.get(scope="ProjectCP", key="S3_ACCESS_KEY")
s3_access_secret = dbutils.secrets.get(scope="ProjectCP", key="S3_ACCESS_SECRET")

# COMMAND ----------

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key",s3_access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key",s3_access_secret)
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint","s3.ap-southeast-1.amazonaws.com")

# COMMAND ----------

df = spark.read.csv("s3a://project-data-landing-chopper/customer_infomation_1.csv",inferSchema=True,header=True)
df.display() ## test connect s3

# COMMAND ----------

df = spark.read.csv("s3a://project-data-landing-chopper/data_sales_1.csv",inferSchema=True,header=True)
df.display() ## test connect s3

# COMMAND ----------

# MAGIC %md
# MAGIC ###Mount S3 and DBFS

# COMMAND ----------

aws_bucket_name = "project-data-landing-chopper"
mount_name = "project-data-landing-chopper"
dbutils.fs.mount(f"s3a://{aws_bucket_name}", f"/mnt/{mount_name}")
display(dbutils.fs.ls(f"/mnt/{mount_name}"))

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/project-data-landing-chopper/data_sales_1.csv",inferSchema=True,header=True)
display(df) ## test read csv from dbfs

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/project-data-landing-chopper/customer_infomation_1.csv",inferSchema=True,header=True)
display(df) ## test read csv from dbfs