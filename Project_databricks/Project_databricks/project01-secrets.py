# Databricks notebook source
# MAGIC %sh
# MAGIC
# MAGIC databricks --version

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks secrets list-scopes

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC databricks secrets create-scope --scope ProjectCP

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC databricks secrets put --scope ProjectCP --key S3_ACCESS_KEY --string-value xxxxxxxxxxx

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC databricks secrets put --scope ProjectCP --key S3_ACCESS_SECRET --string-value xxxxxxxxxxx

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC databricks secrets list --scope ProjectCP