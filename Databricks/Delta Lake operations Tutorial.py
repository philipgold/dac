# Databricks notebook source
# MAGIC %md
# MAGIC # Tutorial: Delta Lake
# MAGIC
# MAGIC [Tutorial: Delta Lake | Databricks on AWS](https://docs.databricks.com/en/delta/tutorial.html#create)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a table

# COMMAND ----------

# Load the data from its source.
df = spark.read.load("/databricks-datasets/learning-spark-v2/people/people-10m.delta")

# Write the data to a table.
table_name = "people_10m"
df.write.saveAsTable(table_name)


# COMMAND ----------

display(spark.sql('DESCRIBE DETAIL people_10m'))

