# Databricks notebook source
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table_name")
path = dbutils.widgets.get("path")
sep = dbutils.widgets.get("sep")

# COMMAND ----------

df_data_sus = (spark.read
    .option("sep", sep)
    .option("header", True)
    .option("inferSchema", True) 
    .csv(f"{path}/*.csv"))

df_data_sus = df_data_sus.dropDuplicates().fillna("")

# COMMAND ----------

df_data_sus.write.format('delta').mode('overwrite').saveAsTable(f'{catalog}.{schema}.{table_name}')
