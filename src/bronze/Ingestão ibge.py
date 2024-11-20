# Databricks notebook source
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table_name")
path = dbutils.widgets.get("path")
sep = dbutils.widgets.get("sep")

# COMMAND ----------

df_municipios = (spark.read
    .option("sep", sep)
    .option("header", True)
    .csv(path))

# COMMAND ----------

df_municipios.write.format('delta').mode('overwrite').saveAsTable(f'{catalog}.{schema}.{table_name}')
