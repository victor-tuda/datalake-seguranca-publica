# Databricks notebook source
# DBTITLE 1,Configuração do Job
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table_name")
path = dbutils.widgets.get("path")

# COMMAND ----------

# DBTITLE 1,df_municipios
df = spark.read.csv(f"{path}", sep=";", header=True)

# COMMAND ----------

# DBTITLE 1,save the dataframes as tables in bronze catalog
df.write.format('delta').mode('overwrite').saveAsTable(f'{catalog}.{schema}.{table_name}')
