# Databricks notebook source
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
sigla = dbutils.widgets.get("sigla")

# COMMAND ----------

df_fogo_cruzado_bronze = spark.sql(f"SELECT * FROM bronze.{schema}.{sigla}")

# COMMAND ----------

df_fogo_cruzado_silver = df_fogo_cruzado_bronze.drop(
    'animalVictims_id',
    'animalVictims_name',
    'animalVictims_occurrenceId',
    'animalVictims_situation',
    'animalVictims_type')

# COMMAND ----------

df_fogo_cruzado_silver.write.format('delta').mode('overwrite').saveAsTable('{catalog}.{schema}.{sigla}')
