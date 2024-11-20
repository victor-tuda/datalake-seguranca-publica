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

# DBTITLE 1,Renomeando colunas
df_municipios = (df_municipios
    .withColumnRenamed('Nome_UF', 'nome_uf')
    .withColumnRenamed('Região Geográfica Intermediária', 'codigo_regiao_geografica_intermediaria')
    .withColumnRenamed('Nome Região Geográfica Intermediária', 'nome_regiao_geografica_intermediaria')
    .withColumnRenamed('Região Geográfica Imediata', 'codigo_regiao_geografica_imediata')
    .withColumnRenamed('Nome Região Geográfica Imediata', 'nome_regiao_geografica_imediata')
    .withColumnRenamed('Mesorregião Geográfica', 'codigo_mesorregiao_geografica')
    .withColumnRenamed('Nome_Mesorregião', 'nome_mesorregiao_geografica')
    .withColumnRenamed('Microrregião Geográfica', 'codigo_microrregiao_geografica')
    .withColumnRenamed('Nome_Microrregião', 'nome_microrregiao_geografica')
    .withColumnRenamed('Município', 'codigo_municipio')
    .withColumnRenamed('Código Município Completo', 'codigo_municipio_completo')
    .withColumnRenamed('Nome_Município', 'nome_municipio')
)

# COMMAND ----------

df_municipios.write.format('delta').mode('overwrite').saveAsTable(f'{catalog}.{schema}.{table_name}')
