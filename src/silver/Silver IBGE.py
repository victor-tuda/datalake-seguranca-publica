# Databricks notebook source
# DBTITLE 1,Configuração dos parâmetros do workflow
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

# DBTITLE 1,Gerando dataframe a partir da tabela bronze
df_municipios_silver = spark.sql(f"SELECT codigo_municipio_completo, nome_municipio FROM bronze.{schema}.{table_name}")

# COMMAND ----------

# DBTITLE 1,Removendo o último dígito do código de município
from pyspark.sql.functions import expr

df_municipios_silver = df_municipios_silver.withColumn("codigo_municipio_completo", expr("substring(codigo_municipio_completo, 1, length(codigo_municipio_completo) - 1)"))


# COMMAND ----------

# DBTITLE 1,Salvando o dataframe em uma nova tabela
df_municipios_silver.write.format('delta').mode('overwrite').saveAsTable(f'{catalog}.{schema}.{table_name}')

# COMMAND ----------


