# Databricks notebook source
# DBTITLE 1,df_municipios
df_municipios = spark.read.csv("/Volumes/raw/basedosdados/s3-basedosdados/fbsp_absp_municipio/br_fbsp_absp_municipio.csv", sep=";", header=True)

# COMMAND ----------

# DBTITLE 1,df_ocorrencias
df_ocorrencias = spark.read.csv("/Volumes/raw/basedosdados/s3-basedosdados/sp-gov-ssp-ocorrencias/br_sp_gov_ssp_ocorrencias_registradas.csv", sep=";", header=True)

# COMMAND ----------

# DBTITLE 1,df_produtividade_policial
df_produtividade_policial = spark.read.csv("/Volumes/raw/basedosdados/s3-basedosdados/sp-gov-ssp-produtividade-policial/br_sp_gov_ssp_produtividade_policial.csv", header=True)

# COMMAND ----------

# DBTITLE 1,save the dataframes as tables in bronze catalog
df_municipios.write.format('delta').mode('overwrite').saveAsTable('bronze.basedosdados.sp_gov_ssp_municipios')
df_ocorrencias.write.format('delta').mode('overwrite').saveAsTable('bronze.basedosdados.sp_gov_ssp_ocorrencias')
df_produtividade_policial.write.format('delta').mode('overwrite').saveAsTable('bronze.basedosdados.sp_gov_ssp_produtividade_policial')
