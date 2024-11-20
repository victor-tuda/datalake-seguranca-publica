# Databricks notebook source
# MAGIC %sql
# MAGIC select * from silver.fogo_cruzado.rj where victims_politicalPosition_name = 'Prefeito'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.basedosdados.br_sp_gov_ssp_ocorrencias_registradas
