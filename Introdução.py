# Databricks notebook source
# MAGIC %md
# MAGIC # Data Lakehouse Segurança Pública

# COMMAND ----------

# MAGIC %md
# MAGIC **Fontes:**<br>
# MAGIC [Basedosdados](https://basedosdados.org/)<br>
# MAGIC [Fogo Cruzado](https://fogocruzado.org.br/)<br>
# MAGIC [Ministério da Saúde | Sistema de Informação sobre Mortalidade – SIM](https://dados.gov.br/dados/conjuntos-dados/sim-1979-2019)<br>
# MAGIC [Código dos Municípios - IBGE](https://www.ibge.gov.br/explica/codigos-dos-municipios.php)

# COMMAND ----------

# MAGIC %md
# MAGIC Qualidade dos dados fornecidos pelas ONGs: <br>
# MAGIC [RJ: candidato à prefeitura de Japeri é atacado](https://www.youtube.com/watch?v=1WBnk7JjEEo)

# COMMAND ----------

# DBTITLE 1,Dados do instituto Fogo Cruzado
# MAGIC %sql
# MAGIC select id,
# MAGIC   date,
# MAGIC   address,
# MAGIC   victims_genre_name,
# MAGIC   victims_situation,
# MAGIC   victims_personType,
# MAGIC   victims_coorporation_name,
# MAGIC   victims_politicalPosition_name,
# MAGIC   victims_politicalStatus_name,
# MAGIC   victims_qualifications
# MAGIC   from silver.fogo_cruzado.rj where id = '122092f0-c993-483b-849b-3c9e413a0e0d'
