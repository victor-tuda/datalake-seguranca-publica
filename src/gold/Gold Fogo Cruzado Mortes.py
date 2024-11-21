# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists gold.fogo_cruzado.mortes as
# MAGIC select id,
# MAGIC   data,
# MAGIC   presenca_agente,
# MAGIC   endereco,
# MAGIC   nome_local,
# MAGIC   nome_estado,
# MAGIC   nome_cidade,
# MAGIC   nome_bairro,
# MAGIC   nome_sub_bairro,
# MAGIC   nome_principal_razao_contexto,
# MAGIC   latitude,
# MAGIC   longitude
# MAGIC from silver.fogo_cruzado.rj_pe
# MAGIC where situacao_vitimas = 'Dead'
