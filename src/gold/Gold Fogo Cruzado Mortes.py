# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists gold.fogo_cruzado.mortes as
# MAGIC select id,
# MAGIC   data,
# MAGIC   presenca_agente,
# MAGIC   endereco,
# MAGIC   localidade_nome,
# MAGIC   estado_nome,
# MAGIC   municipio_nome,
# MAGIC   bairro_nome,
# MAGIC   contexto_motivo_principal_nome,
# MAGIC   latitude,
# MAGIC   longitude
# MAGIC from silver.fogo_cruzado.rj_pe
# MAGIC where vitima_situacao = 'Dead'
