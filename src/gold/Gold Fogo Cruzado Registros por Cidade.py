# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists gold.fogo_cruzado.cidades_registros as
# MAGIC select nome_cidade, count(*) as contagem_de_registros
# MAGIC from silver.fogo_cruzado.rj_pe
# MAGIC group by nome_cidade
# MAGIC order by contagem_de_registros desc
