# Databricks notebook source
# DBTITLE 1,Configurações dos parâmetros do Job
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
sigla = dbutils.widgets.get("sigla")

# COMMAND ----------

# DBTITLE 1,Gerando um dataframe a partir da tabela bronze
df_fogo_cruzado_bronze = spark.sql(f"SELECT * FROM bronze.{schema}.{sigla}")

# COMMAND ----------

# DBTITLE 1,Tradução das colunas para português e padronização para snake_case
from pyspark.sql.types import IntegerType, DoubleType

df_fogo_cruzado_silver = (df_fogo_cruzado_bronze
    .withColumnRenamed('documentNumber', 'numero_documento')
    .withColumnRenamed('date', 'data')
    .withColumnRenamed('agentPresence', 'presenca_agente')
    .withColumnRenamed('address', 'endereco')
    .withColumn('latitude', df_fogo_cruzado_bronze['latitude'].cast(DoubleType()).cast(IntegerType()))
    .withColumn('longitude', df_fogo_cruzado_bronze['longitude'].cast(DoubleType()).cast(IntegerType()))
    .withColumnRenamed('relatedRecord', 'registro_relacionado')
    .withColumnRenamed('locality_id', 'id_local')
    .withColumnRenamed('locality_name', 'nome_local')
    .withColumnRenamed('region_id', 'id_regiao')
    .withColumnRenamed('region_region', 'regiao_regiao')
    .withColumnRenamed('region_enabled', 'regiao_ativa')
    .withColumnRenamed('region_state', 'regiao_estado')
    .withColumnRenamed('state_id', 'id_estado')
    .withColumnRenamed('state_name', 'nome_estado')
    .withColumnRenamed('city_id', 'id_cidade')
    .withColumnRenamed('city_name', 'nome_cidade')
    .withColumnRenamed('victims_id', 'id_vitimas')
    .withColumnRenamed('victims_name', 'nome_vitimas')
    .withColumnRenamed('victims_age', 'idade_vitimas')
    .withColumnRenamed('victims_gender', 'genero_vitimas')
    .withColumnRenamed('victims_situation', 'situacao_vitimas')
    .withColumnRenamed('victims_type', 'tipo_vitimas')
    .withColumnRenamed('victims_occurrenceId', 'id_ocorrencia_vitimas')
    .withColumnRenamed('victims_deathDate', 'data_falecimento_vitimas')
    .withColumnRenamed('victims_personType', 'tipo_pessoa_vitimas')
    .withColumnRenamed('victims_race', 'raca_vitimas')
    .withColumnRenamed('victims_unit', 'unidade_vitimas')
    .withColumnRenamed('victims_ageGroup_id', 'id_faixa_etaria_vitimas')
    .withColumnRenamed('victims_ageGroup_name', 'nome_faixa_etaria_vitimas')
    .withColumnRenamed('victims_agentPosition_id', 'id_posicao_agente_vitimas')
    .withColumnRenamed('victims_agentPosition_name', 'nome_posicao_agente_vitimas')
    .withColumnRenamed('victims_agentPosition_type', 'tipo_posicao_agente_vitimas')
    .withColumnRenamed('victims_agentStatus_id', 'id_status_agente_vitimas')
    .withColumnRenamed('victims_agentStatus_name', 'nome_status_agente_vitimas')
    .withColumnRenamed('victims_agentStatus_type', 'tipo_status_agente_vitimas')
    .withColumnRenamed('victims_coorporation_id', 'id_corporacao_vitimas')
    .withColumnRenamed('victims_coorporation_name', 'nome_corporacao_vitimas')
    .withColumnRenamed('victims_genre_id', 'id_genero_vitimas')
    .withColumnRenamed('victims_genre_name', 'nome_genero_vitimas')
    .withColumnRenamed('victims_partie_id', 'id_partido_vitimas')
    .withColumnRenamed('victims_partie_name', 'nome_partido_vitimas')
    .withColumnRenamed('victims_place_id', 'id_local_vitimas')
    .withColumnRenamed('victims_place_name', 'nome_local_vitimas')
    .withColumnRenamed('victims_politicalPosition_id', 'id_posicao_politica_vitimas')
    .withColumnRenamed('victims_politicalPosition_name', 'nome_posicao_politica_vitimas')
    .withColumnRenamed('victims_politicalPosition_type', 'tipo_posicao_politica_vitimas')
    .withColumnRenamed('victims_politicalStatus_id', 'id_status_politico_vitimas')
    .withColumnRenamed('victims_politicalStatus_name', 'nome_status_politico_vitimas')
    .withColumnRenamed('victims_politicalStatus_type', 'tipo_status_politico_vitimas')
    .withColumnRenamed('victims_serviceStatus_id', 'id_status_servico_vitimas')
    .withColumnRenamed('victims_serviceStatus_name', 'nome_status_servico_vitimas')
    .withColumnRenamed('victims_serviceStatus_type', 'tipo_status_servico_vitimas')
    .withColumnRenamed('victims_qualifications', 'qualificacoes_vitimas')
    .withColumnRenamed('victims_circumstances', 'circunstancias_vitimas')
    .withColumnRenamed('contextInfo_policeUnit', 'unidade_policial_contexto')
    .withColumnRenamed('contextInfo_massacre', 'massacre_contexto')
    .withColumnRenamed('contextInfo_mainReason_id', 'id_principal_razao_contexto')
    .withColumnRenamed('contextInfo_mainReason_name', 'nome_principal_razao_contexto')
    .withColumnRenamed('contextInfo_complementaryReasons_id', 'ids_razoes_complementares_contexto')
    .withColumnRenamed('contextInfo_complementaryReasons_name', 'nomes_razoes_complementares_contexto')
    .withColumnRenamed('contextInfo_clippings_id', 'ids_recortes_contexto')
    .withColumnRenamed('contextInfo_clippings_name', 'nomes_recortes_contexto')
    .withColumnRenamed('neighborhood_id', 'id_bairro')
    .withColumnRenamed('neighborhood_name', 'nome_bairro')
    .withColumnRenamed('subNeighborhood_id', 'id_sub_bairro')
    .withColumnRenamed('subNeighborhood_name', 'nome_sub_bairro')
)


# COMMAND ----------

# DBTITLE 1,Removendo colunas relacionadas a animais e transportes
df_fogo_cruzado_silver = df_fogo_cruzado_bronze.drop(
    'animalVictims_id',
    'animalVictims_name',
    'animalVictims_occurrenceId',
    'animalVictims_situation',
    'animalVictims_type',
    'transports_id',
    'transports_interruptedTransport',
    'transports_dateInterruption',
    'transports_occurrenceId',
    'transports_releaseDate',
    'transports_transportDescription',
    'transport_id',
    'transport_name'
    )

# COMMAND ----------

# DBTITLE 1,Salvando o dataframe em uma tabela silver
df_fogo_cruzado_silver.write.format('delta').mode('overwrite').saveAsTable(f'{catalog}.{schema}.{sigla}')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.fogo_cruzado.rj
