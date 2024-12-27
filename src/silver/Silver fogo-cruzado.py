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
df_fogo_cruzado_silver = (df_fogo_cruzado_bronze
    .withColumnRenamed('documentNumber', 'numero_documento')
    .withColumnRenamed('date', 'data')
    .withColumnRenamed('agentPresence', 'presenca_agente')
    .withColumnRenamed('address', 'endereco')
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
df_fogo_cruzado_silver = df_fogo_cruzado_silver.drop(
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

# DBTITLE 1,Convertendo o tipo das colunas
import pyspark.sql.functions as F

df_fogo_cruzado_silver = (df_fogo_cruzado_silver
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .withColumn("data", F.col("data").cast("date"))
)

# COMMAND ----------

# DBTITLE 1,Corrigindo erros de encoding e input
df_fogo_cruzado_silver = df_fogo_cruzado_silver.withColumn("unidade_policial_contexto", 
    F.when(df_fogo_cruzado_silver.unidade_policial_contexto == 'N o identificado', 'Não identificado')
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == '', 'Não identificado')
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == 'não identificado', 'Não identificado')
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == None, 'Não identificado')
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == 'N�o identificado', 'Não identificado')
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == 'Niter i Presente', 'Niterói Presente')
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == 'Niter i presente', 'Niterói Presente')
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == 'M ier Presente', 'Méier Presente')
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == "UPP (Andara )", "UPP (Andaraí)")
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == "UPP (Arar /Mandela)", "UPP (Arará/Mandela)")
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == "UPP (Arar  /Mandela)", "UPP (Arará/Mandela)")
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == "UPP (Arar�/Mandela)", "UPP (Arará/Mandela)")
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == "UPP (Babil nia/Chap u Mangueira)", "UPP (Babilônia/Chapéu Mangueira)")
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == "UPP (Camarista M ier)", "UPP (Camarista Méier)")
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == "UPP (Complexo do Alem o)", "UPP (Complexo do Alemão)")
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == "UPP (Complexo do Alem�o)", "UPP (Complexo do Alemão)")
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == "UPP (F /Sereno)", "UPP (Fé/Sereno)")
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == "UPP (Pav o Pav ozinho/Cantagalo)", "UPP (Pavão Pavãozinho/Cantagalo)")
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == "UPP (Provid ncia)", "UPP (Providência)")
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == "UPP (S o Carlos)", "UPP (São Carlos)")
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == "UPP (S o Jo o)", "UPP (São João)")
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == "Polícia Civil - DRACO", "DRACO")
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == "Polícia Civil", "PC")
    .when(df_fogo_cruzado_silver.unidade_policial_contexto == "CORE (PC)", "CORE")
    .otherwise(df_fogo_cruzado_silver.unidade_policial_contexto)
    )

# COMMAND ----------

# DBTITLE 1,Removendo espaços em branco no começo e no final
df_fogo_cruzado_silver = df_fogo_cruzado_silver.withColumn('unidade_policial_contexto', F.rtrim(F.col("unidade_policial_contexto")))
df_fogo_cruzado_silver = df_fogo_cruzado_silver.withColumn('unidade_policial_contexto', F.ltrim(F.col("unidade_policial_contexto")))

# COMMAND ----------

# DBTITLE 1,Removendo REGEX
replacements = [
    ("º", ""),
    ("ª", ""),
    ("°", ""),
    (" e ", ", ")
]

col = F.col("unidade_policial_contexto")
for pattern, replacement in replacements:
    col = F.regexp_replace(col, pattern, replacement)

df_fogo_cruzado_silver.drop('unidade_policial_contexto')
df_fogo_cruzado_silver = df_fogo_cruzado_silver.withColumn('unidade_policial_contexto', col)

# COMMAND ----------

# DBTITLE 1,Separando as unidades policiais por vírgula
df_fogo_cruzado_silver = df_fogo_cruzado_silver.withColumn("list_values", F.split(df_fogo_cruzado_silver["unidade_policial_contexto"], ",\\s*"))

df_fogo_cruzado_silver = df_fogo_cruzado_silver.drop('unidade_policial_contexto')
df_fogo_cruzado_silver = df_fogo_cruzado_silver.withColumnRenamed('list_values', 'unidade_policial_contexto')

# COMMAND ----------

# DBTITLE 1,Salvando o dataframe em uma tabela silver
df_fogo_cruzado_silver.write.format('delta').mode('overwrite').saveAsTable(f'{catalog}.{schema}.{sigla}')
