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
df_clean = (df_fogo_cruzado_bronze
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
df_clean = df_clean.drop(
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

df_clean = (df_clean
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .withColumn("data", F.col("data").cast("date"))
)

# COMMAND ----------

# DBTITLE 1,Removendo caracteres especiais
df_regex1 = df_clean.withColumn("unidade_policial_contexto", F.regexp_replace("unidade_policial_contexto", "º", ""))
df_regex2 = df_regex1.withColumn("unidade_policial_contexto", F.regexp_replace("unidade_policial_contexto", "ª", ""))
df_regex3 = df_regex2.withColumn("unidade_policial_contexto", F.regexp_replace("unidade_policial_contexto", "°", ""))


# COMMAND ----------

# DBTITLE 1,Corrigindo erros de encoding e input
df_clean = df_regex3.withColumn("unidade_policial_contexto", 
    F.when(df_regex3.unidade_policial_contexto == 'N o identificado', 'Não identificado')
    .when(df_regex3.unidade_policial_contexto == '', 'Não identificado')
    .when(df_regex3.unidade_policial_contexto == 'NI', 'Não identificado')
    .when(df_regex3.unidade_policial_contexto == 'Não Identificado', 'Não identificado')
    .when(df_regex3.unidade_policial_contexto == 'Não Identificada', 'Não identificado')
    .when(df_regex3.unidade_policial_contexto == 'Não informado', 'Não identificado')
    .when(df_regex3.unidade_policial_contexto == 'não identificada', 'Não identificado')
    .when(df_regex3.unidade_policial_contexto == 'não identificado', 'Não identificado')
    .when(df_regex3.unidade_policial_contexto == 'null', 'Não identificado')
    .when(df_regex3.unidade_policial_contexto == 'policia', 'Não identificado')
    .when(df_regex3.unidade_policial_contexto == 'não se aplica', 'Não identificado')
    .when(df_regex3.unidade_policial_contexto == 'não informado', 'Não identificado')
    .when(df_regex3.unidade_policial_contexto == 'N�o identificado', 'Não identificado')

    .when(df_regex3.unidade_policial_contexto == 'Niter i Presente', 'Niterói Presente')
    .when(df_regex3.unidade_policial_contexto == 'Niter i presente', 'Niterói Presente')
    .when(df_regex3.unidade_policial_contexto == 'Niter�i presente', 'Niterói Presente')
    .when(df_regex3.unidade_policial_contexto == 'M ier Presente', 'Méier Presente')

    .when(df_regex3.unidade_policial_contexto == "UPP (Andara )", "UPP (Andaraí)")
    .when(df_regex3.unidade_policial_contexto == "UPP (Arar /Mandela)", "UPP (Arará/Mandela)")
    .when(df_regex3.unidade_policial_contexto == "UPP (Arar  /Mandela)", "UPP (Arará/Mandela)")
    .when(df_regex3.unidade_policial_contexto == "UPP (Arar�/Mandela)", "UPP (Arará/Mandela)")
    .when(df_regex3.unidade_policial_contexto == "UPP (Babil nia/Chap u Mangueira)", "UPP (Babilônia/Chapéu Mangueira)")
    .when(df_regex3.unidade_policial_contexto == "UPP (Babil�nia/Chap�u Mangueira)", "UPP (Babilônia/Chapéu Mangueira)")
    .when(df_regex3.unidade_policial_contexto == "UPP (Camarista M ier)", "UPP (Camarista Méier)")
    .when(df_regex3.unidade_policial_contexto == "UPP (Complexo do Alem o)", "UPP (Complexo do Alemão)")
    .when(df_regex3.unidade_policial_contexto == "UPP (Complexo do Alem�o)", "UPP (Complexo do Alemão)")
    .when(df_regex3.unidade_policial_contexto == "UPP (F /Sereno)", "UPP (Fé/Sereno)")
    .when(df_regex3.unidade_policial_contexto == "UPP (Pav o Pav ozinho/Cantagalo)", "UPP (Pavão Pavãozinho/Cantagalo)")
    .when(df_regex3.unidade_policial_contexto == "UPP (Provid ncia)", "UPP (Providência)")
    .when(df_regex3.unidade_policial_contexto == "UPP (S o Carlos)", "UPP (São Carlos)")
    .when(df_regex3.unidade_policial_contexto == "UPP (S o Jo o)", "UPP (São João)")

    .when(df_regex3.unidade_policial_contexto == "Polícia Civil", "PC")
    .when(df_regex3.unidade_policial_contexto == "Policia Civil", "PC")
    .when(df_regex3.unidade_policial_contexto == "policia civil", "PC")
    .when(df_regex3.unidade_policial_contexto == "polícia civil", "PC")
    .when(df_regex3.unidade_policial_contexto == "Polícia Militar", "PM")
    .when(df_regex3.unidade_policial_contexto == "Policia Militar", "PM")
    .when(df_regex3.unidade_policial_contexto == "policia militar", "PM")
    .when(df_regex3.unidade_policial_contexto == "polícia militar", "PM")

    .when(df_regex3.unidade_policial_contexto == "Polícia Civil - DRACO", "DRACO")
    .when(df_regex3.unidade_policial_contexto == "Draco", "DRACO")
    .when(df_regex3.unidade_policial_contexto == "PC (DRACO)", "DRACO")
    .when(df_regex3.unidade_policial_contexto == "Ssint", "SSINTE")
    .when(df_regex3.unidade_policial_contexto == "PM (BPCHQ )", "BPChoque")
    .when(df_regex3.unidade_policial_contexto == "CHOQUE", "BPChoque")
    .when(df_regex3.unidade_policial_contexto == "Choque", "BPChoque")
    .when(df_regex3.unidade_policial_contexto == "PM (BOPE)", "BOPE")
    .when(df_regex3.unidade_policial_contexto == "CORE (PC)", "CORE")
    .when(df_regex3.unidade_policial_contexto == "Coordenadoria de Recursos Especiais (Core)", "CORE")
    .when(df_regex3.unidade_policial_contexto == "Delegacia de Repressão a Furtos de Cargas (DRFC)", "DRFC")
    .when(df_regex3.unidade_policial_contexto == "Polícia Rodoviária Federal (PRF)", "PRF")
    .when(df_regex3.unidade_policial_contexto == "Polícia Rodoviária Federal", "PRF")
    .when(df_regex3.unidade_policial_contexto == "PM (BPVR)", "BPVR")
    .when(df_regex3.unidade_policial_contexto == "Batalhão de Policiamento em Vias Expressas (BPVE)", "BPVE")
    .when(df_regex3.unidade_policial_contexto == "Batalhão de Rondas Especiais RECOM", "RECOM")
    .when(df_regex3.unidade_policial_contexto == "Batalhão de Rondas Especiais e Controle de Multidão (Recom)", "RECOM")
    .when(df_regex3.unidade_policial_contexto == "Polícia Civil de Pernambuco (CORE - Comando de Operações e Recursos Especiais da Polícia Civil e Polícia Civil do Rio Grande do Norte (DEICOR/RN - Divisão Especializada de Investigação e Combate ao Crime Organizado)", "CORE, DEICOR")
    .otherwise(df_regex3.unidade_policial_contexto)
    )

# COMMAND ----------

# DBTITLE 1,Convertendo para lista a partir de delimitador
df_clean = df_clean.withColumn("unidade_policial_contexto", F.regexp_replace("unidade_policial_contexto", " e ", ", "))

# COMMAND ----------

# DBTITLE 1,Removendo espaços em branco no começo e no final
df_clean = df_clean.withColumn('unidade_policial_contexto', F.ltrim(F.col("unidade_policial_contexto")))
df_clean = df_clean.withColumn('unidade_policial_contexto', F.rtrim(F.col("unidade_policial_contexto")))

# COMMAND ----------

# DBTITLE 1,Separando as unidades policiais por vírgula
df_list = df_clean.withColumn("list_values", F.split(df_clean["unidade_policial_contexto"], ",\\s*"))

df_list = df_list.drop('unidade_policial_contexto')
df_list = df_list.withColumnRenamed('list_values', 'unidade_policial_contexto')

# COMMAND ----------

# DBTITLE 1,Explode - uma nova linha para cada unidade policial
df_exploded = (
    df_list
    .withColumn("unidade_policial_contexto", F.explode("unidade_policial_contexto"))
)

# COMMAND ----------

# DBTITLE 1,Gerando colunas extras a partir da unidade policial
from pyspark.sql import functions as F

df_exploded = df_exploded.withColumn('unidade_policial_numero', 
    F.trim(
        F.regexp_extract('unidade_policial_contexto', '[0-9]*', 0)
    )
)

df_exploded = df_exploded.withColumn('unidade_policial_info_adicional', 
    F.trim(
        F.regexp_replace(
            F.regexp_extract('unidade_policial_contexto', '[(]([^)]*)', 0),
        '[(]', ''
        )
    )
)

df_exploded = df_exploded.withColumn(
    "unidade_policial_contexto",
    F.trim(
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    "unidade_policial_contexto", '[0-9]*', ""  # Remove numbers
                ),
                '[(]([^)]*)', ""  # Remove content inside parentheses
            ),
        '[)]', ""
        )
    )
)

# COMMAND ----------

# DBTITLE 1,Salvando o dataframe em uma tabela silver
df_fogo_cruzado_silver = df_exploded
df_fogo_cruzado_silver.write.format('delta').mode('overwrite').saveAsTable(f'{catalog}.{schema}.{sigla}')
