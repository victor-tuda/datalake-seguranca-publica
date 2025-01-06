# Databricks notebook source
# DBTITLE 1,Configurações dos parâmetros do Job
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
sigla = dbutils.widgets.get("sigla")

# COMMAND ----------

# DBTITLE 1,Gerando um dataframe a partir da tabela bronze
#df_fogo_cruzado_bronze = spark.sql(f"SELECT * FROM bronze.{schema}.{sigla}")
df_fogo_cruzado_bronze = spark.sql(f"SELECT * FROM bronze.fogo_cruzado.rj")

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

# DBTITLE 1,Dicionário de erros
erros_dict = {
    'N o identificado': 'Não identificado',
    '': 'Não identificado',
    'NI': 'Não identificado',
    'Não Identificado': 'Não identificado',
    'Não Identificada': 'Não identificado',
    'Não informado': 'Não identificado',
    'não identificada': 'Não identificado',
    'não identificado': 'Não identificado',
    'não identificado ': 'Não identificado',
    "Não Informada ": 'Não identificado',
    'null': 'Não identificado',
    'policia': 'Não identificado',
    'não se aplica': 'Não identificado',
    'não informado': 'Não identificado',
    'N�o identificado': 'Não identificado',
    'NÃO IDENTIFICADO ': 'Não identificado',
    'Sem identificação': 'Não identificado',

    'Niter i Presente': 'Niterói Presente',
    'Niter i presente': 'Niterói Presente',
    'Niter�i presente': 'Niterói Presente',
    'M ier Presente': 'Méier Presente',

    "UPP (Andara )": "UPP (Andaraí)",
    "UPP (Andara�)": "UPP (Andaraí)",
    "UPP (Arar /Mandela)": "UPP (Arará/Mandela)",
    "UPP (Arar  /Mandela)": "UPP (Arará/Mandela)",
    "UPP (Arar�/Mandela)": "UPP (Arará/Mandela)",
    "UPP (Babil nia/Chap u Mangueira)": "UPP (Babilônia/Chapéu Mangueira)",
    "UPP (Babil�nia/Chap�u Mangueira)": "UPP (Babilônia/Chapéu Mangueira)",
    "UPP (Camarista M ier)": "UPP (Camarista Méier)",
    "UPP (Complexo do Alem o)": "UPP (Complexo do Alemão)",
    "UPP (Complexo do Alem�o)": "UPP (Complexo do Alemão)",
    "UPP (F /Sereno)": "UPP (Fé/Sereno)",
    "UPP (Pav o Pav ozinho/Cantagalo)": "UPP (Pavão Pavãozinho/Cantagalo)",
    "UPP (Provid ncia)": "UPP (Providência)",
    "UPP (S o Carlos)": "UPP (São Carlos)",
    "UPP (S o Jo o)": "UPP (São João)",
    "UPP (S�o Jo�o)": "UPP (São João)",
    "3 BPM, UPP (S�o Jo�o)": "3 BPM, UPP (São João)",
    "UPP (Jacar�)": "UPP (Jacaraí)",
    "UPP Manguinhos": "UPP (Manguinhos)",
    "UPP MANGUINHOS": "UPP (Manguinhos)",
    "UPP Macacos": "UPP (Macacos)",
    "UPP Mangueira": "UPP (Mangueira)",
    "UPP Fazendinha": "UPP (Fazendinha)",
    "UPP Alemão": "UPP (Alemão)",

    "Polícia Civil": "PC",
    "Policia Civil": "PC",
    "policia civil": "PC",
    "polícia civil": "PC",
    "Polícia Civil ": "PC",
    "Polícia Militar": "PM",
    "Policia Militar": "PM",
    "policia militar": "PM",
    "polícia militar": "PM",
    "Polícia Militar (8 Delegacia de Polícia Judiciária Militar)": "PM (8 Delegacia de Polícia Judiciária Militar)",
    "PM e PC": "PM, PC",
    "a PM": "PM",
    "PM | Choque | COE": "PM, BPChoque, COE",

    "Polícia Civil - DRACO": "DRACO",
    "Draco": "DRACO",
    "PC (DRACO)": "DRACO",
    "Ssint": "SSINTE",
    "Ssinte": "SSINTE",
    "PM (BPCHQ )": "BPChoque",
    "BPChq": "BPChoque",
    "CHOQUE": "BPChoque",
    "Choque": "BPChoque",
    "PM (BOPE)": "BOPE",
    "BOPE E CORE": "BOPE, CORE",
    "CORE (PC)": "CORE",
    "Coordenadoria de Recursos Especiais (Core)": "CORE",
    "Delegacia de Repressão a Furtos de Cargas (DRFC)": "DRFC",
    "Polícia Rodoviária Federal (PRF)": "PRF",
    "Polícia Rodoviária Federal": "PRF",
    "PM (BPVR)": "BPVR",
    "Batalhão de Policiamento em Vias Expressas (BPVE)": "BPVE",
    "Batalhão de Policiamento em Vias Expressas (BPVE) ": "BPVE",
    "Batalhão de Rondas Especiais RECOM": "RECOM",
    "Batalhão de Rondas Especiais e Controle de Multidão (Recom)": "RECOM",
    "SEGURANÇA PRESENTE": "Segurança Presente",
    "Segurança Presente ": "Segurança Presente",
    "Coordenadoria de Polícia Pacificadora (CPP)": "CPP",
    "Polícia Civil de Pernambuco (CORE - Comando de Operações e Recursos Especiais da Polícia Civil e Polícia Civil do Rio Grande do Norte (DEICOR/RN - Divisão Especializada de Investigação e Combate ao Crime Organizado)": "CORE, DEICOR",
    "15BPM, com apoio do 3CPA": "15BPM, 3CPA",
    "16BPM, com apoio de outras unidades do 1CPA": "16BPM, 1CPA",
    "Delegacias de Roubos e Furtos de Automóveis e de Cargas (DRFA) e (DRFC), Coordenadoria de Operações e Recursos Especiais (Core), Delegacia de Repressão a Entorpecentes (DRE) e  Batalhão de Operações Policiais Especiais (Bope)": "DRFA, DRFC, CORE, DRE, Bope",
    "Polícia Civil (DRE, DRACO e DRFC), Polícia Militar (GAM)": "DRE, DRACO, DRFC, GAM",
    "25 DP (Engenho Novo), Coordenadoria de Recursos Especiais (Core), Delegacia de Repressão a Furtos de Cargas (DRFC), Subsecretaria de Segurança e a PM": "25 DP (Engenho Novo), CORE, DRFC, Subsecretaria de Segurança, PM",
    "18BPM,  2 Comando de Policiamento de Área (2 CPA), do Batalhão de Operações de Policiais Especiais (BOPE) e Batalhão de Rondas Especiais e Controle de Multidão (RECOM)": "18BPM, 2 CPA, BOPE, RECOM"
}


# COMMAND ----------

df_clean = df_regex3.replace(erros_dict, subset=["unidade_policial_contexto"])

# COMMAND ----------

df_clean.groupBy("unidade_policial_contexto") \
    .count() \
    .orderBy(F.desc("count")) \
    .display()

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
