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
    .withColumnRenamed('address', 'endereco') \
    .withColumnRenamed('agentPresence', 'presenca_agente') \
    .withColumnRenamed('date', 'data') \
    .withColumnRenamed('documentNumber', 'numero_documento') \
    .withColumnRenamed('id', 'id') \
    .withColumnRenamed('latitude', 'latitude') \
    .withColumnRenamed('longitude', 'longitude') \
    .withColumnRenamed('policeAction', 'acao_policial') \
    .withColumnRenamed('relatedRecord', 'registro_relacionado') \
    .withColumnRenamed('subNeighborhood', 'sub_bairro') \
    .withColumnRenamed('city_id', 'municipio_id') \
    .withColumnRenamed('city_name', 'municipio_nome') \
    .withColumnRenamed('contextInfo_massacre', 'contexto_massacre') \
    .withColumnRenamed('contextInfo_policeUnit', 'contexto_unidade_policial') \
    .withColumnRenamed('locality_id', 'localidade_id') \
    .withColumnRenamed('locality_name', 'localidade_nome') \
    .withColumnRenamed('neighborhood_id', 'bairro_id') \
    .withColumnRenamed('neighborhood_name', 'bairro_nome') \
    .withColumnRenamed('region_enabled', 'regiao_ativa') \
    .withColumnRenamed('region_id', 'regiao_id') \
    .withColumnRenamed('region_region', 'regiao_regiao') \
    .withColumnRenamed('region_state', 'regiao_estado') \
    .withColumnRenamed('state_id', 'estado_id') \
    .withColumnRenamed('state_name', 'estado_nome') \
    .withColumnRenamed('victims_age', 'vitima_idade') \
    .withColumnRenamed('victims_deathDate', 'vitima_data_morte') \
    .withColumnRenamed('victims_id', 'vitima_id') \
    .withColumnRenamed('victims_occurrenceId', 'vitima_id_ocorrencia') \
    .withColumnRenamed('victims_partie', 'vitima_partido') \
    .withColumnRenamed('victims_personType', 'vitima_tipo_pessoa') \
    .withColumnRenamed('victims_race', 'vitima_raca') \
    .withColumnRenamed('victims_situation', 'vitima_situacao') \
    .withColumnRenamed('victims_type', 'vitima_tipo') \
    .withColumnRenamed('victims_unit', 'vitima_unidade') \
    .withColumnRenamed('contextInfo_mainReason_id', 'contexto_motivo_principal_id') \
    .withColumnRenamed('contextInfo_mainReason_name', 'contexto_motivo_principal_nome') \
    .withColumnRenamed('victims_ageGroup_id', 'vitima_faixa_etaria_id') \
    .withColumnRenamed('victims_ageGroup_name', 'vitima_faixa_etaria_nome') \
    .withColumnRenamed('victims_agentPosition_id', 'vitima_agente_cargo_id') \
    .withColumnRenamed('victims_agentPosition_name', 'vitima_agente_cargo_nome') \
    .withColumnRenamed('victims_agentPosition_type', 'vitima_agente_cargo_tipo') \
    .withColumnRenamed('victims_agentStatus_id', 'vitima_agente_status_id') \
    .withColumnRenamed('victims_agentStatus_name', 'vitima_agente_status_nome') \
    .withColumnRenamed('victims_agentStatus_type', 'vitima_agente_status_tipo') \
    .withColumnRenamed('victims_coorporation_id', 'vitima_corporacao_id') \
    .withColumnRenamed('victims_coorporation_name', 'vitima_corporacao_nome') \
    .withColumnRenamed('victims_genre_id', 'vitima_genero_id') \
    .withColumnRenamed('victims_genre_name', 'vitima_genero_nome') \
    .withColumnRenamed('victims_place_id', 'vitima_local_id') \
    .withColumnRenamed('victims_place_name', 'vitima_local_nome') \
    .withColumnRenamed('victims_politicalPosition_id', 'vitima_posicao_politica_id') \
    .withColumnRenamed('victims_politicalPosition_name', 'vitima_posicao_politica_nome') \
    .withColumnRenamed('victims_politicalPosition_type', 'vitima_posicao_politica_tipo') \
    .withColumnRenamed('victims_politicalStatus_id', 'vitima_status_politico_id') \
    .withColumnRenamed('victims_politicalStatus_name', 'vitima_status_politico_nome') \
    .withColumnRenamed('victims_politicalStatus_type', 'vitima_status_politico_tipo') \
    .withColumnRenamed('victims_serviceStatus_id', 'vitima_status_servico_id') \
    .withColumnRenamed('victims_serviceStatus_name', 'vitima_status_servico_nome') \
    .withColumnRenamed('victims_serviceStatus_type', 'vitima_status_servico_tipo') \
    .withColumnRenamed('contextInfo_complementaryReasons_id', 'contexto_motivos_complementares_id') \
    .withColumnRenamed('contextInfo_complementaryReasons_name', 'contexto_motivos_complementares_nome') \
    .withColumnRenamed('victims_circumstances_id', 'vitima_circunstancias_id') \
    .withColumnRenamed('victims_circumstances_name', 'vitima_circunstancias_nome') \
    .withColumnRenamed('victims_circumstances_type', 'vitima_circunstancias_tipo') \
    .withColumnRenamed('victims_qualifications_id', 'vitima_qualificacoes_id') \
    .withColumnRenamed('victims_qualifications_name', 'vitima_qualificacoes_nome') \
    .withColumnRenamed('victims_qualifications_type', 'vitima_qualificacoes_tipo') \
    .withColumnRenamed('contextInfo_clippings_id', 'contexto_recortes_id') \
    .withColumnRenamed('contextInfo_clippings_name', 'contexto_recortes_nome')

)


# COMMAND ----------

df_clean.display()

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
df_regex1 = df_clean.withColumn("contexto_unidade_policial", F.regexp_replace("contexto_unidade_policial", "º", ""))
df_regex2 = df_regex1.withColumn("contexto_unidade_policial", F.regexp_replace("contexto_unidade_policial", "ª", ""))
df_regex3 = df_regex2.withColumn("contexto_unidade_policial", F.regexp_replace("contexto_unidade_policial", "°", ""))


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
    "UPP (Provid�ncia)": "UPP (Providência)",
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

# DBTITLE 1,Aplicando dicionário de erros
df_clean = df_regex3.replace(erros_dict, subset=["contexto_unidade_policial"])

# COMMAND ----------

# DBTITLE 1,Removendo valores nulos
df_clean = df_clean.na.fill("Não identificado", ["contexto_unidade_policial"])

# COMMAND ----------

# DBTITLE 1,Convertendo para lista a partir de delimitador
df_clean = df_clean.withColumn("contexto_unidade_policial", F.regexp_replace("contexto_unidade_policial", " e ", ", "))

# COMMAND ----------

# DBTITLE 1,Removendo espaços em branco no começo e no final
df_clean = df_clean.withColumn('contexto_unidade_policial', F.ltrim(F.col("contexto_unidade_policial")))
df_clean = df_clean.withColumn('contexto_unidade_policial', F.rtrim(F.col("contexto_unidade_policial")))

# COMMAND ----------

# DBTITLE 1,Separando as unidades policiais por vírgula
df_list = df_clean.withColumn("list_values", F.split(df_clean["contexto_unidade_policial"], ",\\s*"))

df_list = df_list.drop('contexto_unidade_policial')
df_list = df_list.withColumnRenamed('list_values', 'contexto_unidade_policial')

# COMMAND ----------

# DBTITLE 1,Explode - uma nova linha para cada unidade policial
df_exploded = (
    df_list
    .withColumn("contexto_unidade_policial", F.explode("contexto_unidade_policial"))
)

# COMMAND ----------

# DBTITLE 1,Gerando colunas extras a partir da unidade policial
from pyspark.sql import functions as F

df_exploded = df_exploded.withColumn('contexto_unidade_policial_numero', 
    F.trim(
        F.regexp_extract('contexto_unidade_policial_contexto', '[0-9]*', 0)
    )
)

df_exploded = df_exploded.withColumn('contexto_unidade_policial_info_adicional', 
    F.trim(
        F.regexp_replace(
            F.regexp_extract('contexto_unidade_policial_contexto', '[(]([^)]*)', 0),
        '[(]', ''
        )
    )
)

df_exploded = df_exploded.withColumn(
    "contexto_unidade_policial_contexto",
    F.trim(
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    "contexto_unidade_policial_contexto", '[0-9]*', ""  # Remove numbers
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
