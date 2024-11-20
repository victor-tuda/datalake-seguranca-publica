# Databricks notebook source
# DBTITLE 1,Configurações dos parâmetros do Workflow
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

# DBTITLE 1,Gerando o dataframe a partir da tabela bronze
df_data_sus_bronze = spark.sql(f"SELECT * FROM bronze.{schema}.{table_name}")
df_ibge = spark.sql(f"SELECT * FROM silver.ibge.codigo_municipios")

# COMMAND ----------

# DBTITLE 1,Função para decodificar a planilha
from pyspark.sql import functions as F

def substituir_valores(df, colunas_substituir):
    for coluna, substituicoes in colunas_substituir.items():
        # Criar a condição para substituições
        condicao = F.col(coluna)
        for valor_antigo, valor_novo in substituicoes.items():
            condicao = F.when(F.col(coluna) == valor_antigo, valor_novo).otherwise(condicao)
        # Aplicar a coluna modificada ao DataFrame
        df = df.withColumn(coluna, condicao)
    return df

# COMMAND ----------

# DBTITLE 1,Regras de decodificação
# Dicionário com as substituições
substituicoes = {
    "ORIGEM": {1: "Oracle", 2: "Banco estadual diponibilizado via FTP", 3: "Banco SEADE", 9: "Ignorado"},
    "TIPOBITO": {1: "Fetal", 2: "Não Fetal"},
    "SEXO": {1: "Masculino", 2: "Feminino"},
    "RACACOR": {1: "Branca", 2: "Preta", 3: "Amarela", 4: "Parda", 5: "Indígena"},
    "ESTCIV": {1: "Solteiro", 2: "Casado", 3: "Viúvo", 4: "Separado judicialmente/divorciado", 5: "União estável", 9:"Ignorado"},
    "ESC": {1: "Nenhuma", 2: "1 a 3 anos", 3: "4 a 7 anos", 4: "8 a 11 anos", 5: "12 anos e mais", 9: "Ignorado"},
    "ESC2010": {0:"Sem escolaridade", 1: "Fundamental I (1ª a 4ª série)", 2: "Fundamental II (5ª a 8ª série)", 3: "Médio (Antigo 2º Grau)", 4: "Superior Incompleto", 5: "Superior Completo", 9: "Ignorado"},
    "LOCOCOR" :{1: "Hospital", 2: "Outros estabelecimentos de saúde", 3: "Domicílio", 4: "Via pública", 5: "Outros", 6: "Aldeia indígena", 9: "Ignorado"},
    "ESCMAE": {1: "Nenhuma", 2: "1 a 3 anos", 3: "4 a 7 anos", 4: "8 a 11 anos", 5: "12 anos e mais", 9: "Ignorado"},
    "ESCMAE2010": {0:"Sem escolaridade", 1: "Fundamental I (1ª a 4ª série)", 2: "Fundamental II (5ª a 8ª série)", 3: "Médio (Antigo 2º Grau)", 4: "Superior Incompleto", 5: "Superior Completo", 9: "Ignorado"},
    "QTDFILVIVO": {9: "Ignorado"},
    "QTDFILMORT": {9: "Ignorado"},
    "GRAVIDEZ": {1: "Única", 2: "Dupla", 3: "Tripla e mais", 9: "Ignorado"},
    "SEMAGESTAC": {9: "Ignorado"},
    "GESTACAO": {1: "Menos de 22 semanas", 2: "22 a 27 semanas", 3: "28 a 31 semanas", 4: "32 a 36 semanas", 5: "37 a 41 semanas", 6: "42 e mais semanas"},
    "PARTO": {1: "Vaginal", 2: "Cesáreo", 9: "Ignorado"},
    "OBITOPARTO": {1: "Antes", 2: "Durante", 3: "Depois", 9: "Ignorado"},
    "TPMORTEOCO": {1: "Na gravidez", 2: "No Parto", 3: "No abortamento", 4: "Até 42 dias após o término do parto", 5: "De 43 dias a 1 ano após o término da gestação", 8: "Não ocorreu nestes períodos", 9: "Ignorado"},
    "OBITOGRAV": {1: "Sim", 2: "Não", 9: "Ignorado"},
    "OBITOPUERP": {1: "Sim, até 42 dias após o parto", 2: "Sim, de 43 dias a 1 ano", 3: "Não", 9: "Ignorado"},
    "ASSISTMED": {1: "Sim", 2: "Não", 9: "Ignorado"},
    "EXAME": {1: "Sim", 2: "Não", 9: "Ignorado"},
    "CIRURGIA": {1: "Sim", 2: "Não", 9: "Ignorado"},
    "NECROPSIA": {1: "Sim", 2: "Não", 9: "Ignorado"},
    "CIRCOBITO": {1: "Acidente", 2: "Suicídio", 3: "Homicídio", 4: "Outros", 9: "Ignorado"},
    "ACIDTRAB": {1: "Sim", 2: "Não", 9: "Ignorado"},
    "FONTE": {1: "Ocorrência policial", 2: "Hospital", 3: "Família", 4: "Outra", 9: "Ignorado"},
    "ATESTANTE": {1: "Sim", 2: "Substituto", 3: "IML", 4: "SVO", 5: "Outros"},
    "FONTEINV": {1: "Comitê de Morte Materna e/ou Infantil", 2: "Visita domiciliar / Entrevista família", 3: "Estabelecimento de Saúde / Prontuário", 4: "Relacionado com outros bancos de dados", 5: "SVO", 6: "IML", 7: "Outra fonte", 8: "Múltiplas fontes", 9: "Ignorado"},
    "STDOEPIDEM": {1: "Sim", 2: "Não"},
    "STDONOVA":  {1: "Sim", 2: "Não"},
    "TPRESGINFO": {1: "Não acrescentou nem corrigiu informação", 2: "Sim, permitiu o resgate de novas informações", 3: "Sim, permitiu a correção de alguma das causas informadas originalmente"},
    "TPNIVELINV": {"E": "Estadual", "R": "Regional", "M": "Municipal"},
    "ALTCAUSA": {1: "Sim", 2: "Não"}
}



# COMMAND ----------

# DBTITLE 1,Aplicando decodificação
df_valores_substituidos = substituir_valores(df_data_sus_bronze, substituicoes)

# COMMAND ----------

# DBTITLE 1,Convertendo o código dos municípios
df_result = df_valores_substituidos.join(df_ibge, df_valores_substituidos["CODMUNNATU"] == df_ibge["codigo_municipio_completo"], "left")
df_result = df_result.withColumn("CODMUNNATU", df_result["nome_municipio"])
df_result = df_result.drop("codigo_municipio_completo", "nome_municipio")

df_result = df_valores_substituidos.join(df_ibge, df_valores_substituidos["CODMUNRES"] == df_ibge["codigo_municipio_completo"], "left")
df_result = df_result.withColumn("CODMUNRES", df_result["nome_municipio"])
df_result = df_result.drop("codigo_municipio_completo", "nome_municipio")

df_result = df_valores_substituidos.join(df_ibge, df_valores_substituidos["CODMUNNATU"] == df_ibge["codigo_municipio_completo"], "left")
df_result = df_result.withColumn("CODMUNNATU", df_result["nome_municipio"])
df_result = df_result.drop("codigo_municipio_completo", "nome_municipio")

df_result = df_result.join(df_ibge, df_result["COMUNSVOIM"] == df_ibge["codigo_municipio_completo"], "left")
df_result = df_result.withColumn("COMUNSVOIM", df_result["nome_municipio"])
df_result = df_result.drop("codigo_municipio_completo", "nome_municipio")

df_result = df_result.join(df_ibge, df_result["CODMUNOCOR"] == df_ibge["codigo_municipio_completo"], "left")
df_result = df_result.withColumn("CODMUNOCOR", df_result["nome_municipio"])
df_result = df_result.drop("codigo_municipio_completo", "nome_municipio")

# COMMAND ----------

df_data_sus_silver = df_result

df_data_sus_silver.write.format('delta').mode('overwrite').saveAsTable(f'{catalog}.{schema}.{table_name}')

