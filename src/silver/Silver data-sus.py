# Databricks notebook source
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

df_data_sus_bronze = spark.sql(f"SELECT * FROM bronze.{schema}.data_sus")

# COMMAND ----------

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

# Dicionário com as substituições
substituicoes = {
    "ORIGEM": {1: "Oracle", 2: "Banco"},
}

# Aplicar a função ao DataFrame
df_data_sus_silver = substituir_valores(df_data_sus_bronze, substituicoes)

# Mostrar os resultados
df_data_sus_silver.show()


# COMMAND ----------

df_data_sus_silver.display()
