# Databricks notebook source
# DBTITLE 1,Configuração do job
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table_name")
sigla = table_name.upper()

# COMMAND ----------

# DBTITLE 1,df_fogo_cruzado formato json
df_raw = spark.read.format("json").option("multiline", "true").load(f"/Volumes/raw/fogo-cruzado/s3-fogo-cruzado-cdc/{sigla}")

# COMMAND ----------

# DBTITLE 1,Realizando o explode do conteúdo
from pyspark.sql.functions import explode_outer, col
df_raw_exploded = df_raw.withColumn('dataExploded', explode_outer('data')).drop('data').select('dataExploded')

# COMMAND ----------

import pyspark.sql.functions as F

def flatten_df(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols +
                               [F.col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    return flat_df

# COMMAND ----------

first_flatten = flatten_df(df_raw_exploded)

# COMMAND ----------

first_flatten_exploded = first_flatten.withColumn('dataExploded_victimsExploded', explode_outer('dataExploded_victims')).drop('dataExploded_victims')

# COMMAND ----------

second_flatten = flatten_df(first_flatten_exploded)

second_flatten_exploded = second_flatten \
    .withColumn('dataExploded_contextInfo_complementaryReasonsExploded', explode_outer('dataExploded_contextInfo_complementaryReasons')) \
    .withColumn('dataExploded_victimsExploded_circumstancesExploded', explode_outer('dataExploded_victimsExploded_circumstances')) \
    .withColumn('dataExploded_victimsExploded_qualificationsExploded', explode_outer('dataExploded_victimsExploded_qualifications')) \
    .drop('dataExploded_contextInfo_complementaryReasons', 'dataExploded_victimsExploded_circumstances', 'dataExploded_victimsExploded_qualifications', 'dataExploded_animalVictims', 'dataExploded_transports')

# COMMAND ----------

third_flatten = flatten_df(second_flatten_exploded)
third_flatten_exploded = third_flatten \
    .withColumn('dataExploded_contextInfo_clippingsExploded', explode_outer('dataExploded_contextInfo_clippings')) \
    .drop('dataExploded_contextInfo_clippings')

# COMMAND ----------

fourth_flatten = flatten_df(third_flatten_exploded)

# COMMAND ----------

result = fourth_flatten

# COMMAND ----------

for column in result.columns:
    result = result.withColumnRenamed(column, column.replace('dataExploded_', ''))

for column in result.columns:
    if 'Exploded' in column:
        result = result.withColumnRenamed(column, column.replace('Exploded', ''))

# COMMAND ----------

result.printSchema()

# COMMAND ----------

result.write.format('delta').mode('overwrite').saveAsTable(f'bronze.fogo_cruzado.{table_name}')
