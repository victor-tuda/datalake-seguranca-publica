# Databricks notebook source
# DBTITLE 1,Configuração do job
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table_name")
sigla = table_name.upper()

# COMMAND ----------

# DBTITLE 1,Definindo schema
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType, IntegerType

schema = StructType([
    StructField("address", StringType(), True),
    StructField("agentPresence", BooleanType(), True),
    StructField("date", DateType(), True),
    StructField("documentNumber", StringType(), True),
    StructField("id", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("policeAction", BooleanType(), True),
    StructField("relatedRecord", StringType(), True),
    StructField("city_id", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("contextInfo_massacre", BooleanType(), True),
    StructField("contextInfo_policeUnit", StringType(), True),
    StructField("locality_id", StringType(), True),
    StructField("locality_name", StringType(), True),
    StructField("neighborhood_id", StringType(), True),
    StructField("neighborhood_name", StringType(), True),
    StructField("region_enabled", BooleanType(), True),
    StructField("region_id", StringType(), True),
    StructField("region_region", StringType(), True),
    StructField("region_state", StringType(), True),
    StructField("state_id", StringType(), True),
    StructField("state_name", StringType(), True),
    StructField("subNeighborhood_id", StringType(), True),
    StructField("subNeighborhood_name", StringType(), True),
    StructField("victims_age", IntegerType(), True),
    StructField("victims_deathDate", DateType(), True),
    StructField("victims_id", StringType(), True),
    StructField("victims_occurrenceId", StringType(), True),
    StructField("victims_personType", StringType(), True),
    StructField("victims_race", StringType(), True),
    StructField("victims_situation", StringType(), True),
    StructField("victims_type", StringType(), True),
    StructField("victims_unit", StringType(), True),
    StructField("contextInfo_mainReason_id", StringType(), True),
    StructField("contextInfo_mainReason_name", StringType(), True),
    StructField("victims_ageGroup_id", StringType(), True),
    StructField("victims_ageGroup_name", StringType(), True),
    StructField("victims_agentPosition_id", StringType(), True),
    StructField("victims_agentPosition_name", StringType(), True),
    StructField("victims_agentPosition_type", StringType(), True),
    StructField("victims_agentStatus_id", StringType(), True),
    StructField("victims_agentStatus_name", StringType(), True),
    StructField("victims_agentStatus_type", StringType(), True),
    StructField("victims_coorporation_id", StringType(), True),
    StructField("victims_coorporation_name", StringType(), True),
    StructField("victims_genre_id", StringType(), True),
    StructField("victims_genre_name", StringType(), True),
    StructField("victims_partie_id", StringType(), True),
    StructField("victims_partie_name", StringType(), True),
    StructField("victims_place_id", StringType(), True),
    StructField("victims_place_name", StringType(), True),
    StructField("victims_politicalPosition_id", StringType(), True),
    StructField("victims_politicalPosition_name", StringType(), True),
    StructField("victims_politicalPosition_type", StringType(), True),
    StructField("victims_politicalStatus_id", StringType(), True),
    StructField("victims_politicalStatus_name", StringType(), True),
    StructField("victims_politicalStatus_type", StringType(), True),
    StructField("victims_serviceStatus_id", StringType(), True),
    StructField("victims_serviceStatus_name", StringType(), True),
    StructField("victims_serviceStatus_type", StringType(), True),
    StructField("contextInfo_complementaryReasons_id", StringType(), True),
    StructField("contextInfo_complementaryReasons_name", StringType(), True),
    StructField("victims_circumstances_id", StringType(), True),
    StructField("victims_circumstances_name", StringType(), True),
    StructField("victims_circumstances_type", StringType(), True),
    StructField("victims_qualifications_id", StringType(), True),
    StructField("victims_qualifications_name", StringType(), True),
    StructField("victims_qualifications_type", StringType(), True),
    StructField("contextInfo_clippings_id", StringType(), True),
    StructField("contextInfo_clippings_name", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,df_fogo_cruzado formato json
df_raw = spark.read.format("json") \
    .option("multiline", "true") \
    .schema(schema) \
    .load(f"/Volumes/raw/fogo-cruzado/s3-fogo-cruzado-cdc/{sigla}")

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

result.write.format('delta').mode('overwrite').saveAsTable(f'bronze.fogo_cruzado.{table_name}')
