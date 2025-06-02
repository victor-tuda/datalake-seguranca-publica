# Databricks notebook source
# DBTITLE 1,Configuração do job
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table_name")
sigla = table_name.upper()

# COMMAND ----------

# DBTITLE 1,Definindo schema
from pyspark.sql.types import *

schema = StructType([
    StructField("msg", StringType(), True),
    StructField("msgCode", StringType(), True),
    StructField("code", IntegerType(), True),
    StructField("pageMeta", StructType([
        StructField("page", IntegerType(), True),
        StructField("take", IntegerType(), True),
        StructField("itemCount", IntegerType(), True),
        StructField("pageCount", IntegerType(), True),
        StructField("hasPreviousPage", BooleanType(), True),
        StructField("hasNextPage", BooleanType(), True),
    ]), True),
    StructField("data", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("documentNumber", StringType(), True),
        StructField("address", StringType(), True),
        StructField("state", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
        ]), True),
        StructField("region", StructType([
            StructField("id", StringType(), True),
            StructField("region", StringType(), True),
            StructField("state", StringType(), True),
            StructField("enabled", BooleanType(), True),
        ]), True),
        StructField("city", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
        ]), True),
        StructField("neighborhood", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
        ]), True),
        StructField("subNeighborhood", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
        ]), True),
        StructField("locality", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
        ]), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("date", TimestampType(), True),  # can be TimestampType if converted
        StructField("policeAction", BooleanType(), True),
        StructField("agentPresence", BooleanType(), True),
        StructField("relatedRecord", StringType(), True),
        StructField("contextInfo", StructType([
            StructField("mainReason", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]), True),
            StructField("complementaryReasons", ArrayType(StringType()), True),
            StructField("clippings", ArrayType(StringType()), True),
            StructField("massacre", BooleanType(), True),
            StructField("policeUnit", StringType(), True),
        ]), True),
        StructField("transports", ArrayType(StringType()), True),
        StructField("victims", ArrayType(StructType([
            StructField("id", StringType(), True),
            StructField("occurrenceId", StringType(), True),
            StructField("type", StringType(), True),
            StructField("situation", StringType(), True),
            StructField("circumstances", ArrayType(StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("type", StringType(), True),
            ])), True),
            StructField("deathDate", TimestampType(), True),  # can be TimestampType if converted
            StructField("personType", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("ageGroup", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]), True),
            StructField("genre", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]), True),
            StructField("race", StringType(), True),
            StructField("place", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]), True),
            StructField("serviceStatus", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("type", StringType(), True),
            ]), True),
            StructField("qualifications", ArrayType(StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("type", StringType(), True),
            ])), True),
            StructField("politicalPosition", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("type", StringType(), True),
            ]), True),
            StructField("politicalStatus", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("type", StringType(), True),
            ]), True),
            StructField("partie", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("type", StringType(), True),
            ]), True),
            StructField("coorporation", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]), True),
            StructField("agentPosition", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("type", StringType(), True),
            ]), True),
            StructField("agentStatus", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("type", StringType(), True),
            ]), True),
            StructField("unit", StringType(), True),
        ])), True),
        StructField("animalVictims", ArrayType(StringType()), True),
    ])), True),
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

from delta.tables import DeltaTable

# Load the existing Delta table
delta_table = DeltaTable.forName(spark, f"bronze.fogo_cruzado.{sigla}")

# Perform the merge (UPSERT)
(
    delta_table.alias("target")
    .merge(
        result.alias("source"),
        """
        target.victims_id = source.victims_id AND
        target.contextInfo_mainReason_id = source.contextInfo_mainReason_id AND
        target.victims_circumstances_id = source.victims_circumstances_id AND
        target.victims_qualifications_id = source.victims_qualifications_id AND
        target.contextInfo_clippings = source.contextInfo_clippings
        """
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)


# COMMAND ----------

#result.write.format('delta').mode('append').saveAsTable(f'bronze.fogo_cruzado.{table_name}')

# COMMAND ----------

dbutils.fs.mv(f's3://victor-datalake-seguranca/fogo-cruzado/cdc/{sigla}/', f's3://victor-datalake-seguranca/fogo-cruzado/processed/{sigla}/', recurse=True)
