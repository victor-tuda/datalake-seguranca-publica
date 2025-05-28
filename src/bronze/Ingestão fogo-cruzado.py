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
from pyspark.sql.functions import explode
df_raw_exploded = df_raw.withColumn('explodedContent', explode('data')).drop('data')

# COMMAND ----------

df_raw_exploded.display()

# COMMAND ----------

# DBTITLE 1,Convertendo json para o formato tabular
from pyspark.sql.functions import col, explode_outer

df_raw_with_columns = df_raw_exploded \
    .withColumn('id', col('explodedContent.id')) \
    .withColumn('documentNumber', col('explodedContent.documentNumber')) \
    .withColumn('date', col('explodedContent.date')) \
    .withColumn('agentPresence', col('explodedContent.agentPresence')) \
    .withColumn('address', col('explodedContent.address')) \
    .withColumn('latitude', col('explodedContent.latitude')) \
    .withColumn('longitude', col('explodedContent.longitude')) \
    .withColumn('relatedRecord', col('explodedContent.relatedRecord')) \
    \
    .withColumn('locality_id', col('explodedContent.locality.id')) \
    .withColumn('locality_name', col('explodedContent.locality.name')) \
    \
    .withColumn('region_id', col('explodedContent.region.id')) \
    .withColumn('region_region', col('explodedContent.region.region')) \
    .withColumn('region_enabled', col('explodedContent.region.enabled')) \
    .withColumn('region_state', col('explodedContent.region.state')) \
    \
    .withColumn('state_id', col('explodedContent.state.id')) \
    .withColumn('state_name', col('explodedContent.state.name')) \
    \
    .withColumn('city_id', col('explodedContent.city.id')) \
    .withColumn('city_name', col('explodedContent.city.name')) \
    \
    \
    \
    .withColumn('victims1', explode_outer('explodedContent.victims')) \
    .withColumn('victims_id', col('victims1.id')) \
    .withColumn('victims_occurrenceId', col('victims1.occurrenceId')) \
    .withColumn('victims_age', col('victims1.age')) \
    .withColumn('victims_deathDate', col('victims1.deathDate')) \
    .withColumn('victims_personType', col('victims1.personType')) \
    .withColumn('victims_race', col('victims1.race')) \
    .withColumn('victims_situation', col('victims1.situation')) \
    .withColumn('victims_type', col('victims1.type')) \
    .withColumn('victims_unit', col('victims1.unit')) \
    .withColumn('victims_party', col('victims1.unit')) \
    \
    .withColumn('victims_ageGroup_id', col('victims1.ageGroup.id')) \
    .withColumn('victims_ageGroup_name', col('victims1.ageGroup.name')) \
    \
    .withColumn('victims_agentPosition_id', col('victims1.agentPosition.id')) \
    .withColumn('victims_agentPosition_name', col('victims1.agentPosition.name')) \
    .withColumn('victims_agentPosition_type', col('victims1.agentPosition.type')) \
    \
    .withColumn('victims_agentStatus_id', col('victims1.agentStatus.id')) \
    .withColumn('victims_agentStatus_name', col('victims1.agentStatus.name')) \
    .withColumn('victims_agentStatus_type', col('victims1.agentStatus.type')) \
    \
    .withColumn('victims_coorporation_id', col('victims1.coorporation.id')) \
    .withColumn('victims_coorporation_name', col('victims1.coorporation.name')) \
    \
    .withColumn('victims_genre_id', col('victims1.genre.id')) \
    .withColumn('victims_genre_name', col('victims1.genre.name')) \
    \
    .withColumn('parties1', explode_outer('explodedContent.victims.partie')) \
    .withColumn('victims_partie_id', col('parties1.id')) \
    .withColumn('victims_partie_name', col('parties1.name')) \
    \
    .withColumn('victims_place_id', col('victims1.place.id')) \
    .withColumn('victims_place_name', col('victims1.place.name')) \
    \
    .withColumn('victims_politicalPosition_id', col('victims1.politicalPosition.id')) \
    .withColumn('victims_politicalPosition_name', col('victims1.politicalPosition.name')) \
    .withColumn('victims_politicalPosition_type', col('victims1.politicalPosition.type')) \
    \
    .withColumn('victims_politicalStatus_id', col('victims1.politicalStatus.id')) \
    .withColumn('victims_politicalStatus_name', col('victims1.politicalStatus.name')) \
    .withColumn('victims_politicalStatus_type', col('victims1.politicalStatus.type')) \
    \
    .withColumn('victims_serviceStatus_id', col('victims1.serviceStatus.id')) \
    .withColumn('victims_serviceStatus_name', col('victims1.serviceStatus.name')) \
    .withColumn('victims_serviceStatus_type', col('victims1.serviceStatus.type')) \
    \
    .withColumn('victims_qualifications', col('victims1.qualifications')) \
    \
    .withColumn('victims_circumstances', col('victims1.circumstances')) \
    \
    \
    \
    .withColumn('contextInfo_policeUnit', col('explodedContent.contextInfo.policeUnit')) \
    .withColumn('contextInfo_massacre', col('explodedContent.contextInfo.massacre')) \
    \
    .withColumn('contextInfo_mainReason_id', col('explodedContent.contextInfo.mainReason.id')) \
    .withColumn('contextInfo_mainReason_name', col('explodedContent.contextInfo.mainReason.name')) \
    \
    .withColumn('contextInfo_complementaryReasons_id', col('explodedContent.contextInfo.complementaryReasons.id')) \
    .withColumn('contextInfo_complementaryReasons_name', col('explodedContent.contextInfo.complementaryReasons.name')) \
    \
    .withColumn('contextInfo_clippings_id', col('explodedContent.contextInfo.clippings.id')) \
    .withColumn('contextInfo_clippings_name', col('explodedContent.contextInfo.clippings.name')) \
    \
    .withColumn('neighborhood_id', col('explodedContent.neighborhood.id')) \
    .withColumn('neighborhood_name', col('explodedContent.neighborhood.name')) \
    \
    .withColumn('subNeighborhood_id', col('explodedContent.subNeighborhood.id')) \
    .withColumn('subNeighborhood_name', col('explodedContent.subNeighborhood.name')) \
    \
    .withColumn('transports1', explode_outer('explodedContent.transports')) \
    .withColumn('transports_id', col('transports1.id')) \
    .withColumn('transports_interruptedTransport', col('transports1.interruptedTransport')) \
    .withColumn('transports_dateInterruption', col('transports1.dateInterruption')) \
    .withColumn('transports_occurrenceId', col('transports1.occurrenceId')) \
    .withColumn('transports_releaseDate', col('transports1.releaseDate')) \
    .withColumn('transports_transportDescription', col('transports1.transportDescription')) \
    \
    .withColumn('transport_id', col('transports1.transport.id')) \
    .withColumn('transport_name', col('transports1.transport.name')) \
    \
    .withColumn('animalVictims1', explode_outer('explodedContent.animalVictims')) \
    .withColumn('animalVictims_id', col('animalVictims1.id')) \
    .withColumn('animalVictims_name', col('animalVictims1.name')) \
    .withColumn('animalVictims_occurrenceId', col('animalVictims1.occurrenceId')) \
    .withColumn('animalVictims_situation', col('animalVictims1.situation')) \
    .withColumn('animalVictims_type', col('animalVictims1.type')) \
    \
    .drop('data', 'code', 'msg', 'msgCode', 'pageMeta', 'explodedContent', 'victims1', 'transports1', 'animalVictims1') \
    

# COMMAND ----------

df_raw_with_columns.write.format('delta').mode('overwrite').saveAsTable(f'bronze.fogo_cruzado.{table_name}')
