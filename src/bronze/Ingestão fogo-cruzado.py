# Databricks notebook source
# DBTITLE 1,Configuração do job
catalog = "bronze"
schema = "fogo_cruzado"
table_name = dbutils.widgets.get("table_name")
id_field = dbutils.widgets.get("id_field")

# COMMAND ----------

# DBTITLE 1,df_fogo_cruzado_rj
df_fogo_cruzado_rj_json = spark.read.format("json").option("multiline", "true").load("/Volumes/raw/fogo-cruzado/s3-fogo-cruzado/RJ/full_load_1_2024_09_28.json")

# COMMAND ----------

# DBTITLE 1,df_fogo_cruzado_pe
df_fogo_cruzado_pe_json = spark.read.format("json").option("multiline", "true").load("/Volumes/raw/fogo-cruzado/s3-fogo-cruzado/PE/full_load_1_2024_09_28.json")

# COMMAND ----------

# DBTITLE 1,Convertendo json para o formato tabular
from pyspark.sql.functions import explode, col, explode_outer

df_fogo_cruzado_rj = df_fogo_cruzado_rj_json.withColumn('data1', explode('data')) \
    .withColumn('id', col('data1.id')) \
    .withColumn('documentNumber', col('data1.documentNumber')) \
    .withColumn('date', col('data1.date')) \
    .withColumn('agentPresence', col('data1.agentPresence')) \
    .withColumn('address', col('data1.address')) \
    .withColumn('latitude', col('data1.latitude')) \
    .withColumn('longitude', col('data1.longitude')) \
    .withColumn('relatedRecord', col('data1.relatedRecord')) \
    \
    .withColumn('locality_id', col('data1.locality.id')) \
    .withColumn('locality_name', col('data1.locality.name')) \
    \
    .withColumn('region_id', col('data1.region.id')) \
    .withColumn('region_region', col('data1.region.region')) \
    .withColumn('region_enabled', col('data1.region.enabled')) \
    .withColumn('region_state', col('data1.region.state')) \
    \
    .withColumn('state_id', col('data1.state.id')) \
    .withColumn('state_name', col('data1.state.name')) \
    \
    .withColumn('city_id', col('data1.city.id')) \
    .withColumn('city_name', col('data1.city.name')) \
    \
    \
    \
    .withColumn('victims1', explode_outer('data1.victims')) \
    .withColumn('victims_id', col('victims1.id')) \
    .withColumn('victims_occurrenceId', col('victims1.occurrenceId')) \
    .withColumn('victims_age', col('victims1.age')) \
    .withColumn('victims_deathDate', col('victims1.deathDate')) \
    .withColumn('victims_personType', col('victims1.personType')) \
    .withColumn('victims_race', col('victims1.race')) \
    .withColumn('victims_situation', col('victims1.situation')) \
    .withColumn('victims_type', col('victims1.type')) \
    .withColumn('victims_unit', col('victims1.unit')) \
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
    .withColumn('victims_partie_id', col('victims1.partie.id')) \
    .withColumn('victims_partie_name', col('victims1.partie.name')) \
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
    .withColumn('contextInfo_policeUnit', col('data1.contextInfo.policeUnit')) \
    .withColumn('contextInfo_massacre', col('data1.contextInfo.massacre')) \
    \
    .withColumn('contextInfo_mainReason_id', col('data1.contextInfo.mainReason.id')) \
    .withColumn('contextInfo_mainReason_name', col('data1.contextInfo.mainReason.name')) \
    \
    .withColumn('contextInfo_complementaryReasons_id', col('data1.contextInfo.complementaryReasons.id')) \
    .withColumn('contextInfo_complementaryReasons_name', col('data1.contextInfo.complementaryReasons.name')) \
    \
    .withColumn('contextInfo_clippings_id', col('data1.contextInfo.clippings.id')) \
    .withColumn('contextInfo_clippings_name', col('data1.contextInfo.clippings.name')) \
    \
    .withColumn('neighborhood_id', col('data1.neighborhood.id')) \
    .withColumn('neighborhood_name', col('data1.neighborhood.name')) \
    \
    .withColumn('subNeighborhood_id', col('data1.subNeighborhood.id')) \
    .withColumn('subNeighborhood_name', col('data1.subNeighborhood.name')) \
    \
    .withColumn('transports1', explode_outer('data1.transports')) \
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
    .withColumn('animalVictims1', explode_outer('data1.animalVictims')) \
    .withColumn('animalVictims_id', col('animalVictims1.id')) \
    .withColumn('animalVictims_name', col('animalVictims1.name')) \
    .withColumn('animalVictims_occurrenceId', col('animalVictims1.occurrenceId')) \
    .withColumn('animalVictims_situation', col('animalVictims1.situation')) \
    .withColumn('animalVictims_type', col('animalVictims1.type')) \
    \
    .drop('data', 'code', 'msg', 'msgCode', 'pageMeta', 'data1', 'victims1', 'transports1', 'animalVictims1') \
    

# COMMAND ----------

from pyspark.sql.functions import explode, col, explode_outer

df_fogo_cruzado_pe = df_fogo_cruzado_pe_json.withColumn('data1', explode('data')) \
    .withColumn('id', col('data1.id')) \
    .withColumn('documentNumber', col('data1.documentNumber')) \
    .withColumn('date', col('data1.date')) \
    .withColumn('agentPresence', col('data1.agentPresence')) \
    .withColumn('address', col('data1.address')) \
    .withColumn('latitude', col('data1.latitude')) \
    .withColumn('longitude', col('data1.longitude')) \
    .withColumn('relatedRecord', col('data1.relatedRecord')) \
    \
    .withColumn('locality_id', col('data1.locality.id')) \
    .withColumn('locality_name', col('data1.locality.name')) \
    \
    .withColumn('region_id', col('data1.region.id')) \
    .withColumn('region_region', col('data1.region.region')) \
    .withColumn('region_enabled', col('data1.region.enabled')) \
    .withColumn('region_state', col('data1.region.state')) \
    \
    .withColumn('state_id', col('data1.state.id')) \
    .withColumn('state_name', col('data1.state.name')) \
    \
    .withColumn('city_id', col('data1.city.id')) \
    .withColumn('city_name', col('data1.city.name')) \
    \
    \
    \
    .withColumn('victims1', explode_outer('data1.victims')) \
    .withColumn('victims_id', col('victims1.id')) \
    .withColumn('victims_occurrenceId', col('victims1.occurrenceId')) \
    .withColumn('victims_age', col('victims1.age')) \
    .withColumn('victims_deathDate', col('victims1.deathDate')) \
    .withColumn('victims_personType', col('victims1.personType')) \
    .withColumn('victims_race', col('victims1.race')) \
    .withColumn('victims_situation', col('victims1.situation')) \
    .withColumn('victims_type', col('victims1.type')) \
    .withColumn('victims_unit', col('victims1.unit')) \
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
    .withColumn('victims_partie_id', col('victims1.partie.id')) \
    .withColumn('victims_partie_name', col('victims1.partie.name')) \
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
    .withColumn('contextInfo_policeUnit', col('data1.contextInfo.policeUnit')) \
    .withColumn('contextInfo_massacre', col('data1.contextInfo.massacre')) \
    \
    .withColumn('contextInfo_mainReason_id', col('data1.contextInfo.mainReason.id')) \
    .withColumn('contextInfo_mainReason_name', col('data1.contextInfo.mainReason.name')) \
    \
    .withColumn('contextInfo_complementaryReasons_id', col('data1.contextInfo.complementaryReasons.id')) \
    .withColumn('contextInfo_complementaryReasons_name', col('data1.contextInfo.complementaryReasons.name')) \
    \
    .withColumn('contextInfo_clippings_id', col('data1.contextInfo.clippings.id')) \
    .withColumn('contextInfo_clippings_name', col('data1.contextInfo.clippings.name')) \
    \
    .withColumn('neighborhood_id', col('data1.neighborhood.id')) \
    .withColumn('neighborhood_name', col('data1.neighborhood.name')) \
    \
    .withColumn('subNeighborhood_id', col('data1.subNeighborhood.id')) \
    .withColumn('subNeighborhood_name', col('data1.subNeighborhood.name')) \
    \
    .withColumn('transports1', explode_outer('data1.transports')) \
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
    .withColumn('animalVictims1', explode_outer('data1.animalVictims')) \
    .withColumn('animalVictims_id', col('animalVictims1.id')) \
    .withColumn('animalVictims_name', col('animalVictims1.name')) \
    .withColumn('animalVictims_occurrenceId', col('animalVictims1.occurrenceId')) \
    .withColumn('animalVictims_situation', col('animalVictims1.situation')) \
    .withColumn('animalVictims_type', col('animalVictims1.type')) \
    \
    .drop('data', 'code', 'msg', 'msgCode', 'pageMeta', 'data1', 'victims1', 'transports1', 'animalVictims1') \
    

    

# COMMAND ----------

df_fogo_cruzado_rj.write.format('delta').mode('overwrite').saveAsTable('bronze.fogo_cruzado.rj')
df_fogo_cruzado_pe.write.format('delta').mode('overwrite').saveAsTable('bronze.fogo_cruzado.pe')
