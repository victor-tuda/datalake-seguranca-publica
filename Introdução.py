# Databricks notebook source
# MAGIC %md
# MAGIC # Data Lakehouse Segurança Pública

# COMMAND ----------

# MAGIC %md
# MAGIC **Fontes:**<br>
# MAGIC [Basedosdados](https://basedosdados.org/)<br>
# MAGIC [Fogo Cruzado](https://fogocruzado.org.br/)<br>
# MAGIC [Ministério da Saúde | Sistema de Informação sobre Mortalidade – SIM](https://dados.gov.br/dados/conjuntos-dados/sim-1979-2019)<br>
# MAGIC [Código dos Municípios - IBGE](https://www.ibge.gov.br/explica/codigos-dos-municipios.php)<br>
# MAGIC
# MAGIC **Repositório:**<br>
# MAGIC [Github](https://github.com/victor-tuda/datalake-seguranca-publica/tree/feat/ingestion_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Qualidade dos dados fornecidos pelas ONGs

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Caso Prefeito de Japeri
# MAGIC

# COMMAND ----------

# DBTITLE 1,Registro do prefeito encontrado
# MAGIC %sql
# MAGIC select id,
# MAGIC   data,
# MAGIC   endereco,
# MAGIC   nome_genero_vitimas,
# MAGIC   situacao_vitimas,
# MAGIC   tipo_pessoa_vitimas,
# MAGIC   nome_corporacao_vitimas,
# MAGIC   nome_posicao_politica_vitimas,
# MAGIC   nome_status_politico_vitimas,
# MAGIC   qualificacoes_vitimas
# MAGIC   from silver.fogo_cruzado.rj where id = '122092f0-c993-483b-849b-3c9e413a0e0d'

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.1.1 Notícia encontrada:
# MAGIC [RJ: candidato à prefeitura de Japeri é atacado](https://www.youtube.com/watch?v=1WBnk7JjEEo)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Caso Vereadora Magé
# MAGIC

# COMMAND ----------

# DBTITLE 1,Como a identificamos na tabela fogo-cruzado
# MAGIC %sql
# MAGIC select * from silver.fogo_cruzado.rj where id = '2d4817d1-e3a1-4c6f-8539-67781c201077'

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.2.1 Notícia Encontrada
# MAGIC [Execução de líder comunitária em Magé pode ter motivação política](https://oglobo.globo.com/rio/execucao-de-lider-comunitaria-em-mage-pode-ter-motivacao-politica-19700096)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingestão dos dados (estruturados e semi-estruturados)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Fogo Cruzado (JSON)

# COMMAND ----------

# MAGIC %md
# MAGIC `{
# MAGIC     "msg": "Success in filtering occurrences",
# MAGIC     "msgCode": "success",
# MAGIC     "code": 200,
# MAGIC     "pageMeta": {
# MAGIC         "page": 1,
# MAGIC         "take": 1,
# MAGIC         "itemCount": 10967,
# MAGIC         "pageCount": 10967,
# MAGIC         "hasPreviousPage": false,
# MAGIC         "hasNextPage": true
# MAGIC     },
# MAGIC     "data": [
# MAGIC         {
# MAGIC             "id": "2ae6b77d-3e79-43cb-b5b2-e7fee524886b",
# MAGIC             "documentNumber": 12623,
# MAGIC             "address": "Pixete, São Lourenço da Mata - PE, Brasil",
# MAGIC             "state": {
# MAGIC                 "id": "813ca36b-91e3-4a18-b408-60b27a1942ef",
# MAGIC                 "name": "Pernambuco"
# MAGIC             },
# MAGIC             "region": {
# MAGIC                 "id": "758e86d0-b5e0-410d-b8d8-b7061299be1a",
# MAGIC                 "region": "Nordeste",
# MAGIC                 "state": "Pernambuco",
# MAGIC                 "enabled": true
# MAGIC             },
# MAGIC             "city": {
# MAGIC                 "id": "3d11bf52-0213-4d6f-a607-77f172cbe3b6",
# MAGIC                 "name": "SAO LOURENCO DA MATA"
# MAGIC             },
# MAGIC             "neighborhood": {
# MAGIC                 "id": "a1018fde-b487-4a0d-90a5-37c75daae09e",
# MAGIC                 "name": "TIUMA"
# MAGIC             },
# MAGIC             "subNeighborhood": null,
# MAGIC             "locality": null,
# MAGIC             "latitude": "-7.9800434000",
# MAGIC             "longitude": "-35.0553350000",
# MAGIC             "date": "2018-04-01T00:00:00.000Z",
# MAGIC             "policeAction": false,
# MAGIC             "agentPresence": false,
# MAGIC             "relatedRecord": null,
# MAGIC             "contextInfo": {
# MAGIC                 "mainReason": {
# MAGIC                     "id": "baa3b299-67ad-41d2-aaf0-23ec8288cadb",
# MAGIC                     "name": "Homicidio/Tentativa"
# MAGIC                 },
# MAGIC                 "complementaryReasons": [],
# MAGIC                 "clippings": [],
# MAGIC                 "massacre": false,
# MAGIC                 "policeUnit": ""
# MAGIC             },
# MAGIC             "transports": [],
# MAGIC             "victims": [
# MAGIC                 {
# MAGIC                     "id": "4455a540-e568-4af3-a7d4-e363c1db913c",
# MAGIC                     "occurrenceId": "2ae6b77d-3e79-43cb-b5b2-e7fee524886b",
# MAGIC                     "type": "People",
# MAGIC                     "situation": "Dead",
# MAGIC                     "circumstances": [],
# MAGIC                     "deathDate": "2018-04-01T00:00:00.000Z",
# MAGIC                     "personType": "Civilian",
# MAGIC                     "age": null,
# MAGIC                     "ageGroup": {
# MAGIC                         "id": "1247dd9f-6796-495f-91c4-ca8d6db45c5e",
# MAGIC                         "name": "Adulto"
# MAGIC                     },
# MAGIC                     "genre": {
# MAGIC                         "id": "bd0504ee-e0b5-4ae1-ae12-b6d9c61802e1",
# MAGIC                         "name": "Homem cis"
# MAGIC                     },
# MAGIC                     "race": "Não identificado",
# MAGIC                     "place": {
# MAGIC                         "id": "e5c2090b-694f-4ff9-8f53-66a496193319",
# MAGIC                         "name": "Sem identificação"
# MAGIC                     },
# MAGIC                     "serviceStatus": {
# MAGIC                         "id": "7304cf1f-f7a3-4108-87b8-edf8f99a56c0",
# MAGIC                         "name": "Não se aplica",
# MAGIC                         "type": "Servico"
# MAGIC                     },
# MAGIC                     "qualifications": [],
# MAGIC                     "politicalPosition": {
# MAGIC                         "id": "84bb4841-1cac-4592-ba57-21714aa84454",
# MAGIC                         "name": "Não se aplica",
# MAGIC                         "type": "Politico"
# MAGIC                     },
# MAGIC                     "politicalStatus": {
# MAGIC                         "id": "5238a823-cab9-47f5-85bc-34b810ae0d71",
# MAGIC                         "name": "Não se aplica",
# MAGIC                         "type": "Politico"
# MAGIC                     },
# MAGIC                     "partie": null,
# MAGIC                     "coorporation": {
# MAGIC                         "id": "52e03783-cb08-49b5-8635-0d462773e250",
# MAGIC                         "name": "Não se aplica"
# MAGIC                     },
# MAGIC                     "agentPosition": {
# MAGIC                         "id": "c27a6a83-bea5-4699-b6d9-2b1be0372701",
# MAGIC                         "name": "Não se aplica",
# MAGIC                         "type": "Agente"
# MAGIC                     },
# MAGIC                     "agentStatus": {
# MAGIC                         "id": "940eb1bd-6f7f-4f73-9d6e-b44eb4fdd791",
# MAGIC                         "name": "Não se aplica",
# MAGIC                         "type": "Agente"
# MAGIC                     },
# MAGIC                     "unit": ""
# MAGIC                 }
# MAGIC             ],
# MAGIC             "animalVictims": []
# MAGIC         }
# MAGIC     ]
# MAGIC }`

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Data SUS (CSV)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.data_sus.data_sus LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC [Tradução da tabela Data SUS](https://diaad.s3.sa-east-1.amazonaws.com/sim/Mortalidade_Geral+-+Estrutura.pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Arquitetura (medalhão)

# COMMAND ----------

# MAGIC %md
# MAGIC [S3](https://us-west-2.console.aws.amazon.com/s3/buckets/victor-datalake-seguranca?region=us-west-2&bucketType=general&tab=objects)<br>
# MAGIC [Linhagem das Tabelas](https://dbc-b10a096c-ef22.cloud.databricks.com/explore/data/silver/fogo_cruzado/rj_pe?o=2859800074602817&activeTab=lineage)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Automação do fluxo

# COMMAND ----------

# MAGIC %md
# MAGIC [S3](https://us-west-2.console.aws.amazon.com/s3/upload/victor-datalake-seguranca?region=us-west-2&bucketType=general&prefix=fogo-cruzado/full-load/RJ/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resultados

# COMMAND ----------

# MAGIC %md
# MAGIC [Dashboard](https://dbc-b10a096c-ef22.cloud.databricks.com/dashboardsv3/01ef81c4280512a89502b89a3c865284/published?o=2859800074602817&view=fullscreen)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_fix_grammar(
# MAGIC   "Trabalho em uma empresa de fabricação de maquinas e painéis, e estamos desenvolvendo nossa propria maquina para corte de painel. Nessa maquina vai cortar paineis e fazer furaçõa apenas. Gostaria de saber se vcs tem algum software que gerasse os codigos G para esse tipo de serviço?"
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC Anterior:<br>
# MAGIC Trabalho em uma empresa de fabricação de maquinas e painéis, e estamos desenvolvendo nossa propria maquina para corte de painel. Nessa maquina vai cortar paineis e fazer furaçõa apenas. Gostaria de saber se vcs tem algum software que gerasse os codigos G para esse tipo de serviço?"
# MAGIC
# MAGIC Formatado:<br>
# MAGIC Trabalho em uma empresa de fabricação de máquinas e painéis, e estamos desenvolvendo nossa própria máquina para corte de painel. Nessa máquina vai cortar painéis e fazer furações apenas. Gostaria de saber se vocês têm algum software que gere os códigos G para esse tipo de serviço?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_extract("Trabalho em uma empresa de fabricação de máquinas e painéis, e estamos desenvolvendo nossa própria máquina para corte de painel. Nessa máquina vai cortar painéis e fazer furações apenas. Gostaria de saber se vocês têm algum software que gere os códigos G para esse tipo de serviço?", array("empresa", "cotação"))

# COMMAND ----------

# MAGIC %pip install geopy

# COMMAND ----------

from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="victor.gtuda@gmail.com")
location = geolocator.geocode("Avenida General Osório 644, Sorocaba São Paulo")
print(location.latitude)
print(location.longitude)

# COMMAND ----------

df = spark.sql('select * from bronze.crm.accounts')
df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.fogo_cruzado.rj
