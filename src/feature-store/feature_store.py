# Databricks notebook source
# MAGIC %pip install databricks.feature_engineering tqdm

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from tqdm  import tqdm
import sys

sys.path.insert(0, '../lib')

import utils
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()


# COMMAND ----------

catalog = 'feature_store'
database = 'cidades'
table = 'cidades_life'
table_name = f'{catalog}.{database}.{table}'
primary_keys = ['id_cidade', 'dt_ref']
partition_by = 'dt_ref'

dt_start = '2024-06-01'
dt_stop = '2024-11-30'
monthly = False

dates = utils.range_date(dt_start, dt_stop, monthly)

query = utils.import_query('cidades_template.sql')

# COMMAND ----------

if not utils.tables_exists(spark, catalog, database, table):
  df = spark.sql(query.format(dt_ref=dates.pop(0)))

  fe.create_table(df = df,
                  name=table_name,
                  primary_keys=primary_keys,
                  partition_columns=partition_by
                  )
  
for d in tqdm(dates):
  df=spark.sql(query.format(dt_ref=d))
  fe.write_table(df=df, name=table_name, mode='merge')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from feature_store.cidades.cidades_life

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.fogo_cruzado.rj_pe

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Saber se houve morte de cada unidade policial no dia anterior */
# MAGIC
# MAGIC WITH tb_base_ativa AS (
# MAGIC   SELECT *
# MAGIC   FROM silver.fogo_cruzado.rj_pe
# MAGIC   WHERE data < '2024-12-19'
# MAGIC   AND data > '2024-12-19' - INTERVAL 28 DAY
# MAGIC ),
# MAGIC
# MAGIC tb_mortos_feridos AS(
# MAGIC   SELECT id_cidade,
# MAGIC   COUNT(DISTINCT id) as qtd_ocorrencias,
# MAGIC   COUNT(CASE WHEN situacao_vitimas = 'Wounded' THEN id END) as qtd_feridos,
# MAGIC   COUNT(CASE WHEN (situacao_vitimas = 'Wounded' AND tipo_pessoa_vitimas != 'Agent') THEN id END) as qtd_feridos_civis,
# MAGIC   COUNT(CASE WHEN (situacao_vitimas = 'Wounded' AND tipo_pessoa_vitimas = 'Agent') THEN id END) as qtd_feridos_agentes,
# MAGIC   COUNT(CASE WHEN situacao_vitimas = 'Dead' THEN id END) as qtd_mortos,
# MAGIC   COUNT(CASE WHEN (situacao_vitimas = 'Dead' AND tipo_pessoa_vitimas != 'Agent') THEN id END) as qtd_mortos_civis,
# MAGIC   COUNT(CASE WHEN (situacao_vitimas = 'Dead' AND tipo_pessoa_vitimas = 'Agent') THEN id END) as qtd_mortos_agentes,
# MAGIC
# MAGIC   /*Substituir a linha por uma nova view*/
# MAGIC   COUNT(CASE WHEN (situacao_vitimas = 'Wounded' AND tipo_pessoa_vitimas = 'Agent' AND array_contains(unidade_policial_contexto, 'M')) THEN id END) as qtd_mortos_pm
# MAGIC   FROM tb_base_ativa
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC
# MAGIC SELECT  'dt' AS dt_ref,
# MAGIC         t2.*
# MAGIC FROM tb_base_ativa AS t1
# MAGIC LEFT JOIN tb_mortos_feridos AS t2 ON t1.id_cidade = t2.id_cidade
# MAGIC GROUP BY ALL
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT DISTINCT unidade_policial_contexto
# MAGIC   FROM silver.fogo_cruzado.rj_pe

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table silver.fogo_cruzado.pe
