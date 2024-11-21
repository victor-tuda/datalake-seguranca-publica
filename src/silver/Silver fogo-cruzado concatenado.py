# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE  IF NOT EXISTS silver.fogo_cruzado.rj_pe AS
# MAGIC SELECT * FROM silver.fogo_cruzado.rj
# MAGIC UNION ALL
# MAGIC SELECT * FROM silver.fogo_cruzado.pe;
# MAGIC

# COMMAND ----------

# Check if the table exists
if spark.catalog.tableExists("silver.fogo_cruzado.rj_pe"):
    spark.sql(f"""
        CREATE TABLE  IF NOT EXISTS silver.fogo_cruzado.rj_pe AS
        SELECT * FROM silver.fogo_cruzado.rj
        UNION ALL
        SELECT * FROM silver.fogo_cruzado.pe;
    """)
else:
    print("test")
