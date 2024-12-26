# Databricks notebook source
# Check if the table exists
if spark.catalog.tableExists("silver.fogo_cruzado.rj_pe"):
    df_fogo_cruzado_concatenado = spark.sql(f"""SELECT * FROM silver.fogo_cruzado.rj AS t1
                                            FULL JOIN silver.fogo_cruzado.pe AS t2 ON t1.id = t2.id""")
    df_fogo_cruzado_concatenado.write.format('delta').mode('overwrite').saveAsTable(f'silver.fogo_cruzado.rj_pe')
else:
    spark.sql(f"""
        CREATE TABLE silver.fogo_cruzado.rj_pe AS
            SELECT * FROM silver.fogo_cruzado.rj
            UNION ALL
            SELECT * FROM silver.fogo_cruzado.pe;
    """)

