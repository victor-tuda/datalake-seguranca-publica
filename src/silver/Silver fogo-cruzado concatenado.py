# Databricks notebook source
# Check if the table exists
if spark.catalog.tableExists("silver.fogo_cruzado.rj_pe"):
    df_fogo_cruzado_concatenado = spark.sql(f"""
                                            SELECT * EXCEPT (sub_bairro) FROM silver.fogo_cruzado.rj
                                            UNION ALL
                                            SELECT * EXCEPT (subNeighborhood_id,subNeighborhood_name) FROM silver.fogo_cruzado.pe
                                            """)
    
    df_fogo_cruzado_concatenado.write.format('delta').mode('overwrite').saveAsTable(f'silver.fogo_cruzado.rj_pe')
else:
    spark.sql(f"""
        CREATE TABLE silver.fogo_cruzado.rj_pe AS
            SELECT * EXCEPT (subNeighborhood) FROM silver.fogo_cruzado.rj
            UNION ALL
            SELECT * EXCEPT (subNeighborhood_id,subNeighborhood_name) FROM silver.fogo_cruzado.pe ;
    """)

