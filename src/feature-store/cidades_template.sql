WITH tb_base_ativa AS (
  SELECT *
  FROM silver.fogo_cruzado.rj_pe
  WHERE data < '{dt_ref}'
  AND data > '{dt_ref}' - INTERVAL 28 DAY
),

tb_mortos_feridos AS(
  SELECT id_cidade,
  COUNT(DISTINCT id) as qtd_ocorrencias,
  COUNT(CASE WHEN situacao_vitimas = 'Wounded' THEN id END) as qtd_feridos,
  COUNT(CASE WHEN (situacao_vitimas = 'Wounded' AND tipo_pessoa_vitimas != 'Agent') THEN id END) as qtd_feridos_civis,
  COUNT(CASE WHEN (situacao_vitimas = 'Wounded' AND tipo_pessoa_vitimas = 'Agent') THEN id END) as qtd_feridos_agentes,
  COUNT(CASE WHEN situacao_vitimas = 'Dead' THEN id END) as qtd_mortos,
  COUNT(CASE WHEN (situacao_vitimas = 'Dead' AND tipo_pessoa_vitimas != 'Agent') THEN id END) as qtd_mortos_civis,
  COUNT(CASE WHEN (situacao_vitimas = 'Dead' AND tipo_pessoa_vitimas = 'Agent') THEN id END) as qtd_mortos_agentes
  FROM tb_base_ativa
  GROUP BY ALL
)

SELECT  '{dt_ref}' AS dt_ref,
        t2.*
FROM tb_base_ativa AS t1
LEFT JOIN tb_mortos_feridos AS t2 ON t1.id_cidade = t2.id_cidade
GROUP BY ALL
