
SELECT
    dl.nome_uf,
    COUNT(DISTINCT dt.tema_id) as total_temas,
    COUNT(*) as total_registros_fato
FROM fato_pos_graduacao f
    INNER JOIN dim_tema dt ON f.tema_sk = dt.tema_sk
    INNER JOIN dim_localidade dl ON f.localidade_sk = dl.localidade_sk
WHERE f.tema_sk > 0  -- Excluir registros com tema "Desconhecido"
GROUP BY dl.nome_uf
ORDER BY dl.nome_uf;

SELECT
  dt.macrotema,
  COUNT(DISTINCT dt.tema) AS qtd_temas,
  COUNT(DISTINCT dt.palavrachave) AS qtd_palavra_chave
FROM
  fato_pos_graduacao f
JOIN
  dim_tema dt ON f.tema_sk = dt.tema_sk
GROUP BY
  dt.macrotema
ORDER BY
  qtd_palavra_chave DESC;