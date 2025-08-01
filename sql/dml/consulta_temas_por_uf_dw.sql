-- =====================================================
-- Consulta 2: Quantidade de temas por UF - Data Warehouse
-- =====================================================
-- Esta consulta conta temas únicos por UF usando as tabelas do DW
-- (tabela fato + dimensões) que devem retornar o mesmo resultado

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

-- Resultado esperado: Mesmo resultado da consulta anterior
-- 27 UFs com a mesma distribuição de temas da tabela raw_tema
