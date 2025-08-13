-- ================================================================
-- ANÁLISE E CORREÇÃO DA TABELA FATO - Data Warehouse CAPES
-- Data: 05/08/2025
-- Objetivo: Simplificar FATO para métricas corretas
-- ================================================================

-- ================================================================
-- 1. ANÁLISE DA ESTRUTURA ATUAL DA FATO
-- ================================================================

-- 1.1 Verificar estrutura atual da fato_pos_graduacao
SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns 
WHERE table_name = 'fato_pos_graduacao'
ORDER BY ordinal_position;

-- 1.2 Verificar amostra de dados atuais
SELECT * FROM fato_pos_graduacao LIMIT 5;

-- ================================================================
-- 2. MÉTRICAS DESEJADAS - CONSULTAS PARA VALIDAR DADOS RAW
-- ================================================================

-- 2.1 Quantidade de temas por UF (dados RAW)
SELECT 
    rt.uf,
    COUNT(DISTINCT rt.tema_id) as qtd_temas_por_uf
FROM raw_tema rt
GROUP BY rt.uf
ORDER BY qtd_temas_por_uf DESC;

-- 2.2 Quantidade de temas por categoria administrativa (dados RAW)
SELECT 
    CASE 
        WHEN ri.status_juridico_ppg = 'Particular' THEN 'PRIVADO'
        ELSE 'PÚBLICO'
    END as categoria_administrativa,
    COUNT(DISTINCT rt.tema_id) as qtd_temas_por_categoria
FROM raw_tema rt
    CROSS JOIN raw_ies ri
WHERE rt.uf = ri.uf  -- Temas e IES na mesma UF
GROUP BY 
    CASE 
        WHEN ri.status_juridico_ppg = 'Particular' THEN 'PRIVADO'
        ELSE 'PÚBLICO'
    END
ORDER BY qtd_temas_por_categoria DESC;

-- 2.3 Quantidade de temas por IES específica (dados RAW)
SELECT 
    ri.nome_ies,
    ri.uf,
    ri.status_juridico_ppg,
    COUNT(DISTINCT rt.tema_id) as qtd_temas_por_ies
FROM raw_tema rt
    CROSS JOIN raw_ies ri
WHERE rt.uf = ri.uf  -- Temas e IES na mesma UF
GROUP BY ri.nome_ies, ri.uf, ri.status_juridico_ppg
ORDER BY qtd_temas_por_ies DESC
LIMIT 20;

-- 2.4 Quantidade de temas por região (dados RAW)
SELECT 
    dl.regiao,
    COUNT(DISTINCT rt.tema_id) as qtd_temas_por_regiao
FROM raw_tema rt
    JOIN dim_localidade dl ON rt.uf = dl.nome_uf
GROUP BY dl.regiao
ORDER BY qtd_temas_por_regiao DESC;

-- ================================================================
-- 3. PROPOSTA DE NOVA ESTRUTURA SIMPLIFICADA DA FATO
-- ================================================================

-- 3.1 Nova tabela FATO simplificada (CREATE)
/*
CREATE TABLE fato_temas_simplificada (
    fato_id SERIAL PRIMARY KEY,
    
    -- Chaves estrangeiras
    tema_sk INTEGER REFERENCES dim_tema(tema_sk),
    ies_sk INTEGER REFERENCES dim_ies(ies_sk),
    localidade_sk INTEGER REFERENCES dim_localidade(localidade_sk),
    tempo_sk INTEGER REFERENCES dim_tempo(tempo_sk),
    
    -- Métricas simples
    qtd_tema_uf INTEGER DEFAULT 1,           -- 1 para cada tema presente na UF
    qtd_tema_categoria INTEGER DEFAULT 1,    -- 1 para cada tema em categoria (público/privado)
    qtd_tema_ies INTEGER DEFAULT 1,          -- 1 para cada tema presente na IES
    qtd_tema_regiao INTEGER DEFAULT 1,       -- 1 para cada tema presente na região
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
*/

-- 3.2 Consulta para popular nova FATO simplificada
/*
INSERT INTO fato_temas_simplificada (
    tema_sk, ies_sk, localidade_sk, tempo_sk,
    qtd_tema_uf, qtd_tema_categoria, qtd_tema_ies, qtd_tema_regiao
)
SELECT DISTINCT
    dt.tema_sk,
    di.ies_sk,
    dl.localidade_sk,
    (SELECT tempo_sk FROM dim_tempo WHERE ano = 2025 LIMIT 1) as tempo_sk,
    1 as qtd_tema_uf,
    1 as qtd_tema_categoria, 
    1 as qtd_tema_ies,
    1 as qtd_tema_regiao
FROM raw_tema rt
    JOIN dim_tema dt ON rt.tema_id = dt.tema_id AND rt.tema = dt.nome_tema
    CROSS JOIN raw_ies ri
    JOIN dim_ies di ON ri.ies_sk = di.ies_sk
    JOIN dim_localidade dl ON rt.uf = dl.nome_uf
WHERE rt.uf = ri.uf;  -- Associar temas e IES na mesma UF
*/

-- ================================================================
-- 4. CONSULTAS DE VALIDAÇÃO PARA NOVA FATO
-- ================================================================

-- 4.1 Qtd temas por UF (nova FATO)
/*
SELECT 
    dl.sigla_uf,
    dl.nome_uf,
    COUNT(DISTINCT dt.tema_id) as qtd_temas_uf
FROM fato_temas_simplificada f
    JOIN dim_tema dt ON f.tema_sk = dt.tema_sk
    JOIN dim_localidade dl ON f.localidade_sk = dl.localidade_sk
GROUP BY dl.sigla_uf, dl.nome_uf
ORDER BY qtd_temas_uf DESC;
*/

-- 4.2 Qtd temas por categoria administrativa (nova FATO)
/*
SELECT 
    di.categoria_administrativa,
    COUNT(DISTINCT dt.tema_id) as qtd_temas_categoria
FROM fato_temas_simplificada f
    JOIN dim_tema dt ON f.tema_sk = dt.tema_sk
    JOIN dim_ies di ON f.ies_sk = di.ies_sk
GROUP BY di.categoria_administrativa
ORDER BY qtd_temas_categoria DESC;
*/

-- 4.3 Qtd temas por IES (nova FATO)
/*
SELECT 
    di.nome_ies,
    di.categoria_administrativa,
    dl.sigla_uf,
    COUNT(DISTINCT dt.tema_id) as qtd_temas_ies
FROM fato_temas_simplificada f
    JOIN dim_tema dt ON f.tema_sk = dt.tema_sk
    JOIN dim_ies di ON f.ies_sk = di.ies_sk
    JOIN dim_localidade dl ON f.localidade_sk = dl.localidade_sk
GROUP BY di.nome_ies, di.categoria_administrativa, dl.sigla_uf
ORDER BY qtd_temas_ies DESC
LIMIT 20;
*/

-- 4.4 Qtd temas por região (nova FATO)
/*
SELECT 
    dl.regiao,
    COUNT(DISTINCT dt.tema_id) as qtd_temas_regiao
FROM fato_temas_simplificada f
    JOIN dim_tema dt ON f.tema_sk = dt.tema_sk
    JOIN dim_localidade dl ON f.localidade_sk = dl.localidade_sk
GROUP BY dl.regiao
ORDER BY qtd_temas_regiao DESC;
*/
