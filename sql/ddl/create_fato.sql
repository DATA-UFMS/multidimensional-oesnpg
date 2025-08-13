-- ================================================================
-- CRIAÇÃO DA NOVA TABELA FATO 
-- Data Warehouse Observatório CAPES - Estrutura Corrigida
-- Data: 05/08/2025
-- ================================================================

-- ================================================================
-- 1. DROPPING TABELA FATO ATUAL (se necessário)
-- ================================================================
-- DROP TABLE IF EXISTS fato_pos_graduacao CASCADE;

-- ================================================================
-- 2. NOVA TABELA FATO 
-- ================================================================
CREATE TABLE fato_pos_graduacao (
    fato_id SERIAL PRIMARY KEY,
    
    -- Chaves estrangeiras (dimensões)
    tema_sk INTEGER NOT NULL,
    ies_sk INTEGER NOT NULL, 
    localidade_sk INTEGER NOT NULL,
    tempo_sk INTEGER NOT NULL,
    
    -- Métricas agregadas simples
    presente_na_uf INTEGER DEFAULT 1,          -- 1 se tema está presente na UF
    presente_na_categoria INTEGER DEFAULT 1,    -- 1 se tema está na categoria (público/privado)
    presente_na_ies INTEGER DEFAULT 1,          -- 1 se tema está presente na IES
    presente_na_regiao INTEGER DEFAULT 1,       -- 1 se tema está presente na região
    
    -- Métricas de contagem (para agregações)
    qtd_registros INTEGER DEFAULT 1,            -- Para contar registros nas agregações
    
    -- Timestamps de controle
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Restrições de integridade referencial
    FOREIGN KEY (localidade_sk) REFERENCES dim_localidade(localidade_sk),
    FOREIGN KEY (tempo_sk) REFERENCES dim_tempo(tempo_sk)
    -- Nota: dim_tema e dim_ies podem não ter FK definidas ainda
);

-- Índices para performance
CREATE INDEX idx_fato_tema_sk ON fato_pos_graduacao(tema_sk);
CREATE INDEX idx_fato_ies_sk ON fato_pos_graduacao(ies_sk);
CREATE INDEX idx_fato_localidade_sk ON fato_pos_graduacao(localidade_sk);
CREATE INDEX idx_fato_tempo_sk ON fato_pos_graduacao(tempo_sk);

-- ================================================================
-- 3. PROCEDIMENTO PARA POPULAR A NOVA FATO
-- ================================================================

-- 3.1 Inserir dados baseados em associações reais tema-UF-IES
INSERT INTO fato_pos_graduacao (
    tema_sk, ies_sk, localidade_sk, tempo_sk,
    presente_na_uf, presente_na_categoria, presente_na_ies, presente_na_regiao, qtd_registros
)
SELECT DISTINCT
    dt.tema_sk,
    di.ies_sk,
    dl.localidade_sk,
    (SELECT tempo_sk FROM dim_tempo WHERE ano = 2025 LIMIT 1) as tempo_sk,
    1 as presente_na_uf,
    1 as presente_na_categoria, 
    1 as presente_na_ies,
    1 as presente_na_regiao,
    1 as qtd_registros
FROM raw_tema rt
    JOIN dim_tema dt ON rt.tema_id = dt.tema_id
    JOIN dim_localidade dl ON rt.uf = dl.nome_uf
    CROSS JOIN raw_ies ri
    JOIN dim_ies di ON ri.ies_sk = di.ies_sk AND ri.uf = dl.sigla_uf
WHERE rt.uf = ri.uf;  -- Associar apenas temas e IES na mesma UF

-- ================================================================
-- 4. CONSULTAS PARA VALIDAR AS MÉTRICAS DESEJADAS
-- ================================================================

-- 4.1 Quantidade de temas por UF
SELECT 
    dl.sigla_uf,
    dl.nome_uf,
    COUNT(DISTINCT dt.tema_id) as qtd_temas_uf,
    SUM(f.qtd_registros) as total_associacoes
FROM fato_pos_graduacao f
    JOIN dim_tema dt ON f.tema_sk = dt.tema_sk
    JOIN dim_localidade dl ON f.localidade_sk = dl.localidade_sk
GROUP BY dl.sigla_uf, dl.nome_uf
ORDER BY qtd_temas_uf DESC;

-- 4.2 Quantidade de temas por categoria administrativa
SELECT 
    di.categoria_administrativa,
    COUNT(DISTINCT dt.tema_id) as qtd_temas_categoria,
    COUNT(DISTINCT di.ies_sk) as qtd_ies,
    SUM(f.qtd_registros) as total_associacoes
FROM fato_pos_graduacao f
    JOIN dim_tema dt ON f.tema_sk = dt.tema_sk
    JOIN dim_ies di ON f.ies_sk = di.ies_sk
GROUP BY di.categoria_administrativa
ORDER BY qtd_temas_categoria DESC;

-- 4.3 Quantidade de temas por IES específica
SELECT 
    di.nome_ies,
    di.categoria_administrativa,
    dl.sigla_uf,
    COUNT(DISTINCT dt.tema_id) as qtd_temas_ies,
    SUM(f.qtd_registros) as total_associacoes
FROM fato_pos_graduacao f
    JOIN dim_tema dt ON f.tema_sk = dt.tema_sk
    JOIN dim_ies di ON f.ies_sk = di.ies_sk
    JOIN dim_localidade dl ON f.localidade_sk = dl.localidade_sk
GROUP BY di.nome_ies, di.categoria_administrativa, dl.sigla_uf
ORDER BY qtd_temas_ies DESC
LIMIT 20;

-- 4.4 Quantidade de temas por região
SELECT 
    dl.regiao,
    COUNT(DISTINCT dt.tema_id) as qtd_temas_regiao,
    COUNT(DISTINCT dl.localidade_sk) as qtd_ufs,
    SUM(f.qtd_registros) as total_associacoes
FROM fato_pos_graduacao f
    JOIN dim_tema dt ON f.tema_sk = dt.tema_sk
    JOIN dim_localidade dl ON f.localidade_sk = dl.localidade_sk
GROUP BY dl.regiao
ORDER BY qtd_temas_regiao DESC;

-- ================================================================
-- 5. COMPARAÇÃO COM DADOS RAW (VALIDAÇÃO)
-- ================================================================

-- 5.1 Comparar total de temas por UF: FATO vs RAW
WITH fato_uf AS (
    SELECT 
        dl.sigla_uf,
        COUNT(DISTINCT dt.tema_id) as qtd_temas_fato
    FROM fato_pos_graduacao f
        JOIN dim_tema dt ON f.tema_sk = dt.tema_sk
        JOIN dim_localidade dl ON f.localidade_sk = dl.localidade_sk
    GROUP BY dl.sigla_uf
),
raw_uf AS (
    SELECT 
        rt.uf as sigla_uf,
        COUNT(DISTINCT rt.tema_id) as qtd_temas_raw
    FROM raw_tema rt
    GROUP BY rt.uf
)
SELECT 
    COALESCE(f.sigla_uf, r.sigla_uf) as uf,
    COALESCE(f.qtd_temas_fato, 0) as fato_temas,
    COALESCE(r.qtd_temas_raw, 0) as raw_temas,
    COALESCE(f.qtd_temas_fato, 0) - COALESCE(r.qtd_temas_raw, 0) as diferenca
FROM fato_uf f
    FULL OUTER JOIN raw_uf r ON f.sigla_uf = r.sigla_uf
ORDER BY raw_temas DESC;

-- ================================================================
-- 6. ESTATÍSTICAS FINAIS
-- ================================================================

-- 6.1 Resumo geral da nova FATO
SELECT 
    COUNT(*) as total_registros,
    COUNT(DISTINCT tema_sk) as total_tema_sks,
    COUNT(DISTINCT ies_sk) as total_ies_sks,
    COUNT(DISTINCT localidade_sk) as total_localidade_sks
FROM fato_pos_graduacao;

-- 6.2 Verificar integridade
SELECT 
    'Temas sem IES' as problema,
    COUNT(*) as qtd_problemas
FROM dim_tema dt
WHERE NOT EXISTS (
    SELECT 1 FROM fato_pos_graduacao f 
    WHERE f.tema_sk = dt.tema_sk
)

UNION ALL

SELECT 
    'IES sem Temas' as problema,
    COUNT(*) as qtd_problemas
FROM dim_ies di
WHERE NOT EXISTS (
    SELECT 1 FROM fato_pos_graduacao f 
    WHERE f.ies_sk = di.ies_sk
);
