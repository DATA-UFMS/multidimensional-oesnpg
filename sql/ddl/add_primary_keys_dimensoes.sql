-- =====================================================
-- Script SIMPLES para adicionar chaves primárias 
-- em todas as dimensões
-- =====================================================

-- Chave primária para dim_tempo
ALTER TABLE dim_tempo 
ADD CONSTRAINT pk_dim_tempo 
PRIMARY KEY (tempo_sk);

-- Chave primária para dim_localidade
ALTER TABLE dim_localidade 
ADD CONSTRAINT pk_dim_localidade 
PRIMARY KEY (localidade_sk);

-- Chave primária para dim_ppg
ALTER TABLE dim_ppg 
ADD CONSTRAINT pk_dim_ppg 
PRIMARY KEY (ppg_sk);

-- Chave primária para dim_ies
ALTER TABLE dim_ies 
ADD CONSTRAINT pk_dim_ies 
PRIMARY KEY (ies_sk);

-- Chave primária para dim_tema
ALTER TABLE dim_tema 
ADD CONSTRAINT pk_dim_tema 
PRIMARY KEY (tema_sk);

-- Chave primária para dim_producao
ALTER TABLE dim_producao 
ADD CONSTRAINT pk_dim_producao 
PRIMARY KEY (producao_sk);

-- Chave primária para dim_ods
ALTER TABLE dim_ods 
ADD CONSTRAINT pk_dim_ods 
PRIMARY KEY (ods_sk);

-- Chave primária para dim_docente
ALTER TABLE dim_docente 
ADD CONSTRAINT pk_dim_docente 
PRIMARY KEY (docente_sk);

-- Verificar PKs criadas
SELECT 
    table_name,
    constraint_name,
    column_name
FROM information_schema.key_column_usage
WHERE table_schema = 'public'
  AND table_name LIKE 'dim_%'
  AND constraint_name LIKE 'pk_%'
ORDER BY table_name;
