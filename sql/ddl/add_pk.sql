-- =====================================================
-- ADICIONAR CHAVES PRIMÁRIAS
-- Script simples para criar PKs em todas as dimensões
-- =====================================================

-- Remover PKs existentes (se houver)
ALTER TABLE dim_tempo DROP CONSTRAINT IF EXISTS pk_dim_tempo CASCADE;
ALTER TABLE dim_localidade DROP CONSTRAINT IF EXISTS pk_dim_localidade CASCADE;
ALTER TABLE dim_tema DROP CONSTRAINT IF EXISTS pk_dim_tema CASCADE;
ALTER TABLE dim_ies DROP CONSTRAINT IF EXISTS pk_dim_ies CASCADE;
ALTER TABLE dim_ppg DROP CONSTRAINT IF EXISTS pk_dim_ppg CASCADE;
ALTER TABLE dim_producao DROP CONSTRAINT IF EXISTS pk_dim_producao CASCADE;
ALTER TABLE dim_ods DROP CONSTRAINT IF EXISTS pk_dim_ods CASCADE;
ALTER TABLE dim_docente DROP CONSTRAINT IF EXISTS pk_dim_docente CASCADE;

-- Criar Primary Keys
ALTER TABLE dim_tempo ADD CONSTRAINT pk_dim_tempo PRIMARY KEY (tempo_sk);
ALTER TABLE dim_localidade ADD CONSTRAINT pk_dim_localidade PRIMARY KEY (localidade_sk);
ALTER TABLE dim_tema ADD CONSTRAINT pk_dim_tema PRIMARY KEY (tema_sk);
ALTER TABLE dim_ies ADD CONSTRAINT pk_dim_ies PRIMARY KEY (ies_sk);
ALTER TABLE dim_ppg ADD CONSTRAINT pk_dim_ppg PRIMARY KEY (ppg_sk);
ALTER TABLE dim_producao ADD CONSTRAINT pk_dim_producao PRIMARY KEY (producao_sk);
ALTER TABLE dim_ods ADD CONSTRAINT pk_dim_ods PRIMARY KEY (ods_sk);
ALTER TABLE dim_docente ADD CONSTRAINT pk_dim_docente PRIMARY KEY (docente_sk);
