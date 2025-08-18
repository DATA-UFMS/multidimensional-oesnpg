-- =====================================================
-- ADICIONAR CHAVES PRIMÁRIAS FALTANTES
-- Script que adiciona apenas PKs que não existem
-- =====================================================

-- dim_tempo 
ALTER TABLE dim_tempo ADD CONSTRAINT pk_dim_tempo PRIMARY KEY (tempo_sk);

-- dim_localidade
ALTER TABLE dim_localidade ADD CONSTRAINT pk_dim_localidade PRIMARY KEY (localidade_sk);

-- dim_ies
ALTER TABLE dim_ies ADD CONSTRAINT pk_dim_ies PRIMARY KEY (ies_sk);

-- dim_ppg (se existir)
ALTER TABLE dim_ppg ADD CONSTRAINT pk_dim_ppg PRIMARY KEY (ppg_sk);

-- dim_producao (se existir)
ALTER TABLE dim_producao ADD CONSTRAINT pk_dim_producao PRIMARY KEY (producao_sk);

-- dim_ods
ALTER TABLE dim_ods ADD CONSTRAINT pk_dim_ods PRIMARY KEY (ods_sk);

-- dim_docente 
ALTER TABLE dim_docente ADD CONSTRAINT pk_dim_docente PRIMARY KEY (docente_sk);
