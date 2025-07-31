-- =====================================================
-- Script SIMPLES para adicionar FKs na tabela fato
-- Somente comandos ALTER TABLE
-- =====================================================

-- FK para dim_tempo
ALTER TABLE fato_pos_graduacao 
ADD CONSTRAINT fk_fato_tempo 
FOREIGN KEY (tempo_sk) REFERENCES dim_tempo(tempo_sk);

-- FK para dim_localidade  
ALTER TABLE fato_pos_graduacao 
ADD CONSTRAINT fk_fato_localidade 
FOREIGN KEY (localidade_sk) REFERENCES dim_localidade(localidade_sk);

-- FK para dim_ppg
ALTER TABLE fato_pos_graduacao 
ADD CONSTRAINT fk_fato_ppg 
FOREIGN KEY (ppg_sk) REFERENCES dim_ppg(ppg_sk);

-- FK para dim_ies
ALTER TABLE fato_pos_graduacao 
ADD CONSTRAINT fk_fato_ies 
FOREIGN KEY (ies_sk) REFERENCES dim_ies(ies_sk);

-- FK para dim_tema
ALTER TABLE fato_pos_graduacao 
ADD CONSTRAINT fk_fato_tema 
FOREIGN KEY (tema_sk) REFERENCES dim_tema(tema_sk);

-- FK para dim_producao
ALTER TABLE fato_pos_graduacao 
ADD CONSTRAINT fk_fato_producao 
FOREIGN KEY (producao_sk) REFERENCES dim_producao(producao_sk);

-- FK para dim_ods
ALTER TABLE fato_pos_graduacao 
ADD CONSTRAINT fk_fato_ods 
FOREIGN KEY (ods_sk) REFERENCES dim_ods(ods_sk);

-- FK para dim_docente
ALTER TABLE fato_pos_graduacao 
ADD CONSTRAINT fk_fato_docente 
FOREIGN KEY (docente_sk) REFERENCES dim_docente(docente_sk);

-- Verificar FKs criadas
SELECT constraint_name, table_name 
FROM information_schema.table_constraints 
WHERE constraint_type = 'FOREIGN KEY' 
AND table_name = 'fato_pos_graduacao';
