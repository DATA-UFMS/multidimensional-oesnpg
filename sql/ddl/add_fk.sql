-- =====================================================
-- ADICIONAR CHAVES ESTRANGEIRAS
-- Script simples para criar FKs na tabela fato
-- =====================================================

-- Remover FKs existentes (se houver)
ALTER TABLE fato_pos_graduacao DROP CONSTRAINT IF EXISTS fk_fato_tempo CASCADE;
ALTER TABLE fato_pos_graduacao DROP CONSTRAINT IF EXISTS fk_fato_localidade CASCADE;
ALTER TABLE fato_pos_graduacao DROP CONSTRAINT IF EXISTS fk_fato_tema CASCADE;
ALTER TABLE fato_pos_graduacao DROP CONSTRAINT IF EXISTS fk_fato_ies CASCADE;

-- Criar Foreign Keys para a tabela fato
ALTER TABLE fato_pos_graduacao ADD CONSTRAINT fk_fato_tempo FOREIGN KEY (tempo_sk) REFERENCES dim_tempo(tempo_sk);
ALTER TABLE fato_pos_graduacao ADD CONSTRAINT fk_fato_localidade FOREIGN KEY (localidade_sk) REFERENCES dim_localidade(localidade_sk);
ALTER TABLE fato_pos_graduacao ADD CONSTRAINT fk_fato_tema FOREIGN KEY (tema_sk) REFERENCES dim_tema(tema_sk);
ALTER TABLE fato_pos_graduacao ADD CONSTRAINT fk_fato_ies FOREIGN KEY (ies_sk) REFERENCES dim_ies(ies_sk);
