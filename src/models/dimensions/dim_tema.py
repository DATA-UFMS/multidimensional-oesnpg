#!/usr/bin/env python3
"""
🎯 DIMENSÃO TEMA - Data Warehouse Observatório CAPES
=======================================================
Cria a dimensão dim_tema baseada nos dados da raw_tema
Estrutura: tema_sk, uf, uf_sigla, tema_id (id da raw), tema, macrotema_id, macrotema, palavrachave
Data: 18/08/2025
"""

import pandas as pd
import os
import sys
from dotenv import load_dotenv
import logging

# Adicionar diretório raiz ao path para imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from src.core.core import get_db_manager

# Carregar variáveis de ambiente
load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def criar_dimensao_tema():
    """
    Cria a dimensão tema baseada na raw_tema com a estrutura solicitada:
    tema_sk, uf, uf_sigla, tema_id, tema, macrotema_id, macrotema, palavrachave
    """
    logger.info("🎯 Criando dimensão TEMA...")
    db = get_db_manager()
    
    try:
        # 1. Remover tabela existente se houver
        logger.info("🗑️  Removendo dim_tema existente...")
        drop_sql = "DROP TABLE IF EXISTS dim_tema CASCADE;"
        db.execute_sql(drop_sql)
        
        # 2. Criar tabela dim_tema com nova estrutura
        logger.info("🏗️  Criando nova estrutura dim_tema...")
        create_sql = """
        CREATE TABLE dim_tema (
            tema_sk SERIAL PRIMARY KEY,
            uf VARCHAR(50),
            uf_sigla VARCHAR(2),
            tema_id INTEGER,
            tema TEXT,
            macrotema_id INTEGER,
            macrotema TEXT,
            palavrachave TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        if not db.execute_sql(create_sql):
            logger.error("❌ Erro ao criar tabela dim_tema")
            return False
            
        # 3. Inserir registro SK=0 (desconhecido)
        logger.info("🔧 Inserindo registro DESCONHECIDO (SK=0)...")
        insert_sk0_sql = """
        INSERT INTO dim_tema (tema_sk, uf, uf_sigla, tema_id, tema, macrotema_id, macrotema, palavrachave)
        VALUES (0, 'DESCONHECIDO', 'XX', 0, 'DESCONHECIDO', 0, 'DESCONHECIDO', 'DESCONHECIDO');
        """
        db.execute_sql(insert_sk0_sql)
        
        # 4. Inserir dados da raw_tema
        logger.info("📊 Populando dim_tema com dados da raw_tema...")
        insert_sql = """
        INSERT INTO dim_tema (uf, uf_sigla, tema_id, tema, macrotema_id, macrotema, palavrachave)
        SELECT DISTINCT
            rt.uf,
            rt.uf_sigla,
            rt.id as tema_id,
            rt.tema,
            rt.macro_tema_id as macrotema_id,
            rt.macro_tema_nome as macrotema,
            rt.palavra_chave as palavrachave
        FROM raw_tema rt
        ORDER BY rt.id;
        """
        
        if db.execute_sql(insert_sql):
            # Verificar quantos registros foram inseridos
            count_query = "SELECT COUNT(*) as total FROM dim_tema;"
            result = db.execute_query(count_query)
            total = result.iloc[0]['total']
            
            logger.info(f"✅ dim_tema criada com {total:,} registros")
            
            # 5. Criar índices para performance
            logger.info("🔍 Criando índices...")
            indices_sql = [
                "CREATE INDEX IF NOT EXISTS idx_dim_tema_id ON dim_tema(tema_id);",
                "CREATE INDEX IF NOT EXISTS idx_dim_tema_uf_sigla ON dim_tema(uf_sigla);",
                "CREATE INDEX IF NOT EXISTS idx_dim_tema_macrotema ON dim_tema(macrotema_id);"
            ]
            
            for idx_sql in indices_sql:
                db.execute_sql(idx_sql)
            
            logger.info("✅ Índices criados")
            return True
            
        else:
            logger.error("❌ Erro ao popular dim_tema")
            return False
            
    except Exception as e:
        logger.error(f"❌ Erro ao criar dimensão tema: {str(e)}")
        return False

def validar_dimensao_tema():
    """Valida os dados da dimensão tema."""
    logger.info("🔍 Validando dimensão TEMA...")
    db = get_db_manager()
    
    try:
        print("\n" + "="*60)
        print("📊 VALIDAÇÃO DA DIMENSÃO TEMA")
        print("="*60)
        
        # 1. Contagem total
        query_total = "SELECT COUNT(*) as total FROM dim_tema;"
        result = db.execute_query(query_total)
        total = result.iloc[0]['total']
        print(f"📊 Total de registros: {total:,}")
        
        # 2. Contagem por UF
        print("\n📍 Top 10 UFs por quantidade de temas:")
        query_uf = """
        SELECT 
            uf,
            uf_sigla,
            COUNT(*) as qtd_temas
        FROM dim_tema 
        WHERE tema_sk > 0
        GROUP BY uf, uf_sigla
        ORDER BY qtd_temas DESC
        LIMIT 10;
        """
        result = db.execute_query(query_uf)
        print(result.to_string(index=False))
        
        # 3. Contagem por macrotema
        print("\n🎯 Top 10 Macrotemas:")
        query_macro = """
        SELECT 
            macrotema,
            COUNT(*) as qtd_temas
        FROM dim_tema 
        WHERE tema_sk > 0
        GROUP BY macrotema
        ORDER BY qtd_temas DESC
        LIMIT 10;
        """
        result = db.execute_query(query_macro)
        print(result.to_string(index=False))
        
        # 4. Verificar estrutura
        print("\n🔍 Estrutura da tabela:")
        query_estrutura = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_name = 'dim_tema'
        ORDER BY ordinal_position;
        """
        result = db.execute_query(query_estrutura)
        print(result.to_string(index=False))
        
        print("\n✅ Validação concluída!")
        
    except Exception as e:
        logger.error(f"❌ Erro na validação: {str(e)}")

def main():
    """Função principal."""
    try:
        logger.info("🚀 Iniciando criação da dimensão TEMA")
        
        # 1. Criar dimensão
        if not criar_dimensao_tema():
            logger.error("❌ Falha na criação da dimensão tema")
            return
            
        # 2. Validar dimensão
        validar_dimensao_tema()
        
        print("\n" + "="*70)
        print("🎉 DIMENSÃO TEMA CRIADA COM SUCESSO!")
        print("="*70)
        print("✅ Tabela: dim_tema")
        print("✅ Estrutura: tema_sk, uf, uf_sigla, tema_id, tema, macrotema_id, macrotema, palavrachave")
        print("✅ Dados: Baseados na raw_tema")
        print("✅ Performance: Índices criados")
        print("="*70)
        
    except Exception as e:
        logger.error(f"❌ Erro durante criação da dimensão TEMA: {str(e)}")

if __name__ == "__main__":
    main()
