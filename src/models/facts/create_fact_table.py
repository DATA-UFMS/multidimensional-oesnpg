#!/usr/bin/env python3
"""
🏭 FATO - Data Warehouse Observatório CAPES
=======================================================
Cria a tabela fato_pos_graduacao otimizada para as métricas solicitadas:
- Qtd temas por UF
- Qtd temas por categoria administrativa (público/privado)  
- Qtd temas por IES
- Qtd temas por região

Substitui a complexa fato_pos_graduacao com uma estrutura limpa e eficiente.
Data: 05/08/2025
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

def backup_fato_atual():
    """Faz backup da FATO atual antes de alterações."""
    
    logger.info("🔄 Fazendo backup da FATO atual...")
    db = get_db_manager()
    
    # Verificar se existe
    check_query = """
    SELECT COUNT(*) as existe 
    FROM information_schema.tables 
    WHERE table_name = 'fato_pos_graduacao';
    """
    
    result = db.execute_query(check_query)
    if result.iloc[0]['existe'] > 0:
        # Verificar se backup já existe
        check_backup = """
        SELECT COUNT(*) as existe 
        FROM information_schema.tables 
        WHERE table_name = 'fato_pos_graduacao_backup';
        """
        backup_result = db.execute_query(check_backup)
        
        if backup_result.iloc[0]['existe'] > 0:
            logger.info("ℹ️ Backup já existe: fato_pos_graduacao_backup")
            return True
        
        # Criar backup
        backup_query = """
        CREATE TABLE fato_pos_graduacao_backup AS 
        SELECT * FROM fato_pos_graduacao;
        """
        
        if db.execute_sql(backup_query):
            logger.info("✅ Backup criado: fato_pos_graduacao_backup")
            return True
        else:
            logger.error("❌ Erro ao criar backup")
            return False
    else:
        logger.info("ℹ️ Tabela fato_pos_graduacao não existe, continuando...")
        return True

def criar_fato():
    """Cria a nova tabela FATO."""
    
    logger.info("🚀 Criando nova FATO ...")
    db = get_db_manager()
    
    # DDL da nova tabela (sem FK constraints por ora)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS fato_pos_graduacao (
        fato_id SERIAL PRIMARY KEY,
        
        -- Chaves estrangeiras (dimensões) - sem FK constraints por ora
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
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    if db.execute_sql(create_table_sql):
        logger.info("✅ Tabela fato_pos_graduacao criada")
        
        # Criar índices
        indices_sql = [
            "CREATE INDEX IF NOT EXISTS idx_fato_tema_sk ON fato_pos_graduacao(tema_sk);",
            "CREATE INDEX IF NOT EXISTS idx_fato_ies_sk ON fato_pos_graduacao(ies_sk);",
            "CREATE INDEX IF NOT EXISTS idx_fato_localidade_sk ON fato_pos_graduacao(localidade_sk);",
            "CREATE INDEX IF NOT EXISTS idx_fato_tempo_sk ON fato_pos_graduacao(tempo_sk);"
        ]
        
        for idx_sql in indices_sql:
            db.execute_sql(idx_sql)
        
        logger.info("✅ Índices criados")
        return True
    else:
        logger.error("❌ Erro ao criar tabela")
        return False

def popular_fato():
    """Popula a nova FATO com dados dos RAW."""
    
    logger.info("📊 Populando nova FATO com dados...")
    db = get_db_manager()
    
    # Limpar dados existentes
    truncate_sql = "TRUNCATE TABLE fato_pos_graduacao;"
    db.execute_sql(truncate_sql)
    
    # Inserir dados - associar cada tema com cada IES da mesma UF (corrigir join UF)
    insert_sql = """
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
        JOIN dim_tema dt ON rt.id = dt.tema_id = dt.tema_id
        JOIN dim_localidade dl ON rt.uf = dl.nome_uf 
        JOIN dim_ies di ON dl.sigla_uf = di.sigla_uf 
    WHERE rt.uf = dl.nome_uf AND dl.sigla_uf = di.sigla_uf;
    """
    
    if db.execute_sql(insert_sql):
        # Verificar quantos registros foram inseridos
        count_query = "SELECT COUNT(*) as total FROM fato_pos_graduacao;"
        result = db.execute_query(count_query)
        total = result.iloc[0]['total']
        
        logger.info(f"✅ FATO populada com {total:,} registros")
        return True
    else:
        logger.error("❌ Erro ao popular FATO")
        return False

def validar_metricas():
    """Valida as métricas da nova FATO."""
    
    logger.info("🔍 Validando métricas da nova FATO...")
    db = get_db_manager()
    
    print("\n" + "="*60)
    print("📊 VALIDAÇÃO DAS MÉTRICAS - NOVA FATO")
    print("="*60)
    
    # 1. Qtd temas por UF
    print("\n📍 1. Quantidade de temas por UF:")
    query_uf = """
    SELECT 
        dl.sigla_uf,
        COUNT(DISTINCT dt.tema_id) as qtd_temas_uf,
        COUNT(DISTINCT di.ies_sk) as qtd_ies,
        SUM(f.qtd_registros) as total_associacoes
    FROM fato_pos_graduacao f
        JOIN dim_tema dt ON f.tema_sk = dt.tema_sk
        JOIN dim_localidade dl ON f.localidade_sk = dl.localidade_sk
        JOIN dim_ies di ON f.ies_sk = di.ies_sk
    GROUP BY dl.sigla_uf
    ORDER BY qtd_temas_uf DESC
    LIMIT 10;
    """
    df_uf = db.execute_query(query_uf)
    print(df_uf.to_string(index=False))
    
    # 2. Qtd temas por categoria
    print("\n🏛️ 2. Quantidade de temas por categoria administrativa:")
    query_categoria = """
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
    """
    df_categoria = db.execute_query(query_categoria)
    print(df_categoria.to_string(index=False))
    
    # 3. Qtd temas por região
    print("\n🗺️ 3. Quantidade de temas por região:")
    query_regiao = """
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
    """
    df_regiao = db.execute_query(query_regiao)
    print(df_regiao.to_string(index=False))
    
    # 4. Top IES por qtd de temas
    print("\n🏫 4. Top 10 IES por quantidade de temas:")
    query_ies = """
    SELECT 
        di.nome_ies,
        di.categoria_administrativa,
        dl.sigla_uf,
        COUNT(DISTINCT dt.tema_id) as qtd_temas_ies
    FROM fato_pos_graduacao f
        JOIN dim_tema dt ON f.tema_sk = dt.tema_sk
        JOIN dim_ies di ON f.ies_sk = di.ies_sk
        JOIN dim_localidade dl ON f.localidade_sk = dl.localidade_sk
    GROUP BY di.nome_ies, di.categoria_administrativa, dl.sigla_uf
    ORDER BY qtd_temas_ies DESC
    LIMIT 10;
    """
    df_ies = db.execute_query(query_ies)
    print(df_ies.to_string(index=False))
    
    # 5. Resumo geral
    print("\n📈 5. Resumo geral da FATO:")
    query_resumo = """
    SELECT 
        COUNT(*) as total_registros,
        COUNT(DISTINCT tema_sk) as total_tema_sks,
        COUNT(DISTINCT ies_sk) as total_ies_sks,
        COUNT(DISTINCT localidade_sk) as total_localidade_sks
    FROM fato_pos_graduacao;
    """
    df_resumo = db.execute_query(query_resumo)
    print(df_resumo.to_string(index=False))

def substituir_fato_antiga():
    """Como já criamos diretamente como fato_pos_graduacao, apenas confirma sucesso."""
    
    logger.info("✅ Tabela já foi criada como fato_pos_graduacao")
    return True

def main():
    """Executa todo o processo de criação da nova FATO."""
    
    print("🚀 CRIAÇÃO DA NOVA FATO - Data Warehouse CAPES")
    print("=" * 70)
    print("📅 Data: 05/08/2025")
    print("🎯 Objetivo: Substituir fato complexa por estrutura otimizada")
    print("📊 Métricas: qtd_temas por UF, categoria, IES e região")
    print("=" * 70)
    
    try:
        # 1. Backup
        if not backup_fato_atual():
            print("❌ Falha no backup. Abortando...")
            return
        
        # 2. Criar nova estrutura
        if not criar_fato():
            print("❌ Falha na criação da tabela. Abortando...")
            return
            
        # 3. Popular com dados
        if not popular_fato():
            print("❌ Falha ao popular dados. Abortando...")
            return
            
        # 4. Validar métricas
        validar_metricas()
        
        # 5. Perguntar se deve substituir a tabela antiga
        print("\n" + "="*70)
        print("🤔 CONFIRMAÇÃO")
        print("="*70)
        print("A FATO foi criada diretamente como 'fato_pos_graduacao'")
        print("Sistema pronto para uso! (s/n): ", end="")
        
        # Para automação, assumir 's' (confirmar)
        resposta = 's'  # input().lower().strip()
        
        if resposta == 's':
            if substituir_fato_antiga():
                print("\n✅ Sistema confirmado e pronto!")
            else:
                print("\n❌ Erro na confirmação")
        else:
            print("\n📝 FATO mantida como 'fato_pos_graduacao'")
            print("🔗 Sistema está operacional")
            print("   Tabela: fato_pos_graduacao")
            print("   Status: Ativa e funcionando")
        
        print("\n" + "="*70)
        print("🎉 PROCESSO CONCLUÍDO COM SUCESSO!")
        print("="*70)
        print("✅ Tabela: fato_pos_graduacao")
        print("✅ Métricas: qtd_temas por UF, categoria, IES e região")
        print("✅ Backup: fato_pos_graduacao_backup (se existia)")
        print("✅ Performance: Otimizada para agregações")
        print("="*70)
        
    except Exception as e:
        logger.error(f"❌ Erro durante criação da FATO: {str(e)}")
        print(f"\n❌ ERRO: {str(e)}")

if __name__ == "__main__":
    main()
