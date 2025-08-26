#!/usr/bin/env python3
"""
DIMENSÃO PRODUÇÃO - Data Warehouse Observatório CAPES
=======================================================
Cria a dimensão dim_producao baseada nos dados da raw_producao
Estrutura: producao_sk, informações de produção acadêmica
Data: 21/08/2025
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
import logging

# Adicionar path para imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from src.core.core import get_db_manager

# Carregar variáveis de ambiente
load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def carregar_dados_raw_producao():
    """
    Carrega os dados da tabela raw_producao para DataFrame.
    """
    logger.info("� Carregando dados da raw_producao...")
    db = get_db_manager()
    
    try:
        query = """
        SELECT 
            id_add_producao_intelectual,
            id_producao_intelectual,
            nm_producao,
            an_base,
            cd_programa_ies,
            nm_programa_ies,
            sg_entidade_ensino,
            nm_entidade_ensino,
            nm_tipo_producao,
            nm_subtipo_producao,
            nm_formulario,
            nm_area_concentracao,
            nm_linha_pesquisa,
            nm_projeto,
            ds_titulo_padronizado
        FROM raw_producao
        WHERE nm_producao IS NOT NULL
        ORDER BY an_base DESC, nm_producao;
        """
        
        df = db.execute_query(query)
        logger.info(f"✅ Carregados {len(df):,} registros da raw_producao")
        return df
        
    except Exception as e:
        logger.error(f"❌ Erro ao carregar dados da raw_producao: {str(e)}")
        return None

def processar_dataframe_producao(df):
    """
    Processa o DataFrame de produção aplicando transformações e limpezas.
    """
    if df is None or df.empty:
        logger.error("❌ DataFrame vazio ou None para processamento")
        return None
        
    logger.info(f"Processando {len(df):,} registros de produção...")
    
    try:
        # Fazer cópia para não alterar o original
        df_processed = df.copy()
        
        # 1. Remover duplicatas baseado no id_add_producao_intelectual
        df_processed = df_processed.drop_duplicates(subset=['id_add_producao_intelectual'], keep='first')
        logger.info(f"Processando {len(df_processed):,} produções únicas (removidas {len(df) - len(df_processed):,} duplicatas)")
        
        # 2. Limpar e padronizar campos de texto
        logger.info("Limpando e padronizando dados...")
        
        colunas_texto = [
            'nm_producao', 'nm_programa_ies', 'sg_entidade_ensino', 'nm_entidade_ensino',
            'nm_tipo_producao', 'nm_subtipo_producao', 'nm_formulario', 'nm_area_concentracao',
            'nm_linha_pesquisa', 'nm_projeto', 'ds_titulo_padronizado'
        ]
        
        for col in colunas_texto:
            if col in df_processed.columns:
                df_processed[col] = df_processed[col].fillna('Não informado')
                df_processed[col] = df_processed[col].astype(str).str.strip()
                df_processed[col] = df_processed[col].replace(['', 'nan', 'None'], 'Não informado')
        
        # 3. Tratar campos numéricos
        colunas_numericas = [
            'an_base', 'cd_programa_ies', 'id_producao_intelectual'
        ]
        
        for col in colunas_numericas:
            if col in df_processed.columns:
                df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').fillna(0).astype(int)
        
        # 4. Classificar tipo de produção baseado no nome
        logger.info("📂 Classificando tipos de produção...")
        df_processed['tipo_producao'] = df_processed['nm_tipo_producao'].fillna('Artigo em Periódico')
        
        # 5. Agregar dados por características similares para reduzir volume
        logger.info("📊 Agregando dados similares...")
        
        colunas_agrupamento = [
            'an_base', 'nm_programa_ies', 'nm_area_concentracao', 
            'nm_tipo_producao', 'tipo_producao'
        ]
        
        # Verificar se todas as colunas existem
        colunas_agrupamento = [col for col in colunas_agrupamento if col in df_processed.columns]
        
        if colunas_agrupamento:
            # Agrupar e contar
            df_agregado = df_processed.groupby(colunas_agrupamento).agg({
                'id_add_producao_intelectual': 'count',  # Quantidade de produções
                'nm_producao': 'first',
                'sg_entidade_ensino': 'first',
                'nm_entidade_ensino': 'first',
                'nm_subtipo_producao': 'first',
                'nm_linha_pesquisa': 'first'
            }).reset_index()
            
            # Renomear coluna de contagem
            df_agregado.rename(columns={'id_add_producao_intelectual': 'quantidade_producoes'}, inplace=True)
            
            df_processed = df_agregado
            logger.info(f"📊 Dados agregados para {len(df_processed):,} registros únicos")
        
        logger.info(f"✅ Processamento concluído: {len(df_processed):,} registros processados")
        return df_processed
        
    except Exception as e:
        logger.error(f"❌ Erro durante processamento: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def criar_dimensao_producao():
    """
    Cria a dimensão produção usando pandas para manipular os dados.
    """
    logger.info("� Criando dimensão PRODUÇÃO com pandas...")
    db = get_db_manager()
    
    try:
        # 1. Remover tabela existente se houver
        logger.info("🗑️  Removendo dim_producao existente...")
        drop_sql = "DROP TABLE IF EXISTS dim_producao CASCADE;"
        db.execute_sql(drop_sql)
        
        # 2. Criar tabela dim_producao
        logger.info("🏗️  Criando nova estrutura dim_producao...")
        create_sql = """
        CREATE TABLE dim_producao (
            producao_sk SERIAL PRIMARY KEY,
            id_producao_original VARCHAR(100),
            nome_producao VARCHAR(1000),
            ano_producao INTEGER,
            programa_ies VARCHAR(500),
            codigo_programa INTEGER,
            nome_programa VARCHAR(500),
            area_concentracao VARCHAR(300),
            area_avaliacao VARCHAR(200),
            area_conhecimento VARCHAR(200),
            grande_area_conhecimento VARCHAR(200),
            palavras_chave TEXT,
            idioma VARCHAR(50),
            tipo_producao VARCHAR(100),
            quantidade_producoes INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        if not db.execute_sql(create_sql):
            logger.error("❌ Erro ao criar tabela dim_producao")
            return False
            
        # 3. Inserir registro SK=0 (desconhecido)
        logger.info("🔧 Inserindo registro DESCONHECIDO (SK=0)...")
        sk0_data = pd.DataFrame({
            'producao_sk': [0],
            'id_producao_original': ['DESCONHECIDO'],
            'nome_producao': ['NÃO INFORMADO'],
            'ano_producao': [0],
            'programa_ies': ['NÃO INFORMADO'],
            'codigo_programa': [0],
            'nome_programa': ['NÃO INFORMADO'],
            'area_concentracao': ['NÃO INFORMADO'],
            'area_avaliacao': ['NÃO INFORMADO'],
            'area_conhecimento': ['NÃO INFORMADO'],
            'grande_area_conhecimento': ['NÃO INFORMADO'],
            'palavras_chave': ['NÃO INFORMADO'],
            'idioma': ['NÃO INFORMADO'],
            'tipo_producao': ['NÃO INFORMADO'],
            'quantidade_producoes': [0]
        })
        
        # Inserir registro SK=0
        db.save_dataframe(sk0_data, 'dim_producao', if_exists='append')
        
        # 4. Carregar e processar dados de produção
        df_raw = carregar_dados_raw_producao()
        if df_raw is None:
            logger.error("❌ Falha ao carregar dados da raw_producao")
            return False
            
        df_producoes = processar_dataframe_producao(df_raw)
        if df_producoes is None:
            logger.error("❌ Falha ao processar DataFrame de produção")
            return False
        
        # 5. Mapear colunas para a estrutura da dimensão
        logger.info("🔄 Mapeando colunas para estrutura da dimensão...")
        df_final = pd.DataFrame()
        
        # Mapeamento de colunas
        mapeamento = {
            'id_producao_original': 'id_add_producao_intelectual' if 'id_add_producao_intelectual' in df_producoes.columns else None,
            'nome_producao': 'nm_producao' if 'nm_producao' in df_producoes.columns else None,
            'ano_producao': 'an_base' if 'an_base' in df_producoes.columns else None,
            'programa_ies': 'nm_programa_ies' if 'nm_programa_ies' in df_producoes.columns else None,
            'codigo_programa': 'cd_programa_ies' if 'cd_programa_ies' in df_producoes.columns else None,
            'nome_programa': 'nm_programa_ies' if 'nm_programa_ies' in df_producoes.columns else None,
            'area_concentracao': 'nm_area_concentracao' if 'nm_area_concentracao' in df_producoes.columns else None,
            'area_avaliacao': 'nm_linha_pesquisa' if 'nm_linha_pesquisa' in df_producoes.columns else None,
            'area_conhecimento': 'nm_area_concentracao' if 'nm_area_concentracao' in df_producoes.columns else None,
            'grande_area_conhecimento': 'nm_tipo_producao' if 'nm_tipo_producao' in df_producoes.columns else None,
            'palavras_chave': 'nm_projeto' if 'nm_projeto' in df_producoes.columns else None,
            'idioma': 'sg_entidade_ensino' if 'sg_entidade_ensino' in df_producoes.columns else None,
            'tipo_producao': 'tipo_producao' if 'tipo_producao' in df_producoes.columns else None,
            'quantidade_producoes': 'quantidade_producoes' if 'quantidade_producoes' in df_producoes.columns else None
        }
        
        # Aplicar mapeamento
        for col_destino, col_origem in mapeamento.items():
            if col_origem and col_origem in df_producoes.columns:
                df_final[col_destino] = df_producoes[col_origem]
            else:
                # Valores padrão baseado no tipo
                if col_destino in ['ano_producao', 'codigo_programa', 'quantidade_producoes']:
                    df_final[col_destino] = 1 if col_destino == 'quantidade_producoes' else 0
                else:
                    df_final[col_destino] = 'Não informado'
        
        # 6. Inserir dados processados no banco
        logger.info("💾 Inserindo dados processados no banco...")
        db.save_dataframe(df_final, 'dim_producao', if_exists='append')
        
        # 7. Verificar inserção
        count_query = "SELECT COUNT(*) as total FROM dim_producao;"
        result = db.execute_query(count_query)
        total = result.iloc[0]['total']
        
        logger.info(f"✅ dim_producao criada com {total:,} registros")
        
        # 8. Criar índices para performance
        logger.info("🔍 Criando índices...")
        indices_sql = [
            "CREATE INDEX IF NOT EXISTS idx_dim_producao_ano ON dim_producao(ano_producao);",
            "CREATE INDEX IF NOT EXISTS idx_dim_producao_area_conhecimento ON dim_producao(area_conhecimento);",
            "CREATE INDEX IF NOT EXISTS idx_dim_producao_tipo ON dim_producao(tipo_producao);",
            "CREATE INDEX IF NOT EXISTS idx_dim_producao_programa ON dim_producao(codigo_programa);"
        ]
        
        for idx_sql in indices_sql:
            db.execute_sql(idx_sql)
        
        logger.info("✅ Índices criados")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao criar dimensão produção: {str(e)}")
        return False

def validar_dimensao_producao():
    """Valida os dados da dimensão produção."""
    logger.info("🔍 Validando dimensão PRODUÇÃO...")
    db = get_db_manager()
    
    try:
        print("\n" + "="*60)
        print("📊 VALIDAÇÃO DA DIMENSÃO PRODUÇÃO")
        print("="*60)
        
        # 1. Contagem total
        query_total = "SELECT COUNT(*) as total FROM dim_producao;"
        result = db.execute_query(query_total)
        total = result.iloc[0]['total']
        print(f"📊 Total de registros: {total:,}")
        
        # 2. Produções por tipo
        print("\n📚 Produções por tipo:")
        query_tipo = """
        SELECT 
            tipo_producao,
            COUNT(*) as qtd_producoes
        FROM dim_producao 
        WHERE producao_sk > 0
        GROUP BY tipo_producao
        ORDER BY qtd_producoes DESC;
        """
        result = db.execute_query(query_tipo)
        print(result.to_string(index=False))
        
        # 3. Produções por ano
        print("\n📅 Produções por ano:")
        query_ano = """
        SELECT 
            ano_producao,
            COUNT(*) as qtd_producoes,
            SUM(quantidade_producoes) as total_producoes
        FROM dim_producao 
        WHERE producao_sk > 0 AND ano_producao > 0
        GROUP BY ano_producao
        ORDER BY ano_producao DESC
        LIMIT 10;
        """
        result = db.execute_query(query_ano)
        print(result.to_string(index=False))
        
        # 4. Top áreas de conhecimento
        print("\n🎓 Top 10 áreas de concentração:")
        query_area = """
        SELECT 
            area_concentracao,
            COUNT(*) as qtd_producoes
        FROM dim_producao 
        WHERE producao_sk > 0
        GROUP BY area_concentracao
        ORDER BY qtd_producoes DESC
        LIMIT 10;
        """
        result = db.execute_query(query_area)
        print(result.to_string(index=False))
        
        # 5. Distribuição por entidade de ensino
        print("\n� Top 10 entidades de ensino (siglas):")
        query_entidade = """
        SELECT 
            idioma as sigla_entidade,
            COUNT(*) as qtd_producoes
        FROM dim_producao 
        WHERE producao_sk > 0 AND idioma != 'Não informado'
        GROUP BY idioma
        ORDER BY qtd_producoes DESC
        LIMIT 10;
        """
        result = db.execute_query(query_entidade)
        print(result.to_string(index=False))
        
        print("\n✅ Validação concluída!")
        
    except Exception as e:
        logger.error(f"❌ Erro na validação: {str(e)}")

def main():
    """Função principal."""
    try:
        logger.info("🚀 Iniciando criação da dimensão PRODUÇÃO")
        
        # 1. Criar dimensão
        if not criar_dimensao_producao():
            logger.error("❌ Falha na criação da dimensão produção")
            return
            
        # 2. Validar dimensão
        validar_dimensao_producao()
        
        print("\n" + "="*70)
        print("🎉 DIMENSÃO PRODUÇÃO CRIADA COM SUCESSO!")
        print("="*70)
        print("✅ Tabela: dim_producao")
        print("✅ Fonte: raw_producao")
        print("✅ Índices: Performance otimizada")
        print("✅ Dados: Tratados e normalizados")
        print("="*70)
        
    except Exception as e:
        logger.error(f"❌ Erro durante criação da dimensão PRODUÇÃO: {str(e)}")

if __name__ == "__main__":
    main()
