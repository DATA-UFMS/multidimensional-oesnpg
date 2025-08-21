#!/usr/bin/env python3
"""
🏛️ DIMENSÃO IES - Data Warehouse Observatório CAPES
=======================================================
Cria a dimensão dim_ies baseada nos dados da raw_ies_api
Estrutura: ies_sk, informações das Instituições de Ensino Superior
Data: 21/08/2025
"""

import pandas as pd
import numpy as np
import os
import sys
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

def carregar_dados_raw_ies():
    """
    Carrega os dados da tabela raw_ies_api para DataFrame.
    """
    logger.info("📥 Carregando dados da raw_ies_api...")
    db = get_db_manager()
    
    try:
        query = """
        SELECT DISTINCT
            cd_entidade_capes,
            cd_entidade_emec,
            nm_entidade_ensino,
            sg_entidade_ensino,
            cs_status_juridico,
            ds_dependencia_administrativa,
            ds_organizacao_academica,
            nm_regiao,
            sg_uf_programa,
            nm_municipio_programa_ies,
            an_base
        FROM raw_ies_api
        WHERE nm_entidade_ensino IS NOT NULL
        ORDER BY nm_entidade_ensino;
        """
        
        df = db.execute_query(query)
        logger.info(f"✅ Carregados {len(df):,} registros da raw_ies_api")
        return df
        
    except Exception as e:
        logger.error(f"❌ Erro ao carregar dados da raw_ies_api: {str(e)}")
        return None

def processar_dataframe_ies(df):
    """
    Processa o DataFrame de IES aplicando transformações e limpezas.
    """
    if df is None or df.empty:
        logger.error("❌ DataFrame vazio ou None para processamento")
        return None
        
    logger.info(f"🔄 Processando {len(df):,} registros de IES...")
    
    try:
        # Fazer cópia para não alterar o original
        df_processed = df.copy()
        
        # 1. Remover duplicatas baseado no código da entidade CAPES
        df_processed = df_processed.drop_duplicates(subset=['cd_entidade_capes'], keep='first')
        logger.info(f"📊 Processando {len(df_processed):,} IES únicas (removidas {len(df) - len(df_processed):,} duplicatas)")
        
        # 2. Limpar e padronizar campos de texto
        logger.info("🧹 Limpando e padronizando dados...")
        
        colunas_texto = [
            'nm_entidade_ensino', 'sg_entidade_ensino', 'cs_status_juridico',
            'ds_dependencia_administrativa', 'ds_organizacao_academica', 
            'nm_regiao', 'sg_uf_programa', 'nm_municipio_programa_ies'
        ]
        
        for col in colunas_texto:
            if col in df_processed.columns:
                df_processed[col] = df_processed[col].fillna('Não informado')
                df_processed[col] = df_processed[col].astype(str).str.strip()
                df_processed[col] = df_processed[col].replace(['', 'nan', 'None'], 'Não informado')
        
        # 3. Tratar campos numéricos
        colunas_numericas = ['cd_entidade_capes', 'cd_entidade_emec', 'an_base']
        
        for col in colunas_numericas:
            if col in df_processed.columns:
                df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').fillna(0).astype(int)
        
        # 4. Padronizar dependência administrativa
        if 'ds_dependencia_administrativa' in df_processed.columns:
            dependencia_map = {
                'PUBLICA FEDERAL': 'Pública Federal',
                'PUBLICA ESTADUAL': 'Pública Estadual',
                'PUBLICA MUNICIPAL': 'Pública Municipal', 
                'PRIVADA': 'Privada',
                'PRIVADA SEM FINS LUCRATIVOS': 'Privada sem fins lucrativos',
                'PRIVADA COM FINS LUCRATIVOS': 'Privada com fins lucrativos'
            }
            df_processed['ds_dependencia_administrativa'] = df_processed['ds_dependencia_administrativa'].str.upper()
            df_processed['ds_dependencia_administrativa'] = df_processed['ds_dependencia_administrativa'].map(dependencia_map).fillna('Não informado')
        
        # 5. Padronizar organização acadêmica
        if 'ds_organizacao_academica' in df_processed.columns:
            organizacao_map = {
                'UNIVERSIDADE': 'Universidade',
                'CENTRO UNIVERSITARIO': 'Centro Universitário',
                'FACULDADE': 'Faculdade',
                'INSTITUTO FEDERAL': 'Instituto Federal'
            }
            df_processed['ds_organizacao_academica'] = df_processed['ds_organizacao_academica'].str.upper()
            df_processed['ds_organizacao_academica'] = df_processed['ds_organizacao_academica'].map(organizacao_map).fillna('Não informado')
        
        # 6. Padronizar regiões
        if 'nm_regiao' in df_processed.columns:
            regiao_map = {
                'NORTE': 'Norte',
                'NORDESTE': 'Nordeste',
                'CENTRO-OESTE': 'Centro-Oeste', 
                'SUDESTE': 'Sudeste',
                'SUL': 'Sul'
            }
            df_processed['nm_regiao'] = df_processed['nm_regiao'].str.upper()
            df_processed['nm_regiao'] = df_processed['nm_regiao'].map(regiao_map).fillna('Não informado')
        
        # 7. Padronizar siglas de UF
        if 'sg_uf_programa' in df_processed.columns:
            df_processed['sg_uf_programa'] = df_processed['sg_uf_programa'].str.upper().str.strip()
        
        logger.info(f"✅ Processamento concluído: {len(df_processed):,} registros processados")
        return df_processed
        
    except Exception as e:
        logger.error(f"❌ Erro durante processamento: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def criar_dimensao_ies():
    """
    Cria a dimensão IES usando pandas para manipular os dados.
    """
    logger.info("🏛️ Criando dimensão IES com pandas...")
    db = get_db_manager()
    
    try:
        # 1. Remover tabela existente se houver
        logger.info("🗑️  Removendo dim_ies existente...")
        drop_sql = "DROP TABLE IF EXISTS dim_ies CASCADE;"
        db.execute_sql(drop_sql)
        
        # 2. Criar tabela dim_ies
        logger.info("🏗️  Criando nova estrutura dim_ies...")
        create_sql = """
        CREATE TABLE dim_ies (
            ies_sk SERIAL PRIMARY KEY,
            codigo_entidade_capes INTEGER,
            codigo_entidade_emec INTEGER,
            nome_ies VARCHAR(500),
            sigla VARCHAR(50),
            status_juridico VARCHAR(100),
            categoria_administrativa VARCHAR(100),
            organizacao_academica VARCHAR(100),
            regiao VARCHAR(50),
            sigla_uf VARCHAR(10),
            municipio VARCHAR(200),
            ano_base INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        if not db.execute_sql(create_sql):
            logger.error("❌ Erro ao criar tabela dim_ies")
            return False
            
        # 3. Inserir registro SK=0 (desconhecido)
        logger.info("🔧 Inserindo registro DESCONHECIDO (SK=0)...")
        sk0_data = pd.DataFrame({
            'ies_sk': [0],
            'codigo_entidade_capes': [0],
            'codigo_entidade_emec': [0],
            'nome_ies': ['NÃO INFORMADO'],
            'sigla': ['XX'],
            'status_juridico': ['NÃO INFORMADO'],
            'categoria_administrativa': ['NÃO INFORMADO'],
            'organizacao_academica': ['NÃO INFORMADO'],
            'regiao': ['NÃO INFORMADO'],
            'sigla_uf': ['XX'],
            'municipio': ['NÃO INFORMADO'],
            'ano_base': [0]
        })
        
        # Inserir registro SK=0
        db.save_dataframe(sk0_data, 'dim_ies', if_exists='append')
        
        # 4. Carregar e processar dados de IES
        df_raw = carregar_dados_raw_ies()
        if df_raw is None:
            logger.error("❌ Falha ao carregar dados da raw_ies_api")
            return False
            
        df_ies = processar_dataframe_ies(df_raw)
        if df_ies is None:
            logger.error("❌ Falha ao processar DataFrame de IES")
            return False
        
        # 5. Mapear colunas para a estrutura da dimensão
        logger.info("🔄 Mapeando colunas para estrutura da dimensão...")
        df_final = pd.DataFrame()
        
        # Mapeamento de colunas
        mapeamento = {
            'codigo_entidade_capes': 'cd_entidade_capes',
            'codigo_entidade_emec': 'cd_entidade_emec',
            'nome_ies': 'nm_entidade_ensino',
            'sigla': 'sg_entidade_ensino',
            'status_juridico': 'cs_status_juridico',
            'categoria_administrativa': 'ds_dependencia_administrativa',
            'organizacao_academica': 'ds_organizacao_academica',
            'regiao': 'nm_regiao',
            'sigla_uf': 'sg_uf_programa',
            'municipio': 'nm_municipio_programa_ies',
            'ano_base': 'an_base'
        }
        
        # Aplicar mapeamento
        for col_destino, col_origem in mapeamento.items():
            if col_origem in df_ies.columns:
                df_final[col_destino] = df_ies[col_origem]
            else:
                # Valores padrão baseado no tipo
                if col_destino in ['codigo_entidade_capes', 'codigo_entidade_emec', 'ano_base']:
                    df_final[col_destino] = 0
                else:
                    df_final[col_destino] = 'Não informado'
        
        # 6. Inserir dados processados no banco
        logger.info("💾 Inserindo dados processados no banco...")
        db.save_dataframe(df_final, 'dim_ies', if_exists='append')
        
        # 7. Verificar inserção
        count_query = "SELECT COUNT(*) as total FROM dim_ies;"
        result = db.execute_query(count_query)
        total = result.iloc[0]['total']
        
        logger.info(f"✅ dim_ies criada com {total:,} registros")
        
        # 8. Criar índices para performance
        logger.info("🔍 Criando índices...")
        indices_sql = [
            "CREATE INDEX IF NOT EXISTS idx_dim_ies_codigo_capes ON dim_ies(codigo_entidade_capes);",
            "CREATE INDEX IF NOT EXISTS idx_dim_ies_sigla_uf ON dim_ies(sigla_uf);",
            "CREATE INDEX IF NOT EXISTS idx_dim_ies_regiao ON dim_ies(regiao);",
            "CREATE INDEX IF NOT EXISTS idx_dim_ies_categoria ON dim_ies(categoria_administrativa);"
        ]
        
        for idx_sql in indices_sql:
            db.execute_sql(idx_sql)
        
        logger.info("✅ Índices criados")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao criar dimensão IES: {str(e)}")
        return False

def validar_dimensao_ies():
    """Valida os dados da dimensão IES."""
    logger.info("🔍 Validando dimensão IES...")
    db = get_db_manager()
    
    try:
        print("\n" + "="*60)
        print("📊 VALIDAÇÃO DA DIMENSÃO IES")
        print("="*60)
        
        # 1. Contagem total
        query_total = "SELECT COUNT(*) as total FROM dim_ies;"
        result = db.execute_query(query_total)
        total = result.iloc[0]['total']
        print(f"📊 Total de registros: {total:,}")
        
        # 2. IES por região
        print("\n🌎 IES por região:")
        query_regiao = """
        SELECT 
            regiao,
            COUNT(*) as qtd_ies
        FROM dim_ies 
        WHERE ies_sk > 0
        GROUP BY regiao
        ORDER BY qtd_ies DESC;
        """
        result = db.execute_query(query_regiao)
        print(result.to_string(index=False))
        
        # 3. IES por categoria administrativa
        print("\n🏛️ IES por categoria administrativa:")
        query_categoria = """
        SELECT 
            categoria_administrativa,
            COUNT(*) as qtd_ies
        FROM dim_ies 
        WHERE ies_sk > 0
        GROUP BY categoria_administrativa
        ORDER BY qtd_ies DESC;
        """
        result = db.execute_query(query_categoria)
        print(result.to_string(index=False))
        
        # 4. IES por organização acadêmica
        print("\n🎓 IES por organização acadêmica:")
        query_organizacao = """
        SELECT 
            organizacao_academica,
            COUNT(*) as qtd_ies
        FROM dim_ies 
        WHERE ies_sk > 0
        GROUP BY organizacao_academica
        ORDER BY qtd_ies DESC;
        """
        result = db.execute_query(query_organizacao)
        print(result.to_string(index=False))
        
        # 5. Top 10 estados com mais IES
        print("\n🗺️ Top 10 estados com mais IES:")
        query_uf = """
        SELECT 
            sigla_uf,
            COUNT(*) as qtd_ies
        FROM dim_ies 
        WHERE ies_sk > 0 AND sigla_uf != 'XX'
        GROUP BY sigla_uf
        ORDER BY qtd_ies DESC
        LIMIT 10;
        """
        result = db.execute_query(query_uf)
        print(result.to_string(index=False))
        
        print("\n✅ Validação concluída!")
        
    except Exception as e:
        logger.error(f"❌ Erro na validação: {str(e)}")

def main():
    """Função principal."""
    try:
        logger.info("🚀 Iniciando criação da dimensão IES")
        
        # 1. Criar dimensão
        if not criar_dimensao_ies():
            logger.error("❌ Falha na criação da dimensão IES")
            return
            
        # 2. Validar dimensão
        validar_dimensao_ies()
        
        print("\n" + "="*70)
        print("🎉 DIMENSÃO IES CRIADA COM SUCESSO!")
        print("="*70)
        print("✅ Tabela: dim_ies")
        print("✅ Fonte: raw_ies_api")
        print("✅ Índices: Performance otimizada")
        print("✅ Dados: Tratados e normalizados")
        print("="*70)
        
    except Exception as e:
        logger.error(f"❌ Erro durante criação da dimensão IES: {str(e)}")

if __name__ == "__main__":
    main()
