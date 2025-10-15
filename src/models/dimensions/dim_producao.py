#!/usr/bin/env python3
"""
dim_producao.py

Módulo para criação e gerenciamento da dimensão de produções no Data Warehouse.

Descrição:
    Este módulo implementa o processo de ETL (Extract, Transform, Load) para a dimensão
    de produções acadêmicas da pós-graduação brasileira. Contém informações sobre as
    diferentes produções (artigos, livros, capítulos, trabalhos em eventos, etc.) 
    registradas nos programas de pós-graduação.
    
    A dimensão inclui:
    - Dados básicos de identificação (id_producao, tipo, subtipo)
    - Informações descritivas (título, ano, editora, periódico)
    - Classificações (tipo de produção, subtipo, natureza)
    - Métricas de qualidade (issn, isbn, doi)

Fontes de Dados:
    - Base Principal: add_producao_*.parquet (MinIO S3) - múltiplos anos
    - Fallback: Arquivo local em data/raw_producao/
    
Estrutura da Dimensão:
    - producao_sk: Surrogate key (chave substituta sequencial, inicia em 0)
    - id_producao: ID da produção no sistema CAPES
    - tipo_producao: Tipo de produção (bibliográfica, técnica, artística)
    - subtipo_producao: Subtipo específico
    - titulo_producao: Título da produção
    - ano_producao: Ano de publicação/realização
    - nome_periodico: Nome do periódico (para artigos)
    - issn: ISSN do periódico
    - isbn: ISBN (para livros)
    - doi: DOI da produção
    - editora: Editora
    - pais_publicacao: País de publicação
    - idioma: Idioma da produção
    - natureza_producao: Natureza (completo, resumo, etc.)
    - meio_divulgacao: Meio de divulgação
    - ano_base: Ano base de referência

Registro SK=0:
    - Registro especial com producao_sk=0 representa valores desconhecidos
    - Usado para integridade referencial nas tabelas fato

Processo ETL:
    1. Extração: Carrega dados do MinIO (S3) de múltiplos anos ou arquivo local
    2. Transformação: 
       - Padronização de nomes de colunas
       - Tratamento de valores nulos
       - Conversão de tipos de dados
       - Truncamento de campos VARCHAR
       - Remoção de duplicatas
    3. Carga: Inserção em chunks de 500 registros no PostgreSQL

Regras de Negócio:
    - Registro SK=0 sempre incluído para valores desconhecidos
    - Duplicatas removidas mantendo registro mais recente (ano_base DESC)
    - Campos VARCHAR truncados para respeitar limites da tabela
    - Valores nulos tratados com valores padrão ('NÃO INFORMADO', 'DESCONHECIDO')

Autor: Sistema DW OESNPG
Data: Outubro/2025
Versão: 1.0
"""

import os
import sys
import pandas as pd
import numpy as np
import logging
import time
from datetime import datetime
from typing import Optional, Dict, Any, List

# Adicionar o diretório raiz ao path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from dotenv import load_dotenv
from src.core.core import get_db_manager

# Carregar variáveis de ambiente
load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def get_logger():
    return logger


def carregar_dados_producao():
    """
    Carrega dados de produção do MinIO S3 ou arquivo local.
    
    Tenta carregar de múltiplos arquivos add_producao_*.parquet do MinIO.
    Se falhar, tenta carregar de arquivo local.
    
    Returns:
        pd.DataFrame: DataFrame com dados de produção ou None se falhar
        
    Raises:
        Exception: Se não conseguir carregar de nenhuma fonte
    """
    logger.info("🚀 Iniciando carregamento de dados de produção...")
    
    # Tentar carregar do MinIO primeiro
    try:
        import s3fs
        
        endpoint = os.getenv('MINIO_ENDPOINT')
        bucket = os.getenv('MINIO_BUCKET')
        access_key = os.getenv('MINIO_ACCESS_KEY')
        secret_key = os.getenv('MINIO_SECRET_KEY')
        
        logger.info(f"Conectando ao MinIO: {endpoint}")
        
        # Garantir que endpoint não tenha http:// duplicado
        if endpoint and not endpoint.startswith('http'):
            endpoint_url = f'http://{endpoint}'
        else:
            endpoint_url = endpoint or 'http://localhost:9000'
            
        s3 = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={'endpoint_url': endpoint_url}
        )
        
        # Listar arquivos de produção disponíveis (apenas add_producao, não _autor)
        files = s3.glob(f'{bucket}/add_capes/*')
        producao_files = [str(f) for f in files if 'add_producao_' in str(f) and '_autor' not in str(f) and str(f).endswith('.parquet') and ('2023' in str(f) or '2024' in str(f))]
        
        if not producao_files:
            logger.warning("Nenhum arquivo add_producao 2023-2024 encontrado no MinIO")
            raise FileNotFoundError("Arquivos de produção 2023-2024 não encontrados no MinIO")
        
        logger.info(f"✅ Encontrados {len(producao_files)} arquivos de produção no MinIO")
        
        # Carregar e concatenar todos os arquivos
        dfs = []
        for file_path in sorted(producao_files):
            logger.info(f"📥 Carregando: {file_path}")
            df_temp = pd.read_parquet(f's3://{file_path}', storage_options={'key': access_key, 'secret': secret_key, 'client_kwargs': {'endpoint_url': endpoint_url}})
            # Extrair ano do nome do arquivo
            year = str(file_path).split('_')[-1].replace('.parquet', '')
            df_temp['ano_base'] = year
            dfs.append(df_temp)
            logger.info(f"   ✓ {len(df_temp):,} registros carregados")
        
        df = pd.concat(dfs, ignore_index=True)
        logger.info(f"✅ Total de {len(df):,} produções carregadas do MinIO")
        
        return df
        
    except ImportError:
        logger.warning("⚠️ Biblioteca s3fs não instalada. Tentando arquivo local...")
    except Exception as e:
        logger.warning(f"⚠️ Erro ao carregar do MinIO: {e}")
        logger.info("Tentando carregar de arquivo local...")
    
    # Fallback: tentar carregar arquivo local
    try:
        local_paths = [
            'data/raw_producao/add_producao.parquet',
            'staging/data/add_producao.parquet',
            '../data/add_producao.parquet'
        ]
        
        for local_path in local_paths:
            full_path = os.path.join(project_root, local_path)
            if os.path.exists(full_path):
                logger.info(f"📂 Carregando arquivo local: {full_path}")
                df = pd.read_parquet(full_path)
                logger.info(f"✅ {len(df):,} produções carregadas localmente")
                return df
        
        raise FileNotFoundError("Nenhum arquivo de produção encontrado localmente")
        
    except Exception as e:
        logger.error(f"❌ Erro ao carregar dados de produção: {e}")
        raise


def transformar_dados_producao(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma e padroniza dados de produção.
    
    Args:
        df: DataFrame bruto de produção
        
    Returns:
        pd.DataFrame: DataFrame transformado e limpo
    """
    logger.info("🔄 Transformando dados de produção...")
    
    df_transformado = df.copy()
    
    # Renomear colunas para padronização
    colunas_map = {
        'ID_ADD_PRODUCAO': 'id_producao',
        'ID_TIPO_PRODUCAO': 'id_tipo_producao',
        'ID_SUBTIPO_PRODUCAO': 'id_subtipo_producao',
        'NM_TIPO_PRODUCAO': 'tipo_producao',
        'NM_SUBTIPO_PRODUCAO': 'subtipo_producao',
        'DS_TITULO': 'titulo_producao',
        'AN_BASE': 'ano_producao',
        'NM_PERIODICO': 'nome_periodico',
        'DS_ISSN': 'issn',
        'DS_ISBN': 'isbn',
        'DS_DOI': 'doi',
        'NM_EDITORA': 'editora',
        'SG_PAIS_PUBLICACAO': 'pais_publicacao',
        'NM_IDIOMA': 'idioma',
        'DS_NATUREZA': 'natureza_producao',
        'DS_MEIO_DIVULGACAO': 'meio_divulgacao',
        'AN_BASE_PRODUCAO': 'ano_base'
    }
    
    # Renomear colunas que existem
    cols_to_rename = {k: v for k, v in colunas_map.items() if k in df_transformado.columns}
    df_transformado = df_transformado.rename(columns=cols_to_rename)
    
    # Garantir que colunas essenciais existam
    colunas_essenciais = [
        'id_producao', 'tipo_producao', 'subtipo_producao', 'titulo_producao',
        'ano_producao', 'ano_base'
    ]
    
    for col in colunas_essenciais:
        if col not in df_transformado.columns:
            # Tentar encontrar coluna similar
            similar_cols = [c for c in df_transformado.columns if col.replace('_', '').lower() in c.replace('_', '').lower()]
            if similar_cols:
                df_transformado[col] = df_transformado[similar_cols[0]]
            else:
                df_transformado[col] = 'NÃO INFORMADO' if col in ['tipo_producao', 'subtipo_producao', 'titulo_producao'] else 0
    
    # Tratar valores nulos
    df_transformado['tipo_producao'] = df_transformado['tipo_producao'].fillna('NÃO INFORMADO')
    df_transformado['subtipo_producao'] = df_transformado['subtipo_producao'].fillna('NÃO INFORMADO')
    df_transformado['titulo_producao'] = df_transformado['titulo_producao'].fillna('SEM TÍTULO')
    df_transformado['ano_producao'] = pd.to_numeric(df_transformado['ano_producao'], errors='coerce').fillna(0).astype(int)
    
    # Truncar campos VARCHAR para limites da tabela
    if 'titulo_producao' in df_transformado.columns:
        df_transformado['titulo_producao'] = df_transformado['titulo_producao'].astype(str).str[:500]
    
    if 'nome_periodico' in df_transformado.columns:
        df_transformado['nome_periodico'] = df_transformado['nome_periodico'].fillna('NÃO INFORMADO').astype(str).str[:300]
    
    if 'editora' in df_transformado.columns:
        df_transformado['editora'] = df_transformado['editora'].fillna('NÃO INFORMADO').astype(str).str[:200]
    
    if 'issn' in df_transformado.columns:
        df_transformado['issn'] = df_transformado['issn'].fillna('').astype(str).str[:20]
    
    if 'isbn' in df_transformado.columns:
        df_transformado['isbn'] = df_transformado['isbn'].fillna('').astype(str).str[:20]
    
    if 'doi' in df_transformado.columns:
        df_transformado['doi'] = df_transformado['doi'].fillna('').astype(str).str[:100]
    
    if 'pais_publicacao' in df_transformado.columns:
        df_transformado['pais_publicacao'] = df_transformado['pais_publicacao'].fillna('BRA').astype(str).str[:10]
    
    if 'idioma' in df_transformado.columns:
        df_transformado['idioma'] = df_transformado['idioma'].fillna('PORTUGUÊS').astype(str).str[:50]
    
    if 'natureza_producao' in df_transformado.columns:
        df_transformado['natureza_producao'] = df_transformado['natureza_producao'].fillna('NÃO INFORMADO').astype(str).str[:100]
    
    if 'meio_divulgacao' in df_transformado.columns:
        df_transformado['meio_divulgacao'] = df_transformado['meio_divulgacao'].fillna('NÃO INFORMADO').astype(str).str[:100]
    
    # Selecionar apenas colunas finais
    colunas_finais = [
        'id_producao', 'tipo_producao', 'subtipo_producao', 'titulo_producao',
        'ano_producao', 'ano_base'
    ]
    
    # Adicionar colunas opcionais se existirem
    colunas_opcionais = [
        'nome_periodico', 'issn', 'isbn', 'doi', 'editora', 
        'pais_publicacao', 'idioma', 'natureza_producao', 'meio_divulgacao'
    ]
    
    for col in colunas_opcionais:
        if col in df_transformado.columns:
            colunas_finais.append(col)
    
    df_transformado = df_transformado[colunas_finais]
    
    # Remover duplicatas mantendo o registro mais recente
    logger.info(f"Registros antes de remover duplicatas: {len(df_transformado):,}")
    df_transformado = df_transformado.sort_values('ano_base', ascending=False)
    df_transformado = df_transformado.drop_duplicates(subset=['id_producao'], keep='first')
    logger.info(f"Registros após remover duplicatas: {len(df_transformado):,}")
    
    logger.info(f"✅ Transformação concluída: {len(df_transformado):,} produções")
    
    return df_transformado


def criar_tabela(db):
    """
    Cria a tabela dim_producao no banco de dados.
    
    Args:
        db: Instância do DatabaseManager
    """
    logger.info("🔨 Criando tabela dim_producao...")
    
    sql_drop = "DROP TABLE IF EXISTS dim_producao CASCADE;"
    
    sql_create = """
    CREATE TABLE dim_producao (
        producao_sk SERIAL PRIMARY KEY,
        id_producao VARCHAR(50) UNIQUE NOT NULL,
        tipo_producao VARCHAR(100) NOT NULL,
        subtipo_producao VARCHAR(100),
        titulo_producao VARCHAR(500),
        ano_producao INTEGER,
        nome_periodico VARCHAR(300),
        issn VARCHAR(20),
        isbn VARCHAR(20),
        doi VARCHAR(100),
        editora VARCHAR(200),
        pais_publicacao VARCHAR(10),
        idioma VARCHAR(50),
        natureza_producao VARCHAR(100),
        meio_divulgacao VARCHAR(100),
        ano_base VARCHAR(4) NOT NULL,
        data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT chk_ano_producao CHECK (ano_producao = 0 OR (ano_producao >= 1900 AND ano_producao <= 2100))
    );
    
    CREATE INDEX idx_dim_producao_id ON dim_producao(id_producao);
    CREATE INDEX idx_dim_producao_tipo ON dim_producao(tipo_producao);
    CREATE INDEX idx_dim_producao_ano ON dim_producao(ano_producao);
    CREATE INDEX idx_dim_producao_ano_base ON dim_producao(ano_base);
    """
    
    # Usar engine.begin() para executar comandos DDL
    with db.engine.begin() as conn:
        conn.exec_driver_sql(sql_drop)
        conn.exec_driver_sql(sql_create)
        
        # Inserir registro SK=0 para valores desconhecidos/não informados
        sql_insert_sk0 = """
        INSERT INTO dim_producao (
            producao_sk, id_producao, tipo_producao, subtipo_producao, 
            titulo_producao, ano_producao, ano_base
        ) VALUES (
            0, '0', 'Não informado', 'Não informado', 
            'Não informado', 0, '0'
        );
        
        -- Resetar sequence para começar do 1
        SELECT setval('dim_producao_producao_sk_seq', 1, false);
        """
        conn.exec_driver_sql(sql_insert_sk0)
    
    logger.info("✅ Tabela dim_producao criada com sucesso!")


def inserir_dados_producao(df: pd.DataFrame, db):
    """
    Insere dados de produção na tabela dim_producao em chunks.
    
    Args:
        df: DataFrame com dados transformados
        db: Instância do DatabaseManager
    """
    logger.info(f"📥 Inserindo {len(df):,} produções na tabela dim_producao...")
    
    chunk_size = 500
    total_chunks = (len(df) + chunk_size - 1) // chunk_size
    
    logger.info(f"Dividindo em {total_chunks} chunks de {chunk_size} registros")
    
    start_time = time.time()
    
    for i in range(0, len(df), chunk_size):
        chunk_num = (i // chunk_size) + 1
        chunk = df.iloc[i:i + chunk_size].copy()
        
        logger.info(f"📦 Inserindo chunk {chunk_num}/{total_chunks} - Registros {i} a {min(i + chunk_size, len(df))} ({len(chunk)} registros)")
        
        chunk_start = time.time()
        
        try:
            inserir_chunk_direto(chunk, db)
            
            chunk_time = time.time() - chunk_start
            logger.info(f"✅ Chunk {chunk_num} inserido com sucesso em {chunk_time:.2f}s")
            
        except Exception as e:
            logger.error(f"❌ Erro ao inserir chunk {chunk_num}: {e}")
            raise
    
    elapsed_time = time.time() - start_time
    logger.info(f"✅ Inserção concluída em {elapsed_time:.2f}s ({len(df)/elapsed_time:.0f} registros/s)")
    
    # Verificar total inserido
    result = db.execute_query("SELECT COUNT(*) as total FROM dim_producao WHERE producao_sk > 0")
    total_inserido = result[0]['total'] if result else 0
    
    logger.info(f"✅ Inserção concluída! Total de registros inseridos: {total_inserido:,}")
    logger.info(f"📊 Esperado: {len(df):,}, Inserido: {total_inserido:,}")


def inserir_chunk_direto(chunk, db):
    """
    Insere um chunk de dados diretamente usando to_sql do pandas.
    
    Args:
        chunk: DataFrame com chunk de dados
        db: Gerenciador de banco de dados
    """
    chunk.to_sql(
        'dim_producao',
        db.engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=100
    )
    
    logger.info("✅ Chunk inserido com sucesso usando to_sql")


def executar_etl_producao():
    """
    Executa o processo completo de ETL para dim_producao.
    """
    logger.info("=" * 80)
    logger.info("INICIANDO ETL - DIMENSÃO PRODUÇÃO")
    logger.info("=" * 80)
    
    start_time = time.time()
    
    try:
        # 1. Carregar dados
        df = carregar_dados_producao()
        
        if df is None or len(df) == 0:
            logger.error("❌ Nenhum dado de produção carregado!")
            return
        
        # 2. Transformar dados
        df_transformado = transformar_dados_producao(df)
        
        # 3. Conectar ao banco
        logger.info("🔌 Conectando ao banco de dados...")
        db = get_db_manager()
        
        # 4. Criar tabela
        criar_tabela(db)
        
        # 5. Inserir dados
        inserir_dados_producao(df_transformado, db)
        
        elapsed_time = time.time() - start_time
        logger.info("=" * 80)
        logger.info(f"🎉 ETL CONCLUÍDO COM SUCESSO em {elapsed_time:.2f}s!")
        logger.info("=" * 80)
        
        # Estatísticas finais
        result = db.execute_query("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT tipo_producao) as tipos_distintos,
                COUNT(DISTINCT ano_producao) as anos_distintos,
                MIN(ano_producao) as ano_min,
                MAX(ano_producao) as ano_max
            FROM dim_producao 
            WHERE producao_sk > 0
        """)
        
        if len(result) > 0:
            stats = result[0]
            logger.info(f"📊 Total de produções: {stats['total']:,}")
            logger.info(f"📊 Tipos distintos: {stats['tipos_distintos']:,}")
            logger.info(f"📊 Anos distintos: {stats['anos_distintos']:,}")
            logger.info(f"📊 Período: {stats['ano_min']} a {stats['ano_max']}")
        
    except Exception as e:
        logger.error(f"❌ Erro durante ETL: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    executar_etl_producao()
