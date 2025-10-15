#!/usr/bin/env python3
"""
fact_producao.py

Módulo para criação e gerenciamento da tabela fato de produção intelectual no Data Warehouse.

Descrição:
    Este módulo implementa o processo de ETL (Extract, Transform, Load) para a tabela fato
    de produção intelectual da pós-graduação brasileira. Registra a produção científica
    (artigos, livros, trabalhos em eventos, etc.) e suas autorias.
    
    A tabela fato contém:
    - Chaves estrangeiras para dimensões (docente, discente, titulado, posdoc, tempo, localidade)
    - Métricas de produção
    - Fatos sobre a autoria (tipo de autor, ordem, categoria)
    
Fontes de Dados:
    - Base Principal: add_producao_autor_2023.parquet (MinIO S3)
    - Fallback: Arquivo local em data/raw_producao/add_producao_autor_2023.parquet
    - Dimensões: dim_docente, dim_discente, dim_titulado, dim_posdoc, dim_tempo, dim_localidade

Estrutura da Tabela Fato:
    - producao_id: ID único da produção intelectual
    - tempo_sk: Chave estrangeira para dim_tempo (ano base)
    - docente_sk: Chave estrangeira para dim_docente (autor docente)
    - discente_sk: Chave estrangeira para dim_discente (autor discente)
    - titulado_sk: Chave estrangeira para dim_titulado (autor titulado/egresso)
    - posdoc_sk: Chave estrangeira para dim_posdoc (autor pós-doc)
    - localidade_sk: Chave estrangeira para dim_localidade (IES do programa)
    - tipo_producao: Tipo da produção (1=Bibliográfica, 2=Técnica, 3=Artística)
    - subtipo_producao: Subtipo específico da produção
    - tipo_autor: Categoria do autor (DOCENTE, DISCENTE, PÓS-DOC, EGRESSO, PARTICIPANTE EXTERNO)
    - ordem_autor: Ordem do autor na produção
    - qtd_producao: Sempre 1 (para agregação)
    
Processo ETL:
    1. Carregamento: Lê dados do MinIO ou arquivo local
    2. Transformação: 
       - Mapeia IDs de pessoas para surrogate keys das dimensões
       - Trata valores nulos com SK=0 (unknown)
       - Calcula métricas agregadas
    3. Carga: Insere em chunks no PostgreSQL

Regras de Negócio:
    - Cada registro representa uma autoria de produção
    - Uma produção pode ter múltiplos autores
    - SKs ausentes são preenchidos com 0 (unknown)
    - Produção sem autor conhecido é registrada com todos SKs=0
    - Agregações são feitas por soma de qtd_producao
    
Autor: Sistema DW OESNPG
Data: 2025-10-14
"""

import sys
import os
import pandas as pd
import logging
from datetime import datetime

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
    Carrega dados de produção intelectual do arquivo Parquet (MinIO ou local).
    
    Returns:
        DataFrame com dados de produção e autoria
    """
    try:
        logger.info("📚 Carregando dados de produção intelectual...")
        
        # Tentar carregar dados locais primeiro
        local_path = os.path.join(project_root, 'data', 'raw_producao', 'add_producao_autor_2023.parquet')
        
        if os.path.exists(local_path):
            logger.info(f"Carregando dados locais de {local_path}")
            df = pd.read_parquet(local_path)
            logger.info(f"✅ Dados locais carregados: {len(df):,} registros")
            return df
        
        # Se não encontrar local, tentar MinIO
        logger.info("Arquivo local não encontrado. Tentando MinIO...")
        
        # Obter credenciais das variáveis de ambiente
        endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        bucket = os.getenv("MINIO_BUCKET", "observatorio-servicos-bronze")
        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")
        
        if not access_key or not secret_key:
            raise ValueError("❌ Credenciais do MinIO não configuradas (MINIO_ACCESS_KEY, MINIO_SECRET_KEY)")
        
        storage_options = {
            'key': access_key,
            'secret': secret_key,
            'client_kwargs': {'endpoint_url': endpoint}
        }
        
        path = f"s3://{bucket}/add_capes/add_producao_autor_2023.parquet"
        logger.info(f"Tentando carregar de: {path}")
        
        df = pd.read_parquet(path, storage_options=storage_options)
        logger.info(f"✅ Dados carregados do MinIO: {len(df):,} registros")
        return df
        
    except Exception as e:
        logger.error(f"❌ Falha ao carregar dados: {e}")
        raise


def carregar_dimensoes(db):
    """
    Carrega os mapeamentos de IDs das dimensões para surrogate keys.
    
    Args:
        db: Gerenciador de banco de dados
        
    Returns:
        dict: Dicionários de mapeamento para cada dimensão
    """
    logger.info("📊 Carregando mapeamentos das dimensões...")
    
    mapeamentos = {}
    
    # dim_docente: id_pessoa -> docente_sk
    logger.info("  Carregando dim_docente...")
    docentes = db.execute_query("SELECT CAST(id_pessoa AS VARCHAR) as id_pessoa, docente_sk FROM dim_docente WHERE docente_sk > 0")
    mapeamentos['docente'] = dict(zip(docentes['id_pessoa'].astype(str), docentes['docente_sk']))
    logger.info(f"    ✅ {len(mapeamentos['docente']):,} docentes mapeados")
    
    # dim_discente: id_pessoa -> discente_sk
    logger.info("  Carregando dim_discente...")
    discentes = db.execute_query("SELECT CAST(id_pessoa AS VARCHAR) as id_pessoa, discente_sk FROM dim_discente WHERE discente_sk > 0")
    mapeamentos['discente'] = dict(zip(discentes['id_pessoa'].astype(str), discentes['discente_sk']))
    logger.info(f"    ✅ {len(mapeamentos['discente']):,} discentes mapeados")
    
    # dim_titulado: id_pessoa -> titulado_sk
    logger.info("  Carregando dim_titulado...")
    titulados = db.execute_query("SELECT CAST(id_pessoa AS VARCHAR) as id_pessoa, titulado_sk FROM dim_titulado WHERE titulado_sk > 0")
    mapeamentos['titulado'] = dict(zip(titulados['id_pessoa'].astype(str), titulados['titulado_sk']))
    logger.info(f"    ✅ {len(mapeamentos['titulado']):,} titulados mapeados")
    
    # dim_posdoc: id_pessoa -> posdoc_sk
    logger.info("  Carregando dim_posdoc...")
    posdocs = db.execute_query("SELECT CAST(id_pessoa AS VARCHAR) as id_pessoa, posdoc_sk FROM dim_posdoc WHERE posdoc_sk > 0")
    mapeamentos['posdoc'] = dict(zip(posdocs['id_pessoa'].astype(str), posdocs['posdoc_sk']))
    logger.info(f"    ✅ {len(mapeamentos['posdoc']):,} pós-docs mapeados")
    
    # dim_tempo: ano -> tempo_sk
    logger.info("  Carregando dim_tempo...")
    tempos = db.execute_query("SELECT ano, tempo_sk FROM dim_tempo WHERE tempo_sk > 0")
    mapeamentos['tempo'] = dict(zip(tempos['ano'].astype(int), tempos['tempo_sk']))
    logger.info(f"    ✅ {len(mapeamentos['tempo']):,} anos mapeados")
    
    logger.info("✅ Todos os mapeamentos carregados com sucesso")
    return mapeamentos


def transformar_dados_producao(df, mapeamentos):
    """
    Transforma os dados de produção para a tabela fato.
    
    Args:
        df: DataFrame com dados brutos de produção
        mapeamentos: Dicionários de mapeamento das dimensões
        
    Returns:
        DataFrame transformado para inserção na tabela fato
    """
    logger.info("🔄 Transformando dados para tabela fato de produção...")
    logger.info(f"Total de registros a transformar: {len(df):,}")
    
    # Selecionar colunas relevantes
    df_fato = df[[
        'ID_ADD_PRODUCAO_INTELECTUAL',
        'AN_BASE_PRODUCAO',
        'ID_TIPO_PRODUCAO',
        'ID_SUBTIPO_PRODUCAO',
        'TP_AUTOR',
        'NR_ORDEM',
        'ID_PESSOA_DOCENTE',
        'ID_PESSOA_DISCENTE',
        'ID_PESSOA_POS_DOC',
        'ID_PESSOA_EGRESSO',
        'CD_PROGRAMA',
        'SG_IES'
    ]].copy()
    
    # Renomear colunas
    df_fato = df_fato.rename(columns={
        'ID_ADD_PRODUCAO_INTELECTUAL': 'producao_id',
        'AN_BASE_PRODUCAO': 'ano_base',
        'ID_TIPO_PRODUCAO': 'tipo_producao',
        'ID_SUBTIPO_PRODUCAO': 'subtipo_producao',
        'TP_AUTOR': 'tipo_autor',
        'NR_ORDEM': 'ordem_autor'
    })
    
    # Mapear IDs para surrogate keys
    logger.info("Mapeando IDs para surrogate keys...")
    
    # Mapear tempo_sk
    df_fato['tempo_sk'] = df_fato['ano_base'].fillna(0).astype(int).astype(str).map(mapeamentos['tempo']).fillna(0).astype(int)
    
    # Mapear docente_sk (converter float -> int -> string para evitar '177173.0')
    df_fato['docente_sk'] = df_fato['ID_PESSOA_DOCENTE'].fillna(0).astype(int).astype(str).map(mapeamentos['docente']).fillna(0).astype(int)
    
    # Mapear discente_sk (converter float -> int -> string para evitar '177173.0')
    df_fato['discente_sk'] = df_fato['ID_PESSOA_DISCENTE'].fillna(0).astype(int).astype(str).map(mapeamentos['discente']).fillna(0).astype(int)
    
    # Mapear posdoc_sk (converter float -> int -> string para evitar '177173.0')
    df_fato['posdoc_sk'] = df_fato['ID_PESSOA_POS_DOC'].fillna(0).astype(int).astype(str).map(mapeamentos['posdoc']).fillna(0).astype(int)
    
    # Mapear titulado_sk (egressos) (converter float -> int -> string para evitar '177173.0')
    df_fato['titulado_sk'] = df_fato['ID_PESSOA_EGRESSO'].fillna(0).astype(int).astype(str).map(mapeamentos['titulado']).fillna(0).astype(int)
    
    # Por enquanto, localidade_sk = 0 (não temos dim_ies ainda)
    df_fato['localidade_sk'] = 0
    
    # Adicionar métrica de contagem
    df_fato['qtd_producao'] = 1
    
    # Tratar valores nulos no tipo_autor
    df_fato['tipo_autor'] = df_fato['tipo_autor'].fillna('NÃO INFORMADO')
    df_fato['tipo_autor'] = df_fato['tipo_autor'].replace('-', 'PARTICIPANTE EXTERNO')
    
    # Selecionar apenas colunas finais
    colunas_finais = [
        'producao_id',
        'tempo_sk',
        'docente_sk',
        'discente_sk',
        'titulado_sk',
        'posdoc_sk',
        'localidade_sk',
        'tipo_producao',
        'subtipo_producao',
        'tipo_autor',
        'ordem_autor',
        'qtd_producao'
    ]
    
    df_fato = df_fato[colunas_finais]
    
    # Estatísticas de mapeamento
    logger.info("\n📊 Estatísticas de mapeamento:")
    logger.info(f"  Autores docentes mapeados: {(df_fato['docente_sk'] > 0).sum():,}")
    logger.info(f"  Autores discentes mapeados: {(df_fato['discente_sk'] > 0).sum():,}")
    logger.info(f"  Autores titulados mapeados: {(df_fato['titulado_sk'] > 0).sum():,}")
    logger.info(f"  Autores pós-docs mapeados: {(df_fato['posdoc_sk'] > 0).sum():,}")
    logger.info(f"  Produções com ao menos um autor mapeado: {((df_fato['docente_sk'] > 0) | (df_fato['discente_sk'] > 0) | (df_fato['titulado_sk'] > 0) | (df_fato['posdoc_sk'] > 0)).sum():,}")
    
    logger.info(f"✅ Tabela fato transformada: {len(df_fato):,} registros")
    return df_fato


def criar_tabela(db):
    """
    Cria a tabela fact_producao no banco de dados.
    
    Args:
        db: Gerenciador de banco de dados
    """
    logger.info("🗄️ Criando tabela fact_producao...")
    
    # Dropar tabela se existir
    drop_sql = "DROP TABLE IF EXISTS fact_producao CASCADE;"
    db.execute_sql(drop_sql)
    logger.info("🗑️ Tabela fact_producao removida se existia")
    
    # Criar tabela
    create_sql = """
    CREATE TABLE fact_producao (
        producao_id BIGINT NOT NULL,
        tempo_sk INTEGER NOT NULL DEFAULT 0,
        docente_sk INTEGER NOT NULL DEFAULT 0,
        discente_sk INTEGER NOT NULL DEFAULT 0,
        titulado_sk INTEGER NOT NULL DEFAULT 0,
        posdoc_sk INTEGER NOT NULL DEFAULT 0,
        localidade_sk INTEGER NOT NULL DEFAULT 0,
        tipo_producao INTEGER NOT NULL,
        subtipo_producao INTEGER NOT NULL,
        tipo_autor VARCHAR(50),
        ordem_autor INTEGER,
        qtd_producao INTEGER NOT NULL DEFAULT 1,
        CONSTRAINT fk_tempo FOREIGN KEY (tempo_sk) REFERENCES dim_tempo(tempo_sk),
        CONSTRAINT fk_docente FOREIGN KEY (docente_sk) REFERENCES dim_docente(docente_sk),
        CONSTRAINT fk_discente FOREIGN KEY (discente_sk) REFERENCES dim_discente(discente_sk),
        CONSTRAINT fk_titulado FOREIGN KEY (titulado_sk) REFERENCES dim_titulado(titulado_sk),
        CONSTRAINT fk_posdoc FOREIGN KEY (posdoc_sk) REFERENCES dim_posdoc(posdoc_sk)
    );
    """
    
    db.execute_sql(create_sql)
    logger.info("✅ Tabela fact_producao criada com sucesso")
    
    # Adicionar comentários
    comment_sql = """
    COMMENT ON TABLE fact_producao IS 'Tabela fato de produção intelectual da pós-graduação';
    COMMENT ON COLUMN fact_producao.producao_id IS 'ID único da produção intelectual';
    COMMENT ON COLUMN fact_producao.tempo_sk IS 'FK para dim_tempo (ano base da produção)';
    COMMENT ON COLUMN fact_producao.docente_sk IS 'FK para dim_docente (autor docente), 0 se não aplicável';
    COMMENT ON COLUMN fact_producao.discente_sk IS 'FK para dim_discente (autor discente), 0 se não aplicável';
    COMMENT ON COLUMN fact_producao.titulado_sk IS 'FK para dim_titulado (autor egresso), 0 se não aplicável';
    COMMENT ON COLUMN fact_producao.posdoc_sk IS 'FK para dim_posdoc (autor pós-doc), 0 se não aplicável';
    COMMENT ON COLUMN fact_producao.localidade_sk IS 'FK para dim_localidade (IES do programa)';
    COMMENT ON COLUMN fact_producao.tipo_producao IS 'Tipo de produção (1=Bibliográfica, 2=Técnica, 3=Artística)';
    COMMENT ON COLUMN fact_producao.subtipo_producao IS 'Subtipo específico da produção';
    COMMENT ON COLUMN fact_producao.tipo_autor IS 'Categoria do autor (DOCENTE, DISCENTE, etc)';
    COMMENT ON COLUMN fact_producao.ordem_autor IS 'Ordem do autor na lista de autoria';
    COMMENT ON COLUMN fact_producao.qtd_producao IS 'Quantidade (sempre 1 para agregação)';
    """
    db.execute_sql(comment_sql)
    logger.info("✅ Comentários adicionados à tabela")


def inserir_dados_producao(df, db, chunk_size=500):
    """
    Insere dados na tabela fact_producao em chunks.
    
    Args:
        df: DataFrame com dados transformados
        db: Gerenciador de banco de dados
        chunk_size: Tamanho dos chunks para inserção
    """
    logger.info(f"💾 Iniciando inserção de {len(df):,} registros de produção...")
    
    total_chunks = (len(df) + chunk_size - 1) // chunk_size
    logger.info(f"📦 Dados serão inseridos em {total_chunks:,} chunks de {chunk_size} registros")
    
    registros_inseridos = 0
    
    for i in range(0, len(df), chunk_size):
        chunk_num = (i // chunk_size) + 1
        chunk = df.iloc[i:i + chunk_size]
        
        logger.info(f"📦 Inserindo chunk {chunk_num}/{total_chunks} - Registros {i} a {min(i+chunk_size-1, len(df)-1)} ({len(chunk)} registros)")
        
        start_time = datetime.now()
        
        try:
            inserir_chunk_direto(chunk, db)
            registros_inseridos += len(chunk)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"✅ Chunk {chunk_num} inserido com sucesso em {elapsed:.2f}s")
            
        except Exception as e:
            logger.error(f"❌ Erro ao inserir chunk {chunk_num}: {e}")
            raise
    
    # Verificar total inserido
    total_db = db.execute_query("SELECT COUNT(*) as total FROM fact_producao")['total'].iloc[0]
    logger.info(f"✅ Inserção concluída! Total de registros inseridos: {total_db:,}")
    logger.info(f"📊 Esperado: {len(df):,}, Inserido: {total_db:,}")


def inserir_chunk_direto(chunk, db):
    """
    Insere um chunk de dados diretamente usando to_sql do pandas.
    
    Args:
        chunk: DataFrame com chunk de dados
        db: Gerenciador de banco de dados
    """
    chunk.to_sql(
        'fact_producao',
        db.engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=100
    )
    
    logger.info("✅ Chunk inserido com sucesso usando to_sql")


def main():
    """Função principal que executa todo o processo ETL."""
    try:
        logger.info("📚 INICIANDO CRIAÇÃO DA FACT_PRODUCAO")
        logger.info("=" * 70)
        
        # 1. Conectar ao banco
        logger.info("1️⃣ Conectando ao banco de dados...")
        db = get_db_manager()
        
        # 2. Carregar dados de produção
        logger.info("2️⃣ Carregando dados de produção...")
        df_producao = carregar_dados_producao()
        
        # 3. Carregar mapeamentos das dimensões
        logger.info("3️⃣ Carregando mapeamentos das dimensões...")
        mapeamentos = carregar_dimensoes(db)
        
        # 4. Transformar dados
        logger.info("4️⃣ Transformando dados...")
        df_fato = transformar_dados_producao(df_producao, mapeamentos)
        
        # 5. Criar tabela
        logger.info("5️⃣ Criando tabela no banco...")
        criar_tabela(db)
        
        # 6. Inserir dados
        logger.info("6️⃣ Inserindo dados...")
        inserir_dados_producao(df_fato, db)
        
        logger.info("🎉 FACT_PRODUCAO CRIADA COM SUCESSO!")
        
    except Exception as e:
        logger.error(f"💥 Erro no processo: {e}")
        raise


if __name__ == "__main__":
    main()
