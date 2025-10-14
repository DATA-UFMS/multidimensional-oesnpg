#!/usr/bin/env python3
"""
dim_posdoc.py

Módulo para criação e gerenciamento da dimensão de pós-doutorandos no Data Warehouse.

Descrição:
    Este módulo implementa o processo de ETL (Extract, Transform, Load) para a dimensão
    de pós-doutorandos da pós-graduação brasileira. Contém informações sobre pesquisadores
    que realizam ou realizaram estágio pós-doutoral em programas de pós-graduação.
    
    A dimensão inclui:
    - Dados básicos de identificação (id_pessoa, id_posdoc, nome, documentos)
    - Informações demográficas (sexo, raça/cor, nacionalidade)
    - Dados do pós-doutorado (programa, data de início, data de término, situação)
    - Informações acadêmicas (orientador, área de concentração)
    - Métricas de tempo (meses de duração)

Fontes de Dados:
    - Base Principal: add_posdoc.parquet (MinIO S3)
    - Fallback: Arquivo local em data/raw_posdoc/add_posdoc.parquet
    
Estrutura da Dimensão:
    - posdoc_sk: Surrogate key (chave substituta sequencial, inicia em 0)
    - id_posdoc: ID do pós-doutorando no sistema CAPES
    - id_pessoa: Identificador de pessoa no sistema CAPES
    - nome_posdoc: Nome completo do pós-doutorando
    - tipo_documento: Tipo do documento
    - numero_documento: Número do documento
    - sexo: Sexo do pós-doutorando
    - data_nascimento: Data de nascimento
    - idade_ano_base: Idade no ano base
    - pais_nacionalidade: País de nacionalidade
    - raca_cor: Raça/cor declarada
    - data_inicio: Data de início do pós-doutorado
    - data_termino: Data de término/saída do pós-doutorado
    - situacao_posdoc: Situação do pós-doutorando
    - faixa_etaria: Faixa etária
    - orientador_principal: Nome do orientador/supervisor
    - area_concentracao: Área de concentração do pós-doutorado
    - meses_duracao: Duração em meses do pós-doutorado
    - id_lattes: Identificador Lattes
    - ano_base: Ano base de referência

Registro SK=0:
    - Registro especial com posdoc_sk=0 representa valores desconhecidos
    - Usado para integridade referencial nas tabelas fato

Processo ETL:
    1. Extração: Carrega dados do MinIO (S3) ou arquivo local
    2. Transformação: 
       - Padronização de nomes de colunas
       - Tratamento de valores nulos
       - Conversão de tipos de dados
       - Criação de campos derivados (meses_duracao, faixa_etaria)
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
from typing import Optional, Dict, Any

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


def carregar_dados_posdoc():
    """
    Carrega dados de pós-doutorandos do arquivo Parquet (MinIO ou local).
    
    Returns:
        DataFrame com dados dos pós-doutorandos
    """
    logger.info("📚 Carregando dados de pós-doutorandos...")
    
    try:
        # Tentar carregar dados locais primeiro
        local_path = os.path.join(project_root, 'data', 'raw_posdoc', 'add_posdoc.parquet')
        
        if os.path.exists(local_path):
            logger.info(f"Carregando dados locais de {local_path}")
            df = pd.read_parquet(local_path)
            logger.info(f"✅ Dados locais carregados: {len(df):,} registros")
            return df
        
        # Se não encontrar local, tentar MinIO
        logger.info("Arquivo local não encontrado. Tentando MinIO...")
        
        # Obter credenciais das variáveis de ambiente
        endpoint = os.getenv("MINIO_ENDPOINT")
        bucket = os.getenv("MINIO_BUCKET")
        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")
        
        if not access_key or not secret_key:
            raise ValueError("❌ Credenciais do MinIO não configuradas (MINIO_ACCESS_KEY, MINIO_SECRET_KEY)")
        
        storage_options = {
            'key': access_key,
            'secret': secret_key,
            'client_kwargs': {'endpoint_url': endpoint}
        }
        
        path = f"s3://{bucket}/add_capes/add_posdoc.parquet"
        logger.info(f"Tentando carregar de: {path}")
        
        df = pd.read_parquet(path, storage_options=storage_options)
        logger.info(f"✅ Dados carregados do MinIO: {len(df):,} registros")
        return df
        
    except Exception as e:
        logger.error(f"❌ Falha ao carregar dados: {e}")
        raise


def transformar_dados_posdoc(df):
    """
    Transforma os dados de pós-doutorandos para a dimensão final.
    
    Args:
        df: DataFrame com dados brutos dos pós-doutorandos
        
    Returns:
        DataFrame com estrutura da dim_posdoc
    """
    logger.info("🔄 Transformando dados para dimensão de pós-doutorandos...")
    logger.info(f"Total de registros a transformar: {len(df):,}")
    
    # Mapeamento de colunas (baseado na estrutura real do add_posdoc.parquet)
    column_mapping = {
        'ID_POS_DOC': 'id_posdoc',
        'NM_PESSOA_POS_DOC': 'nome_posdoc',
        'NM_TP_IDENTIFICADOR': 'tipo_documento',
        'NR_DOCUMENTO': 'numero_documento',
        'TP_SEXO': 'sexo',
        'DT_NASCIMENTO': 'data_nascimento',
        'NM_PAIS_NACIONALIDADE': 'pais_nacionalidade',
        'DH_INICIO_POS_DOC': 'data_inicio',
        'DH_FIM_POS_DOC': 'data_termino',
        'NR_ORCID_POS_DOC': 'id_lattes',
        'AN_BASE': 'ano_base',
        # Campos adicionais de contexto
        'CD_PROGRAMA': 'codigo_programa',
        'NM_PROGRAMA': 'nome_programa',
        'SG_IES': 'sigla_ies',
        'NM_IES': 'nome_ies',
        'SG_UF_PROGRAMA_IES': 'uf_programa',
        'NM_AREA_AVALIACAO': 'area_avaliacao'
    }
    
    # Identificar colunas que existem
    available_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
    
    if len(available_columns) == 0:
        logger.warning("⚠️ Nenhuma coluna mapeada encontrada. Usando colunas originais...")
        df_dim = df.copy()
    else:
        df_dim = df.rename(columns=available_columns).copy()
    
    # Garantir que colunas essenciais existem
    if 'id_posdoc' not in df_dim.columns:
        # Tentar usar primeira coluna com ID
        id_cols = [col for col in df_dim.columns if 'id' in col.lower() and 'posdoc' in col.lower()]
        if id_cols:
            df_dim['id_posdoc'] = df_dim[id_cols[0]]
        else:
            df_dim['id_posdoc'] = df_dim.index.astype(str)
    
    # Remover duplicatas por ID do pós-doutorando (manter mais recente)
    logger.info(f"Removendo duplicatas de {len(df_dim):,} registros...")
    
    if 'ano_base' in df_dim.columns:
        df_dim = df_dim.sort_values('ano_base', ascending=False)
    
    df_dim = df_dim.drop_duplicates(subset=['id_posdoc'], keep='first')
    logger.info(f"Após remoção de duplicatas: {len(df_dim):,} pós-doutorandos únicos")
    
    # Criar surrogate key
    df_dim = df_dim.reset_index(drop=True)
    df_dim['posdoc_sk'] = df_dim.index + 1
    
    # Tratamento de valores nulos
    logger.info("Tratando valores nulos...")
    fillna_map = {
        'nome_posdoc': 'NÃO INFORMADO',
        'sexo': 'NÃO INFORMADO',
        'pais_nacionalidade': 'BRASIL',
        'tipo_documento': 'CPF',
        'numero_documento': '00000000000'
    }
    
    for col, fill_value in fillna_map.items():
        if col in df_dim.columns:
            df_dim[col] = df_dim[col].fillna(fill_value)
    
    # Converter tipos de dados
    logger.info("Convertendo tipos de dados...")
    
    if 'ano_base' in df_dim.columns:
        df_dim['ano_base'] = pd.to_numeric(df_dim['ano_base'], errors='coerce').fillna(2025).astype(int)
    
    # Calcular idade_ano_base a partir da data de nascimento e ano_base
    if 'data_nascimento' in df_dim.columns and 'ano_base' in df_dim.columns:
        df_dim['data_nascimento'] = pd.to_datetime(df_dim['data_nascimento'], errors='coerce')
        df_dim['idade_ano_base'] = df_dim['ano_base'] - df_dim['data_nascimento'].dt.year
        df_dim['idade_ano_base'] = df_dim['idade_ano_base'].fillna(0).astype(int)
    else:
        df_dim['idade_ano_base'] = 0
    
    # Criar campos derivados que não existem no parquet
    df_dim['id_pessoa'] = df_dim['id_posdoc'].astype(str)  # usar id_posdoc como id_pessoa
    df_dim['raca_cor'] = 'NÃO DECLARADO'  # não disponível no parquet
    df_dim['situacao_posdoc'] = 'ATIVO'  # assumir ativo se não tiver data_termino
    if 'data_termino' in df_dim.columns:
        df_dim.loc[df_dim['data_termino'].notna(), 'situacao_posdoc'] = 'CONCLUÍDO'
    df_dim['faixa_etaria'] = pd.cut(df_dim['idade_ano_base'], 
                                     bins=[0, 25, 35, 45, 55, 65, 100], 
                                     labels=['ATÉ 25', '26-35', '36-45', '46-55', '56-65', 'ACIMA DE 65'],
                                     right=True).astype(str)
    df_dim['faixa_etaria'] = df_dim['faixa_etaria'].replace('nan', 'NÃO INFORMADO')
    df_dim['orientador_principal'] = 'NÃO INFORMADO'  # não disponível
    df_dim['area_concentracao'] = df_dim.get('area_avaliacao', 'NÃO INFORMADO')
    
    # Calcular duração em meses se houver datas de início e término
    logger.info("Calculando campos derivados...")
    
    if 'data_inicio' in df_dim.columns and 'data_termino' in df_dim.columns:
        df_dim['data_inicio'] = pd.to_datetime(df_dim['data_inicio'], errors='coerce')
        df_dim['data_termino'] = pd.to_datetime(df_dim['data_termino'], errors='coerce')
        
        # Calcular meses de duração
        df_dim['meses_duracao'] = ((df_dim['data_termino'] - df_dim['data_inicio']).dt.days / 30.44).round(0)
        df_dim['meses_duracao'] = df_dim['meses_duracao'].fillna(0).astype(int)
    else:
        df_dim['meses_duracao'] = 0
    
    # Truncar campos VARCHAR para respeitar limites da tabela
    logger.info("Truncando campos VARCHAR...")
    varchar_limits = {
        'id_posdoc': 50,
        'id_pessoa': 50,
        'nome_posdoc': 255,
        'tipo_documento': 50,
        'numero_documento': 50,
        'sexo': 20,
        'pais_nacionalidade': 100,
        'raca_cor': 50,
        'situacao_posdoc': 100,
        'faixa_etaria': 50,
        'orientador_principal': 255,
        'area_concentracao': 255,
        'id_lattes': 50,
        # Campos adicionais
        'codigo_programa': 50,
        'nome_programa': 255,
        'sigla_ies': 20,
        'nome_ies': 255,
        'uf_programa': 5,
        'area_avaliacao': 100
    }
    
    for col, max_len in varchar_limits.items():
        if col in df_dim.columns:
            df_dim[col] = df_dim[col].astype(str).str[:max_len]
            # Substituir 'nan' string por None
            df_dim[col] = df_dim[col].replace('nan', None)
    
    # Adicionar registro SK=0 para unknown
    logger.info("Adicionando registro SK=0...")
    registro_sk0 = pd.DataFrame([{
        'posdoc_sk': 0,
        'id_posdoc': 'UNKNOWN_0',
        'id_pessoa': 'UNKNOWN_0',
        'nome_posdoc': 'PÓS-DOUTORANDO DESCONHECIDO',
        'tipo_documento': 'DESCONHECIDO',
        'numero_documento': 'UNKNOWN_0',
        'sexo': 'NÃO INFORMADO',
        'data_nascimento': pd.NaT,
        'idade_ano_base': 0,
        'pais_nacionalidade': 'DESCONHECIDO',
        'raca_cor': 'DESCONHECIDO',
        'data_inicio': pd.NaT,
        'data_termino': pd.NaT,
        'situacao_posdoc': 'DESCONHECIDO',
        'faixa_etaria': 'DESCONHECIDO',
        'orientador_principal': 'DESCONHECIDO',
        'area_concentracao': 'DESCONHECIDO',
        'meses_duracao': 0,
        'id_lattes': 'UNKNOWN_0',
        'ano_base': 0
    }])
    
    df_dim_final = pd.concat([registro_sk0, df_dim], ignore_index=True)
    
    # Reordenar colunas finais
    colunas_ordenadas = [
        'posdoc_sk',
        'id_posdoc',
        'id_pessoa',
        'nome_posdoc',
        'tipo_documento',
        'numero_documento',
        'sexo',
        'data_nascimento',
        'idade_ano_base',
        'pais_nacionalidade',
        'raca_cor',
        'data_inicio',
        'data_termino',
        'situacao_posdoc',
        'faixa_etaria',
        'orientador_principal',
        'area_concentracao',
        'meses_duracao',
        'id_lattes',
        'ano_base'
    ]
    
    # Selecionar apenas colunas que existem
    colunas_existentes = [col for col in colunas_ordenadas if col in df_dim_final.columns]
    df_dim_final = df_dim_final[colunas_existentes]
    
    logger.info(f"✅ Dimensão de pós-doutorandos criada: {len(df_dim_final):,} registros (incluindo SK=0)")
    
    return df_dim_final


def criar_tabela(db):
    """
    Cria a tabela dim_posdoc no banco de dados.
    
    Args:
        db: DatabaseManager instance
    """
    logger.info("🗄️ Criando tabela dim_posdoc...")
    
    # Primeiro remove se existir
    drop_sql = "DROP TABLE IF EXISTS dim_posdoc CASCADE"
    db.execute_sql(drop_sql)
    logger.info("🗑️ Tabela dim_posdoc removida se existia")
    
    # Depois cria novamente
    create_sql = """
    CREATE TABLE IF NOT EXISTS dim_posdoc (
        posdoc_sk INTEGER PRIMARY KEY,
        id_posdoc VARCHAR(50) NOT NULL,
        id_pessoa VARCHAR(50),
        nome_posdoc VARCHAR(255),
        tipo_documento VARCHAR(50),
        numero_documento VARCHAR(50),
        sexo VARCHAR(20),
        data_nascimento DATE,
        idade_ano_base INTEGER,
        pais_nacionalidade VARCHAR(100),
        raca_cor VARCHAR(50),
        data_inicio DATE,
        data_termino DATE,
        situacao_posdoc VARCHAR(100),
        faixa_etaria VARCHAR(50),
        orientador_principal VARCHAR(255),
        area_concentracao VARCHAR(255),
        meses_duracao INTEGER,
        id_lattes VARCHAR(50),
        ano_base INTEGER
    );
    """
    
    db.execute_sql(create_sql)
    logger.info("✅ Tabela dim_posdoc criada com sucesso")
    
    # Adiciona comentários
    comment_sql = """
    COMMENT ON TABLE dim_posdoc IS 'Dimensão de pós-doutorandos - pesquisadores em estágio pós-doutoral';
    """
    
    db.execute_sql(comment_sql)
    logger.info("✅ Comentários adicionados à tabela")


def inserir_dados_posdoc(df_dim_posdoc, db):
    """
    Insere dados da dimensão pós-doutorandos no banco usando estratégia de chunks otimizada.
    
    Args:
        df_dim_posdoc: DataFrame com dados dos pós-doutorandos
        db: DatabaseManager instance
    """
    logger.info(f"💾 Iniciando inserção de {len(df_dim_posdoc):,} registros de pós-doutorandos...")
    
    try:
        # Configuração de chunks otimizados para evitar overflow de parâmetros SQL
        chunk_size = 500  # Tamanho reduzido para evitar overflow (PostgreSQL limite: 32.767 parâmetros)
        total_chunks = (len(df_dim_posdoc) + chunk_size - 1) // chunk_size
        
        logger.info(f"📦 Dados serão inseridos em {total_chunks} chunks de {chunk_size} registros")
        
        for chunk_num in range(total_chunks):
            start_idx = chunk_num * chunk_size
            end_idx = min((chunk_num + 1) * chunk_size, len(df_dim_posdoc))
            
            chunk_df = df_dim_posdoc.iloc[start_idx:end_idx].copy()
            
            logger.info(f"📦 Inserindo chunk {chunk_num + 1}/{total_chunks} - Registros {start_idx} a {end_idx-1} ({len(chunk_df)} registros)")
            
            try:
                # Tentativa de inserção do chunk
                start_time = time.time()
                
                # Usar método mais simples e direto para inserção
                resultado = inserir_chunk_direto(chunk_df, db)
                
                end_time = time.time()
                duration = end_time - start_time
                
                if resultado:
                    logger.info(f"✅ Chunk {chunk_num + 1} inserido com sucesso em {duration:.2f}s")
                else:
                    logger.error(f"❌ Falha ao inserir chunk {chunk_num + 1}")
                    raise Exception(f"Falha na inserção do chunk {chunk_num + 1}")
                    
            except Exception as e:
                logger.error(f"❌ Erro no chunk {chunk_num + 1}: {str(e)}")
                raise Exception(f"Falha na inserção do chunk {chunk_num + 1}")
        
        # Verificação final
        count_query = "SELECT COUNT(*) as total FROM dim_posdoc WHERE posdoc_sk > 0"
        resultado_count = db.execute_query(count_query)
        total_inserido = resultado_count.iloc[0]['total'] if not resultado_count.empty else 0
        
        logger.info(f"✅ Inserção concluída! Total de registros inseridos: {total_inserido:,}")
        logger.info(f"📊 Esperado: {len(df_dim_posdoc):,}, Inserido: {total_inserido:,}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao inserir dados: {str(e)}")
        raise


def inserir_chunk_direto(chunk_df, db):
    """
    Insere um chunk usando método direto to_sql.
    
    Args:
        chunk_df: DataFrame com chunk de dados
        db: DatabaseManager instance
        
    Returns:
        bool: True se sucesso, False caso contrário
    """
    try:
        # Usar to_sql diretamente que é mais eficiente
        chunk_df.to_sql(
            name='dim_posdoc',
            con=db.engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=100  # Inserir em chunks de 100 registros
        )
        logger.info(f"✅ Chunk inserido com sucesso usando to_sql")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro na inserção direta: {str(e)}")
        return False


def main():
    """
    Função principal para criação da dimensão de pós-doutorandos.
    """
    logger.info("📚 INICIANDO CRIAÇÃO DA DIM_POSDOC")
    logger.info("=" * 50)
    
    try:
        # 1. Conectar ao banco
        logger.info("1️⃣ Conectando ao banco de dados...")
        db = get_db_manager()
        
        # 2. Carregar dados
        logger.info("2️⃣ Carregando dados de pós-doutorandos...")
        df_bruto = carregar_dados_posdoc()
        
        # 3. Transformar dados
        logger.info("3️⃣ Transformando dados...")
        df_dim_final = transformar_dados_posdoc(df_bruto)
        
        # 4. Criar tabela
        logger.info("4️⃣ Criando tabela no banco...")
        criar_tabela(db)
        
        # 5. Inserir dados
        logger.info("5️⃣ Inserindo dados...")
        inserir_dados_posdoc(df_dim_final, db)
        
        logger.info("🎉 DIM_POSDOC CRIADA COM SUCESSO!")
        
    except Exception as e:
        logger.error(f"💥 Erro no processo: {e}")
        raise


if __name__ == "__main__":
    main()
