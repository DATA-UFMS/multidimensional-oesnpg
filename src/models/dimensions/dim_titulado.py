#!/usr/bin/env python3
"""
🎓 DIMENSÃO TITULADO - Data Warehouse Observatório CAPES
=======================================================
Cria a dimensão dim_titulado baseada nos discentes que JÁ CONCLUÍRAM
mestrado ou doutorado (subconjunto filtrado da dim_discente)

CRITÉRIOS DE INCLUSÃO:
- grau_academico IN ('MESTRADO', 'DOUTORADO', 'MESTRADO PROFISSIONAL')
- situacao_discente = 'TITULADO' OU meses_para_titulacao > 0
- Dados de titulação válidos e consistentes

Data: 22/09/2025 - Criação baseada em análise de discentes titulados
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

from src.utils.naming_conventions import NamingConventions
from src.validation.data_validator import validate_dimension_data, get_validation_summary
from src.core.exceptions import DimensionCreationError, DataValidationError


# Adicionar o diretório raiz ao path de forma relativa
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from src.core.core import get_db_manager

# Configurar logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def get_logger():
    return logger


def carregar_dados_discentes_titulados():
    """
    Carrega dados de discentes do arquivo parquet local.
    """
    logger.info("🎓 Carregando dados de discentes TITULADOS...")
    
    try:
        # Primeiro tentar o arquivo local
        local_path = "parquet_output/dim_discente.parquet"
        if os.path.exists(local_path):
            logger.info(f"Carregando dados do arquivo local: {local_path}")
            df = pd.read_parquet(local_path)
            logger.info(f"Dados locais carregados: {len(df):,} registros")
            return df
        
        # Se não encontrar local, tentar MinIO
        logger.info("Carregando dados do MinIO...")
        path = "s3://oesnpg/add_capes/add_discentes.parquet"
        storage_options = {
            'key': 'minioadmin',
            'secret': 'minioadmin',
            'client_kwargs': {'endpoint_url': 'http://localhost:9000'}
        }
        
        df = pd.read_parquet(path, storage_options=storage_options)
        logger.info(f"Dados brutos carregados: {len(df):,} registros")
        return df
        
    except Exception as e:
        logger.error(f"❌ Falha ao carregar dados: {e}")
        raise

def filtrar_titulados(df):
    """
    Filtra apenas os discentes que são titulados (concluíram mestrado/doutorado).
    """
    logger.info("🔍 Aplicando filtros para identificar titulados...")
    
    # Renomear colunas essenciais para o filtro
    df_filtro = df.rename(columns={
        'DS_GRAU_ACADEMICO_DISCENTE': 'grau_academico',
        'NM_SITUACAO_DISCENTE': 'situacao_discente', 
        'QT_MES_TITULACAO': 'meses_para_titulacao'
    })
    
    logger.info(f"Total de registros antes dos filtros: {len(df_filtro):,}")
    
    # FILTRO 1: Graus acadêmicos relevantes
    graus_validos = ['MESTRADO', 'DOUTORADO', 'MESTRADO PROFISSIONAL', 'DOUTORADO PROFISSIONAL']
    mask_grau = df_filtro['grau_academico'].isin(graus_validos)
    df_grau = df_filtro[mask_grau]
    logger.info(f"Após filtro de grau acadêmico: {len(df_grau):,} registros")
    
    # FILTRO 2: Situação indica titulação
    criterios_titulacao = [
        "TITULADO",
        "EGRESSO", 
        "FORMADO",
        "CONCLUÍDO",
        "DEFENDEU"
    ]
    
    mask_situacao = df_grau['situacao_discente'].str.contains(
        '|'.join(criterios_titulacao), case=False, na=False
    )
    
    # FILTRO 3: Tempo de titulação válido
    df_grau['meses_para_titulacao_num'] = pd.to_numeric(
        df_grau['meses_para_titulacao'], errors='coerce'
    )
    mask_tempo = df_grau['meses_para_titulacao_num'] > 0
    
    # COMBINAR CRITÉRIOS: situação OU tempo válido
    mask = mask_situacao | mask_tempo
    df_titulados = df_grau[mask].copy()
    
    logger.info(f"🎯 Titulados identificados: {len(df_titulados):,} registros")
    logger.info(f"📈 Taxa de titulação: {(len(df_titulados)/len(df_grau))*100:.1f}%")
    
    return df_titulados

def transformar_dados_titulado(df):
    """
    Transforma os dados de discentes titulados para a dimensão final.
    """
    logger.info("🔄 Transformando dados para dimensão de titulados...")
    
    # Debug: mostrar colunas disponíveis
    logger.info(f"🔍 Colunas disponíveis: {list(df.columns)}")
    
    # Primeiro, verificar qual é o campo ID correto
    id_column = None
    for col in ['ID_DISCENTE', 'id_discente', 'discente_id', 'ID_PESSOA', 'id_pessoa']:
        if col in df.columns:
            id_column = col
            break
    
    if id_column is None:
        logger.error("❌ Não foi possível identificar coluna de ID do discente")
        raise ValueError("Coluna de ID não encontrada")
    
    logger.info(f"✅ Usando coluna de ID: {id_column}")
    
    # Mapeamento de colunas específico para titulados (baseado nas colunas reais)
    column_mapping = {}
    
    # Tentar mapear as colunas principais
    possible_columns = {
        'id_discente_original': ['ID_DISCENTE', 'id_discente', 'discente_id'],
        'id_pessoa': ['ID_PESSOA', 'id_pessoa', 'pessoa_id'],
        'nome_titulado': ['NM_DISCENTE', 'nm_discente', 'nome_discente', 'nome'],
        'tipo_documento': ['TP_DOCUMENTO_DISCENTE', 'tipo_documento'],
        'numero_documento': ['NR_DOCUMENTO_DISCENTE', 'numero_documento'],
        'sexo': ['TP_SEXO_DISCENTE', 'sexo'],
        'data_nascimento': ['DT_NASCIMENTO_DISCENTE', 'data_nascimento'],
        'idade_ano_base': ['DS_IDADE_ANOBASE', 'idade_ano_base'],
        'pais_nacionalidade': ['NM_PAIS_NACIONALIDADE_DISCENTE', 'pais_nacionalidade'],
        'raca_cor': ['NM_RACA_COR', 'raca_cor'],
        'grau_titulacao': ['grau_academico', 'DS_GRAU_ACADEMICO_DISCENTE'],
        'data_matricula': ['DT_MATRICULA_DISCENTE', 'data_matricula'],
        'situacao_titulacao': ['situacao_discente', 'DS_SITUACAO_DISCENTE'],
        'data_titulacao': ['DT_SITUACAO_DISCENTE', 'data_titulacao'],
        'faixa_etaria': ['DS_FAIXA_ETARIA', 'faixa_etaria'],
        'orientador_principal': ['NM_ORIENTADOR_PRINCIPAL', 'orientador_principal'],
        'titulo_trabalho_final': ['NM_TESE_DISSERTACAO', 'titulo_trabalho_final'],
        'meses_para_titulacao': ['meses_para_titulacao', 'QT_MES_TITULACAO'],
        'id_lattes': ['ID_LATTES', 'id_lattes'],
        'ano_base': ['AN_BASE', 'ano_base']
    }
    
    # Criar mapeamento real baseado nas colunas disponíveis
    column_mapping = {}
    for target_col, source_options in possible_columns.items():
        for source_col in source_options:
            if source_col in df.columns:
                column_mapping[source_col] = target_col
                break
    
    logger.info(f"🔄 Mapeamento de colunas identificado: {len(column_mapping)} colunas")
    
    # Aplicar mapeamento
    df_mapped = df.rename(columns=column_mapping)
    
    # Remover duplicatas por ID do discente (manter mais recente)
    logger.info(f"Removendo duplicatas de {len(df_mapped):,} registros...")
    
    # Identificar coluna de ID correta após mapeamento
    id_col_mapped = 'id_discente_original'
    if id_col_mapped not in df_mapped.columns:
        # Tentar usar a primeira coluna de ID disponível
        for col in df_mapped.columns:
            if 'id' in col.lower() and 'discente' in col.lower():
                id_col_mapped = col
                break
    
    df_dim = df_mapped.drop_duplicates(subset=[id_col_mapped], keep='last')
    logger.info(f"Após remoção de duplicatas: {len(df_dim):,} titulados únicos")
    
    # Criar surrogate key específica para titulados
    df_dim = df_dim.reset_index(drop=True)
    df_dim['titulado_sk'] = df_dim.index + 1
    
    # Tratamento de valores nulos específico para titulados
    logger.info("Tratando valores nulos...")
    
    fillna_map = {
        'nome_titulado': 'NÃO INFORMADO',
        'sexo': 'NÃO INFORMADO',
        'pais_nacionalidade': 'BRASIL',
        'raca_cor': 'NÃO DECLARADO',
        'grau_titulacao': 'NÃO INFORMADO',
        'situacao_titulacao': 'TITULADO',
        'faixa_etaria': 'NÃO INFORMADO',
        'orientador_principal': 'NÃO INFORMADO',
        'titulo_trabalho_final': 'NÃO INFORMADO'
    }
    
    for col, fill_value in fillna_map.items():
        if col in df_dim.columns:
            df_dim[col] = df_dim[col].fillna(fill_value)
    
    # Converter tipos de dados
    logger.info("Convertendo tipos de dados...")
    df_dim['meses_para_titulacao'] = pd.to_numeric(df_dim['meses_para_titulacao'], errors='coerce').fillna(0)
    df_dim['idade_ano_base'] = pd.to_numeric(df_dim['idade_ano_base'], errors='coerce').fillna(0)
    df_dim['ano_base'] = pd.to_numeric(df_dim['ano_base'], errors='coerce').fillna(2025)
    
    # Criar campos derivados específicos para titulados
    logger.info("Criando campos derivados...")
    
    # Calcular anos para titulação
    df_dim['anos_para_titulacao'] = (df_dim['meses_para_titulacao'] / 12).round(1)
    
    # Indicador de nível de titulação
    def nivel_titulacao(grau):
        if 'MESTRADO' in str(grau).upper():
            return 'MESTRE'
        elif 'DOUTORADO' in str(grau).upper():
            return 'DOUTOR'
        else:
            return 'OUTROS'
    
    df_dim['nivel_titulacao'] = df_dim['grau_titulacao'].apply(nivel_titulacao)
    
    # Adicionar registro SK=0 para unknown
    logger.info("Adicionando registro SK=0...")
    registro_sk0 = {
        'titulado_sk': 0,
        'id_discente_original': 'UNKNOWN_0',
        'id_pessoa': 'UNKNOWN_0',
        'nome_titulado': 'TITULADO DESCONHECIDO',
        'tipo_documento': 'DESCONHECIDO',
        'numero_documento': 'UNKNOWN_0',
        'sexo': 'X',
        'data_nascimento': None,
        'idade_ano_base': 0,
        'pais_nacionalidade': 'DESCONHECIDO',
        'raca_cor': 'DESCONHECIDO',
        'grau_titulacao': 'DESCONHECIDO',
        'data_matricula': None,
        'situacao_titulacao': 'DESCONHECIDO',
        'data_titulacao': None,
        'faixa_etaria': 'DESCONHECIDO',
        'orientador_principal': 'DESCONHECIDO',
        'titulo_trabalho_final': 'DESCONHECIDO',
        'meses_para_titulacao': 0,
        'id_lattes': 'UNKNOWN_0',
        'ano_base': 0,
        'anos_para_titulacao': 0.0,
        'nivel_titulacao': 'DESCONHECIDO'
    }
    
    df_sk0 = pd.DataFrame([registro_sk0])
    df_dim_final = pd.concat([df_sk0, df_dim], ignore_index=True)
    
    # Reordenar colunas finais
    colunas_ordenadas = [
        'titulado_sk',
        'id_discente_original', 
        'id_pessoa',
        'nome_titulado',
        'tipo_documento',
        'numero_documento',
        'sexo',
        'data_nascimento',
        'idade_ano_base',
        'pais_nacionalidade',
        'raca_cor',
        'grau_titulacao',
        'nivel_titulacao',
        'data_matricula',
        'situacao_titulacao',
        'data_titulacao',
        'faixa_etaria',
        'orientador_principal',
        'titulo_trabalho_final',
        'meses_para_titulacao',
        'anos_para_titulacao',
        'id_lattes',
        'ano_base'
    ]
    
    # Selecionar apenas colunas que existem
    colunas_existentes = [col for col in colunas_ordenadas if col in df_dim_final.columns]
    df_dim_final = df_dim_final[colunas_existentes]
    
    logger.info(f"✅ Dimensão de titulados criada: {len(df_dim_final):,} registros (incluindo SK=0)")
    
    return df_dim_final

def criar_tabela(db):
    """
    Cria a tabela dim_titulado no banco de dados.
    """
    logger = get_logger()
    logger.info("🗄️ Criando tabela dim_titulado...")
    
    # Primeiro remove se existir
    drop_sql = "DROP TABLE IF EXISTS dim_titulado"
    db.execute_sql(drop_sql)
    logger.info("🗑️ Tabela dim_titulado removida se existia")
    
    # Depois cria novamente
    create_sql = """
    CREATE TABLE IF NOT EXISTS dim_titulado (
        titulado_sk INTEGER PRIMARY KEY,
        id_discente_original VARCHAR(50) NOT NULL,
        id_pessoa VARCHAR(50),
        nome_titulado VARCHAR(255),
        tipo_documento VARCHAR(50),
        numero_documento VARCHAR(50),
        sexo VARCHAR(20),
        data_nascimento DATE,
        idade_ano_base INTEGER,
        pais_nacionalidade VARCHAR(100),
        raca_cor VARCHAR(50),
        grau_titulacao VARCHAR(100),
        nivel_titulacao VARCHAR(20),
        data_matricula DATE,
        situacao_titulacao VARCHAR(100),
        data_titulacao DATE,
        faixa_etaria VARCHAR(50),
        orientador_principal VARCHAR(255),
        titulo_trabalho_final TEXT,
        meses_para_titulacao INTEGER,
        anos_para_titulacao DECIMAL(4,1),
        id_lattes VARCHAR(50),
        ano_base INTEGER
    );
    """
    
    db.execute_sql(create_sql)
    logger.info("✅ Tabela dim_titulado criada com sucesso")
    
    # Adiciona comentários
    comment_sql = """
    COMMENT ON TABLE dim_titulado IS 'Dimensão de titulados - estudantes que concluíram seus cursos de pós-graduação';
    """
    
    db.execute_sql(comment_sql)
    logger.info("✅ Comentários adicionados à tabela")

def inserir_dados_titulado(df_dim_titulado, db):
    """
    Insere dados da dimensão titulado no banco usando estratégia de chunks otimizada.
    
    Args:
        df_dim_titulado: DataFrame com dados dos titulados
        db: DatabaseManager instance
    """
    logger = get_logger()
    
    try:
        logger.info(f"🔄 Iniciando inserção de {len(df_dim_titulado)} registros de titulados")
        
        # Configuração de chunks muito pequenos para evitar problemas de memória SQL
        chunk_size = 1000  # Chunks menores para evitar excesso de parâmetros SQL
        total_chunks = (len(df_dim_titulado) + chunk_size - 1) // chunk_size
        
        logger.info(f"� Dados serão inseridos em {total_chunks} chunks de {chunk_size} registros")
        
        for chunk_num in range(total_chunks):
            start_idx = chunk_num * chunk_size
            end_idx = min((chunk_num + 1) * chunk_size, len(df_dim_titulado))
            
            chunk_df = df_dim_titulado.iloc[start_idx:end_idx].copy()
            
            logger.info(f"� Inserindo chunk {chunk_num + 1}/{total_chunks} - Registros {start_idx} a {end_idx-1} ({len(chunk_df)} registros)")
            
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
        count_query = "SELECT COUNT(*) as total FROM dim_titulado WHERE sk > 0"
        resultado_count = db.execute_query(count_query)
        total_inserido = resultado_count.iloc[0]['total'] if not resultado_count.empty else 0
        
        logger.info(f"✅ Inserção concluída! Total de registros inseridos: {total_inserido}")
        logger.info(f"📊 Esperado: {len(df_dim_titulado)}, Inserido: {total_inserido}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao inserir dados: {str(e)}")
        raise


def inserir_chunk_direto(chunk_df, db):
    """
    Insere um chunk usando método direto to_sql.
    """
    logger = get_logger()
    
    try:
        # Usar to_sql diretamente que é mais eficiente
        chunk_df.to_sql(
            name='dim_titulado',
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
    Função principal para criação da dimensão de titulados.
    """
    logger.info("🎓 INICIANDO CRIAÇÃO DA DIM_TITULADO")
    logger.info("=" * 50)
    
    try:
        # 1. Conectar ao banco
        logger.info("1️⃣ Conectando ao banco de dados...")
        db = get_db_manager()
        
        # 2. Carregar dados
        logger.info("2️⃣ Carregando dados do MinIO...")
        df_bruto = carregar_dados_discentes_titulados()
        
        # 3. Filtrar titulados
        logger.info("3️⃣ Filtrando discentes titulados...")
        df_titulados = filtrar_titulados(df_bruto)
        
        # 4. Transformar dados
        logger.info("4️⃣ Transformando dados...")
        df_dim_final = transformar_dados_titulado(df_titulados)
        
        # 5. Criar tabela
        logger.info("5️⃣ Criando tabela no banco...")
        criar_tabela(db)
        
        # 6. Inserir dados
        logger.info("6️⃣ Inserindo dados...")
        inserir_dados_titulado(df_dim_final, db)
        
        logger.info("🎉 DIM_TITULADO CRIADA COM SUCESSO!")
        
    except Exception as e:
        logger.error(f"💥 Erro no processo: {e}")
        raise

if __name__ == "__main__":
    main()