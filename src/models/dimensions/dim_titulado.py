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


def carregar_dados_discentes_titulados(db):
    """
    Carrega dados de discentes da tabela dim_discente no PostgreSQL.
    
    Args:
        db: DatabaseManager instance
        
    Returns:
        DataFrame com todos os discentes da dim_discente
    """
    logger.info("🎓 Carregando dados de discentes da tabela dim_discente...")
    
    try:
        # Carregar dados da dim_discente (excluindo SK=0)
        query = """
        SELECT 
            discente_sk,
            id_discente,
            id_pessoa,
            nome_discente,
            tipo_documento,
            numero_documento,
            sexo,
            data_nascimento,
            idade_ano_base,
            pais_nacionalidade,
            tipo_nacionalidade,
            raca_cor,
            necessidade_especial,
            status_ingressante,
            grau_academico,
            data_matricula,
            situacao_discente,
            data_situacao,
            faixa_etaria,
            orientador_principal,
            titulo_tese_dissertacao,
            meses_para_titulacao,
            id_lattes,
            ano_base
        FROM dim_discente
        WHERE discente_sk > 0
        """
        
        df = db.execute_query(query)
        logger.info(f"✅ Dados carregados da dim_discente: {len(df):,} registros")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Falha ao carregar dados da dim_discente: {e}")
        raise

def filtrar_titulados(df):
    """
    Filtra apenas os discentes que são titulados (concluíram mestrado/doutorado).
    Utiliza os nomes de colunas da dim_discente.
    
    Args:
        df: DataFrame com dados da dim_discente
        
    Returns:
        DataFrame filtrado com apenas titulados
    """
    logger.info("🔍 Aplicando filtros para identificar titulados...")
    logger.info(f"Total de registros antes dos filtros: {len(df):,}")
    
    # FILTRO 1: Graus acadêmicos relevantes (usar nome da coluna da dim_discente)
    graus_validos = ['MESTRADO', 'DOUTORADO', 'MESTRADO PROFISSIONAL', 'DOUTORADO PROFISSIONAL']
    
    # Converter para string e uppercase para comparação
    df['grau_academico_upper'] = df['grau_academico'].astype(str).str.upper()
    mask_grau = df['grau_academico_upper'].isin(graus_validos)
    
    df_grau = df[mask_grau].copy()
    logger.info(f"Após filtro de grau acadêmico: {len(df_grau):,} registros")
    
    # FILTRO 2: Situação indica titulação
    criterios_titulacao = [
        "TITULADO",
        "EGRESSO", 
        "FORMADO",
        "CONCLUÍDO",
        "DEFENDEU"
    ]
    
    # Converter situacao_discente para string antes de aplicar str.contains
    df_grau['situacao_str'] = df_grau['situacao_discente'].astype(str).str.upper()
    mask_situacao = df_grau['situacao_str'].str.contains(
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
    
    # Remover colunas auxiliares
    df_titulados = df_titulados.drop(columns=['grau_academico_upper', 'situacao_str'], errors='ignore')
    
    logger.info(f"🎯 Titulados identificados: {len(df_titulados):,} registros")
    if len(df_grau) > 0:
        logger.info(f"📈 Taxa de titulação: {(len(df_titulados)/len(df_grau))*100:.1f}%")
    
    return df_titulados

def transformar_dados_titulado(df):
    """
    Transforma os dados de discentes titulados para a dimensão final.
    Usa as colunas já padronizadas da dim_discente.
    
    Args:
        df: DataFrame filtrado com titulados da dim_discente
        
    Returns:
        DataFrame com estrutura da dim_titulado
    """
    logger.info("🔄 Transformando dados para dimensão de titulados...")
    logger.info(f"Total de registros a transformar: {len(df):,}")
    
    # Mapeamento direto das colunas da dim_discente para dim_titulado
    df_dim = df.rename(columns={
        'id_discente': 'id_discente_original',
        'nome_discente': 'nome_titulado',
        'grau_academico': 'grau_titulacao',
        'situacao_discente': 'situacao_titulacao',
        'data_situacao': 'data_titulacao',
        'titulo_tese_dissertacao': 'titulo_trabalho_final'
    }).copy()
    
    # Remover duplicatas por ID do discente (manter mais recente por ano_base)
    logger.info(f"Removendo duplicatas de {len(df_dim):,} registros...")
    df_dim = df_dim.sort_values('ano_base', ascending=False)
    df_dim = df_dim.drop_duplicates(subset=['id_discente_original'], keep='first')
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
    df_dim['meses_para_titulacao'] = pd.to_numeric(df_dim['meses_para_titulacao'], errors='coerce').fillna(0).astype(int)
    df_dim['idade_ano_base'] = pd.to_numeric(df_dim['idade_ano_base'], errors='coerce').fillna(0).astype(int)
    df_dim['ano_base'] = pd.to_numeric(df_dim['ano_base'], errors='coerce').fillna(2025).astype(int)
    
    # Criar campos derivados específicos para titulados
    logger.info("Criando campos derivados...")
    
    # Calcular anos para titulação
    df_dim['anos_para_titulacao'] = (df_dim['meses_para_titulacao'] / 12).round(1)
    
    # Indicador de nível de titulação
    def nivel_titulacao(grau):
        grau_upper = str(grau).upper()
        if 'MESTRADO' in grau_upper:
            return 'MESTRE'
        elif 'DOUTORADO' in grau_upper:
            return 'DOUTOR'
        else:
            return 'OUTROS'
    
    df_dim['nivel_titulacao'] = df_dim['grau_titulacao'].apply(nivel_titulacao)
    
    # Truncar campos VARCHAR para respeitar limites da tabela
    logger.info("Truncando campos VARCHAR...")
    varchar_limits = {
        'id_discente_original': 50,
        'id_pessoa': 50,
        'nome_titulado': 255,
        'tipo_documento': 50,
        'numero_documento': 50,
        'sexo': 20,
        'pais_nacionalidade': 100,
        'raca_cor': 50,
        'grau_titulacao': 100,
        'nivel_titulacao': 20,
        'situacao_titulacao': 100,
        'faixa_etaria': 50,
        'orientador_principal': 255,
        'id_lattes': 50
    }
    
    for col, max_len in varchar_limits.items():
        if col in df_dim.columns:
            df_dim[col] = df_dim[col].astype(str).str[:max_len]
            # Substituir 'nan' string por None
            df_dim[col] = df_dim[col].replace('nan', None)
    
    # Adicionar registro SK=0 para unknown
    logger.info("Adicionando registro SK=0...")
    registro_sk0 = pd.DataFrame([{
        'titulado_sk': 0,
        'id_discente_original': 'UNKNOWN_0',
        'id_pessoa': 'UNKNOWN_0',
        'nome_titulado': 'TITULADO DESCONHECIDO',
        'tipo_documento': 'DESCONHECIDO',
        'numero_documento': 'UNKNOWN_0',
        'sexo': 'NÃO INFORMADO',
        'data_nascimento': pd.NaT,
        'idade_ano_base': 0,
        'pais_nacionalidade': 'DESCONHECIDO',
        'raca_cor': 'DESCONHECIDO',
        'grau_titulacao': 'DESCONHECIDO',
        'data_matricula': pd.NaT,
        'situacao_titulacao': 'DESCONHECIDO',
        'data_titulacao': pd.NaT,
        'faixa_etaria': 'DESCONHECIDO',
        'orientador_principal': 'DESCONHECIDO',
        'titulo_trabalho_final': 'DESCONHECIDO',
        'meses_para_titulacao': 0,
        'id_lattes': 'UNKNOWN_0',
        'ano_base': 0,
        'anos_para_titulacao': 0.0,
        'nivel_titulacao': 'DESCONHECIDO'
    }])
    
    df_dim_final = pd.concat([registro_sk0, df_dim], ignore_index=True)
    
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
        logger.info(f"� Iniciando inserção de {len(df_dim_titulado):,} registros de titulados...")
        
        # Configuração de chunks otimizados para evitar overflow de parâmetros SQL
        chunk_size = 500  # Tamanho reduzido para evitar overflow (PostgreSQL limite: 32.767 parâmetros)
        total_chunks = (len(df_dim_titulado) + chunk_size - 1) // chunk_size
        
        logger.info(f"📦 Dados serão inseridos em {total_chunks} chunks de {chunk_size} registros")
        
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
        count_query = "SELECT COUNT(*) as total FROM dim_titulado WHERE titulado_sk > 0"
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
        logger.info("2️⃣ Carregando dados da dim_discente...")
        df_bruto = carregar_dados_discentes_titulados(db)
        
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