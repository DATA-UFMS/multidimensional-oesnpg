#!/usr/bin/env python3
"""
dim_discente.py

Módulo para criação e gerenciamento da dimensão de discentes no Data Warehouse.

Descrição:
    Este módulo implementa o processo de ETL (Extract, Transform, Load) para a dimensão
    de discentes da pós-graduação brasileira, consolidando dados de múltiplas fontes e
    enriquecendo com informações sobre perfil acadêmico, situação no programa e dados
    demográficos.
    
    A dimensão contém:
    - Dados básicos de identificação (id_pessoa, id_discente, nome, documentos)
    - Informações demográficas (sexo, raça/cor, necessidade especial, nacionalidade)
    - Dados acadêmicos (grau acadêmico, situação, orientador, título da tese/dissertação)
    - Informações temporais (data de nascimento, matrícula, situação, idade)
    - Status no programa (ingressante, situação, tempo para titulação)
    - Identificadores únicos (id_lattes, id_pessoa, id_discente)

Fontes de Dados:
    - Base Principal: add_discentes.parquet (MinIO) - Dados consolidados dos discentes
    
Estrutura da Dimensão:
    - discente_sk: Surrogate key (chave substituta sequencial, inicia em 0)
    - id_discente: Identificador único do discente no sistema CAPES
    - id_pessoa: Identificador de pessoa no sistema CAPES
    - nome_discente: Nome completo do discente
    - tipo_documento: Tipo do documento (RG, CPF, Passaporte, etc.)
    - numero_documento: Número do documento
    - sexo: Sexo do discente
    - data_nascimento: Data de nascimento
    - idade_ano_base: Idade do discente no ano base
    - pais_nacionalidade: País de nacionalidade
    - tipo_nacionalidade: Tipo de nacionalidade (Brasileira, Estrangeira)
    - raca_cor: Raça/cor declarada
    - necessidade_especial: Indicador de necessidade especial (S/N)
    - status_ingressante: Status de ingressante (Sim/Não)
    - grau_academico: Grau acadêmico do programa (Mestrado, Doutorado)
    - data_matricula: Data de matrícula no programa
    - situacao_discente: Situação atual no programa (Matriculado, Titulado, Desligado, etc.)
    - data_situacao: Data da última atualização de situação
    - faixa_etaria: Faixa etária do discente
    - orientador_principal: Nome do orientador principal
    - titulo_tese_dissertacao: Título da tese ou dissertação
    - meses_para_titulacao: Quantidade de meses até a titulação
    - id_lattes: Identificador do currículo Lattes
    - ano_base: Ano base de referência dos dados

Registro SK=0:
    - Registro especial com discente_sk=0 representa valores desconhecidos
    - Usado para integridade referencial em casos de dados ausentes nas tabelas fato

Autor: Sistema DW OESNPG
Data: Outubro/2025
Versão: 2.0
"""

import pandas as pd
import numpy as np
import os
import sys
from dotenv import load_dotenv
import logging
from pathlib import Path
# Adicionar o diretório raiz ao path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from src.utils.naming_conventions import NamingConventions
from src.validation.data_validator import validate_dimension_data, get_validation_summary
from src.core.exceptions import DimensionCreationError, DataValidationError


# Adicionar path para imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from src.core.core import get_db_manager

def get_project_root() -> Path:
    """Encontra o diretório raiz do projeto de forma robusta."""
    current_path = Path(__file__).resolve()
    while not (current_path / '.env').exists() and not (current_path / '.git').exists() and current_path.parent != current_path:
        current_path = current_path.parent
    return current_path

# Carregar variáveis de ambiente
project_root_path = get_project_root()
load_dotenv(dotenv_path=project_root_path / '.env')

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def carregar_dados_add_discentes():
    """
    Carrega os dados do arquivo add_discentes.parquet do MinIO com chunks.
    """
    logger.info("📥 Carregando dados do add_discentes.parquet...")
    
    # Configurações do MinIO a partir do .env
    endpoint = os.getenv("MINIO_ENDPOINT")
    bucket = os.getenv("MINIO_BUCKET")
    parquet_path = os.getenv("MINIO_PARQUET_PATH")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not all([endpoint, bucket, parquet_path, access_key, secret_key]):
        raise ValueError("As variáveis de ambiente do MinIO não estão configuradas.")

    storage_options = {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {"endpoint_url": endpoint},
    }

    # Caminho completo para o arquivo no MinIO
    path = f"s3://{bucket}/{parquet_path}/add_discentes.parquet"
    
    try:
        # Carregar apenas as colunas necessárias para otimizar memória
        colunas_necessarias = [
            'ID_DISCENTE',
            'ID_PESSOA', 
            'NM_DISCENTE',
            'TP_DOCUMENTO_DISCENTE',
            'NR_DOCUMENTO_DISCENTE',
            'TP_SEXO_DISCENTE',
            'DT_NASCIMENTO_DISCENTE',
            'DS_IDADE_ANOBASE',
            'NM_PAIS_NACIONALIDADE_DISCENTE',
            'DS_TIPO_NACIONALIDADE_DISCENTE',
            'NM_RACA_COR',
            'IN_NECESSIDADE_PESSOAL',
            'ST_INGRESSANTE',
            'DS_GRAU_ACADEMICO_DISCENTE',
            'DT_MATRICULA_DISCENTE',
            'NM_SITUACAO_DISCENTE',
            'DT_SITUACAO_DISCENTE',
            'DS_FAIXA_ETARIA',
            'NM_ORIENTADOR_PRINCIPAL',
            'NM_TESE_DISSERTACAO',
            'QT_MES_TITULACAO',
            'ID_LATTES',
            'AN_BASE'
        ]
        
        df = pd.read_parquet(path, storage_options=storage_options, columns=colunas_necessarias)
        logger.info(f"✅ Dados carregados com sucesso: {len(df):,} registros")
        return df
    except Exception as e:
        logger.error(f"❌ Falha ao carregar dados do MinIO: {e}")
        raise

def transformar_dados_discente(df):
    """
    Transforma os dados brutos em dimensão de discente.
    """
    logger.info("🔄 Transformando dados para dimensão de discente...")
    
    # Renomear colunas primeiro para liberar memória
    df = df.rename(columns={
        'ID_DISCENTE': 'id_discente',
        'ID_PESSOA': 'id_pessoa',
        'NM_DISCENTE': 'nome_discente',
        'TP_DOCUMENTO_DISCENTE': 'tipo_documento',
        'NR_DOCUMENTO_DISCENTE': 'numero_documento',
        'TP_SEXO_DISCENTE': 'sexo',
        'DT_NASCIMENTO_DISCENTE': 'data_nascimento',
        'DS_IDADE_ANOBASE': 'idade_ano_base',
        'NM_PAIS_NACIONALIDADE_DISCENTE': 'pais_nacionalidade',
        'DS_TIPO_NACIONALIDADE_DISCENTE': 'tipo_nacionalidade',
        'NM_RACA_COR': 'raca_cor',
        'IN_NECESSIDADE_PESSOAL': 'necessidade_especial',
        'ST_INGRESSANTE': 'status_ingressante',
        'DS_GRAU_ACADEMICO_DISCENTE': 'grau_academico',
        'DT_MATRICULA_DISCENTE': 'data_matricula',
        'NM_SITUACAO_DISCENTE': 'situacao_discente',
        'DT_SITUACAO_DISCENTE': 'data_situacao',
        'DS_FAIXA_ETARIA': 'faixa_etaria',
        'NM_ORIENTADOR_PRINCIPAL': 'orientador_principal',
        'NM_TESE_DISSERTACAO': 'titulo_tese_dissertacao',
        'QT_MES_TITULACAO': 'meses_para_titulacao',
        'ID_LATTES': 'id_lattes',
        'AN_BASE': 'ano_base'
    })
    
    logger.info(f"Removendo duplicatas de {len(df):,} registros...")
    
    # Remover duplicatas baseadas no ID do discente (usar last para pegar dados mais recentes)
    df_dim = df.drop_duplicates(subset=['id_discente'], keep='last')
    
    logger.info(f"Após remoção de duplicatas: {len(df_dim):,} registros únicos")
    
    # Liberar memória do DataFrame original
    del df
    
    # Criar surrogate key
    df_dim = df_dim.reset_index(drop=True)
    df_dim['discente_sk'] = df_dim.index + 1
    
    # Tratar valores nulos e padronizar dados de forma otimizada
    logger.info("Tratando valores nulos...")
    
    fillna_map = {
        'nome_discente': 'NÃO INFORMADO',
        'sexo': 'NÃO INFORMADO',
        'pais_nacionalidade': 'BRASIL',
        'tipo_nacionalidade': 'BRASILEIRA',
        'raca_cor': 'NÃO DECLARADO',
        'necessidade_especial': 'N',
        'status_ingressante': 'NÃO INFORMADO',
        'grau_academico': 'NÃO INFORMADO',
        'situacao_discente': 'NÃO INFORMADO',
        'faixa_etaria': 'NÃO INFORMADO',
        'orientador_principal': 'NÃO INFORMADO',
        'titulo_tese_dissertacao': 'NÃO INFORMADO'
    }
    
    for col, fill_value in fillna_map.items():
        if col in df_dim.columns:
            df_dim[col] = df_dim[col].fillna(fill_value)
    
    # Converter tipos de dados numéricos
    logger.info("Convertendo tipos de dados...")
    df_dim['meses_para_titulacao'] = pd.to_numeric(df_dim['meses_para_titulacao'], errors='coerce').fillna(0)
    df_dim['idade_ano_base'] = pd.to_numeric(df_dim['idade_ano_base'], errors='coerce').fillna(0)
    df_dim['ano_base'] = pd.to_numeric(df_dim['ano_base'], errors='coerce').fillna(2025)
    
    # Reordenar colunas
    colunas_ordenadas = [
        'discente_sk',
        'id_discente',
        'id_pessoa',
        'nome_discente',
        'tipo_documento',
        'numero_documento',
        'sexo',
        'data_nascimento',
        'idade_ano_base',
        'pais_nacionalidade',
        'tipo_nacionalidade',
        'raca_cor',
        'necessidade_especial',
        'status_ingressante',
        'grau_academico',
        'data_matricula',
        'situacao_discente',
        'data_situacao',
        'faixa_etaria',
        'orientador_principal',
        'titulo_tese_dissertacao',
        'meses_para_titulacao',
        'id_lattes',
        'ano_base'
    ]
    
    df_dim = df_dim[colunas_ordenadas]
    
    logger.info(f"✅ Transformação concluída: {len(df_dim):,} discentes únicos")
    
    return df_dim

def criar_dim_discente():
    """
    Executa o processo completo de criação da dimensão de discente.
    """
    logger.info("🚀 Iniciando criação da dimensão dim_discente...")
    
    try:
        # 1. Carregar dados do parquet
        df_raw = carregar_dados_add_discentes()
        
        # 2. Transformar dados
        df_dim = transformar_dados_discente(df_raw)
        
        # 2.1. Adicionar registro SK=0 para valores não informados
        logger.info("🔧 Adicionando registro SK=0 para valores não informados...")
        
        sk0_record = pd.DataFrame([{
            'discente_sk': 0,
            'id_discente': '0',
            'id_pessoa': '0',
            'nome_discente': 'Não informado',
            'tipo_documento': 'Não informado',
            'numero_documento': '0',
            'sexo': 'Não informado',
            'data_nascimento': pd.NaT,
            'idade_ano_base': 0,
            'pais_nacionalidade': 'Não informado',
            'tipo_nacionalidade': 'Não informado',
            'raca_cor': 'Não informado',
            'necessidade_especial': 'N',
            'status_ingressante': 'Não informado',
            'grau_academico': 'Não informado',
            'data_matricula': pd.NaT,
            'situacao_discente': 'Não informado',
            'data_situacao': pd.NaT,
            'faixa_etaria': 'Não informado',
            'orientador_principal': 'Não informado',
            'titulo_tese_dissertacao': 'Não informado',
            'meses_para_titulacao': 0,
            'id_lattes': '0',
            'ano_base': 0
        }])
        
        # Combinar SK=0 com dados reais
        df_dim_final = pd.concat([sk0_record, df_dim], ignore_index=True)
        
        logger.info(f"✅ Dimensão final criada com {len(df_dim_final):,} registros (incluindo SK=0)")
        
        # 3. Conectar ao banco
        db = get_db_manager()
        
        # 4. Criar tabela
        logger.info("📊 Criando tabela dim_discente no banco...")
        
        create_table_sql = """
        DROP TABLE IF EXISTS dim_discente CASCADE;
        
        CREATE TABLE dim_discente (
            discente_sk INTEGER PRIMARY KEY,
            id_discente VARCHAR(50) UNIQUE NOT NULL,
            id_pessoa VARCHAR(50),
            nome_discente VARCHAR(255),
            tipo_documento VARCHAR(20),
            numero_documento VARCHAR(50),
            sexo VARCHAR(20),
            data_nascimento DATE,
            idade_ano_base INTEGER,
            pais_nacionalidade VARCHAR(100),
            tipo_nacionalidade VARCHAR(50),
            raca_cor VARCHAR(50),
            necessidade_especial VARCHAR(5),
            status_ingressante VARCHAR(50),
            grau_academico VARCHAR(50),
            data_matricula DATE,
            situacao_discente VARCHAR(100),
            data_situacao DATE,
            faixa_etaria VARCHAR(50),
            orientador_principal VARCHAR(255),
            titulo_tese_dissertacao TEXT,
            meses_para_titulacao INTEGER,
            id_lattes VARCHAR(50),
            ano_base INTEGER,
            
            -- Metadados
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Índices
        CREATE INDEX idx_dim_discente_id ON dim_discente(id_discente);
        CREATE INDEX idx_dim_discente_pessoa ON dim_discente(id_pessoa);
        CREATE INDEX idx_dim_discente_situacao ON dim_discente(situacao_discente);
        CREATE INDEX idx_dim_discente_grau ON dim_discente(grau_academico);
        CREATE INDEX idx_dim_discente_ano ON dim_discente(ano_base);
        
        -- Comentários
        COMMENT ON TABLE dim_discente IS 'Dimensão de Discentes do Data Warehouse';
        COMMENT ON COLUMN dim_discente.discente_sk IS 'Chave surrogate da dimensão discente (0=desconhecido)';
        COMMENT ON COLUMN dim_discente.id_discente IS 'ID natural do discente (CAPES)';
        COMMENT ON COLUMN dim_discente.nome_discente IS 'Nome completo do discente';
        COMMENT ON COLUMN dim_discente.situacao_discente IS 'Situação atual do discente no programa';
        """
        
        db.execute_sql(create_table_sql)
        
        # 5. Inserir dados em lotes
        logger.info("💾 Inserindo dados na dim_discente...")
        
        # Remover a coluna sk para permitir inserção manual
        df_insert = df_dim_final.copy()
        
        # Truncar campos VARCHAR para respeitar limites da tabela
        varchar_limits = {
            'id_discente': 50,
            'id_pessoa': 50,
            'nome_discente': 255,
            'tipo_documento': 20,
            'numero_documento': 50,
            'sexo': 20,
            'pais_nacionalidade': 100,
            'tipo_nacionalidade': 50,
            'raca_cor': 50,
            'necessidade_especial': 5,
            'status_ingressante': 50,
            'grau_academico': 50,
            'situacao_discente': 100,
            'faixa_etaria': 50,
            'orientador_principal': 255,
            'id_lattes': 50
        }
        
        for col, max_len in varchar_limits.items():
            if col in df_insert.columns:
                df_insert[col] = df_insert[col].astype(str).str[:max_len]
                # Substituir 'nan' string por None
                df_insert[col] = df_insert[col].replace('nan', None)
        
        # Processar em lotes para evitar sobrecarga de memória e overflow de parâmetros SQL
        CHUNK_SIZE = 500  # Tamanho reduzido para evitar overflow de parâmetros SQL (32.767 limite PostgreSQL)
        total_chunks = (len(df_insert) + CHUNK_SIZE - 1) // CHUNK_SIZE
        
        logger.info(f"Inserindo {len(df_insert):,} registros em {total_chunks} chunks de {CHUNK_SIZE}...")
        
        for i in range(0, len(df_insert), CHUNK_SIZE):
            chunk = df_insert.iloc[i:i+CHUNK_SIZE]
            chunk_num = (i // CHUNK_SIZE) + 1
            
            logger.info(f"Chunk {chunk_num}/{total_chunks} inserido ({len(chunk)} registros)")
            
            success = db.save_dataframe(
                df=chunk,
                table_name='dim_discente',
                if_exists='append'
            )
            
            if not success:
                logger.error(f"❌ Falha ao inserir chunk {chunk_num}")
                raise Exception(f"Erro na inserção do chunk {chunk_num}")
            
            # Liberar memória
            del chunk
        
        # 6. Validar dados inseridos
        count_query = "SELECT COUNT(*) as total FROM dim_discente"
        result = db.execute_query(count_query)
        total_inserido = result.iloc[0]['total']
        
        logger.info(f"✅ Dimensão dim_discente criada com sucesso!")
        logger.info(f"📊 Total de registros inseridos: {total_inserido:,}")
        
        # 7. Exibir estatísticas
        stats_query = """
        SELECT 
            COUNT(*) as total_discentes,
            COUNT(DISTINCT sexo) as sexos_diferentes,
            COUNT(DISTINCT situacao_discente) as situacoes_diferentes,
            COUNT(DISTINCT grau_academico) as graus_diferentes,
            MIN(ano_base) as ano_min,
            MAX(ano_base) as ano_max
        FROM dim_discente
        """
        
        stats = db.execute_query(stats_query)
        logger.info("📈 Estatísticas da dimensão:")
        for col, val in stats.iloc[0].items():
            logger.info(f"   {col}: {val}")
        
        # Estatísticas excluindo SK=0
        stats_sem_sk0_query = """
        SELECT 
            COUNT(*) as discentes_reais,
            COUNT(DISTINCT sexo) as sexos_reais,
            COUNT(DISTINCT situacao_discente) as situacoes_reais,
            COUNT(DISTINCT grau_academico) as graus_reais
        FROM dim_discente
        WHERE discente_sk != 0
        """
        
        stats_reais = db.execute_query(stats_sem_sk0_query)
        logger.info("📈 Estatísticas dos dados reais (excluindo SK=0):")
        for col, val in stats_reais.iloc[0].items():
            logger.info(f"   {col}: {val}")
            
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro na criação da dim_discente: {e}")
        raise

def main():
    """Função principal"""
    print("=" * 60)
    print("🎓 CRIAÇÃO DA DIMENSÃO DISCENTE")
    print("=" * 60)
    print("⚠️  ATENÇÃO: Este processo consome muita memória (~5M registros)")
    print("   Se ocorrer erro 137 (SIGKILL), execute em máquina com mais RAM")
    print("=" * 60)
    
    try:
        criar_dim_discente()
        print("\n✅ Processo concluído com sucesso!")
        
    except Exception as e:
        print(f"\n❌ Erro no processo: {e}")
        print("\n💡 Dicas para resolver problemas de memória:")
        print("   - Execute em máquina com mais RAM (recomendado: 16GB+)")
        print("   - Feche outros aplicativos que consomem memória")
        print("   - Use a versão otimizada para chunks menores")
        raise

if __name__ == "__main__":
    main()
