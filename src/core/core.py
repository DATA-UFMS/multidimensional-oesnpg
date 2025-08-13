"""
Core Utils - Funcionalidades essenciais consolidadas para o Data Warehouse
Observatório CAPES - Pós-graduação brasileira
Versão - 30/07/2025
"""

import os
import pandas as pd
import logging
import time
import requests
from datetime import datetime
from typing import Dict, List, Optional, Union, Any
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# =================================================================
# CONFIGURAÇÕES CENTRALIZADAS
# =================================================================

class Config:
    """Configurações centralizadas do Data Warehouse"""
    
    # Banco de dados
    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    DB_PORT = os.getenv("DB_PORT")
    
    @property
    def DATABASE_URL(self):
        return f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    # API CAPES
    CAPES_API_URL = 'https://dadosabertos.capes.gov.br/api/3/action/datastore_search'
    
    # Resource IDs
    RESOURCE_IDS = {
        'discentes': 'fda8010e-d5db-4f00-a159-7940b3197d50',
        'docentes': 'cb95cea4-e4a8-4249-a58c-bc14d57f9889',
        'ies': '62f82787-3f45-4b9e-8457-3366f60c264b',
        'ppg': '21be9dd6-d4fa-470e-a5b9-b59c20879f10',
        'producoes': '7cd574be-7a3d-4750-a246-2ed0a7573073',
    }
    
    # Processamento
    BATCH_SIZE = int(os.getenv("BATCH_SIZE"))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES"))
    USE_CSV = os.getenv("USE_CSV").lower() == "true"

# =================================================================
# SCHEMA DO DATA WAREHOUSE
# =================================================================

class Schema:
    """Definições do schema do Data Warehouse"""
    
    # Nomes das tabelas
    TABLES = {
        'dim_tempo': 'dim_tempo',
        'dim_ies': 'dim_ies',
        'dim_ppg': 'dim_ppg',
        'dim_localidade': 'dim_localidade',
        'dim_tema': 'dim_tema',
        'dim_producao': 'dim_producao',
        'dim_ods': 'dim_ods',
        'dim_docente': 'dim_docente',
        'fact_pos_graduacao': 'fato_pos_graduacao'
    }
    
    # Chaves primárias
    PRIMARY_KEYS = {
        'dim_tempo': 'tempo_sk',
        'dim_ies': 'ies_sk',
        'dim_ppg': 'ppg_sk',
        'dim_localidade': 'localidade_sk',
        'dim_tema': 'tema_sk',
        'dim_producao': 'producao_sk',
        'dim_ods': 'ods_sk',
        'dim_docente': 'docente_sk',
        'fato_pos_graduacao': 'fato_id'
    }

# =================================================================
# LOGGING E DECORATORS
# =================================================================

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('dw_etl.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

def log_execution(func):
    """Decorator para logging de execução"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"🚀 Iniciando: {func.__name__}")
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            logger.info(f"✅ {func.__name__} concluída em {elapsed:.2f}s")
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"❌ {func.__name__} falhou após {elapsed:.2f}s: {e}")
            raise
    return wrapper

# =================================================================
# CONEXÃO COM BANCO DE DADOS
# =================================================================

class DatabaseManager:
    """Gerenciador de conexão com banco de dados"""
    
    def __init__(self):
        self.config = Config()
        self._engine = None
    
    @property
    def engine(self):
        """Lazy loading da engine"""
        if self._engine is None:
            self._engine = create_engine(
                self.config.DATABASE_URL,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600
            )
        return self._engine
    
    @log_execution
    def test_connection(self) -> bool:
        """Testa conexão com banco"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Erro na conexão: {e}")
            return False
    
    @log_execution
    def execute_query(self, query: str, params: Dict = None) -> pd.DataFrame:
        """Executa query e retorna DataFrame"""
        try:
            with self.engine.connect() as conn:
                return pd.read_sql(query, conn, params=params)
        except Exception as e:
            logger.error(f"Erro ao executar query: {e}")
            raise
    
    @log_execution
    def execute_sql(self, sql: str, params: Dict = None) -> bool:
        """Executa comando SQL"""
        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql), params or {})
            return True
        except Exception as e:
            logger.error(f"Erro ao executar SQL: {e}")
            return False
    
    @log_execution
    def save_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace') -> bool:
        """Salva DataFrame no banco"""
        if df.empty:
            logger.warning(f"DataFrame vazio para tabela {table_name}")
            return False
        
        try:
            with self.engine.begin() as conn:
                df.to_sql(table_name, conn, if_exists=if_exists, index=False, method='multi')
            
            logger.info(f"✅ {len(df)} registros salvos em {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao salvar em {table_name}: {e}")
            return False
    
    def table_exists(self, table_name: str) -> bool:
        """Verifica se tabela existe"""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()
    
    def get_table_count(self, table_name: str) -> int:
        """Retorna número de registros na tabela"""
        try:
            result = self.execute_query(f"SELECT COUNT(*) as count FROM {table_name}")
            return result.iloc[0]['count']
        except:
            return 0

# =================================================================
# API CAPES
# =================================================================

class CapesAPI:
    """Cliente para API da CAPES"""
    
    def __init__(self):
        self.config = Config()
        self.base_url = self.config.CAPES_API_URL
    
    @log_execution
    def fetch_data(self, resource_id: str, limit: int = 1000, offset: int = 0) -> Dict:
        """Busca dados da API CAPES"""
        params = {
            'resource_id': resource_id,
            'limit': limit,
            'offset': offset
        }
        
        for attempt in range(self.config.MAX_RETRIES):
            try:
                response = requests.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Tentativa {attempt + 1} falhou: {e}")
                if attempt == self.config.MAX_RETRIES - 1:
                    raise
                time.sleep(2 ** attempt)  # Backoff exponencial
    
    @log_execution
    def fetch_all_data(self, resource_id: str) -> pd.DataFrame:
        """Busca todos os dados de um resource"""
        all_records = []
        offset = 0
        batch_size = self.config.BATCH_SIZE
        
        logger.info(f"Iniciando busca completa para resource: {resource_id}")
        
        while True:
            data = self.fetch_data(resource_id, limit=batch_size, offset=offset)
            records = data.get('result', {}).get('records', [])
            
            if not records:
                break
            
            all_records.extend(records)
            offset += batch_size
            
            logger.info(f"Coletados {len(all_records)} registros...")
            
            # Limite de segurança
            if len(all_records) > 500000:
                logger.warning("Limite de 500k registros atingido")
                break
        
        logger.info(f"✅ Total coletado: {len(all_records)} registros")
        return pd.DataFrame(all_records)

# =================================================================
# UTILIDADES GERAIS
# =================================================================

@log_execution
def clean_text(text: Union[str, None]) -> str:
    """Limpa e padroniza texto"""
    if pd.isna(text) or text is None:
        return ""
    
    text = str(text).strip()
    text = text.replace('\n', ' ').replace('\r', ' ')
    text = ' '.join(text.split())  # Remove espaços múltiplos
    return text

@log_execution
def normalize_cpf(cpf: Union[str, None]) -> str:
    """Normaliza CPF removendo caracteres especiais"""
    if pd.isna(cpf) or cpf is None:
        return ""
    
    cpf = str(cpf).strip()
    cpf = ''.join(filter(str.isdigit, cpf))
    
    if len(cpf) == 11:
        return cpf
    return ""

@log_execution
def safe_int(value: Any, default: int = 0) -> int:
    """Conversão segura para inteiro"""
    try:
        if pd.isna(value):
            return default
        return int(float(str(value)))
    except:
        return default

@log_execution
def safe_float(value: Any, default: float = 0.0) -> float:
    """Conversão segura para float"""
    try:
        if pd.isna(value):
            return default
        return float(str(value))
    except:
        return default

# =================================================================
# FUNÇÕES DE COMPATIBILIDADE
# =================================================================

# Instâncias globais para compatibilidade
_db_manager = None
_capes_api = None

def get_db_manager():
    """Retorna instância do DatabaseManager"""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager

def get_capes_api():
    """Retorna instância do CapesAPI"""
    global _capes_api
    if _capes_api is None:
        _capes_api = CapesAPI()
    return _capes_api

# Funções de compatibilidade com código legado
def conectar_bd():
    """Função de compatibilidade - conectar ao banco"""
    db = get_db_manager()
    if db.test_connection():
        return db.engine
    return None

def salvar_df_bd(df: pd.DataFrame, table_name: str, engine=None):
    """Função de compatibilidade - salvar DataFrame"""
    db = get_db_manager()
    return db.save_dataframe(df, table_name)

def buscar_dados_capes(resource_id: str):
    """Função de compatibilidade - buscar dados CAPES"""
    api = get_capes_api()
    return api.fetch_all_data(resource_id)

def fetch_all_from_api(resource_id: str):
    """Função de compatibilidade - buscar dados da API"""
    return buscar_dados_capes(resource_id)

# =================================================================
# EXPORTAÇÕES
# =================================================================

__all__ = [
    'Config', 'Schema', 'DatabaseManager', 'CapesAPI',
    'log_execution', 'logger',
    'clean_text', 'normalize_cpf', 'safe_int', 'safe_float',
    'get_db_manager', 'get_capes_api',
    'conectar_bd', 'salvar_df_bd', 'buscar_dados_capes', 'fetch_all_from_api'
]
