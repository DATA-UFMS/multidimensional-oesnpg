"""
Utils Package - Funcionalidades essenciais do Data Warehouse
Observatório CAPES - Pós-graduação brasileira
"""

from .core import (
    # Classes principais
    Config,
    Schema,
    DatabaseManager,
    CapesAPI,
    
    # Decorators e logging
    log_execution,
    logger,
    
    # Utilidades
    clean_text,
    normalize_cpf,
    safe_int,
    safe_float,
    
    # Gerenciadores
    get_db_manager,
    get_capes_api,
    
    # Compatibilidade
    conectar_bd,
    salvar_df_bd,
    buscar_dados_capes,
    fetch_all_from_api
)

__version__ = "2.0.0"
__author__ = "Data Warehouse Team"

# Configuração padrão
config = Config()
schema = Schema()

__all__ = [
    'Config', 'Schema', 'DatabaseManager', 'CapesAPI',
    'log_execution', 'logger',
    'clean_text', 'normalize_cpf', 'safe_int', 'safe_float',
    'get_db_manager', 'get_capes_api',
    'conectar_bd', 'salvar_df_bd', 'buscar_dados_capes', 'fetch_all_from_api',
    'config', 'schema'
]
