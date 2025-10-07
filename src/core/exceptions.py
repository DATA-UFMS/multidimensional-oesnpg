#!/usr/bin/env python3
"""
Sistema de Exceções Personalizadas
==================================
Define exceções específicas para o Data Warehouse do Observatório CAPES.

Data: 2025-01-27
Versão: 1.0
"""

from typing import Optional, Dict, Any
import logging


class DWBaseException(Exception):
    """Exceção base para o Data Warehouse"""
    
    def __init__(self, message: str, context: Optional[Dict[str, Any]] = None):
        self.message = message
        self.context = context or {}
        super().__init__(self.message)
    
    def __str__(self):
        if self.context:
            context_str = ", ".join([f"{k}={v}" for k, v in self.context.items()])
            return f"{self.message} (Context: {context_str})"
        return self.message


class DataExtractionError(DWBaseException):
    """Erro na extração de dados"""
    pass


class DataTransformationError(DWBaseException):
    """Erro na transformação de dados"""
    pass


class DataLoadingError(DWBaseException):
    """Erro no carregamento de dados"""
    pass


class DatabaseConnectionError(DWBaseException):
    """Erro de conexão com banco de dados"""
    pass


class DatabaseQueryError(DWBaseException):
    """Erro na execução de query"""
    pass


class DataValidationError(DWBaseException):
    """Erro na validação de dados"""
    pass


class ConfigurationError(DWBaseException):
    """Erro de configuração"""
    pass


class APIConnectionError(DWBaseException):
    """Erro de conexão com API"""
    pass


class FileNotFoundError(DWBaseException):
    """Arquivo não encontrado"""
    pass


class DimensionCreationError(DWBaseException):
    """Erro na criação de dimensão"""
    pass


class FactTableCreationError(DWBaseException):
    """Erro na criação de tabela fato"""
    pass


class ETLPipelineError(DWBaseException):
    """Erro no pipeline ETL"""
    pass


class DataQualityError(DWBaseException):
    """Erro de qualidade de dados"""
    pass


class RetryableError(DWBaseException):
    """Erro que pode ser tentado novamente"""
    pass


class NonRetryableError(DWBaseException):
    """Erro que não deve ser tentado novamente"""
    pass


# =================================================================
# DECORATORS PARA TRATAMENTO DE ERROS
# =================================================================

def handle_database_errors(func):
    """Decorator para tratamento de erros de banco de dados"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger = logging.getLogger(func.__module__)
            logger.error(f"Database error in {func.__name__}: {str(e)}")
            raise DatabaseQueryError(
                f"Database operation failed in {func.__name__}",
                context={'function': func.__name__, 'error': str(e)}
            )
    return wrapper


def handle_api_errors(func):
    """Decorator para tratamento de erros de API"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger = logging.getLogger(func.__module__)
            logger.error(f"API error in {func.__name__}: {str(e)}")
            raise APIConnectionError(
                f"API operation failed in {func.__name__}",
                context={'function': func.__name__, 'error': str(e)}
            )
    return wrapper


def handle_etl_errors(func):
    """Decorator para tratamento de erros ETL"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger = logging.getLogger(func.__module__)
            logger.error(f"ETL error in {func.__name__}: {str(e)}")
            raise ETLPipelineError(
                f"ETL operation failed in {func.__name__}",
                context={'function': func.__name__, 'error': str(e)}
            )
    return wrapper


# =================================================================
# FUNÇÕES UTILITÁRIAS
# =================================================================

def is_retryable_error(error: Exception) -> bool:
    """Verifica se um erro pode ser tentado novamente"""
    if isinstance(error, RetryableError):
        return True
    if isinstance(error, NonRetryableError):
        return False
    
    # Erros que geralmente são retryable
    retryable_types = (
        DatabaseConnectionError,
        APIConnectionError,
        DataExtractionError
    )
    
    return isinstance(error, retryable_types)


def get_error_context(error: Exception) -> Dict[str, Any]:
    """Extrai contexto de um erro"""
    if isinstance(error, DWBaseException):
        return error.context
    return {'error_type': type(error).__name__, 'message': str(error)}


def log_error_with_context(error: Exception, logger: logging.Logger, 
                          additional_context: Optional[Dict[str, Any]] = None):
    """Loga erro com contexto completo"""
    context = get_error_context(error)
    if additional_context:
        context.update(additional_context)
    
    logger.error(
        f"Error: {str(error)}",
        extra={'error_context': context}
    )


# =================================================================
# EXPORTAÇÕES
# =================================================================

__all__ = [
    'DWBaseException',
    'DataExtractionError',
    'DataTransformationError', 
    'DataLoadingError',
    'DatabaseConnectionError',
    'DatabaseQueryError',
    'DataValidationError',
    'ConfigurationError',
    'APIConnectionError',
    'FileNotFoundError',
    'DimensionCreationError',
    'FactTableCreationError',
    'ETLPipelineError',
    'DataQualityError',
    'RetryableError',
    'NonRetryableError',
    'handle_database_errors',
    'handle_api_errors',
    'handle_etl_errors',
    'is_retryable_error',
    'get_error_context',
    'log_error_with_context'
]
