"""
Módulo de Validação de Dados
============================
Sistema de validação e qualidade de dados para o Data Warehouse.
"""

from .data_validator import (
    DataValidator,
    DimensionValidator,
    ValidationRule,
    ValidationResult,
    validate_dimension_data,
    get_validation_summary
)

__all__ = [
    'DataValidator',
    'DimensionValidator', 
    'ValidationRule',
    'ValidationResult',
    'validate_dimension_data',
    'get_validation_summary'
]
