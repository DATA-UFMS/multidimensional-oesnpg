#!/usr/bin/env python3
"""
Sistema de Validação de Dados
=============================
Valida qualidade e consistência dos dados no Data Warehouse.

Data: 2025-01-27
Versão: 1.0
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import logging

from src.core.exceptions import DataValidationError


@dataclass
class ValidationRule:
    """Regra de validação"""
    name: str
    column: str
    rule_type: str
    parameters: Dict[str, Any]
    severity: str = 'ERROR'  # ERROR, WARNING, INFO
    description: str = ''


@dataclass
class ValidationResult:
    """Resultado de validação"""
    rule_name: str
    column: str
    passed: bool
    failed_count: int
    total_count: int
    message: str
    severity: str
    failed_records: Optional[List[int]] = None


class DataValidator:
    """Validador de qualidade de dados"""
    
    def __init__(self):
        self.rules: List[ValidationRule] = []
        self.logger = logging.getLogger(__name__)
    
    def add_rule(self, name: str, column: str, rule_type: str, 
                 parameters: Dict[str, Any], severity: str = 'ERROR',
                 description: str = '') -> 'DataValidator':
        """Adiciona regra de validação"""
        rule = ValidationRule(
            name=name,
            column=column,
            rule_type=rule_type,
            parameters=parameters,
            severity=severity,
            description=description
        )
        self.rules.append(rule)
        return self
    
    def validate(self, df: pd.DataFrame) -> List[ValidationResult]:
        """Valida DataFrame contra todas as regras"""
        results = []
        
        for rule in self.rules:
            try:
                result = self._validate_rule(df, rule)
                results.append(result)
            except Exception as e:
                self.logger.error(f"Error validating rule {rule.name}: {e}")
                results.append(ValidationResult(
                    rule_name=rule.name,
                    column=rule.column,
                    passed=False,
                    failed_count=0,
                    total_count=len(df),
                    message=f"Validation error: {str(e)}",
                    severity='ERROR'
                ))
        
        return results
    
    def _validate_rule(self, df: pd.DataFrame, rule: ValidationRule) -> ValidationResult:
        """Valida uma regra específica"""
        column = rule.column
        
        # Verificar se coluna existe
        if column not in df.columns:
            return ValidationResult(
                rule_name=rule.name,
                column=column,
                passed=False,
                failed_count=0,
                total_count=len(df),
                message=f"Column '{column}' not found",
                severity='ERROR'
            )
        
        # Aplicar regra baseada no tipo
        if rule.rule_type == 'not_null':
            return self._validate_not_null(df, rule)
        elif rule.rule_type == 'unique':
            return self._validate_unique(df, rule)
        elif rule.rule_type == 'range':
            return self._validate_range(df, rule)
        elif rule.rule_type == 'format':
            return self._validate_format(df, rule)
        elif rule.rule_type == 'values':
            return self._validate_values(df, rule)
        elif rule.rule_type == 'length':
            return self._validate_length(df, rule)
        else:
            return ValidationResult(
                rule_name=rule.name,
                column=column,
                passed=False,
                failed_count=0,
                total_count=len(df),
                message=f"Unknown rule type: {rule.rule_type}",
                severity='ERROR'
            )
    
    def _validate_not_null(self, df: pd.DataFrame, rule: ValidationRule) -> ValidationResult:
        """Valida se valores não são nulos"""
        column = rule.column
        null_mask = df[column].isnull()
        failed_count = null_mask.sum()
        total_count = len(df)
        
        failed_records = df[null_mask].index.tolist() if failed_count > 0 else None
        
        return ValidationResult(
            rule_name=rule.name,
            column=column,
            passed=failed_count == 0,
            failed_count=failed_count,
            total_count=total_count,
            message=f"Found {failed_count} null values in {column}",
            severity=rule.severity,
            failed_records=failed_records
        )
    
    def _validate_unique(self, df: pd.DataFrame, rule: ValidationRule) -> ValidationResult:
        """Valida se valores são únicos"""
        column = rule.column
        duplicate_mask = df[column].duplicated()
        failed_count = duplicate_mask.sum()
        total_count = len(df)
        
        failed_records = df[duplicate_mask].index.tolist() if failed_count > 0 else None
        
        return ValidationResult(
            rule_name=rule.name,
            column=column,
            passed=failed_count == 0,
            failed_count=failed_count,
            total_count=total_count,
            message=f"Found {failed_count} duplicate values in {column}",
            severity=rule.severity,
            failed_records=failed_records
        )
    
    def _validate_range(self, df: pd.DataFrame, rule: ValidationRule) -> ValidationResult:
        """Valida se valores estão em um range"""
        column = rule.column
        min_val = rule.parameters.get('min')
        max_val = rule.parameters.get('max')
        
        # Converter para numérico se possível
        try:
            numeric_series = pd.to_numeric(df[column], errors='coerce')
        except:
            return ValidationResult(
                rule_name=rule.name,
                column=column,
                passed=False,
                failed_count=0,
                total_count=len(df),
                message=f"Cannot convert {column} to numeric for range validation",
                severity='ERROR'
            )
        
        failed_mask = pd.Series([False] * len(df))
        
        if min_val is not None:
            failed_mask |= (numeric_series < min_val)
        
        if max_val is not None:
            failed_mask |= (numeric_series > max_val)
        
        failed_count = failed_mask.sum()
        total_count = len(df)
        failed_records = df[failed_mask].index.tolist() if failed_count > 0 else None
        
        range_str = f"[{min_val}, {max_val}]" if min_val is not None and max_val is not None else f">= {min_val}" if min_val is not None else f"<= {max_val}"
        
        return ValidationResult(
            rule_name=rule.name,
            column=column,
            passed=failed_count == 0,
            failed_count=failed_count,
            total_count=total_count,
            message=f"Found {failed_count} values outside range {range_str} in {column}",
            severity=rule.severity,
            failed_records=failed_records
        )
    
    def _validate_format(self, df: pd.DataFrame, rule: ValidationRule) -> ValidationResult:
        """Valida formato de strings"""
        column = rule.column
        pattern = rule.parameters.get('pattern')
        
        if not pattern:
            return ValidationResult(
                rule_name=rule.name,
                column=column,
                passed=False,
                failed_count=0,
                total_count=len(df),
                message="No pattern provided for format validation",
                severity='ERROR'
            )
        
        import re
        failed_mask = ~df[column].astype(str).str.match(pattern, na=False)
        failed_count = failed_mask.sum()
        total_count = len(df)
        failed_records = df[failed_mask].index.tolist() if failed_count > 0 else None
        
        return ValidationResult(
            rule_name=rule.name,
            column=column,
            passed=failed_count == 0,
            failed_count=failed_count,
            total_count=total_count,
            message=f"Found {failed_count} values not matching pattern '{pattern}' in {column}",
            severity=rule.severity,
            failed_records=failed_records
        )
    
    def _validate_values(self, df: pd.DataFrame, rule: ValidationRule) -> ValidationResult:
        """Valida se valores estão em uma lista permitida"""
        column = rule.column
        allowed_values = rule.parameters.get('values', [])
        
        if not allowed_values:
            return ValidationResult(
                rule_name=rule.name,
                column=column,
                passed=False,
                failed_count=0,
                total_count=len(df),
                message="No allowed values provided for validation",
                severity='ERROR'
            )
        
        failed_mask = ~df[column].isin(allowed_values)
        failed_count = failed_mask.sum()
        total_count = len(df)
        failed_records = df[failed_mask].index.tolist() if failed_count > 0 else None
        
        return ValidationResult(
            rule_name=rule.name,
            column=column,
            passed=failed_count == 0,
            failed_count=failed_count,
            total_count=total_count,
            message=f"Found {failed_count} values not in allowed list {allowed_values} in {column}",
            severity=rule.severity,
            failed_records=failed_records
        )
    
    def _validate_length(self, df: pd.DataFrame, rule: ValidationRule) -> ValidationResult:
        """Valida comprimento de strings"""
        column = rule.column
        min_length = rule.parameters.get('min_length', 0)
        max_length = rule.parameters.get('max_length', float('inf'))
        
        string_lengths = df[column].astype(str).str.len()
        failed_mask = (string_lengths < min_length) | (string_lengths > max_length)
        failed_count = failed_mask.sum()
        total_count = len(df)
        failed_records = df[failed_mask].index.tolist() if failed_count > 0 else None
        
        return ValidationResult(
            rule_name=rule.name,
            column=column,
            passed=failed_count == 0,
            failed_count=failed_count,
            total_count=total_count,
            message=f"Found {failed_count} values with length outside [{min_length}, {max_length}] in {column}",
            severity=rule.severity,
            failed_records=failed_records
        )


class DimensionValidator(DataValidator):
    """Validador específico para dimensões"""
    
    def __init__(self, dimension_type: str):
        super().__init__()
        self.dimension_type = dimension_type
        self._add_standard_rules()
    
    def _add_standard_rules(self):
        """Adiciona regras padrão para dimensões"""
        
        # Obter nome específico da SK para o tipo de dimensão
        from src.utils.naming_conventions import NamingConventions
        sk_name = NamingConventions.get_dimension_sk_name(self.dimension_type)
        
        # Regra para SK (surrogate key)
        self.add_rule(
            name='sk_not_null',
            column=sk_name,
            rule_type='not_null',
            parameters={},
            severity='ERROR',
            description='Surrogate key cannot be null'
        )
        
        self.add_rule(
            name='sk_unique',
            column=sk_name,
            rule_type='unique',
            parameters={},
            severity='ERROR',
            description='Surrogate key must be unique'
        )
        
        self.add_rule(
            name='sk_range',
            column=sk_name,
            rule_type='range',
            parameters={'min': 0},
            severity='ERROR',
            description='Surrogate key must be >= 0'
        )
        
        # Regra para nome
        if 'nome' in self._get_expected_columns():
            self.add_rule(
                name='nome_not_null',
                column='nome',
                rule_type='not_null',
                parameters={},
                severity='ERROR',
                description='Name cannot be null'
            )
        
        # Regras específicas por tipo de dimensão
        if self.dimension_type == 'tempo':
            self._add_tempo_rules()
        elif self.dimension_type == 'localidade':
            self._add_localidade_rules()
        elif self.dimension_type == 'ies':
            self._add_ies_rules()
    
    def _get_expected_columns(self) -> List[str]:
        """Retorna colunas esperadas para o tipo de dimensão"""
        from src.utils.naming_conventions import NamingConventions
        return NamingConventions.get_standard_columns_for_dimension(self.dimension_type)
    
    def _add_tempo_rules(self):
        """Regras específicas para dimensão tempo"""
        self.add_rule(
            name='ano_range',
            column='ano',
            rule_type='range',
            parameters={'min': 2000, 'max': 2030},
            severity='WARNING',
            description='Year should be between 2000 and 2030'
        )
        
        self.add_rule(
            name='mes_range',
            column='mes',
            rule_type='range',
            parameters={'min': 1, 'max': 12},
            severity='ERROR',
            description='Month should be between 1 and 12'
        )
    
    def _add_localidade_rules(self):
        """Regras específicas para dimensão localidade"""
        self.add_rule(
            name='uf_format',
            column='uf',
            rule_type='format',
            parameters={'pattern': r'^[A-Z]{2}$'},
            severity='ERROR',
            description='UF should be 2 uppercase letters'
        )
        
        self.add_rule(
            name='uf_values',
            column='uf',
            rule_type='values',
            parameters={'values': ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO', 'XX']},
            severity='WARNING',
            description='UF should be a valid Brazilian state code'
        )
    
    def _add_ies_rules(self):
        """Regras específicas para dimensão IES"""
        self.add_rule(
            name='nome_length',
            column='nome',
            rule_type='length',
            parameters={'min_length': 3, 'max_length': 255},
            severity='WARNING',
            description='IES name should be between 3 and 255 characters'
        )


def validate_dimension_data(df: pd.DataFrame, dimension_type: str) -> List[ValidationResult]:
    """Função utilitária para validar dados de dimensão"""
    validator = DimensionValidator(dimension_type)
    return validator.validate(df)


def get_validation_summary(results: List[ValidationResult]) -> Dict[str, Any]:
    """Retorna resumo das validações"""
    total_rules = len(results)
    passed_rules = sum(1 for r in results if r.passed)
    failed_rules = total_rules - passed_rules
    
    error_count = sum(1 for r in results if not r.passed and r.severity == 'ERROR')
    warning_count = sum(1 for r in results if not r.passed and r.severity == 'WARNING')
    
    return {
        'total_rules': total_rules,
        'passed_rules': passed_rules,
        'failed_rules': failed_rules,
        'error_count': error_count,
        'warning_count': warning_count,
        'success_rate': passed_rules / total_rules if total_rules > 0 else 0
    }


# =================================================================
# EXPORTAÇÕES
# =================================================================

__all__ = [
    'DataValidator',
    'DimensionValidator',
    'ValidationRule',
    'ValidationResult',
    'validate_dimension_data',
    'get_validation_summary'
]
