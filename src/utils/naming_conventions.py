#!/usr/bin/env python3
"""
Convenções de Nomenclatura Padronizadas
========================================
Define padrões consistentes para nomes de campos, tabelas e funções
no Data Warehouse do Observatório CAPES.

Data: 2025-01-27
Versão: 1.0
"""

from typing import Dict, List, Optional
from dataclasses import dataclass


@dataclass
class FieldMapping:
    """Mapeamento de campos padronizados"""
    standard_name: str
    description: str
    data_type: str
    required: bool = True
    default_value: Optional[str] = None


class NamingConventions:
    """Convenções de nomenclatura padronizadas para o Data Warehouse"""
    
    # =================================================================
    # CAMPOS DE IDENTIFICAÇÃO
    # =================================================================
    
    IDENTIFICATION_FIELDS = {
        'sk': FieldMapping(
            standard_name='surrogate_key',
            description='Chave substituta (surrogate key)',
            data_type='INTEGER',
            required=True
        ),
        'id': FieldMapping(
            standard_name='business_key', 
            description='Chave de negócio (business key)',
            data_type='VARCHAR(50)',
            required=True
        ),
        'codigo': FieldMapping(
            standard_name='code',
            description='Código identificador',
            data_type='VARCHAR(20)',
            required=False
        )
    }
    
    # Mapeamento específico para SKs por tipo de dimensão
    DIMENSION_SK_MAPPING = {
        'tempo': 'tempo_sk',
        'localidade': 'localidade_sk', 
        'tema': 'tema_sk',
        'ods': 'ods_sk',
        'ppg': 'ppg_sk',
        'docente': 'docente_sk',
        'discente': 'discente_sk',
        'titulado': 'titulado_sk',
        'ies': 'ies_sk',
        'producao': 'producao_sk'
    }
    
    # =================================================================
    # CAMPOS DESCRITIVOS
    # =================================================================
    
    DESCRIPTIVE_FIELDS = {
        'nome': FieldMapping(
            standard_name='name',
            description='Nome completo',
            data_type='VARCHAR(255)',
            required=True
        ),
        'descricao': FieldMapping(
            standard_name='description',
            description='Descrição detalhada',
            data_type='TEXT',
            required=False
        ),
        'sigla': FieldMapping(
            standard_name='acronym',
            description='Sigla ou abreviação',
            data_type='VARCHAR(10)',
            required=False
        ),
        'titulo': FieldMapping(
            standard_name='title',
            description='Título',
            data_type='VARCHAR(255)',
            required=False
        )
    }
    
    # =================================================================
    # CAMPOS DE LOCALIZAÇÃO
    # =================================================================
    
    LOCATION_FIELDS = {
        'uf': FieldMapping(
            standard_name='state',
            description='Unidade Federativa',
            data_type='VARCHAR(2)',
            required=False
        ),
        'municipio': FieldMapping(
            standard_name='city',
            description='Município',
            data_type='VARCHAR(100)',
            required=False
        ),
        'regiao': FieldMapping(
            standard_name='region',
            description='Região geográfica',
            data_type='VARCHAR(50)',
            required=False
        ),
        'pais': FieldMapping(
            standard_name='country',
            description='País',
            data_type='VARCHAR(50)',
            required=False,
            default_value='BRASIL'
        )
    }
    
    # =================================================================
    # CAMPOS TEMPORAIS
    # =================================================================
    
    TEMPORAL_FIELDS = {
        'data': FieldMapping(
            standard_name='date',
            description='Data',
            data_type='DATE',
            required=False
        ),
        'data_inicio': FieldMapping(
            standard_name='start_date',
            description='Data de início',
            data_type='DATE',
            required=False
        ),
        'data_fim': FieldMapping(
            standard_name='end_date',
            description='Data de fim',
            data_type='DATE',
            required=False
        ),
        'ano': FieldMapping(
            standard_name='year',
            description='Ano',
            data_type='INTEGER',
            required=False
        ),
        'mes': FieldMapping(
            standard_name='month',
            description='Mês',
            data_type='INTEGER',
            required=False
        )
    }
    
    # =================================================================
    # CAMPOS DE STATUS E CATEGORIZAÇÃO
    # =================================================================
    
    STATUS_FIELDS = {
        'situacao': FieldMapping(
            standard_name='status',
            description='Situação atual',
            data_type='VARCHAR(50)',
            required=False,
            default_value='ATIVO'
        ),
        'categoria': FieldMapping(
            standard_name='category',
            description='Categoria',
            data_type='VARCHAR(100)',
            required=False
        ),
        'tipo': FieldMapping(
            standard_name='type',
            description='Tipo',
            data_type='VARCHAR(50)',
            required=False
        ),
        'nivel': FieldMapping(
            standard_name='level',
            description='Nível',
            data_type='VARCHAR(20)',
            required=False
        )
    }
    
    # =================================================================
    # CAMPOS DE CONTROLE
    # =================================================================
    
    CONTROL_FIELDS = {
        'created_at': FieldMapping(
            standard_name='created_at',
            description='Data de criação',
            data_type='TIMESTAMP',
            required=True,
            default_value='CURRENT_TIMESTAMP'
        ),
        'updated_at': FieldMapping(
            standard_name='updated_at',
            description='Data de atualização',
            data_type='TIMESTAMP',
            required=True,
            default_value='CURRENT_TIMESTAMP'
        ),
        'ativo': FieldMapping(
            standard_name='active',
            description='Indicador de ativo',
            data_type='BOOLEAN',
            required=False,
            default_value='TRUE'
        )
    }
    
    # =================================================================
    # PADRÕES DE NOMENCLATURA
    # =================================================================
    
    @classmethod
    def get_standard_field_name(cls, field_name: str) -> str:
        """Converte nome de campo para padrão"""
        # Mapeamento direto
        field_mappings = {
            **cls.IDENTIFICATION_FIELDS,
            **cls.DESCRIPTIVE_FIELDS,
            **cls.LOCATION_FIELDS,
            **cls.TEMPORAL_FIELDS,
            **cls.STATUS_FIELDS,
            **cls.CONTROL_FIELDS
        }
        
        # Buscar mapeamento direto
        if field_name in field_mappings:
            return field_mappings[field_name].standard_name
        
        # Buscar por similaridade
        for key, mapping in field_mappings.items():
            if key.lower() in field_name.lower() or field_name.lower() in key.lower():
                return mapping.standard_name
        
        # Se não encontrar, retornar o nome original em snake_case
        return field_name.lower().replace(' ', '_').replace('-', '_')
    
    @classmethod
    def get_dimension_sk_name(cls, dimension_type: str) -> str:
        """Retorna o nome específico da SK para o tipo de dimensão"""
        return cls.DIMENSION_SK_MAPPING.get(dimension_type, f"{dimension_type}_sk")
    
    @classmethod
    def get_standard_columns_for_dimension(cls, dimension_type: str) -> List[str]:
        """Retorna colunas padrão para cada tipo de dimensão com SK específica"""
        
        # Obter SK específica para o tipo de dimensão
        sk_name = cls.get_dimension_sk_name(dimension_type)
        
        base_columns = [sk_name, 'id', 'nome', 'created_at', 'updated_at']
        
        dimension_specific = {
            'tempo': base_columns + ['ano', 'mes', 'data', 'semestre', 'trimestre'],
            'localidade': base_columns + ['uf', 'municipio', 'regiao', 'pais'],
            'ies': base_columns + ['sigla', 'categoria', 'tipo', 'uf', 'municipio'],
            'ppg': base_columns + ['codigo', 'nivel', 'modalidade', 'area_avaliacao'],
            'tema': base_columns + ['codigo', 'area_concentracao', 'palavras_chave'],
            'docente': base_columns + ['titulacao', 'regime', 'vinculo', 'genero'],
            'discente': base_columns + ['genero', 'idade', 'situacao', 'grau'],
            'titulado': base_columns + ['genero', 'grau_titulacao', 'data_titulacao'],
            'producao': base_columns + ['tipo', 'categoria', 'veiculo', 'qualis'],
            'ods': base_columns + ['numero', 'titulo_original', 'meta_principal']
        }
        
        return dimension_specific.get(dimension_type, base_columns)
    
    @classmethod
    def get_table_name(cls, entity_name: str) -> str:
        """Gera nome padronizado para tabela"""
        # Prefixo para dimensões
        if entity_name.startswith('dim_'):
            return entity_name.lower()
        
        # Prefixo para fatos
        if entity_name.startswith('fato_') or entity_name.startswith('fact_'):
            return f"fato_{entity_name.lower()}"
        
        # Prefixo para dimensões
        return f"dim_{entity_name.lower()}"
    
    @classmethod
    def get_column_definition(cls, field_name: str) -> Dict[str, str]:
        """Retorna definição completa da coluna"""
        field_mappings = {
            **cls.IDENTIFICATION_FIELDS,
            **cls.DESCRIPTIVE_FIELDS,
            **cls.LOCATION_FIELDS,
            **cls.TEMPORAL_FIELDS,
            **cls.STATUS_FIELDS,
            **cls.CONTROL_FIELDS
        }
        
        if field_name in field_mappings:
            mapping = field_mappings[field_name]
            return {
                'name': mapping.standard_name,
                'type': mapping.data_type,
                'description': mapping.description,
                'required': mapping.required,
                'default': mapping.default_value
            }
        
        return {
            'name': field_name.lower(),
            'type': 'VARCHAR(255)',
            'description': f'Campo {field_name}',
            'required': False,
            'default': None
        }
    
    @classmethod
    def get_standard_columns_for_dimension(cls, dimension_type: str) -> List[str]:
        """Retorna colunas padrão para cada tipo de dimensão (com SK específica)."""
        sk_name = cls.get_dimension_sk_name(dimension_type)

        # Base geral com 'id' e 'nome'
        base_with_nome = [sk_name, 'id', 'nome', 'created_at', 'updated_at']

        # Base sem 'id' e 'nome' (para dimensões que não usam estes campos)
        base_without_nome = [sk_name, 'created_at', 'updated_at']

        if dimension_type == 'tempo':
            return base_without_nome + ['ano', 'mes', 'data', 'semestre', 'trimestre']
        if dimension_type == 'localidade':
            return base_with_nome + ['uf', 'municipio', 'regiao', 'pais']
        if dimension_type == 'ies':
            return base_with_nome + ['sigla', 'categoria', 'tipo', 'uf', 'municipio']
        if dimension_type == 'ppg':
            return base_with_nome + ['codigo', 'nivel', 'modalidade', 'area_avaliacao']
        if dimension_type == 'tema':
            return base_with_nome + ['codigo', 'area_concentracao', 'palavras_chave']
        if dimension_type == 'docente':
            return base_with_nome + ['titulacao', 'regime', 'vinculo', 'genero']
        if dimension_type == 'discente':
            return base_with_nome + ['genero', 'idade', 'situacao', 'grau']
        if dimension_type == 'titulado':
            return base_with_nome + ['genero', 'grau_titulacao', 'data_titulacao']
        if dimension_type == 'producao':
            return base_with_nome + ['tipo', 'categoria', 'veiculo', 'qualis']
        if dimension_type == 'ods':
            return base_with_nome + ['numero', 'titulo_original', 'meta_principal']

        return base_with_nome
    
    @classmethod
    def validate_field_name(cls, field_name: str) -> bool:
        """Valida se nome do campo segue convenções"""
        # Verificar se é snake_case
        if not field_name.islower() or ' ' in field_name:
            return False
        
        # Verificar se não tem caracteres especiais
        allowed_chars = set('abcdefghijklmnopqrstuvwxyz0123456789_')
        if not set(field_name).issubset(allowed_chars):
            return False
        
        # Verificar se não começa com número
        if field_name[0].isdigit():
            return False
        
        return True
    
    @classmethod
    def get_standard_unknown_record(cls, dimension_type: str) -> Dict[str, any]:
        """Retorna registro padrão SK=0 para dimensões"""
        
        # Obter nome específico da SK para o tipo de dimensão
        sk_name = cls.get_dimension_sk_name(dimension_type)
        
        base_record = {
            sk_name: 0,  # Usar nome específico da SK
            'id': 'UNKNOWN_0',
            'nome': 'DESCONHECIDO',
            'created_at': 'CURRENT_TIMESTAMP',
            'updated_at': 'CURRENT_TIMESTAMP'
        }
        
        dimension_specific = {
            'tempo': {
                **base_record,
                'ano': None,
                'mes': None,
                'data': None,
                'semestre': None,
                'trimestre': None
            },
            'localidade': {
                **base_record,
                'uf': 'XX',
                'municipio': 'DESCONHECIDO',
                'regiao': 'DESCONHECIDO',
                'pais': 'DESCONHECIDO'
            },
            'ies': {
                **base_record,
                'sigla': 'XX',
                'categoria': 'DESCONHECIDO',
                'tipo': 'DESCONHECIDO',
                'uf': 'XX',
                'municipio': 'DESCONHECIDO'
            }
        }
        
        return dimension_specific.get(dimension_type, base_record)


# =================================================================
# FUNÇÕES UTILITÁRIAS
# =================================================================

def standardize_field_name(field_name: str) -> str:
    """Função utilitária para padronizar nome de campo"""
    return NamingConventions.get_standard_field_name(field_name)


def validate_dataframe_columns(df_columns: List[str]) -> Dict[str, List[str]]:
    """Valida colunas de DataFrame contra convenções"""
    issues = {
        'invalid_names': [],
        'suggestions': {}
    }
    
    for col in df_columns:
        if not NamingConventions.validate_field_name(col):
            issues['invalid_names'].append(col)
            issues['suggestions'][col] = NamingConventions.get_standard_field_name(col)
    
    return issues


def get_dimension_standard_schema(dimension_type: str) -> Dict[str, Dict]:
    """Retorna schema padrão para uma dimensão"""
    columns = NamingConventions.get_standard_columns_for_dimension(dimension_type)
    schema = {}
    
    for col in columns:
        schema[col] = NamingConventions.get_column_definition(col)
    
    return schema


# =================================================================
# EXPORTAÇÕES
# =================================================================

__all__ = [
    'NamingConventions',
    'FieldMapping',
    'standardize_field_name',
    'validate_dataframe_columns',
    'get_dimension_standard_schema'
]
