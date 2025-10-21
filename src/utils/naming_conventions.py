"""
Convenções de nomenclatura compartilhadas entre camadas do DW OESNPG.

Este módulo reúne regras centralizadas para padronizar colunas, nomes de
surrogate keys (SKs) e registros "desconhecidos" utilizados para manter
integridade referencial em dimensões e fatos.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


class NamingConventions:
    """Coleção de helpers estáticos para nomenclaturas padronizadas."""

    # Mapa de surrogate keys por dimensão
    DIMENSION_SK_MAP: Dict[str, str] = {
        "tempo": "tempo_sk",
        "localidade": "localidade_sk",
        "ies": "ies_sk",
        "ppg": "ppg_sk",
        "tema": "tema_sk",
        "ods": "ods_sk",
        "docente": "docente_sk",
        "discente": "discente_sk",
        "titulado": "titulado_sk",
        "posdoc": "posdoc_sk",
        "producao": "producao_sk",
    }

    # Colunas padrão (utilizadas principalmente pelo validador)
    DIMENSION_STANDARD_COLUMNS: Dict[str, List[str]] = {
        "tempo": ["tempo_sk", "ano", "mes", "nome_mes", "trimestre", "semestre"],
        "localidade": [
            "localidade_sk",
            "uf",
            "nome",
            "municipio",
            "regiao",
            "sigla_regiao",
            "latitude",
            "longitude",
        ],
        "ies": ["ies_sk", "codigo_ies", "sigla_ies", "nome"],
        "ppg": ["ppg_sk", "codigo_ppg", "nome", "sigla_ies", "codigo_ies"],
        "tema": ["tema_sk", "tema_id", "tema_nome", "macrotema_id", "macrotema_nome"],
        "ods": ["ods_sk", "numero_ods", "nome"],
        "docente": ["docente_sk", "id_pessoa", "nome"],
        "discente": ["discente_sk", "id_pessoa", "nome_discente"],
        "titulado": ["titulado_sk", "id_pessoa", "nome"],
        "posdoc": ["posdoc_sk", "id_pessoa", "nome"],
        "producao": ["producao_sk", "id_producao", "titulo_producao"],
    }

    # Padrões de registros desconhecidos
    UNKNOWN_RECORD_TEMPLATES: Dict[str, Dict[str, object]] = {
        "default": {"nome": "Não informado"},
        "localidade": {
            "uf": "XX",
            "sigla_regiao": "XX",
            "regiao": "Não informado",
            "municipio": None,
            "nome": "Não informado",
            "latitude": None,
            "longitude": None,
            "codigo_ibge": None,
            "capital": 0,
            "nivel": "Não informado",
        },
        "tema": {
            "tema_id": 0,
            "tema_nome": "Não informado",
            "macrotema_id": 0,
            "macrotema_nome": "Não informado",
            "palavrachave_id": 0,
            "palavra_chave": "Não informado",
        },
        "producao": {
            "id_producao": 0,
            "titulo_producao": "Não informado",
            "tipo_producao": "Desconhecido",
            "ano_producao": 0,
        },
    }

    @classmethod
    def _normalize_dimension_type(cls, dimension_type: str) -> str:
        norm = (dimension_type or "").strip().lower()
        if norm not in cls.DIMENSION_SK_MAP:
            raise ValueError(f"Tipo de dimensão desconhecido: {dimension_type}")
        return norm

    @classmethod
    def get_dimension_sk_name(cls, dimension_type: str) -> str:
        """Retorna o nome padronizado da SK para a dimensão informada."""
        dimension_type = cls._normalize_dimension_type(dimension_type)
        return cls.DIMENSION_SK_MAP[dimension_type]

    @classmethod
    def get_standard_columns_for_dimension(cls, dimension_type: str) -> List[str]:
        """Retorna colunas esperadas padrão para o tipo de dimensão."""
        dimension_type = cls._normalize_dimension_type(dimension_type)
        return cls.DIMENSION_STANDARD_COLUMNS.get(dimension_type, [])

    @classmethod
    def get_standard_unknown_record(cls, dimension_type: str) -> Dict[str, object]:
        """
        Retorna um dicionário contendo valores padrão para o registro "desconhecido"
        (SK=0) da dimensão.
        """
        dimension_type = cls._normalize_dimension_type(dimension_type)
        template = dict(cls.UNKNOWN_RECORD_TEMPLATES.get("default", {}))
        template.update(cls.UNKNOWN_RECORD_TEMPLATES.get(dimension_type, {}))

        sk_name = cls.get_dimension_sk_name(dimension_type)
        template.setdefault(sk_name, 0)
        template.setdefault("nome", "Não informado")
        return template


__all__ = ["NamingConventions"]
