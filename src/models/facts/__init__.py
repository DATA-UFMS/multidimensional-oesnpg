"""Fatos disponíveis no DW OESNPG."""

from .fact_producao_tema import FactProducaoTemaETL
from .fact_titulacao import FactTitulacaoETL

__all__ = ["FactProducaoTemaETL", "FactTitulacaoETL"]
