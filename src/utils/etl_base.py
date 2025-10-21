"""
Camada base para execução padronizada dos pipelines de ETL do DW OESNPG.

Fornece classes abstratas que encapsulam fluxo padrão (extract → transform →
validate → load) e padronizam logging, opções de CLI e conexão com o banco.
"""

from __future__ import annotations

import argparse
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import pandas as pd


def _configure_default_logging():
    """Garante que exista configuração básica de logging."""
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="[%(asctime)s] [%(levelname)s] %(name)s - %(message)s",
        )


@dataclass
class ETLContext:
    """Metadados de execução para rastreamento e telemetria simples."""

    dry_run: bool = False
    limit: Optional[int] = None
    skip_load: bool = False
    extra: Dict[str, Any] = field(default_factory=dict)


class BaseETL(ABC):
    """
    Classe abstrata que encapsula o ciclo de vida padrão de pipelines de ETL.

    Subclasses devem implementar obrigatoriamente os métodos ``extract`` e
    ``transform``. Os métodos ``validate`` e ``load`` possuem comportamento
    padrão que pode ser sobrescrito conforme necessário.
    """

    layer: str = "generic"

    def __init__(
        self,
        table_name: str,
        *,
        name: Optional[str] = None,
        if_exists: str = "replace",
        enable_db_load: bool = True,
    ) -> None:
        _configure_default_logging()

        self.table_name = table_name
        self.name = name or self.__class__.__name__
        self.if_exists = if_exists
        self.enable_db_load = enable_db_load

        self.logger = logging.getLogger(self.name)
        self._db_manager = None

    # ------------------------------------------------------------------ #
    # Métodos que as subclasses devem (ou podem) sobrescrever
    # ------------------------------------------------------------------ #

    @abstractmethod
    def extract(self, context: ETLContext) -> pd.DataFrame:
        """Obtém dados da fonte de origem."""

    @abstractmethod
    def transform(self, data: pd.DataFrame, context: ETLContext) -> pd.DataFrame:
        """Aplica transformações necessárias para o modelo alvo."""

    def validate(self, data: pd.DataFrame, context: ETLContext) -> pd.DataFrame:
        """Validação padrão (no-op). Subclasses podem reforçar regras."""
        return data

    def load(self, data: pd.DataFrame, context: ETLContext) -> None:
        """Carga padrão utilizando DatabaseManager do core."""
        if not self.enable_db_load:
            self.logger.info("Carga em banco desabilitada para este pipeline.")
            return

        if data.empty:
            self.logger.warning("DataFrame vazio recebido para carga; operação ignorada.")
            return

        db = self.get_db_manager()
        self.logger.info(
            "Carregando %s registros na tabela %s (if_exists=%s)",
            f"{len(data):,}",
            self.table_name,
            self.if_exists,
        )
        db.save_dataframe(data, self.table_name, if_exists=self.if_exists)

    # ------------------------------------------------------------------ #
    # Fluxo principal
    # ------------------------------------------------------------------ #

    def run(self, *, dry_run: bool = False, limit: Optional[int] = None, skip_load: bool = False, **extra: Any) -> pd.DataFrame:
        """
        Executa o pipeline completo respeitando opções informadas.

        Args:
            dry_run: Se verdadeiro, pula a etapa de carga.
            limit: Número máximo de linhas para processar (apenas debugging).
            skip_load: Força pular a carga mesmo sem dry_run.
            extra: Metadados adicionais repassados para o contexto.
        """
        context = ETLContext(dry_run=dry_run, limit=limit, skip_load=skip_load, extra=extra)

        self.logger.info("Iniciando pipeline %s [%s]", self.name, self.layer)
        data = self.extract(context)

        if limit is not None and isinstance(data, pd.DataFrame):
            self.logger.info("Aplicando limit=%s ao conjunto extraído", limit)
            data = data.head(limit)

        transformed = self.transform(data, context)
        validated = self.validate(transformed, context)

        if dry_run or skip_load:
            self.logger.info("Dry-run/Skip-load ativado; etapa de carga não executada.")
            return validated

        self.load(validated, context)
        self.logger.info("Pipeline %s finalizado com sucesso.", self.name)
        return validated

    # ------------------------------------------------------------------ #
    # Helpers e CLI
    # ------------------------------------------------------------------ #

    def get_db_manager(self):
        """Lazy import para evitar dependências circulares."""
        if self._db_manager is None:
            from src.core.core import get_db_manager

            self._db_manager = get_db_manager()
        return self._db_manager

    @classmethod
    def cli(cls):
        """
        Executor CLI padrão para os pipelines.

        Subclasses podem sobrescrever este método se precisarem de argumentos
        adicionais. Por padrão são oferecidos --dry-run, --limit, --no-load e
        --if-exists.
        """
        parser = argparse.ArgumentParser(description=f"Executa pipeline {cls.__name__}")
        parser.add_argument("--dry-run", action="store_true", help="Executa apenas extract/transform/validate, sem carga")
        parser.add_argument("--limit", type=int, default=None, help="Processa apenas as primeiras N linhas (debug)")
        parser.add_argument("--no-load", action="store_true", help="Ignora etapa de carga no banco")
        parser.add_argument(
            "--if-exists",
            choices=["fail", "replace", "append"],
            default=None,
            help="Modo de escrita ao salvar DataFrame",
        )

        args = parser.parse_args()
        instance = cls()  # type: ignore[call-arg]
        if args.if_exists:
            instance.if_exists = args.if_exists

        instance.run(dry_run=args.dry_run, limit=args.limit, skip_load=args.no_load)


class RawETL(BaseETL):
    """Convenções específicas para pipelines da camada Raw."""

    layer = "raw"

    def __init__(self, table_name: str, *, name: Optional[str] = None, if_exists: str = "replace", enable_db_load: bool = True) -> None:
        super().__init__(table_name, name=name, if_exists=if_exists, enable_db_load=enable_db_load)


class DimensionETL(BaseETL):
    """Convenções específicas para dimensões."""

    layer = "dimension"

    def __init__(
        self,
        table_name: str,
        *,
        dimension_type: str,
        name: Optional[str] = None,
        if_exists: str = "replace",
        enable_db_load: bool = True,
    ) -> None:
        super().__init__(table_name, name=name, if_exists=if_exists, enable_db_load=enable_db_load)
        self.dimension_type = dimension_type

    def validate(self, data: pd.DataFrame, context: ETLContext) -> pd.DataFrame:
        from src.validation.data_validator import get_validation_summary, validate_dimension_data
        from src.core.exceptions import DataValidationError

        results = validate_dimension_data(data, self.dimension_type)
        summary = get_validation_summary(results)
        if summary.get("error_count", 0) > 0:
            self.logger.error("Validação falhou para %s: %s", self.dimension_type, summary)
            raise DataValidationError(f"Falha na validação da dimensão {self.dimension_type}", results=results)

        warn_count = summary.get("warning_count", 0)
        if warn_count:
            self.logger.warning("Validação concluiu com %s aviso(s) para %s", warn_count, self.dimension_type)

        return data


class FactETL(BaseETL):
    """Convenções específicas para tabelas fato."""

    layer = "fact"

    def __init__(self, table_name: str, *, name: Optional[str] = None, if_exists: str = "replace", enable_db_load: bool = True) -> None:
        super().__init__(table_name, name=name, if_exists=if_exists, enable_db_load=enable_db_load)

    def validate(self, data: pd.DataFrame, context: ETLContext) -> pd.DataFrame:
        if data.empty:
            self.logger.warning("Tabela fato resultou vazia.")
        return data


__all__ = ["BaseETL", "RawETL", "DimensionETL", "FactETL", "ETLContext"]
