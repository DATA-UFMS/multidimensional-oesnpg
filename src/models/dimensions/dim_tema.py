#!/usr/bin/env python3
"""
Dimensão Tema (dim_tema)
-----------------------

Pipeline padronizado para construir a dimensão de temas e palavras-chave
provenientes da camada raw_raw_tema. Utiliza a infraestrutura comum de ETL
definida em ``src.utils.etl_base`` para garantir consistência de logging,
validação e carga.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Garantir raiz do projeto no PYTHONPATH para execuções diretas
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.etl_base import DimensionETL, ETLContext
from src.utils.naming_conventions import NamingConventions


UF_MAPPING = {
    "ACRE": "AC",
    "ALAGOAS": "AL",
    "AMAPÁ": "AP",
    "AMAZONAS": "AM",
    "BAHIA": "BA",
    "CEARÁ": "CE",
    "DISTRITO FEDERAL": "DF",
    "ESPÍRITO SANTO": "ES",
    "GOIÁS": "GO",
    "MARANHÃO": "MA",
    "MATO GROSSO": "MT",
    "MATO GROSSO DO SUL": "MS",
    "MINAS GERAIS": "MG",
    "PARÁ": "PA",
    "PARAÍBA": "PB",
    "PARANÁ": "PR",
    "PERNAMBUCO": "PE",
    "PIAUÍ": "PI",
    "RIO DE JANEIRO": "RJ",
    "RIO GRANDE DO NORTE": "RN",
    "RIO GRANDE DO SUL": "RS",
    "RONDÔNIA": "RO",
    "RORAIMA": "RR",
    "SANTA CATARINA": "SC",
    "SÃO PAULO": "SP",
    "SERGIPE": "SE",
    "TOCANTINS": "TO",
}


class DimTemaETL(DimensionETL):
    """Implementação padronizada da dim_tema."""

    QUERY_RAW_TEMA = """
        SELECT
            macrotema_id,
            macrotema_nome,
            tema_id,
            tema_nome,
            palavrachave_id,
            palavrachave_nome,
            uf
        FROM public.raw_tema
    """

    def __init__(self, *, table_name: str = "dim_tema") -> None:
        super().__init__(table_name=table_name, dimension_type="tema", name="DIM_TEMA")

    def extract(self, context: ETLContext) -> pd.DataFrame:
        self.logger.info("Carregando dados de raw_tema...")
        db = self.get_db_manager()
        df = db.execute_query(self.QUERY_RAW_TEMA)
        self.logger.info("Dados extraídos: %s registros", f"{len(df):,}")
        return df

    def transform(self, data: pd.DataFrame, context: ETLContext) -> pd.DataFrame:
        self.logger.info("Transformando dados para dimensão tema...")
        df = data.rename(columns={"palavrachave_nome": "palavra_chave"}).copy()

        if "uf" in df.columns:
            df["sigla_uf"] = (
                df["uf"]
                .fillna("")
                .astype(str)
                .str.upper()
                .map(UF_MAPPING)
                .fillna("XX")
            )
        else:
            df["sigla_uf"] = "XX"

        df = df.drop_duplicates().reset_index(drop=True)
        df.insert(0, "tema_sk", df.index + 1)

        registro_zero = NamingConventions.get_standard_unknown_record("tema")
        registro_zero.update({"sigla_uf": "XX"})

        df_final = pd.concat(
            [
                pd.DataFrame([registro_zero]),
                df[
                    [
                        "tema_sk",
                        "macrotema_id",
                        "macrotema_nome",
                        "tema_id",
                        "tema_nome",
                        "palavrachave_id",
                        "palavra_chave",
                        "sigla_uf",
                    ]
                ],
            ],
            ignore_index=True,
        )

        if "nome" in df_final.columns:
            df_final = df_final.drop(columns=["nome"])

        self.logger.info(
            "Dimensão tema preparada: %s registros (inclui SK=0)",
            f"{len(df_final):,}",
        )
        self._log_quick_stats(df_final)
        return df_final

    def load(self, data: pd.DataFrame, context: ETLContext) -> None:
        """Sobrescreve carga padrão garantindo PK explícita."""
        if data.empty:
            self.logger.warning("DataFrame vazio recebido; carga ignorada.")
            return

        db = self.get_db_manager()
        ddl = """
        CREATE TABLE {table} (
            tema_sk INTEGER PRIMARY KEY,
            macrotema_id INTEGER,
            macrotema_nome VARCHAR(255),
            tema_id INTEGER,
            tema_nome VARCHAR(255),
            palavrachave_id INTEGER,
            palavra_chave VARCHAR(255),
            sigla_uf VARCHAR(2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """.format(table=self.table_name)

        with db.engine.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {self.table_name} CASCADE;")
            conn.exec_driver_sql(ddl)
            data.to_sql(self.table_name, conn, if_exists="append", index=False, method="multi")

        self.logger.info("Dimensão tema persistida com PK em %s.", self.table_name)

    def _log_quick_stats(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        registros_sem_zero = df[df["tema_sk"] != 0]
        self.logger.info(
            "Estatísticas: macrotemas=%s | temas=%s | palavras-chave=%s | UFs=%s",
            registros_sem_zero["macrotema_id"].nunique(),
            registros_sem_zero["tema_id"].nunique(),
            registros_sem_zero["palavrachave_id"].nunique(),
            registros_sem_zero["sigla_uf"].nunique(),
        )


if __name__ == "__main__":
    DimTemaETL.cli()
