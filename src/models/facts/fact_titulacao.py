#!/usr/bin/env python3
"""Fato de titulação cruzando titulados e temas por ano base."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.etl_base import ETLContext, FactETL


class FactTitulacaoETL(FactETL):
    """Pipeline padronizado para carregar `fact_titulacao`."""

    def __init__(
        self,
        *,
        parquet_path: Optional[Path] = None,
        ano_base: int = 2023,
        table_name: str = "fact_titulacao",
    ) -> None:
        super().__init__(table_name=table_name, name="FACT_TITULACAO")
        self.parquet_path = parquet_path
        self.ano_base = ano_base
        self._dimension_cache: Dict[str, pd.DataFrame] = {}

    # ------------------------------------------------------------------
    # Ciclo ETL
    # ------------------------------------------------------------------

    def extract(self, context: ETLContext) -> pd.DataFrame:
        parquet_file = self._resolve_parquet_path()
        self.logger.info("Lendo parquet de mapeamentos de titulados: %s", parquet_file)
        df = pd.read_parquet(parquet_file)
        self.logger.info("Registros carregados: %s", f"{len(df):,}")
        return df

    def transform(self, data: pd.DataFrame, context: ETLContext) -> pd.DataFrame:
        if data.empty:
            self.logger.warning("Dataset de origem vazio; nada a transformar.")
            return data

        df = self._normalize_columns(data)
        df["ano_base"] = context.extra.get("ano_base", self.ano_base)

        def series_or(name: str, default_value) -> pd.Series:
            if name in df.columns:
                return df[name]
            return pd.Series(default_value, index=df.index)

        numeric_cols = [
            "score_title",
            "score_abstract",
            "score_keywords",
            "score_final",
        ]
        for column in numeric_cols:
            df[column] = pd.to_numeric(series_or(column, 0.0), errors="coerce").fillna(0.0)

        df["hash_id"] = series_or("hash_id", "").fillna("").astype(str).str.strip()
        df["tema_id"] = pd.to_numeric(series_or("tema_id", 0), errors="coerce").fillna(0).astype(int)
        df["uf_tema"] = series_or("uf_tema", "").fillna("").astype(str).str.upper()
        df["uf_pesquisador"] = series_or("uf_pesquisador", "").fillna("").astype(str).str.upper()

        dims = self._load_dimensions()
        df["tempo_sk"] = self._map_tempo(df, dims)
        df["tema_sk"] = self._map_tema(df, dims)
        df["titulado_sk"], df["match_status"] = self._map_titulado(df, dims)

        df_fact = pd.DataFrame(
            {
                "titulado_sk": df["titulado_sk"].fillna(0).astype(int).clip(lower=0),
                "tema_sk": df["tema_sk"].fillna(0).astype(int).clip(lower=0),
                "tempo_sk": df["tempo_sk"].fillna(0).astype(int).clip(lower=0),
                "hash_id": df["hash_id"],
                "tema_id": df["tema_id"],
                "ano_base": df["ano_base"].astype(int),
                "uf_tema": df["uf_tema"],
                "uf_pesquisador": df["uf_pesquisador"],
                "score_title": df["score_title"],
                "score_abstract": df["score_abstract"],
                "score_keywords": df["score_keywords"],
                "score_final": df["score_final"],
                "modelo_nivel": series_or("modelo_nivel", "NA").fillna("NA").astype(str).str.upper(),
                "modelo_explicacao": series_or("modelo_explicacao", "").fillna("").astype(str),
                "modelo_erro": series_or("modelo_erro", "").fillna("").astype(str),
                "model": series_or("model", "").fillna("").astype(str),
                "match_status": df["match_status"],
                "fonte_dados": series_or("fonte_dados", "mapeamentos_titulados_2023").fillna("mapeamentos_titulados_2023"),
            }
        )

        self._log_summary(df_fact)
        return df_fact

    def load(self, data: pd.DataFrame, context: ETLContext) -> None:
        if data.empty:
            self.logger.warning("DataFrame vazio recebido; carga não executada.")
            return

        db = self.get_db_manager()
        dims_available = self._detect_dimensions(db)

        fk_clauses = []
        if dims_available.get("dim_titulado"):
            fk_clauses.append("CONSTRAINT fk_fact_titulacao_titulado FOREIGN KEY (titulado_sk) REFERENCES dim_titulado(titulado_sk)")
        if dims_available.get("dim_tema"):
            fk_clauses.append("CONSTRAINT fk_fact_titulacao_tema FOREIGN KEY (tema_sk) REFERENCES dim_tema(tema_sk)")
        if dims_available.get("dim_tempo"):
            fk_clauses.append("CONSTRAINT fk_fact_titulacao_tempo FOREIGN KEY (tempo_sk) REFERENCES dim_tempo(tempo_sk)")

        fk_sql = ",\n        ".join(fk_clauses)
        fk_sql = f",\n        {fk_sql}" if fk_sql else ""

        ddl = f"""
        DROP TABLE IF EXISTS {self.table_name} CASCADE;
        CREATE TABLE {self.table_name} (
            titulacao_id SERIAL PRIMARY KEY,
            titulado_sk INTEGER NOT NULL DEFAULT 0,
            tema_sk INTEGER NOT NULL DEFAULT 0,
            tempo_sk INTEGER NOT NULL DEFAULT 0,
            hash_id VARCHAR(128),
            tema_id INTEGER,
            ano_base INTEGER,
            uf_tema VARCHAR(2),
            uf_pesquisador VARCHAR(2),
            score_title NUMERIC(10,6),
            score_abstract NUMERIC(10,6),
            score_keywords NUMERIC(10,6),
            score_final NUMERIC(10,6),
            modelo_nivel VARCHAR(20),
            modelo_explicacao TEXT,
            modelo_erro TEXT,
            model VARCHAR(120),
            match_status VARCHAR(20) DEFAULT 'UNMATCHED',
            fonte_dados VARCHAR(120) DEFAULT 'mapeamentos_titulados_2023',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            {fk_sql}
        );
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_tema ON {self.table_name}(tema_sk);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_titulado ON {self.table_name}(titulado_sk);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_tempo ON {self.table_name}(tempo_sk);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_match ON {self.table_name}(match_status);
        """

        with db.engine.begin() as conn:
            conn.exec_driver_sql(ddl)
            data.to_sql(self.table_name, conn, if_exists="append", index=False, method="multi", chunksize=2000)

        self.logger.info("Tabela %s carregada com %s registros.", self.table_name, f"{len(data):,}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        renamed = {}
        for column in df.columns:
            new_name = column.strip().lower()
            new_name = new_name.replace(" ", "_").replace("-", "_")
            new_name = new_name.replace("__", "_")
            renamed[column] = new_name
        return df.rename(columns=renamed)

    def _resolve_parquet_path(self) -> Path:
        if self.parquet_path:
            path = self.parquet_path if isinstance(self.parquet_path, Path) else Path(self.parquet_path)
            if not path.exists():
                raise FileNotFoundError(f"Arquivo parquet informado não encontrado: {path}")
            return path

        default_path = PROJECT_ROOT / "staging" / "data" / "mapeamentos_titulados_2023.parquet"
        if default_path.exists():
            return default_path
        raise FileNotFoundError("Não foi possível localizar mapeamentos_titulados_2023.parquet.")

    def _load_dimensions(self) -> Dict[str, pd.DataFrame]:
        if self._dimension_cache:
            return self._dimension_cache

        db = self.get_db_manager()
        tables = {
            "dim_tema": self._safe_query(db, "dim_tema"),
            "dim_tempo": self._safe_query(db, "dim_tempo"),
            "dim_titulado": self._safe_query(db, "dim_titulado"),
        }
        self._dimension_cache = tables
        return tables

    def _safe_query(self, db, table: str) -> pd.DataFrame:
        try:
            if not db.table_exists(table):
                self.logger.warning("Tabela %s não encontrada; mapeamentos ficarão zerados.", table)
                return pd.DataFrame()
            return db.execute_query(f"SELECT * FROM {table}")
        except Exception as exc:  # pragma: no cover
            self.logger.warning("Falha ao carregar %s: %s", table, exc)
            return pd.DataFrame()

    def _map_tempo(self, df: pd.DataFrame, dims: Dict[str, pd.DataFrame]) -> pd.Series:
        tempo_df = dims.get("dim_tempo", pd.DataFrame())
        if tempo_df.empty or "ano" not in tempo_df.columns:
            return pd.Series(0, index=df.index)

        mapping = {int(row["ano"]): int(row["tempo_sk"]) for _, row in tempo_df.iterrows() if not pd.isna(row.get("ano"))}
        return df["ano_base"].map(mapping).fillna(0).astype(int)

    def _map_tema(self, df: pd.DataFrame, dims: Dict[str, pd.DataFrame]) -> pd.Series:
        tema_df = dims.get("dim_tema", pd.DataFrame())
        if tema_df.empty or "tema_id" not in tema_df.columns:
            return pd.Series(0, index=df.index)

        mapping = {int(row["tema_id"]): int(row["tema_sk"]) for _, row in tema_df.iterrows() if not pd.isna(row.get("tema_id"))}
        return df["tema_id"].map(mapping).fillna(0).astype(int)

    def _map_titulado(self, df: pd.DataFrame, dims: Dict[str, pd.DataFrame]):
        titulado_df = dims.get("dim_titulado", pd.DataFrame())
        if titulado_df.empty:
            return pd.Series(0, index=df.index), pd.Series("DIM_ABSENT", index=df.index)

        mapping_columns: Iterable[str] = (
            "id_discente_original",
            "id_pessoa",
            "id_lattes",
        )
        mapping: Dict[str, int] = {}
        for column in mapping_columns:
            if column not in titulado_df.columns:
                continue
            for value, sk in zip(titulado_df[column], titulado_df["titulado_sk"]):
                if pd.isna(value) or pd.isna(sk):
                    continue
                key = str(value).strip().upper()
                if key and key not in mapping:
                    mapping[key] = int(sk)

        matched = df["hash_id"].str.upper().map(mapping).fillna(0).astype(int)
        status = matched.apply(lambda sk: "MATCHED" if sk > 0 else "UNMATCHED")
        self.logger.info(
            "Titulado mapeado para %s de %s registros (%.2f%%).",
            f"{(matched > 0).sum():,}",
            f"{len(df):,}",
            ((matched > 0).sum() / max(len(df), 1)) * 100,
        )
        return matched, status

    def _log_summary(self, df: pd.DataFrame) -> None:
        total = len(df)
        if total == 0:
            return
        matched = (df["titulado_sk"] > 0).sum()
        temas = df["tema_sk"].gt(0).sum()
        self.logger.info(
            "Resumo fact_titulacao: linhas=%s | titulares_vinculados=%s | temas_mapeados=%s",
            f"{total:,}",
            f"{matched:,}",
            f"{temas:,}",
        )

    def _detect_dimensions(self, db) -> Dict[str, bool]:
        tables = ["dim_titulado", "dim_tema", "dim_tempo"]
        return {table: db.table_exists(table) for table in tables}

    # ------------------------------------------------------------------
    # CLI
    # ------------------------------------------------------------------

    @classmethod
    def cli(cls):  # pragma: no cover
        parser = argparse.ArgumentParser(description="Gera a tabela fact_titulacao.")
        parser.add_argument("--parquet", type=Path, default=None, help="Caminho para mapeamentos_titulados_2023.parquet")
        parser.add_argument("--ano-base", type=int, default=2023, help="Ano base utilizado para lookup em dim_tempo")
        parser.add_argument("--dry-run", action="store_true", help="Executa sem carregar no banco")
        parser.add_argument("--limit", type=int, default=None, help="Processa somente as primeiras N linhas")
        parser.add_argument("--no-load", action="store_true", help="Ignora etapa de carga no banco")

        args = parser.parse_args()
        instance = cls(parquet_path=args.parquet, ano_base=args.ano_base)
        instance.run(dry_run=args.dry_run, limit=args.limit, skip_load=args.no_load, ano_base=args.ano_base)


if __name__ == "__main__":  # pragma: no cover
    FactTitulacaoETL.cli()
