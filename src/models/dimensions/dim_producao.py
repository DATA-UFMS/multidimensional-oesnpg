#!/usr/bin/env python3
"""Pipeline padronizado da dimensão de produção intelectual (dim_producao)."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from dotenv import load_dotenv

# Garantir acesso ao projeto quando o script for executado diretamente.
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.etl_base import DimensionETL, ETLContext
from src.utils.naming_conventions import NamingConventions

load_dotenv()


class DimProducaoETL(DimensionETL):
    """Implementação padronizada para a dimensão de produção."""

    def __init__(self, table_name: str = "dim_producao") -> None:
        super().__init__(
            table_name=table_name,
            dimension_type="producao",
            name="DIM_PRODUCAO",
        )

    # ------------------------------------------------------------------
    # Ciclo ETL
    # ------------------------------------------------------------------

    def extract(self, context: ETLContext) -> pd.DataFrame:
        anos_filter = self._normalize_year_filters(context.extra.get("anos"))
        prefer_local = bool(context.extra.get("prefer_local"))

        loaders = [self._load_from_minio, self._load_from_local]
        if prefer_local:
            loaders.reverse()

        for loader in loaders:
            df = loader(anos_filter)
            if df is not None and not df.empty:
                self.logger.info(
                    "Fonte utilizada: %s | Registros: %s",
                    loader.__name__,
                    f"{len(df):,}",
                )
                return df

        raise FileNotFoundError(
            "Nenhum dataset de produção encontrado nas fontes configuradas."
        )

    def transform(self, data: pd.DataFrame, context: ETLContext) -> pd.DataFrame:
        if data.empty:
            self.logger.warning("Dataset de produção vazio; nada a transformar.")
            return data

        df = data.copy()

        rename_map = {
            "ID_ADD_PRODUCAO": "id_producao",
            "ID_ADD_PRODUCAO_INTELECTUAL": "id_producao",
            "ID_TIPO_PRODUCAO": "id_tipo_producao",
            "ID_SUBTIPO_PRODUCAO": "id_subtipo_producao",
            "NM_TIPO_PRODUCAO": "tipo_producao",
            "NM_SUBTIPO_PRODUCAO": "subtipo_producao",
            "DS_TITULO": "titulo_producao",
            "AN_BASE": "ano_producao",
            "AN_PRODUCAO": "ano_producao",
            "AN_BASE_PRODUCAO": "ano_base",
            "NM_PERIODICO": "nome_periodico",
            "DS_ISSN": "issn",
            "DS_ISBN": "isbn",
            "DS_DOI": "doi",
            "NM_EDITORA": "editora",
            "SG_PAIS_PUBLICACAO": "pais_publicacao",
            "NM_IDIOMA": "idioma",
            "DS_NATUREZA": "natureza_producao",
            "DS_MEIO_DIVULGACAO": "meio_divulgacao",
        }

        df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

        essential_defaults = {
            "id_producao": pd.NA,
            "tipo_producao": "NÃO INFORMADO",
            "subtipo_producao": "NÃO INFORMADO",
            "titulo_producao": "SEM TÍTULO",
            "ano_producao": 0,
            "ano_base": "0000",
        }

        for column, default in essential_defaults.items():
            if column not in df.columns:
                df[column] = default

        df["id_producao"] = df["id_producao"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
        before_filter = len(df)
        df = df[df["id_producao"].notna() & (df["id_producao"] != "")]
        removed = before_filter - len(df)
        if removed:
            self.logger.info("Registros descartados sem id_producao: %s", f"{removed:,}")

        df["tipo_producao"] = df["tipo_producao"].fillna("NÃO INFORMADO").astype(str).str[:100]
        df["subtipo_producao"] = df["subtipo_producao"].fillna("NÃO INFORMADO").astype(str).str[:100]
        df["titulo_producao"] = df["titulo_producao"].fillna("SEM TÍTULO").astype(str).str[:500]

        df["ano_producao"] = pd.to_numeric(df["ano_producao"], errors="coerce").fillna(0).astype("Int64")

        df["ano_base"] = pd.to_numeric(df["ano_base"], errors="coerce").fillna(0).astype(int).astype(str).str.zfill(4)

        text_specs: Sequence[Tuple[str, str, int]] = (
            ("nome_periodico", "NÃO INFORMADO", 300),
            ("issn", "", 20),
            ("isbn", "", 20),
            ("doi", "", 100),
            ("editora", "NÃO INFORMADO", 200),
            ("pais_publicacao", "BRA", 10),
            ("idioma", "PORTUGUÊS", 50),
            ("natureza_producao", "NÃO INFORMADO", 100),
            ("meio_divulgacao", "NÃO INFORMADO", 100),
        )

        for column, default, limit in text_specs:
            if column in df.columns:
                df[column] = df[column].fillna(default).astype(str).str.strip().str[:limit]

        df.sort_values(["ano_base", "ano_producao"], ascending=[False, False], inplace=True)
        df = df.drop_duplicates(subset=["id_producao"], keep="first").reset_index(drop=True)

        df.insert(0, "producao_sk", range(1, len(df) + 1))

        registro_zero = NamingConventions.get_standard_unknown_record("producao")
        registro_zero.setdefault("producao_sk", 0)
        registro_zero.setdefault("id_producao", "0")
        registro_zero.setdefault("tipo_producao", "NÃO INFORMADO")
        registro_zero.setdefault("subtipo_producao", "NÃO INFORMADO")
        registro_zero.setdefault("titulo_producao", "NÃO INFORMADO")
        registro_zero.setdefault("ano_producao", 0)
        registro_zero.setdefault("nome_periodico", "NÃO INFORMADO")
        registro_zero.setdefault("pais_publicacao", "BRA")
        registro_zero.setdefault("idioma", "NÃO INFORMADO")
        registro_zero.setdefault("natureza_producao", "NÃO INFORMADO")
        registro_zero.setdefault("meio_divulgacao", "NÃO INFORMADO")
        registro_zero.setdefault("ano_base", "0000")

        df_final = pd.concat([pd.DataFrame([registro_zero]), df], ignore_index=True)

        final_columns: List[str] = [
            "producao_sk",
            "id_producao",
            "tipo_producao",
            "subtipo_producao",
            "titulo_producao",
            "ano_producao",
            "nome_periodico",
            "issn",
            "isbn",
            "doi",
            "editora",
            "pais_publicacao",
            "idioma",
            "natureza_producao",
            "meio_divulgacao",
            "ano_base",
        ]

        for column in final_columns:
            if column not in df_final.columns:
                df_final[column] = pd.NA

        df_final = df_final[final_columns]

        df_final["producao_sk"] = pd.to_numeric(df_final["producao_sk"], errors="coerce").fillna(0).astype(int)
        df_final["ano_producao"] = pd.to_numeric(df_final["ano_producao"], errors="coerce").fillna(0).astype("Int64")
        df_final["ano_base"] = df_final["ano_base"].astype(str).str.zfill(4)

        self._log_summary(df_final)
        return df_final

    def load(self, data: pd.DataFrame, context: ETLContext) -> None:
        if data.empty:
            self.logger.warning("DataFrame vazio recebido; carga não executada.")
            return

        db = self.get_db_manager()

        ddl = f"""
        CREATE TABLE {self.table_name} (
            producao_sk INTEGER PRIMARY KEY,
            id_producao VARCHAR(50) NOT NULL,
            tipo_producao VARCHAR(100) NOT NULL,
            subtipo_producao VARCHAR(100),
            titulo_producao VARCHAR(500),
            ano_producao INTEGER,
            nome_periodico VARCHAR(300),
            issn VARCHAR(20),
            isbn VARCHAR(20),
            doi VARCHAR(100),
            editora VARCHAR(200),
            pais_publicacao VARCHAR(10),
            idioma VARCHAR(50),
            natureza_producao VARCHAR(100),
            meio_divulgacao VARCHAR(100),
            ano_base VARCHAR(4) NOT NULL,
            data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_dim_producao_id UNIQUE (id_producao)
        );
        """

        index_statements = [
            f"CREATE INDEX idx_{self.table_name}_tipo ON {self.table_name}(tipo_producao);",
            f"CREATE INDEX idx_{self.table_name}_ano ON {self.table_name}(ano_producao);",
            f"CREATE INDEX idx_{self.table_name}_ano_base ON {self.table_name}(ano_base);",
        ]

        with db.engine.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {self.table_name} CASCADE;")
            conn.exec_driver_sql(ddl)
            data.to_sql(self.table_name, conn, if_exists="append", index=False, method="multi", chunksize=1000)
            for statement in index_statements:
                conn.exec_driver_sql(statement)

        self.logger.info(
            "Dimensão %s carregada com %s registros.",
            self.table_name,
            f"{len(data):,}",
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _normalize_year_filters(self, value: Optional[Iterable[str]]) -> Optional[List[str]]:
        if value is None:
            env_years = os.getenv("DIM_PRODUCAO_ANOS")
            if env_years:
                value = [item.strip() for item in env_years.split(",") if item.strip()]

        if value is None:
            return None

        if isinstance(value, (str, int)):
            normalized = {str(value).strip()}
        else:
            normalized = {str(item).strip() for item in value if str(item).strip()}

        return sorted(normalized) if normalized else None

    def _extract_year_from_name(self, name: str) -> Optional[str]:
        parts = name.replace(".parquet", "").split("_")
        for part in reversed(parts):
            if part.isdigit() and len(part) == 4:
                return part
        return None

    def _load_from_minio(self, anos: Optional[Sequence[str]]) -> Optional[pd.DataFrame]:
        endpoint = os.getenv("MINIO_ENDPOINT")
        bucket = os.getenv("MINIO_BUCKET")
        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")
        prefix = os.getenv("MINIO_PRODUCAO_PREFIX", "add_capes")

        if not all([endpoint, bucket, access_key, secret_key]):
            self.logger.info("Variáveis do MinIO não configuradas; ignorando essa fonte.")
            return None

        try:
            import s3fs
        except ImportError:
            self.logger.warning("Biblioteca s3fs ausente; não foi possível acessar o MinIO.")
            return None

        endpoint_str = str(endpoint)
        endpoint_url = endpoint_str if endpoint_str.startswith("http") else f"http://{endpoint_str}"

        fs = s3fs.S3FileSystem(
            key=str(access_key),
            secret=str(secret_key),
            client_kwargs={"endpoint_url": endpoint_url},
        )

        try:
            files = fs.glob(f"{bucket}/{prefix}/add_producao_*.parquet")
        except Exception as exc:  # pragma: no cover - falha externa
            self.logger.warning("Falha ao listar arquivos no MinIO: %s", exc)
            return None

        selected: List[Tuple[str, Optional[str]]] = []
        for file_path in sorted(str(path) for path in files):
            name = os.path.basename(file_path)
            if "_autor" in name:
                continue
            year = self._extract_year_from_name(name)
            if anos and year not in anos:
                continue
            selected.append((file_path, year))

        if not selected:
            return None

        storage_options = {
            "key": str(access_key),
            "secret": str(secret_key),
            "client_kwargs": {"endpoint_url": endpoint_url},
        }
        frames: List[pd.DataFrame] = []

        for file_path, year in selected:
            try:
                self.logger.info("Carregando %s via MinIO", file_path)
                df = pd.read_parquet(f"s3://{file_path}", storage_options=storage_options)
                if "ano_base" not in df.columns and year:
                    df["ano_base"] = year
                frames.append(df)
            except Exception as exc:  # pragma: no cover - falha externa
                self.logger.warning("Falha ao ler %s: %s", file_path, exc)

        if not frames:
            return None

        return pd.concat(frames, ignore_index=True)

    def _load_from_local(self, anos: Optional[Sequence[str]]) -> Optional[pd.DataFrame]:
        candidate_dirs = [
            PROJECT_ROOT / "data" / "raw_producao",
            PROJECT_ROOT / "staging" / "data",
            PROJECT_ROOT / "parquet_output",
        ]

        for base_dir in candidate_dirs:
            if not base_dir.exists():
                continue

            frames: List[pd.DataFrame] = []
            for path in sorted(base_dir.glob("add_producao_*.parquet")):
                if "_autor" in path.name:
                    continue
                year = self._extract_year_from_name(path.name)
                if anos and year not in anos:
                    continue
                try:
                    self.logger.info("Carregando %s", path)
                    df = pd.read_parquet(path)
                    if "ano_base" not in df.columns and year:
                        df["ano_base"] = year
                    frames.append(df)
                except Exception as exc:  # pragma: no cover - falha externa
                    self.logger.warning("Falha ao ler %s: %s", path, exc)

            if frames:
                return pd.concat(frames, ignore_index=True)

        return None

    def _log_summary(self, df: pd.DataFrame) -> None:
        if df.empty:
            return

        efetivos = df[df["producao_sk"] != 0]
        if efetivos.empty:
            return

        self.logger.info(
            "Resumo: produções=%s | tipos=%s | subtipos=%s | anos_base=%s",
            f"{len(efetivos):,}",
            efetivos["tipo_producao"].nunique(dropna=True),
            efetivos["subtipo_producao"].nunique(dropna=True),
            efetivos["ano_base"].nunique(dropna=True),
        )

        if "ano_producao" in efetivos.columns:
            ano_min = efetivos["ano_producao"].min(skipna=True)
            ano_max = efetivos["ano_producao"].max(skipna=True)
            self.logger.info("Período produzido: %s - %s", ano_min, ano_max)

    # ------------------------------------------------------------------
    # CLI
    # ------------------------------------------------------------------

    @classmethod
    def cli(cls) -> None:  # pragma: no cover - interface CLI
        parser = argparse.ArgumentParser(description="Executa pipeline DIM_PRODUCAO")
        parser.add_argument("--dry-run", action="store_true", help="Executa sem carregar no banco")
        parser.add_argument("--limit", type=int, default=None, help="Processa apenas as primeiras N linhas")
        parser.add_argument("--no-load", action="store_true", help="Pula etapa de carga")
        parser.add_argument(
            "--if-exists",
            choices=["fail", "replace", "append"],
            default=None,
            help="Modo de escrita ao persistir a dimensão",
        )
        parser.add_argument(
            "--anos",
            nargs="+",
            help="Filtra arquivos por ano base (ex.: --anos 2023 2024)",
        )
        parser.add_argument(
            "--prefer-local",
            action="store_true",
            help="Prioriza leitura local antes do MinIO",
        )

        args = parser.parse_args()
        instance = cls()
        if args.if_exists:
            instance.if_exists = args.if_exists

        extra = {}
        if args.anos:
            extra["anos"] = args.anos
        if args.prefer_local:
            extra["prefer_local"] = True

        instance.run(
            dry_run=args.dry_run,
            limit=args.limit,
            skip_load=args.no_load,
            **extra,
        )


if __name__ == "__main__":  # pragma: no cover - execução direta
    DimProducaoETL.cli()
