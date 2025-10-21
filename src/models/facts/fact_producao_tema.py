#!/usr/bin/env python3
"""
Fato Produção x Tema (fact_producao_tema)
----------------------------------------

Tabela fato padronizada que relaciona produções intelectuais com temas,
programas (PPG), IES, autores (docentes/discentes/titulados/pós-docs),
período (dim_tempo) e, quando disponível, associações com ODS via
fact_tema_ods. Foi concebida para permitir análises cruzadas entre temas
e diferentes eixos do DW OESNPG.
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd

# Garantir que o diretório raiz esteja no PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.etl_base import ETLContext, FactETL

# --------------------------------------------------------------------------- #
# Helpers de padronização
# --------------------------------------------------------------------------- #


def normalize_column_name(name: str) -> str:
    """Converte nomes de colunas para snake_case ASCII."""
    ascii_name = (
        unicodedata.normalize("NFKD", name)
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    ascii_name = re.sub(r"[^0-9a-zA-Z]+", "_", ascii_name).strip("_")
    ascii_name = re.sub(r"__+", "_", ascii_name)
    return ascii_name.lower()


def clean_identifier(value: object) -> str:
    """Normaliza identificadores numéricos/textuais mantendo apenas dígitos/letras."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none"}:
        return ""
    text = text.replace(".0", "")  # remove sufixo comum de floats
    return text.upper()


def build_mapping(df: pd.DataFrame, key_col: str, value_col: str) -> Dict[str, int]:
    """Cria dicionário {chave -> surrogate key} a partir de um DataFrame."""
    if key_col not in df.columns or value_col not in df.columns:
        return {}
    mapping: Dict[str, int] = {}
    for key, value in zip(df[key_col], df[value_col]):
        if pd.isna(key) or pd.isna(value):
            continue
        key_norm = clean_identifier(key)
        if key_norm:
            mapping[key_norm] = int(value)
    return mapping


def first_available(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    """Retorna o primeiro nome de coluna existente entre os candidatos."""
    for col in candidates:
        if col in df.columns:
            return col
    return None


def ensure_int(series: pd.Series, default: int = 0) -> pd.Series:
    """Converte série para inteiro (fallback default)."""
    return pd.to_numeric(series, errors="coerce").fillna(default).astype(int)


# --------------------------------------------------------------------------- #
# Implementação do ETL
# --------------------------------------------------------------------------- #


class FactProducaoTemaETL(FactETL):
    """Pipeline padronizado da tabela fact_producao_tema."""

    COLUMN_ALIASES: Dict[str, Tuple[str, ...]] = {
        "producao_id": (
            "id_add_producao_intelectual",
            "id_producao",
            "id_producao_autor",
        ),
        "ano_base": ("an_base_producao", "ano_base", "an_base"),
        "codigo_programa": ("cd_programa", "cd_programa_ies", "codigo_programa"),
        "codigo_ies": ("cd_entidade_capes", "codigo_capes_da_ies", "codigo_ies"),
        "sigla_ies": ("sg_ies", "sigla_ies"),
        "tema_id": ("tema_id", "id_tema", "tema", "tema_codigo"),
        "macrotema_nome": ("macrotema_nome", "macro_tema", "macrotema"),
        "tema_nome": ("tema_nome", "tema", "nm_tema"),
        "palavra_chave": (
            "palavra_chave",
            "palavrachave_nome",
            "palavras_chave",
            "keyword",
        ),
        "tipo_autor": ("tp_autor", "tipo_autor"),
        "ordem_autor": ("nr_ordem", "ordem_autor"),
        "docente_id": ("id_pessoa_docente", "id_docente", "docente_id"),
        "discente_id": ("id_pessoa_discente", "id_discente"),
        "titulado_id": ("id_pessoa_egresso", "id_pessoa_titulado"),
        "posdoc_id": ("id_pessoa_pos_doc", "id_posdoc"),
    }

    def __init__(
        self,
        *,
        parquet_path: Optional[Path] = None,
        table_name: str = "fact_producao_tema",
    ) -> None:
        super().__init__(table_name=table_name, name="FACT_PRODUCAO_TEMA", if_exists="replace")
        self.parquet_path = parquet_path
        self.project_root = PROJECT_ROOT
        self._dimension_cache: Dict[str, pd.DataFrame] = {}
        self._dimension_warned: set[str] = set()

    # ------------------------------------------------------------------ #
    # Etapas do pipeline
    # ------------------------------------------------------------------ #

    def extract(self, context: ETLContext) -> pd.DataFrame:
        parquet_file = self._resolve_parquet_path()
        self.logger.info("Lendo parquet de produção/autoria: %s", parquet_file)
        df = pd.read_parquet(parquet_file)
        self.logger.info("Registros carregados do parquet: %s", f"{len(df):,}")
        df.columns = [normalize_column_name(col) for col in df.columns]
        return df

    def transform(self, data: pd.DataFrame, context: ETLContext) -> pd.DataFrame:
        if data.empty:
            self.logger.warning("Dataset de origem está vazio; nada para transformar.")
            return data

        df = data.copy()
        self._assign_aliases(df)
        self._attach_dimension_keys(df)
        df_fact = self._build_fact_dataframe(df)
        df_fact = self._attach_ods(df_fact)
        df_fact = df_fact.drop_duplicates()
        self.logger.info("Fato produzido com %s linhas após deduplicação.", f"{len(df_fact):,}")
        return df_fact

    def load(self, data: pd.DataFrame, context: ETLContext) -> None:
        if data.empty:
            self.logger.warning("DataFrame vazio; carga ignorada.")
            return

        db = self.get_db_manager()
        if self.if_exists == "replace":
            self._drop_table(db)
            self._create_table(db)
        else:
            self._create_table(db, if_not_exists=True)

        self.logger.info("Carregando DataFrame em %s", self.table_name)
        data.to_sql(
            self.table_name,
            db.engine,
            if_exists="append",
            index=False,
            chunksize=2000,
            method="multi",
        )
        self.logger.info("Carga concluída (%s registros).", f"{len(data):,}")

    # ------------------------------------------------------------------ #
    # Preparação e mapeamentos
    # ------------------------------------------------------------------ #

    def _assign_aliases(self, df: pd.DataFrame) -> None:
        """Cria colunas padronizadas com base nos aliases conhecidos."""
        for target, candidates in self.COLUMN_ALIASES.items():
            col = first_available(df, candidates)
            if col:
                df[target] = df[col]
            else:
                df[target] = None

        # Metadados auxiliares
        df["fonte_dados"] = df.get("fonte_dados", "add_producao_autor")

    def _attach_dimension_keys(self, df: pd.DataFrame) -> None:
        """Mapeia chaves de negócio para surrogate keys das dimensões."""
        dims = self._load_dimensions()

        df["tempo_sk"] = self._map_dimension(df, dims, column="ano_base", dim="tempo", lookup_columns=["ano"])
        df["tema_sk"] = self._map_dimension(
            df,
            dims,
            column_candidates=["tema_id", "palavra_chave"],
            dim="tema",
            lookup_columns=["tema_id", "palavrachave_id", "palavra_chave"],
        )
        df["ppg_sk"] = self._map_dimension(
            df,
            dims,
            column_candidates=["codigo_programa"],
            dim="ppg",
            lookup_columns=["codigo_programa", "codigo_do_ppg"],
        )
        df["ies_sk"] = self._map_dimension(
            df,
            dims,
            column_candidates=["codigo_ies", "sigla_ies"],
            dim="ies",
            lookup_columns=["codigo_ies", "sigla_ies", "codigo_capes_da_ies"],
        )
        df["docente_sk"] = self._map_dimension(
            df,
            dims,
            column_candidates=["docente_id"],
            dim="docente",
            lookup_columns=["id_pessoa", "id_docente"],
        )
        df["discente_sk"] = self._map_dimension(
            df,
            dims,
            column_candidates=["discente_id"],
            dim="discente",
            lookup_columns=["id_pessoa", "id_discente"],
        )
        df["titulado_sk"] = self._map_dimension(
            df,
            dims,
            column_candidates=["titulado_id"],
            dim="titulado",
            lookup_columns=["id_pessoa"],
        )
        df["posdoc_sk"] = self._map_dimension(
            df,
            dims,
            column_candidates=["posdoc_id"],
            dim="posdoc",
            lookup_columns=["id_pessoa"],
        )

    def _build_fact_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Seleciona colunas finais e aplica tipagem/padrões."""
        df_fact = pd.DataFrame(
            {
                "producao_id": ensure_int(df["producao_id"]),
                "tempo_sk": ensure_int(df["tempo_sk"]),
                "tema_sk": ensure_int(df["tema_sk"]),
                "ppg_sk": ensure_int(df["ppg_sk"]),
                "ies_sk": ensure_int(df["ies_sk"]),
                "docente_sk": ensure_int(df["docente_sk"]),
                "discente_sk": ensure_int(df["discente_sk"]),
                "titulado_sk": ensure_int(df["titulado_sk"]),
                "posdoc_sk": ensure_int(df["posdoc_sk"]),
                "tipo_autor": df.get("tipo_autor", "NÃO INFORMADO").fillna("NÃO INFORMADO"),
                "ordem_autor": ensure_int(df.get("ordem_autor", 0)),
                "qtd_autores": 1,
                "qtd_producoes": 1,
                "ano_base": ensure_int(df.get("ano_base", 0)),
                "fonte_dados": df.get("fonte_dados", "add_producao_autor").fillna("add_producao_autor"),
            }
        )

        if df_fact["producao_id"].eq(0).all():
            self.logger.warning(
                "Nenhum identificador de produção foi mapeado. "
                "Verifique se o parquet possui coluna equivalente a ID_ADD_PRODUCAO_INTELECTUAL."
            )

        # Garantir ranges mínimos (SK >= 0)
        for column in [
            "tempo_sk",
            "tema_sk",
            "ppg_sk",
            "ies_sk",
            "docente_sk",
            "discente_sk",
            "titulado_sk",
            "posdoc_sk",
        ]:
            df_fact[column] = df_fact[column].clip(lower=0)

        return df_fact

    def _attach_ods(self, df: pd.DataFrame) -> pd.DataFrame:
        """Anexa, quando disponível, as associações tema -> ODS."""
        dims = self._load_dimensions()
        ods_df = dims.get("fact_tema_ods")
        if ods_df is None or ods_df.empty:
            df["ods_sk"] = 0
            return df

        ods_df = ods_df[["tema_sk", "ods_sk"]].drop_duplicates()
        merged = df.merge(ods_df, on="tema_sk", how="left")
        merged["ods_sk"] = merged["ods_sk"].fillna(0).astype(int)
        return merged

    # ------------------------------------------------------------------ #
    # Suporte a dimensões
    # ------------------------------------------------------------------ #

    def _load_dimensions(self) -> Dict[str, pd.DataFrame]:
        """Carrega e cacheia DataFrames de dimensões relevantes."""
        if self._dimension_cache:
            return self._dimension_cache

        db = self.get_db_manager()

        def fetch(table: str) -> Optional[pd.DataFrame]:
            try:
                if not db.table_exists(table):
                    self.logger.warning("Dimensão %s não encontrada.", table)
                    return None
                return db.execute_query(f"SELECT * FROM {table}")
            except Exception as exc:  # pylint: disable=broad-except
                self.logger.warning("Falha ao carregar %s: %s", table, exc)
                return None

        tables = {
            "tempo": fetch("dim_tempo"),
            "tema": fetch("dim_tema"),
            "ppg": fetch("dim_ppg"),
            "ies": fetch("dim_ies"),
            "docente": fetch("dim_docente"),
            "discente": fetch("dim_discente"),
            "titulado": fetch("dim_titulado"),
            "posdoc": fetch("dim_posdoc"),
            "fact_tema_ods": fetch("fact_tema_ods"),
        }

        for key, df in tables.items():
            if df is not None:
                tables[key] = self._standardize_sk(df, key)
            else:
                tables[key] = pd.DataFrame()

        self._dimension_cache = tables
        return self._dimension_cache

    @staticmethod
    def _standardize_sk(df: pd.DataFrame, dim: str) -> pd.DataFrame:
        """Garante que a coluna da SK possua nome consistente."""
        if df.empty:
            return df

        expected_sk = f"{dim}_sk" if dim not in {"fact_tema_ods"} else "tema_sk"
        if expected_sk in df.columns:
            return df

        candidates = [col for col in df.columns if col.endswith("_sk")] + ["sk", "id"]
        for candidate in candidates:
            if candidate in df.columns:
                df = df.rename(columns={candidate: expected_sk})
                return df
        return df

    def _map_dimension(
        self,
        df: pd.DataFrame,
        dims: Dict[str, pd.DataFrame],
        *,
        column: Optional[str] = None,
        column_candidates: Optional[Iterable[str]] = None,
        dim: str,
        lookup_columns: Iterable[str],
    ) -> pd.Series:
        dim_df = dims.get(dim)
        if dim_df is None or dim_df.empty:
            return pd.Series(0, index=df.index)

        target_column = column or first_available(df, column_candidates or [])
        if not target_column:
            return pd.Series(0, index=df.index)

        df_values = df[target_column].copy()
        df_values = df_values.fillna("").astype(str).apply(clean_identifier)

        for lookup in lookup_columns:
            if lookup not in dim_df.columns:
                continue
            mapping = build_mapping(dim_df, lookup, f"{dim}_sk" if dim != "fact_tema_ods" else "tema_sk")
            if not mapping:
                continue
            mapped = df_values.map(mapping).fillna(0).astype(int)
            if mapped.sum() > 0:
                return mapped

        if dim not in self._dimension_warned:
            self.logger.warning("Não foi possível mapear chaves para dimensão %s (coluna base: %s).", dim, target_column)
            self._dimension_warned.add(dim)
        return pd.Series(0, index=df.index)

    # ------------------------------------------------------------------ #
    # DDL da tabela fato
    # ------------------------------------------------------------------ #

    def _drop_table(self, db) -> None:
        db.execute_sql(f"DROP TABLE IF EXISTS {self.table_name} CASCADE;")

    def _create_table(self, db, *, if_not_exists: bool = False) -> None:
        exists = db.table_exists(self.table_name)
        if if_not_exists and exists:
            return

        dims_available = self._detect_dimensions(db)
        fk_clauses = []
        if dims_available.get("dim_tempo"):
            fk_clauses.append("CONSTRAINT fk_producao_tema_tempo FOREIGN KEY (tempo_sk) REFERENCES dim_tempo(tempo_sk)")
        if dims_available.get("dim_tema"):
            fk_clauses.append("CONSTRAINT fk_producao_tema_tema FOREIGN KEY (tema_sk) REFERENCES dim_tema(tema_sk)")
        if dims_available.get("dim_ppg"):
            fk_clauses.append("CONSTRAINT fk_producao_tema_ppg FOREIGN KEY (ppg_sk) REFERENCES dim_ppg(ppg_sk)")
        if dims_available.get("dim_ies"):
            fk_clauses.append("CONSTRAINT fk_producao_tema_ies FOREIGN KEY (ies_sk) REFERENCES dim_ies(ies_sk)")
        if dims_available.get("dim_docente"):
            fk_clauses.append("CONSTRAINT fk_producao_tema_docente FOREIGN KEY (docente_sk) REFERENCES dim_docente(docente_sk)")
        if dims_available.get("dim_discente"):
            fk_clauses.append("CONSTRAINT fk_producao_tema_discente FOREIGN KEY (discente_sk) REFERENCES dim_discente(discente_sk)")
        if dims_available.get("dim_titulado"):
            fk_clauses.append("CONSTRAINT fk_producao_tema_titulado FOREIGN KEY (titulado_sk) REFERENCES dim_titulado(titulado_sk)")
        if dims_available.get("dim_posdoc"):
            fk_clauses.append("CONSTRAINT fk_producao_tema_posdoc FOREIGN KEY (posdoc_sk) REFERENCES dim_posdoc(posdoc_sk)")
        if dims_available.get("dim_ods"):
            fk_clauses.append("CONSTRAINT fk_producao_tema_ods FOREIGN KEY (ods_sk) REFERENCES dim_ods(ods_sk)")

        fk_sql = ",\n        ".join(fk_clauses)
        fk_sql = f",\n        {fk_sql}" if fk_sql else ""

        create_sql = f"""
        CREATE TABLE {"IF NOT EXISTS " if if_not_exists else ""}{self.table_name} (
            producao_tema_id SERIAL PRIMARY KEY,
            producao_id BIGINT NOT NULL,
            tempo_sk INTEGER NOT NULL DEFAULT 0,
            tema_sk INTEGER NOT NULL DEFAULT 0,
            ods_sk INTEGER NOT NULL DEFAULT 0,
            ppg_sk INTEGER NOT NULL DEFAULT 0,
            ies_sk INTEGER NOT NULL DEFAULT 0,
            docente_sk INTEGER NOT NULL DEFAULT 0,
            discente_sk INTEGER NOT NULL DEFAULT 0,
            titulado_sk INTEGER NOT NULL DEFAULT 0,
            posdoc_sk INTEGER NOT NULL DEFAULT 0,
            tipo_autor VARCHAR(100) DEFAULT 'NÃO INFORMADO',
            ordem_autor INTEGER DEFAULT 0,
            qtd_autores INTEGER DEFAULT 1,
            qtd_producoes INTEGER DEFAULT 1,
            ano_base INTEGER,
            fonte_dados VARCHAR(60) DEFAULT 'add_producao_autor',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            {fk_sql}
        );
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_tema ON {self.table_name}(tema_sk);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_ppg ON {self.table_name}(ppg_sk);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_ies ON {self.table_name}(ies_sk);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_tempo ON {self.table_name}(tempo_sk);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_ods ON {self.table_name}(ods_sk);
        CREATE UNIQUE INDEX IF NOT EXISTS ux_{self.table_name}_grain ON {self.table_name}(
            producao_id, tema_sk, docente_sk, discente_sk, titulado_sk, posdoc_sk, ppg_sk, ies_sk, ordem_autor, tipo_autor, ano_base
        );
        """
        db.execute_sql(create_sql)

    def _detect_dimensions(self, db) -> Dict[str, bool]:
        tables = [
            "dim_tempo",
            "dim_tema",
            "dim_ppg",
            "dim_ies",
            "dim_docente",
            "dim_discente",
            "dim_titulado",
            "dim_posdoc",
            "dim_ods",
        ]
        return {table: db.table_exists(table) for table in tables}

    # ------------------------------------------------------------------ #
    # Resolução de caminhos e CLI
    # ------------------------------------------------------------------ #

    def _resolve_parquet_path(self) -> Path:
        if self.parquet_path:
            path = self.parquet_path if isinstance(self.parquet_path, Path) else Path(self.parquet_path)
            if not path.exists():
                raise FileNotFoundError(f"Arquivo Parquet informado não encontrado: {path}")
            return path

        staging_dir = self.project_root / "staging" / "data"
        patterns = [
            staging_dir / "add_producao_autor.parquet",
            staging_dir / "add_autor_producao.parquet",
        ]
        patterns.extend(staging_dir.glob("add*producao*autor*.parquet"))

        for candidate in patterns:
            if candidate.exists():
                return candidate

        raise FileNotFoundError(
            "Não foi possível localizar o Parquet add_producao_autor/add_autor_producao. "
            "Informe o caminho manualmente com --parquet."
        )

    @classmethod
    def cli(cls):
        parser = argparse.ArgumentParser(description="Gera a tabela fact_producao_tema padronizada.")
        parser.add_argument("--parquet", type=Path, default=None, help="Caminho para o parquet add_producao_autor/add_autor_producao.")
        parser.add_argument("--dry-run", action="store_true", help="Executa sem carregar no banco.")
        parser.add_argument("--limit", type=int, default=None, help="Processa somente as primeiras N linhas (debug).")
        parser.add_argument("--no-load", action="store_true", help="Ignora etapa de carga no banco.")
        parser.add_argument("--if-exists", choices=["replace", "append"], default="replace", help="Comportamento de carga ao existir tabela destino.")

        args = parser.parse_args()
        instance = cls(parquet_path=args.parquet)
        instance.if_exists = args.if_exists
        instance.run(dry_run=args.dry_run, limit=args.limit, skip_load=args.no_load)


if __name__ == "__main__":
    FactProducaoTemaETL.cli()
