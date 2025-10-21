#!/usr/bin/env python3
"""
Camada RAW: Docentes CAPES
-------------------------

Pipeline padronizado responsável por consolidar arquivos brutos de docentes,
normalizar colunas e carregar o resultado na tabela ``raw_docente``.
"""

from __future__ import annotations

import argparse
import glob
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# Garantir que o diretório raiz esteja no PYTHONPATH (execução via CLI)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.etl_base import RawETL, ETLContext

# --------------------------------------------------------------------------- #
# Constantes e metadados
# --------------------------------------------------------------------------- #

DEFAULT_TABLE = "raw_docente"
DEFAULT_PRIORITY_COLS = [
    "id_pessoa",
    "nm_docente",
    "ano_base",
    "nm_entidade_ensino",
    "sg_uf_programa",
    "ds_categoria_docente",
    "nm_programa_ies",
    "nm_area_avaliacao",
    "in_doutor",
    "fonte_arquivo",
    "created_at",
]


# --------------------------------------------------------------------------- #
# Funções utilitárias herdadas do script original
# --------------------------------------------------------------------------- #

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nomes das colunas para padrão snake_case."""
    rename_map: Dict[str, str] = {
        "AN_BASE": "ano_base",
        "CD_AREA_AVALIACAO": "cd_area_avaliacao",
        "NM_AREA_AVALIACAO": "nm_area_avaliacao",
        "NM_GRANDE_AREA_CONHECIMENTO": "nm_grande_area_conhecimento",
        "NM_AREA_CONHECIMENTO": "nm_area_conhecimento",
        "CD_PROGRAMA_IES": "cd_programa_ies",
        "NM_PROGRAMA_IES": "nm_programa_ies",
        "NM_GRAU_PROGRAMA": "nm_grau_programa",
        "NM_MODALIDADE_PROGRAMA": "nm_modalidade_programa",
        "CD_CONCEITO_PROGRAMA": "cd_conceito_programa",
        "CD_ENTIDADE_CAPES": "cd_entidade_capes",
        "CD_ENTIDADE_EMEC": "cd_entidade_emec",
        "SG_ENTIDADE_ENSINO": "sg_entidade_ensino",
        "NM_ENTIDADE_ENSINO": "nm_entidade_ensino",
        "DS_DEPENDENCIA_ADMINISTRATIVA": "ds_dependencia_administrativa",
        "CS_STATUS_JURIDICO": "cs_status_juridico",
        "NM_MUNICIPIO_PROGRAMA_IES": "nm_municipio_programa_ies",
        "SG_UF_PROGRAMA": "sg_uf_programa",
        "NM_REGIAO": "nm_regiao",
        "ID_PESSOA": "id_pessoa",
        "TP_DOCUMENTO_DOCENTE": "tp_documento_docente",
        "NR_DOCUMENTO_DOCENTE": "nr_documento_docente",
        "NM_DOCENTE": "nm_docente",
        "AN_NASCIMENTO_DOCENTE": "an_nascimento_docente",
        "DS_FAIXA_ETARIA": "ds_faixa_etaria",
        "DS_TIPO_NACIONALIDADE_DOCENTE": "ds_tipo_nacionalidade_docente",
        "NM_PAIS_NACIONALIDADE_DOCENTE": "nm_pais_nacionalidade_docente",
        "DS_CATEGORIA_DOCENTE": "ds_categoria_docente",
        "DS_TIPO_VINCULO_DOCENTE_IES": "ds_tipo_vinculo_docente_ies",
        "DS_REGIME_TRABALHO": "ds_regime_trabalho",
        "CD_CAT_BOLSA_PRODUTIVIDADE": "cd_cat_bolsa_produtividade",
        "IN_DOUTOR": "in_doutor",
        "AN_TITULACAO": "an_titulacao",
        "NM_GRAU_TITULACAO": "nm_grau_titulacao",
        "CD_AREA_BASICA_TITULACAO": "cd_area_basica_titulacao",
        "NM_AREA_BASICA_TITULACAO": "nm_area_basica_titulacao",
        "SG_IES_TITULACAO": "sg_ies_titulacao",
        "NM_IES_TITULACAO": "nm_ies_titulacao",
        "NM_PAIS_IES_TITULACAO": "nm_pais_ies_titulacao",
        "ID_ADD_FOTO_PROGRAMA": "id_add_foto_programa",
        "ID_ADD_FOTO_PROGRAMA_IES": "id_add_foto_programa_ies",
    }
    return df.rename(columns=rename_map)


def load_and_consolidate_docente_files(data_dir: Path) -> pd.DataFrame:
    """Carrega todos os arquivos CSV de docentes presentes no diretório."""
    pattern = str(data_dir / "br-capes-colsucup-docente-*.csv")
    docente_files = sorted(glob.glob(pattern))

    if not docente_files:
        raise FileNotFoundError(f"Nenhum arquivo de docente encontrado em {data_dir}")

    print(f"📁 Encontrados {len(docente_files)} arquivos de docentes:")
    for file_path in docente_files:
        print(f"   • {Path(file_path).name}")

    frames: List[pd.DataFrame] = []
    total_records = 0

    for file_path in docente_files:
        file_name = Path(file_path).name
        print(f"📥 Processando {file_name}...")

        try:
            try:
                df = pd.read_csv(file_path, encoding="latin-1", sep=";", dtype=str)
            except Exception:
                print("   ⚠️  Tentando com engine='python'...")
                df = pd.read_csv(file_path, encoding="latin-1", sep=";", dtype=str, engine="python")

            df = normalize_column_names(df)
            df["fonte_arquivo"] = file_name
            df["created_at"] = pd.Timestamp.now().normalize()

            print(f"   ✔ {len(df):,} registros carregados")
            frames.append(df)
            total_records += len(df)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"   ❌ Erro ao processar {file_name}: {exc}")

    if not frames:
        raise ValueError("Nenhum arquivo foi processado com sucesso")

    print(f"\n🔄 Consolidando {total_records:,} registros de {len(frames)} arquivos...")
    return pd.concat(frames, ignore_index=True)


def clean_and_deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Executa rotinas de limpeza, tipagem e deduplicação."""
    print("🧹 Limpando dados...")

    for col in df.columns:
        if df[col].dtype == object and col not in ["fonte_arquivo", "created_at"]:
            df[col] = df[col].fillna("").astype(str).str.strip()

    numeric_cols = [
        "ano_base",
        "id_pessoa",
        "an_nascimento_docente",
        "an_titulacao",
        "cd_area_avaliacao",
        "cd_programa_ies",
        "cd_conceito_programa",
        "cd_entidade_capes",
        "cd_entidade_emec",
        "cd_area_basica_titulacao",
        "id_add_foto_programa",
        "id_add_foto_programa_ies",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    text_normalize_cols = [
        "sg_entidade_ensino",
        "sg_uf_programa",
        "tp_documento_docente",
        "ds_categoria_docente",
        "in_doutor",
        "sg_ies_titulacao",
    ]
    for col in text_normalize_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper()

    print(f"   ✔ Dados limpos: {len(df):,} registros")

    dedup_keys = ["id_pessoa", "ano_base", "cd_programa_ies"]
    duplicates_before = len(df)
    df_dedup = df.drop_duplicates(subset=[k for k in dedup_keys if k in df.columns], keep="last")
    duplicates_removed = duplicates_before - len(df_dedup)

    print(f"🔍 Duplicatas removidas: {duplicates_removed:,}")
    print(f"   ✔ Registros únicos mantidos: {len(df_dedup):,}")
    return df_dedup


def reorder_columns(df: pd.DataFrame, priority: List[str]) -> pd.DataFrame:
    """Reordena colunas priorizando campos principais."""
    remaining = [col for col in df.columns if col not in priority]
    final_cols = [col for col in priority if col in df.columns] + remaining
    return df[final_cols]


# --------------------------------------------------------------------------- #
# Implementação padronizada via RawETL
# --------------------------------------------------------------------------- #

class RawDocenteETL(RawETL):
    """Pipeline padronizado para carga da tabela raw_docente."""

    def __init__(self, *, data_dir: Optional[Path] = None, table_name: str = DEFAULT_TABLE) -> None:
        super().__init__(table_name=table_name, name="RAW_DOCENTE")
        self.data_dir = self._resolve_data_dir(data_dir)

    @staticmethod
    def _resolve_data_dir(data_dir: Optional[Path]) -> Path:
        if data_dir is not None:
            return data_dir.resolve()
        return (Path(__file__).resolve().parent / ".." / "data").resolve()

    def extract(self, context: ETLContext) -> pd.DataFrame:
        self.logger.info("Lendo arquivos de docentes em %s", self.data_dir)
        return load_and_consolidate_docente_files(self.data_dir)

    def transform(self, data: pd.DataFrame, context: ETLContext) -> pd.DataFrame:
        df_clean = clean_and_deduplicate(data)
        df_final = reorder_columns(df_clean, DEFAULT_PRIORITY_COLS)
        self._log_overview(df_final)
        return df_final

    def validate(self, data: pd.DataFrame, context: ETLContext) -> pd.DataFrame:
        if data.empty:
            raise ValueError("DataFrame de docentes está vazio após transformações.")
        if "id_pessoa" not in data.columns:
            raise ValueError("Coluna obrigatória 'id_pessoa' ausente.")
        return data

    def _log_overview(self, df: pd.DataFrame) -> None:
        self.logger.info("Estatísticas finais:")
        self.logger.info(" • Total de registros: %s", f"{len(df):,}")
        if "id_pessoa" in df.columns:
            self.logger.info(" • Docentes únicos: %s", f"{df['id_pessoa'].nunique():,}")
        if "ano_base" in df.columns:
            self.logger.info(" • Anos base: %s", sorted(df["ano_base"].unique()))
        if "nm_entidade_ensino" in df.columns:
            self.logger.info(
                " • Instituições: %s", f"{df['nm_entidade_ensino'].nunique():,}"
            )
        if "sg_uf_programa" in df.columns:
            self.logger.info(" • UFs: %s", sorted(df["sg_uf_programa"].unique()))

    @classmethod
    def cli(cls):
        parser = argparse.ArgumentParser(description="Processa camada RAW de docentes CAPES.")
        parser.add_argument("--data-dir", type=Path, default=None, help="Diretório com os arquivos brutos CSV.")
        parser.add_argument("--table", default=DEFAULT_TABLE, help="Nome da tabela destino (default: raw_docente).")
        parser.add_argument("--dry-run", action="store_true", help="Executa apenas extract/transform/validate.")
        parser.add_argument("--limit", type=int, default=None, help="Processa apenas as primeiras N linhas (debug).")
        parser.add_argument("--no-load", action="store_true", help="Ignora etapa de carga no banco.")

        args = parser.parse_args()
        instance = cls(data_dir=args.data_dir, table_name=args.table)
        instance.run(dry_run=args.dry_run, limit=args.limit, skip_load=args.no_load)


if __name__ == "__main__":
    RawDocenteETL.cli()
