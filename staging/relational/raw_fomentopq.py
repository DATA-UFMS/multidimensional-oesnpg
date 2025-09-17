#!/usr/bin/env python3
"""
Gera a tabela raw_fomentopq a partir do arquivo
staging/data/Planilha_Mapa_Fomento_PQ.xlsx - Sheet 1.csv.
Opcionalmente, salva também no PostgreSQL (tabela raw_pq).
"""
import argparse
import os
from pathlib import Path
from typing import Dict
import pandas as pd
from sqlalchemy import create_engine, text
CSV_NAME = "Planilha_Mapa_Fomento_PQ.xlsx - Sheet 1.csv"
DEFAULT_TABLE = "raw_fomentopq"

def save_to_postgres(df: pd.DataFrame, table_name: str) -> bool:
    """Salva o DataFrame na tabela indicada do PostgreSQL."""
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    database = os.getenv("POSTGRES_DB")
    username = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    conn_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    try:
        print(f"🔗 Conectando ao PostgreSQL: {host}:{port}/{database}")
        engine = create_engine(conn_string)
        with engine.connect() as conn:
            row = conn.execute(text("SELECT version()")).fetchone()
            if row:
                print(f"✅ Conectado (versão: {row[0][:50]}...)")
            else:
                print("✅ Conectado ao PostgreSQL.")
        print(f"💾 Gravando tabela {table_name}...")
        df.to_sql(
            table_name,
            engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )
        print("✅ Dados enviados ao PostgreSQL.")
        return True
    except Exception as exc:  # pylint: disable=broad-except
        print(f"❌ Erro ao conectar/salvar no PostgreSQL: {exc}")
        print("💡 Verifique se o banco está disponível e as variáveis de ambiente foram definidas.")
        return False


def load_raw_dataframe(csv_path: Path) -> pd.DataFrame:
    """Lê e normaliza os dados de fomento PQ."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")
    # Lê como texto para preservar zeros à esquerda
    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8")
    print(f"📥 Arquivo carregado: {len(df):,} linhas brutas.")
    first_column = df.columns[0]
    df = df.rename(columns={first_column: "id_registro"})
    rename_map: Dict[str, str] = {
        "# Id Lattes": "id_lattes",
        "# Nome Beneficiário": "des_beneficiario",
        "# Nome País": "des_pais",
        "# Nome Região": "des_regiao",
        "# Nome UF": "des_uf",
        "# Nome Cidade": "des_cidade",
        "# Nome Grande Área": "des_grande_area",
        "# Nome Área": "des_area",
        "# Nome Sub-área": "des_subarea",
        "# Cod Modalidade": "cod_modalidade",
        "# Cod Categoria Nível": "cod_categoria_nivel",
        "# Nome Instituto": "des_instituto",
        "# Data Início Processo": "data_inicio_processo",
        "# Data Término Processo": "data_termino_processo",
    }
    df = df.rename(columns={old: new for old, new in rename_map.items() if old in df.columns})
    # Limpeza básica dos textos
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].fillna("").astype(str).str.strip()
    # Conversões específicas
    # Converte id_registro para numérico, mantendo NaN como valores nulos
    df["id_registro"] = pd.to_numeric(df["id_registro"], errors="coerce")
    # Preenche NaN com 0 antes de converter para Int64, ou mantém como float64
    df["id_registro"] = df["id_registro"].fillna(0).astype(int)
    df["id_lattes"] = df["id_lattes"].astype(str).str.strip()
    for col in ("cod_modalidade", "cod_categoria_nivel"):
        if col in df.columns:
            df[col] = df[col].str.upper()
    for col in ("data_inicio_processo", "data_termino_processo"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")
    df["fonte_arquivo"] = csv_path.name
    df["created_at"] = pd.Timestamp.now().normalize()
    ordered_cols = [
        "id_registro",
        "id_lattes",
        "des_beneficiario",
        "des_pais",
        "des_regiao",
        "des_uf",
        "des_cidade",
        "des_grande_area",
        "des_area",
        "des_subarea",
        "cod_modalidade",
        "cod_categoria_nivel",
        "des_instituto",
        "data_inicio_processo",
        "data_termino_processo",
        "fonte_arquivo",
        "created_at",
    ]
    df = df[[col for col in ordered_cols if col in df.columns]]
    print("✅ Dados normalizados.")
    print(f"   • Registros: {len(df):,}")
    print(f"   • Colunas: {list(df.columns)}")
    return df

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gera a tabela raw_fomentopq a partir do CSV de fomento PQ."
    )
    parser.add_argument(
        "--postgres",
        action="store_true",
        help="Envia a tabela também para o PostgreSQL (default: não envia).",
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help=f"Nome da tabela destino no PostgreSQL (default: {DEFAULT_TABLE}).",
    )
    parser.add_argument(
        "--csv",
        default=CSV_NAME,
        help="Nome do arquivo CSV dentro de staging/data (default: Planilha_Mapa_Fomento_PQ...).",
    )
    args = parser.parse_args()
    base_dir = Path(__file__).resolve().parent
    csv_path = (base_dir / ".." / "data" / args.csv).resolve()
    print(f"📖 Lendo dados de: {csv_path}")
    df = load_raw_dataframe(csv_path)
    
    if args.postgres:
        save_to_postgres(df, args.table)
    else:
        print("💡 Use --postgres para enviar a tabela ao banco.")
        print("💡 Dados processados apenas em memória (sem geração de arquivos).")
    print("\n📋 Amostra dos dados:")
    print(df.head())

if __name__ == "__main__":
    main()
