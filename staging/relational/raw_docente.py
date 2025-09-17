#!/usr/bin/env python3
"""
br-capes-colsucup-docente-*.csv em staging/data/.
Consolida todos os anos e remove duplicatas baseado em ID_PESSOA + AN_BASE.
Opcionalmente, salva também no PostgreSQL (tabela raw_docente).
"""

import argparse
import os
import glob
from pathlib import Path
from typing import Dict, List

import pandas as pd
from sqlalchemy import create_engine, text


DEFAULT_TABLE = "raw_docente"


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
    """Carrega e consolida todos os arquivos de docentes."""
    # Busca todos os arquivos de docentes
    pattern = str(data_dir / "br-capes-colsucup-docente-*.csv")
    docente_files = glob.glob(pattern)
    
    if not docente_files:
        raise FileNotFoundError(f"Nenhum arquivo de docente encontrado em {data_dir}")
    
    print(f"📁 Encontrados {len(docente_files)} arquivos de docentes:")
    for file_path in sorted(docente_files):
        print(f"   • {Path(file_path).name}")
    
    dataframes: List[pd.DataFrame] = []
    total_records = 0
    
    for file_path in sorted(docente_files):
        file_name = Path(file_path).name
        print(f"📥 Processando {file_name}...")
        
        try:
            # Lê o arquivo com encoding latin-1 e separador ;
            # Tenta primeiro com engine padrão, depois com python se falhar
            try:
                df = pd.read_csv(file_path, encoding="latin-1", sep=";", dtype=str)
            except Exception:
                print(f"   ⚠️  Tentando com engine='python'...")
                df = pd.read_csv(file_path, encoding="latin-1", sep=";", dtype=str, engine="python")
            
            # Normaliza nomes das colunas
            df = normalize_column_names(df)
            
            # Adiciona metadados
            df["fonte_arquivo"] = file_name
            df["created_at"] = pd.Timestamp.now().normalize()
            
            print(f"   ✔ {len(df):,} registros carregados")
            total_records += len(df)
            dataframes.append(df)
            
        except Exception as exc:
            print(f"   ❌ Erro ao processar {file_name}: {exc}")
            continue
    
    if not dataframes:
        raise ValueError("Nenhum arquivo foi processado com sucesso")
    
    print(f"\n🔄 Consolidando {total_records:,} registros de {len(dataframes)} arquivos...")
    df_consolidated = pd.concat(dataframes, ignore_index=True)
    
    return df_consolidated


def clean_and_deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Limpa os dados e remove duplicatas."""
    print("🧹 Limpando dados...")
    
    # Limpeza básica dos textos
    for col in df.columns:
        if df[col].dtype == object and col not in ["fonte_arquivo", "created_at"]:
            df[col] = df[col].fillna("").astype(str).str.strip()
    
    # Conversões específicas
    numeric_cols = ["ano_base", "id_pessoa", "an_nascimento_docente", "an_titulacao", 
                   "cd_area_avaliacao", "cd_programa_ies", "cd_conceito_programa",
                   "cd_entidade_capes", "cd_entidade_emec", "cd_area_basica_titulacao",
                   "id_add_foto_programa", "id_add_foto_programa_ies"]
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    
    # Normaliza campos de texto importantes
    text_normalize_cols = ["sg_entidade_ensino", "sg_uf_programa", "tp_documento_docente",
                          "ds_categoria_docente", "in_doutor", "sg_ies_titulacao"]
    
    for col in text_normalize_cols:
        if col in df.columns:
            df[col] = df[col].str.upper()
    
    print(f"   ✔ Dados limpos: {len(df):,} registros")
    
    # Remove duplicatas baseado em ID_PESSOA + ANO_BASE + CD_PROGRAMA_IES
    print("🔍 Removendo duplicatas...")
    duplicates_before = len(df)
    
    # Identifica duplicatas baseado nas chaves principais
    dedup_keys = ["id_pessoa", "ano_base", "cd_programa_ies"]
    df_dedup = df.drop_duplicates(subset=dedup_keys, keep="last")
    
    duplicates_removed = duplicates_before - len(df_dedup)
    print(f"   ✔ {duplicates_removed:,} duplicatas removidas")
    print(f"   ✔ {len(df_dedup):,} registros únicos mantidos")
    
    return df_dedup


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gera a tabela raw_docente consolidando todos os arquivos de docentes CAPES."
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
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent
    data_dir = (base_dir / ".." / "data").resolve()

    print("🎓 === PROCESSAMENTO RAW_DOCENTE CAPES ===")
    print(f"📖 Diretório de dados: {data_dir}")
    
    # Carrega e consolida todos os arquivos
    df = load_and_consolidate_docente_files(data_dir)
    
    # Limpa e remove duplicatas
    df_clean = clean_and_deduplicate(df)
    
    # Reorganiza colunas para melhor visualização
    priority_cols = [
        "id_pessoa", "nm_docente", "ano_base", "nm_entidade_ensino", "sg_uf_programa",
        "ds_categoria_docente", "nm_programa_ies", "nm_area_avaliacao", "in_doutor",
        "fonte_arquivo", "created_at"
    ]
    other_cols = [col for col in df_clean.columns if col not in priority_cols]
    final_cols = priority_cols + other_cols
    df_final = df_clean[[col for col in final_cols if col in df_clean.columns]]
    
    # Salva no PostgreSQL se solicitado
    if args.postgres:
        save_to_postgres(df_final, args.table)
    else:
        print("💡 Use --postgres para enviar a tabela ao banco.")
        print("💡 Dados processados apenas em memória (sem geração de arquivos).")

    # Estatísticas finais
    print("\n📊 Estatísticas finais:")
    print(f"   • Total de registros: {len(df_final):,}")
    print(f"   • Docentes únicos: {df_final['id_pessoa'].nunique():,}")
    print(f"   • Anos de base: {sorted(df_final['ano_base'].unique())}")
    print(f"   • Instituições: {df_final['nm_entidade_ensino'].nunique():,}")
    print(f"   • UFs: {sorted(df_final['sg_uf_programa'].unique())}")
    
    print("\n📋 Amostra dos dados:")
    display_cols = ["id_pessoa", "nm_docente", "ano_base", "nm_entidade_ensino", "sg_uf_programa"]
    print(df_final[display_cols].head().to_string())


if __name__ == "__main__":
    main()
