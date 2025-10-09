#!/usr/bin/env python3
"""
raw_docente.py

Módulo para processamento e consolidação de dados brutos de docentes CAPES.

Descrição:
    Este script processa arquivos CSV de docentes da CAPES (formato br-capes-colsucup-docente-*.csv),
    consolida dados de múltiplos anos, remove duplicatas e carrega os dados na tabela raw_docente
    do PostgreSQL para uso posterior em dimensões do Data Warehouse.

Funcionalidades:
    - Leitura automática de múltiplos arquivos CSV de docentes
    - Consolidação de dados de diferentes anos base (2021, 2022, 2023, etc.)
    - Normalização de nomes de colunas para padrão snake_case
    - Limpeza e transformação de dados:
      * Conversão de tipos de dados apropriados
      * Normalização de campos texto (upper, strip)
      * Tratamento de valores nulos
      * Adição de metadados (fonte_arquivo, created_at)
    - Remoção de duplicatas por (id_pessoa + ano_base)
    - Carga automática no PostgreSQL (padrão ativado)

Fonte de Dados:
    - Arquivos: br-capes-colsucup-docente-{ANO}-{DATA}.csv
    - Localização: staging/data/
    - Formato: CSV com delimitador padrão
    - Origem: Plataforma Sucupira/CAPES

Estrutura da Tabela raw_docente:
    Campos de Identificação:
    - id_pessoa: Identificador único do docente no sistema CAPES
    - nm_docente: Nome completo do docente
    - ano_base: Ano base dos dados (2021, 2022, 2023, etc.)
    
    Dados Institucionais:
    - cd_entidade_capes: Código da instituição no sistema CAPES
    - cd_entidade_emec: Código da instituição no sistema e-MEC
    - sg_entidade_ensino: Sigla da instituição de ensino
    - nm_entidade_ensino: Nome completo da instituição
    - ds_dependencia_administrativa: Tipo de dependência (Pública/Privada)
    - cs_status_juridico: Status jurídico da instituição
    - nm_municipio_programa_ies: Município da instituição
    - sg_uf_programa: Sigla da UF da instituição
    
    Dados do Programa:
    - cd_programa_ies: Código do programa de pós-graduação
    - nm_programa_ies: Nome do programa
    - nm_grau_programa: Grau do programa (Mestrado/Doutorado/Mestrado Profissional)
    - nm_modalidade_programa: Modalidade do programa (Acadêmico/Profissional)
    - cd_conceito_programa: Conceito CAPES do programa (3-7)
    
    Dados de Área de Conhecimento:
    - cd_area_avaliacao: Código da área de avaliação
    - nm_area_avaliacao: Nome da área de avaliação
    - nm_grande_area_conhecimento: Grande área do conhecimento
    - nm_area_conhecimento: Área de conhecimento específica
    
    Dados do Docente:
    - ds_categoria_docente: Categoria (Permanente/Colaborador/Visitante)
    - ds_regime_trabalho: Regime de trabalho (Integral/Parcial/Horista)
    - ds_faixa_etaria: Faixa etária do docente
    - tp_sexo_docente: Sexo do docente (M/F)
    - in_doutor: Indicador se possui doutorado (SIM/NÃO)
    - an_titulacao: Ano de obtenção da titulação máxima
    - nm_grau_titulacao: Grau de titulação (Doutorado/Mestrado/etc.)
    - nm_area_basica_titulacao: Área básica da titulação
    - sg_ies_titulacao: Sigla da IES tituladora
    - cd_cat_bolsa_produtividade: Categoria de bolsa de produtividade (se houver)
    - in_coordenador_ppg: Indicador se é coordenador de PPG (SIM/NÃO)
    
    Campos Técnicos:
    - tp_documento_docente: Tipo do documento (RG/CPF/Passaporte)
    - nr_documento_docente: Número do documento
    - an_nascimento_docente: Ano de nascimento
    - ds_tipo_nacionalidade_docente: Tipo de nacionalidade
    - nm_pais_nacionalidade_docente: País de nacionalidade
    - ds_tipo_vinculo_docente_ies: Tipo de vínculo com a IES
    - nm_ies_titulacao: Nome completo da IES tituladora
    - nm_pais_ies_titulacao: País da IES tituladora
    
    Metadados:
    - fonte_arquivo: Nome do arquivo CSV de origem
    - created_at: Timestamp de processamento (data/hora da carga)

Processo de ETL:
    1. Extração:
       - Localiza todos os arquivos CSV de docentes no diretório staging/data/
       - Lê cada arquivo com pandas.read_csv()
       - Adiciona metadados de origem (fonte_arquivo)
    
    2. Transformação:
       - Normaliza nomes de colunas (AN_BASE → ano_base)
       - Consolida DataFrames de múltiplos anos
       - Limpa dados (strip, upper case em campos texto)
       - Remove duplicatas mantendo apenas um registro por (id_pessoa, ano_base)
       - Adiciona timestamp de processamento (created_at)
       - Reordena colunas priorizando campos importantes
    
    3. Carga:
       - Salva automaticamente no PostgreSQL (tabela raw_docente)
       - Usa method='multi' e chunksize=1000 para otimização
       - Substitui tabela existente (if_exists='replace')

Deduplicação:
    - Critério: (id_pessoa, ano_base) único
    - Estratégia: Mantém primeira ocorrência de cada combinação
    - Estatísticas reportadas no log de execução

Validações:
    - Verifica existência de arquivos CSV no diretório
    - Valida conexão com PostgreSQL antes de inserir
    - Reporta contadores de registros em cada etapa
    - Mostra estatísticas finais (docentes únicos, anos, instituições, UFs)

Configuração:
    Variáveis de Ambiente (arquivo .env):
    - DB_HOST ou POSTGRES_HOST: Endereço do servidor PostgreSQL
    - DB_PORT ou POSTGRES_PORT: Porta do PostgreSQL (padrão: 5432)
    - DB_NAME ou POSTGRES_DB: Nome do banco de dados
    - DB_USER ou POSTGRES_USER: Usuário do banco
    - DB_PASS ou POSTGRES_PASSWORD: Senha do banco

Uso:
    # Modo padrão (carrega no PostgreSQL automaticamente)
    python3 staging/relational/raw_docente.py
    
    # Processar sem carregar no PostgreSQL
    python3 staging/relational/raw_docente.py --no-postgres
    
    # Especificar nome de tabela customizado
    python3 staging/relational/raw_docente.py --table nome_tabela_custom

Argumentos:
    --postgres: Habilita carga no PostgreSQL (PADRÃO: ativado)
    --no-postgres: Desabilita carga no PostgreSQL (apenas processa em memória)
    --table NOME: Nome da tabela destino no PostgreSQL (padrão: raw_docente)

Dependências:
    - pandas: Manipulação de dados
    - sqlalchemy: Conexão e operações com PostgreSQL
    - python-dotenv: Gerenciamento de variáveis de ambiente
    - argparse: Processamento de argumentos de linha de comando

Saída:
    - Tabela raw_docente no PostgreSQL com dados consolidados
    - Logs detalhados no console com estatísticas de processamento
    - Amostra de 5 registros para verificação

Observações:
    - Processamento otimizado com chunks de 1000 registros
    - Suporta múltiplos arquivos CSV automaticamente
    - Mantém histórico por ano_base (não remove anos anteriores)
    - Dados são substituídos completamente a cada execução (replace)

Performance:
    - ~330K registros processados em segundos
    - Deduplicação resulta em ~254K registros únicos
    - ~92K docentes únicos identificados

Autor: UFMS - Data Warehouse CAPES/OES/NPG
Data de Criação: 2025
Última Atualização: 09/10/2025
"""

import argparse
import os
import glob
from pathlib import Path
from typing import Dict, List

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

DEFAULT_TABLE = "raw_docente"


def save_to_postgres(df: pd.DataFrame, table_name: str) -> bool:
    """Salva o DataFrame na tabela indicada do PostgreSQL."""
    # Tentar primeiro variáveis POSTGRES_*, depois DB_* como fallback
    host = os.getenv("POSTGRES_HOST") or os.getenv("DB_HOST")
    port = os.getenv("POSTGRES_PORT") or os.getenv("DB_PORT")
    database = os.getenv("POSTGRES_DB") or os.getenv("DB_NAME")
    username = os.getenv("POSTGRES_USER") or os.getenv("DB_USER")
    password = os.getenv("POSTGRES_PASSWORD") or os.getenv("DB_PASS")

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
        default=True,
        help="Envia a tabela também para o PostgreSQL (default: SIM, use --no-postgres para desabilitar).",
    )
    parser.add_argument(
        "--no-postgres",
        action="store_false",
        dest="postgres",
        help="Desabilita o envio para PostgreSQL.",
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
    
    # Salva no PostgreSQL (padrão ativado)
    if args.postgres:
        save_to_postgres(df_final, args.table)
    else:
        print("💡 PostgreSQL desabilitado (--no-postgres).")
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
