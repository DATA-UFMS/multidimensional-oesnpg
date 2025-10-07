#!/usr/bin/env python3
"""
Geração da Dimensão Tema (dim_tema) a partir da tabela raw_tema.

Este script lê os dados pré-processados da tabela 'raw_tema', que já contém
os IDs de negócio, e constrói a dimensão final, convertendo os nomes de UF
para siglas (ex: 'SÃO PAULO' -> 'SP').
"""

import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import argparse
from pathlib import Path
# Adicionar o diretório raiz ao path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from src.utils.naming_conventions import NamingConventions
from src.validation.data_validator import validate_dimension_data, get_validation_summary
from src.core.exceptions import DimensionCreationError, DataValidationError


# Mapeamento de UF para garantir consistência
UF_MAPPING = {
    'ACRE': 'AC', 'ALAGOAS': 'AL', 'AMAPÁ': 'AP', 'AMAZONAS': 'AM', 'BAHIA': 'BA', 
    'CEARÁ': 'CE', 'DISTRITO FEDERAL': 'DF', 'ESPÍRITO SANTO': 'ES', 'GOIÁS': 'GO',
    'MARANHÃO': 'MA', 'MATO GROSSO': 'MT', 'MATO GROSSO DO SUL': 'MS', 'MINAS GERAIS': 'MG',
    'PARÁ': 'PA', 'PARAÍBA': 'PB', 'PARANÁ': 'PR', 'PERNAMBUCO': 'PE', 'PIAUÍ': 'PI',
    'RIO DE JANEIRO': 'RJ', 'RIO GRANDE DO NORTE': 'RN', 'RIO GRANDE DO SUL': 'RS',
    'RONDÔNIA': 'RO', 'RORAIMA': 'RR', 'SANTA CATARINA': 'SC', 'SÃO PAULO': 'SP',
    'SERGIPE': 'SE', 'TOCANTINS': 'TO'
}

def get_project_root() -> Path:
    """Encontra o diretório raiz do projeto de forma robusta."""
    current_path = Path(__file__).resolve()
    while not (current_path / '.env').exists() and not (current_path / '.git').exists() and current_path.parent != current_path:
        current_path = current_path.parent
    return current_path

def get_db_engine():
    """Conecta ao PostgreSQL usando variáveis de ambiente."""
    project_root = get_project_root()
    load_dotenv(dotenv_path=project_root / '.env')
    
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    if not all([db_user, db_pass, db_host, db_port, db_name]):
        raise ValueError("As variáveis de ambiente do banco de dados não estão configuradas.")

    db_uri = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    try:
        engine = create_engine(db_uri)
        with engine.connect() as connection:
            print(f"Conexão com o banco '{db_name}' estabelecida com sucesso.")
        return engine
    except Exception as e:
        print(f"ERRO: Falha ao conectar com o banco de dados: {e}")
        raise

def extract_from_raw_tema(engine) -> pd.DataFrame:
    """
    Extrai os dados necessários da tabela 'raw_tema' no schema public.
    """
    print("Lendo dados da tabela 'public.raw_tema'...")
    query = """
    SELECT
        macrotema_id, macrotema_nome,
        tema_id, tema_nome,
        palavrachave_id, palavrachave_nome,
        uf
    FROM public.raw_tema
    """
    try:
        df = pd.read_sql(query, engine)
        print(f"Dados extraídos com sucesso: {len(df)} registros.")
        return df
    except Exception as e:
        print(f"ERRO: Falha ao ler da tabela 'raw_tema'. Verifique se ela existe. Detalhes: {e}")
        raise

def create_dimension_from_raw(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Cria a dimensão final a partir do DataFrame extraído da tabela raw_tema.
    """
    print("Processando dados para criar a dimensão tema...")

    df_dim = df_raw.rename(columns={'palavrachave_nome': 'palavra_chave'})

    # **CORREÇÃO APLICADA AQUI**
    # Mapeia a coluna 'uf' (com nomes completos) para uma nova coluna 'sigla_uf'.
    print("Mapeando nomes de UF para siglas...")
    df_dim['sigla_uf'] = df_dim['uf'].str.upper().map(UF_MAPPING).fillna('XX')

    # Adiciona o registro SK=0 para valores desconhecidos
    sk0_record = pd.DataFrame([{
        'tema_sk': 0, 'macrotema_id': 0, 'macrotema_nome': 'DESCONHECIDO',
        'tema_id': 0, 'tema_nome': 'DESCONHECIDO',
        'palavrachave_id': 0, 'palavra_chave': 'DESCONHECIDO',
        'sigla_uf': 'XX'
    }])
    
    df_dim = df_dim.drop_duplicates().reset_index(drop=True)
    df_dim['tema_sk'] = df_dim.index + 1
    
    final_dim = pd.concat([sk0_record, df_dim], ignore_index=True)
    
    # Reordena as colunas para o formato final, usando 'sigla_uf'
    final_cols = [
        'tema_sk',
        'macrotema_id', 'macrotema_nome',
        'tema_id', 'tema_nome',
        'palavrachave_id', 'palavra_chave',
        'sigla_uf'  # Usando a coluna de sigla
    ]
    # Seleciona apenas as colunas finais, descartando a 'uf' original
    final_dim = final_dim[final_cols]
    
    print(f"Dimensão final criada com {len(final_dim)} registros (incluindo SK=0).")
    return final_dim

def save_dimension(df: pd.DataFrame, engine, table_name='dim_tema'):
    """Salva a dimensão final no schema public do PostgreSQL."""
    print(f"Salvando dimensão na tabela 'public.{table_name}'...")
    try:
        df.to_sql(
            table_name,
            engine,
            if_exists='replace',
            index=False,
            method='multi'
        )
        print("Dimensão salva com sucesso!")
    except Exception as e:
        print(f"ERRO: Falha ao salvar a dimensão: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Gera a dim_tema a partir da tabela raw_tema.")
    parser.add_argument('--table', default='dim_tema', help='Nome da tabela de destino no banco de dados.')
    args = parser.parse_args()

    print("INICIANDO GERAÇÃO DA DIMENSÃO TEMA")
    print("=" * 50)

    try:
        engine = get_db_engine()
        df_raw = extract_from_raw_tema(engine)
        dim_tema = create_dimension_from_raw(df_raw)

        print("\nPreview da dimensão gerada:")
        print(dim_tema.head(10))
        
        print("\nEstatísticas da dimensão:")
        print(f"  - Total de registros: {len(dim_tema)}")
        print(f"  - Macrotemas únicos: {dim_tema[dim_tema['tema_sk'] != 0]['macrotema_id'].nunique()}")
        print(f"  - Temas únicos: {dim_tema[dim_tema['tema_sk'] != 0]['tema_id'].nunique()}")
        print(f"  - Palavras-chave únicas: {dim_tema[dim_tema['tema_sk'] != 0]['palavrachave_id'].nunique()}")
        print(f"  - UFs únicas: {dim_tema[dim_tema['tema_sk'] != 0]['sigla_uf'].nunique()}")

        save_dimension(dim_tema, engine, table_name=args.table)

    except Exception as e:
        print(f"\nO processo falhou. Motivo: {e}")
    
    print("\nProcesso concluído.")

if __name__ == "__main__":
    main()
