#!/usr/bin/env python3
"""
Geração da Dimensão de IES (dim_ies) a partir de um arquivo Parquet no MinIO.

Este script lê o arquivo 'add_docentes.parquet' do MinIO, extrai e desduplica
as informações das Instituições de Ensino Superior (IES) para criar uma dimensão
limpa e a salva no banco de dados PostgreSQL.
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path

def get_project_root() -> Path:
    """Encontra o diretório raiz do projeto de forma robusta."""
    current_path = Path(__file__).resolve()
    while not (current_path / '.env').exists() and not (current_path / '.git').exists() and current_path.parent != current_path:
        current_path = current_path.parent
    return current_path

def get_db_engine(env_path: Path):
    """Conecta ao PostgreSQL usando variáveis de ambiente."""
    load_dotenv(dotenv_path=env_path)
    
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

def load_parquet_from_minio(env_path: Path) -> pd.DataFrame:
    """
    Lê um arquivo Parquet do MinIO com base nas variáveis de ambiente.
    """
    load_dotenv(dotenv_path=env_path)

    endpoint = os.getenv("MINIO_ENDPOINT")
    bucket = os.getenv("MINIO_BUCKET")
    parquet_path = os.getenv("MINIO_PARQUET_PATH")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not all([endpoint, bucket, parquet_path, access_key, secret_key]):
        raise ValueError("As variáveis de ambiente do MinIO não estão configuradas.")

    storage_options = {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {"endpoint_url": endpoint},
    }

    path = f"s3://{bucket}/{parquet_path}/add_docentes.parquet"
    
    print(f"Lendo arquivo Parquet do MinIO: {path}")
    try:
        df = pd.read_parquet(path, storage_options=storage_options)
        print(f"Dados carregados com sucesso: {len(df):,} registros.")
        return df
    except Exception as e:
        print(f"ERRO: Falha ao ler o arquivo Parquet do MinIO: {e}")
        raise

def create_ies_dimension(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma o DataFrame bruto na dimensão de IES final.
    """
    print("Processando dados para criar a dimensão de IES...")

    # 1. Selecionar e renomear colunas relevantes para IES
    column_mapping = {
        'CD_ENTIDADE_CAPES': 'cod_entidade_capes',
        'SG_ENTIDADE_ENSINO': 'sg_ies',
        'NM_ENTIDADE_ENSINO': 'des_ies',
        'CS_STATUS_JURIDICO': 'des_status_juridico',
        'DS_DEPENDENCIA_ADMINISTRATIVA': 'des_dependencia_adm',
        'NM_REGIAO': 'des_regiao',
        'SG_UF_PROGRAMA': 'sg_uf',
        'NM_MUNICIPIO_PROGRAMA_IES': 'des_municipio'
    }
    
    df_ies = df_raw[list(column_mapping.keys())].copy()
    df_ies.rename(columns=column_mapping, inplace=True)

    # 2. Remover duplicatas para ter um registro único por IES
    # O `cod_entidade_capes` é a chave de negócio perfeita para isso.
    print(f"Registros antes da desduplicação: {len(df_ies):,}")
    df_ies = df_ies.drop_duplicates(subset=['cod_entidade_capes']).reset_index(drop=True)
    print(f"Registros após desduplicação: {len(df_ies):,} IES únicas encontradas.")

    # 3. Adicionar registro SK=0 para 'Desconhecido'
    sk0_record = pd.DataFrame([{
        'sk_ies': 0,
        'cod_entidade_capes': 0,
        'des_ies': 'Desconhecido',
        'sg_ies': 'XX'
    }])
    
    # 4. Gerar a chave substituta (Surrogate Key)
    df_ies['sk_ies'] = df_ies.index + 1
    
    # 5. Concatenar o registro SK=0
    final_dim = pd.concat([sk0_record, df_ies], ignore_index=True)
    
    # 6. Reordenar e selecionar colunas finais
    final_cols = [
        'sk_ies', 'cod_entidade_capes', 'sg_ies', 'des_ies', 
        'des_status_juridico', 'des_dependencia_adm', 
        'des_regiao', 'sg_uf', 'des_municipio'
    ]
    final_dim = final_dim[[col for col in final_cols if col in final_dim.columns]]
    
    print(f"Dimensão final de IES criada com {len(final_dim):,} registros.")
    return final_dim

def save_to_postgres(df: pd.DataFrame, engine, table_name: str):
    """Salva o DataFrame final no PostgreSQL."""
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
    """Função principal para orquestrar a criação da dimensão."""
    TABLE_NAME = 'dim_ies'
    
    print(f"INICIANDO GERAÇÃO DA TABELA {TABLE_NAME.upper()}")
    print("=" * 60)

    try:
        project_root = get_project_root()
        
        # 1. Ler dados do MinIO
        df_raw = load_parquet_from_minio(project_root / '.env')
        
        # 2. Criar a dimensão de IES
        dim_ies = create_ies_dimension(df_raw)
        
        # 3. Conectar ao banco de dados
        engine = get_db_engine(project_root / '.env')
        
        # 4. Salvar a dimensão no PostgreSQL
        save_to_postgres(dim_ies, engine, table_name=TABLE_NAME)
        
        # 5. Exibir preview e estatísticas
        print("\nPreview da dimensão gerada:")
        print(dim_ies.head())
        
        print("\nEstatísticas da dimensão:")
        print(f"  - Total de registros (IES únicas + SK=0): {len(dim_ies):,}")
        
        # Contagem por dependência administrativa
        if 'des_dependencia_adm' in dim_ies.columns:
            print("\nContagem por Dependência Administrativa:")
            print(dim_ies['des_dependencia_adm'].value_counts().to_string())

    except Exception as e:
        print(f"\nO processo falhou. Motivo: {e}")
    
    print("\nProcesso concluído.")

if __name__ == "__main__":
    main()
