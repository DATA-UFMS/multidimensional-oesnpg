import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# ==============================================================================
# ÁREA DE CONFIGURAÇÃO
# ==============================================================================
# Edite esta lista para especificar exatamente quais tabelas você quer converter.
SCHEMA_ALVO = 'public'
TABELAS_PARA_CONVERTER = [
    ##"dim_tempo",
    ##"dim_localidade",
    ##"dim_ies",
    ##"dim_tema",
    ##"dim_ppg",
     'dim_discente'
]
PASTA_SAIDA_LOCAL = 'parquet_output'

# --- Configuração de Upload para o MinIO ---
# Defina como True para salvar também no MinIO, ou False para salvar apenas localmente.
ESCREVER_NO_MINIO = True
CAMINHO_BASE_MINIO = 'multidimensional' # O caminho dentro do bucket
# ==============================================================================


def get_db_engine():
    """Lê as variáveis de ambiente e cria uma engine de conexão SQLAlchemy."""
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
        print(f"Falha ao conectar com o banco de dados: {e}")
        raise

def get_minio_storage_options():
    """Lê as variáveis de ambiente do MinIO e retorna um dicionário para o Pandas."""
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not all([endpoint, access_key, secret_key]):
        raise ValueError("As variáveis de ambiente do MinIO não estão configuradas.")

    # O Pandas/s3fs usa este dicionário para configurar a conexão S3.
    storage_options = {
        'key': access_key,
        'secret': secret_key,
        'client_kwargs': {
            'endpoint_url': endpoint
        }
    }
    print("Configurações do MinIO carregadas com sucesso.")
    return storage_options

def export_tables_to_parquet(engine, schema_name, table_list, output_dir_local, write_to_minio, minio_base_path):
    """Lê tabelas de um schema e as exporta para Parquet, localmente e/ou no MinIO."""
    print(f"\nIniciando exportação de {len(table_list)} tabelas do schema '{schema_name}'...")
    
    # Prepara o diretório local
    if not os.path.exists(output_dir_local):
        os.makedirs(output_dir_local)
        print(f"Pasta de saída local '{output_dir_local}' criada.")

    # Prepara a conexão com o MinIO se necessário
    minio_storage_options = None
    minio_bucket = None
    if write_to_minio:
        try:
            minio_storage_options = get_minio_storage_options()
            minio_bucket = os.getenv("MINIO_BUCKET")
            if not minio_bucket:
                raise ValueError("A variável de ambiente MINIO_BUCKET não está definida.")
        except ValueError as e:
            print(f"AVISO: Não será possível escrever no MinIO. Motivo: {e}")
            write_to_minio = False # Desativa o upload se a configuração falhar

    total_files_generated = 0
    for table_name in table_list:
        print(f"  - Processando tabela: '{table_name}'...")
        
        try:
            query = f'SELECT * FROM "{schema_name}"."{table_name}"'
            df = pd.read_sql(query, engine)
            
            if df.empty:
                print(f"    - AVISO: Tabela '{table_name}' está vazia. Pulando.")
                continue

            # 1. Escrever o arquivo Parquet localmente
            local_path = os.path.join(output_dir_local, f"{table_name}.parquet")
            df.to_parquet(local_path, engine='pyarrow', compression='snappy', index=False)
            print(f"    - SUCESSO (Local): Arquivo '{table_name}.parquet' salvo em '{output_dir_local}'.")

            # 2. Escrever o mesmo arquivo no MinIO, se ativado
            if write_to_minio:
                # O caminho no MinIO será: s3://<bucket>/<base_path>/<table_name>.parquet
                minio_path = f"s3://{minio_bucket}/{minio_base_path}/{table_name}.parquet"
                try:
                    df.to_parquet(
                        minio_path,
                        engine='pyarrow',
                        compression='snappy',
                        index=False,
                        storage_options=minio_storage_options
                    )
                    print(f"    - SUCESSO (MinIO): Arquivo salvo em '{minio_bucket}/{minio_base_path}'.")
                except Exception as e:
                    print(f"    - ERRO (MinIO): Falha ao salvar '{table_name}.parquet' no MinIO. Motivo: {e}")

            total_files_generated += 1

        except Exception as e:
            print(f"    - ERRO (Geral): Falha ao processar a tabela '{table_name}'. Motivo: {e}")

    print(f"\nProcesso concluído. {total_files_generated} de {len(table_list)} tabelas foram processadas.")

def main():
    """Função principal para orquestrar a conversão de tabelas para Parquet."""
    print("INICIANDO SCRIPT DE CONVERSÃO: PostgreSQL para Parquet e MinIO")
    print("=" * 60)
    
    load_dotenv()
    
    if not TABELAS_PARA_CONVERTER:
        print("AVISO: A lista 'TABELAS_PARA_CONVERTER' está vazia. Nenhuma tabela será processada.")
        print("=" * 60)
        return
        
    try:
        engine = get_db_engine()
        export_tables_to_parquet(
            engine,
            schema_name=SCHEMA_ALVO,
            table_list=TABELAS_PARA_CONVERTER,
            output_dir_local=PASTA_SAIDA_LOCAL,
            write_to_minio=ESCREVER_NO_MINIO,
            minio_base_path=CAMINHO_BASE_MINIO
        )
    except Exception as e:
        print(f"\nO script falhou. Motivo: {e}")
    
    print("=" * 60)

if __name__ == "__main__":
    main()