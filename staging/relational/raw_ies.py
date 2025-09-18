#!/usr/bin/env python3
"""
Geração da Tabela Raw de IES (raw_ies) a partir da API da CAPES.

Este script extrai os dados cadastrais das Instituições de Ensino Superior (IES)
diretamente do Portal de Dados Abertos da CAPES, processa-os e os salva
em uma tabela 'raw_ies_api' no banco de dados PostgreSQL.
"""

import os
import sys
import pandas as pd
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path

# --- Funções de Utilidade (Poderiam estar em src/core/utils.py) ---
# Incluídas aqui para tornar o script autocontido e fácil de executar.

def padronizar_nomes_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica um conjunto completo de regras de padronização aos nomes das colunas."""
    import re
    import unicodedata
    
    colunas_renomeadas = {}
    for col in df.columns:
        s = str(col).strip().lower()
        s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('utf-8')
        s = re.sub(r'[^a-z0-9\s_]', '', s)
        s = re.sub(r'\s+', '_', s)

        if 'codigo' in s: s = s.replace('codigo', 'cod')
        if s.startswith('cd_'): s = s.replace('cd_', 'cod_')
        if s.startswith('ds_'): s = s.replace('ds_', 'des_')
        if s.startswith('nm_'): s = s.replace('nm_', 'des_')
        if 'nome' in s: s = s.replace('nome', 'des')
        if 'quantidade' in s: s = s.replace('quantidade', 'qtd')
        
        if s.endswith('_id'):
            s = s.replace('_id', '_id_original')
        
        colunas_renomeadas[col] = s
        
    df = df.rename(columns=colunas_renomeadas)
    return df

def limpar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Executa uma limpeza geral em um DataFrame."""
    df.dropna(axis=1, how='all', inplace=True)
    df.dropna(axis=0, how='all', inplace=True)

    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip().replace(['', 'nan', 'NaN', 'NULL'], None)
        
    return df

def definir_schema_tabela(df: pd.DataFrame, schema_desejado: dict) -> pd.DataFrame:
    """Aplica um schema de tipos de dados a um DataFrame."""
    schema_aplicavel = {k: v for k, v in schema_desejado.items() if k in df.columns}
    df_convertido = df.astype(schema_aplicavel, errors='ignore')
    
    for col, dtype in schema_aplicavel.items():
        if dtype == 'Int64':
            df_convertido[col] = pd.to_numeric(df_convertido[col], errors='coerce').astype('Int64')
            
    return df_convertido

# --- Lógica Principal do Script ---

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

def fetch_all_from_api(resource_id: str, api_url: str) -> pd.DataFrame:
    """
    Busca todos os registros de um recurso na API CKAN da CAPES, lidando com paginação.
    """
    print(f"Iniciando extração da API para o resource_id: {resource_id}")
    all_records = []
    offset = 0
    limit = 5000  # Aumentar o limite por requisição para mais eficiência

    while True:
        params = {'resource_id': resource_id, 'limit': limit, 'offset': offset}
        try:
            response = requests.get(api_url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            if not data.get('success'):
                print(f"ERRO: A API retornou um erro: {data.get('error')}")
                break

            records = data.get('result', {}).get('records', [])
            if not records:
                print("Extração concluída. Não há mais registros a serem buscados.")
                break

            all_records.extend(records)
            print(f"  - Registros buscados: {len(all_records):,}")
            offset += limit

        except requests.exceptions.RequestException as e:
            print(f"ERRO: Falha na requisição à API: {e}")
            break
        except Exception as e:
            print(f"ERRO: Ocorreu um erro inesperado durante a extração: {e}")
            break
            
    if not all_records:
        return pd.DataFrame()

    return pd.DataFrame(all_records)

def save_to_postgres(df: pd.DataFrame, engine, table_name: str):
    """Salva o DataFrame final no PostgreSQL."""
    print(f"Salvando dados na tabela 'public.{table_name}'...")
    try:
        df.to_sql(
            table_name,
            engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=5000
        )
        print(f"SUCESSO: Tabela '{table_name}' criada/atualizada com {len(df):,} registros.")
    except Exception as e:
        print(f"ERRO: Falha ao salvar os dados no banco: {e}")
        raise

def main():
    """Função principal para orquestrar a criação da tabela raw_ies_api."""
    print("INICIANDO GERAÇÃO DA TABELA RAW_IES")
    print("=" * 50)

    # Constantes da API
    RESOURCE_ID = '62f82787-3f45-4b9e-8457-3366f60c264b'
    API_URL = 'https://dadosabertos.capes.gov.br/api/3/action/datastore_search'
    TABLE_NAME = 'raw_ies'

    try:
        # 1. Conectar ao banco de dados
        engine = get_db_engine( )

        # 2. Extrair dados da API
        df_raw = fetch_all_from_api(resource_id=RESOURCE_ID, api_url=API_URL)
        if df_raw.empty:
            print("AVISO: Nenhum dado foi extraído da API. O processo será encerrado.")
            return

        # 3. Processar e limpar os dados
        print("Processando e limpando os dados extraídos...")
        df_padronizado = padronizar_nomes_colunas(df_raw)
        df_limpo = limpar_dataframe(df_padronizado)
        
        # 4. Definir tipos de dados
        schema_ies = {
            'an_base': 'Int64',
            'cod_entidade_capes': 'Int64',
            'cod_entidade_emec': 'Int64',
            'ano_inicio_programa': 'Int64',
            'cod_conceito_programa': 'Int64'
        }
        df_final = definir_schema_tabela(df_limpo, schema_ies)
        
        print(f"Processamento concluído. Total de {len(df_final):,} registros prontos para salvar.")

        # 5. Salvar no PostgreSQL
        save_to_postgres(df_final, engine, table_name=TABLE_NAME)

    except Exception as e:
        print(f"\nO processo falhou. Motivo: {e}")
    
    print("\nProcesso concluído.")

if __name__ == "__main__":
    main()
