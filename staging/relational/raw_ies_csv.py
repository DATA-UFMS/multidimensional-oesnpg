#!/usr/bin/env python3
"""
Script simplificado para criar tabela raw_ies.
Lê dados do CSV de PPG e extrai informações únicas de IES.
"""

import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Configuração do banco via .env
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME") 
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

# Caminho para o arquivo CSV (absoluto)
script_dir = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(script_dir, '..', 'ppg_2024.csv')

def main():
    print("🏗️ CRIANDO TABELA RAW_IES")
    print("=" * 50)
    
    # Carregar dados
    print("📥 Carregando dados do CSV PPG...")
    
    # Tentar diferentes encodings
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
    df = None
    
    for encoding in encodings:
        try:
            # Tentar com separador padrão primeiro
            df = pd.read_csv(CSV_PATH, encoding=encoding)
            print(f"✅ CSV carregado com encoding: {encoding}")
            break
        except (UnicodeDecodeError, FileNotFoundError):
            continue
        except:
            # Se falhar, tentar com separador de ponto e vírgula
            try:
                df = pd.read_csv(CSV_PATH, sep=';', encoding=encoding)
                print(f"✅ CSV carregado com encoding: {encoding} e separador ';'")
                break
            except:
                continue
    
    if df is None:
        print(f"❌ Não foi possível carregar o CSV: {CSV_PATH}")
        print(f"📁 Arquivo existe? {os.path.exists(CSV_PATH)}")
        return
    
    # Extrair dados únicos de IES do PPG
    print("🔄 Extraindo dados únicos de IES...")
    
    # Colunas relacionadas às IES no arquivo PPG
    ies_columns = [
        'Codigo capes da IES', 
        'Nome da IES', 
        'UF da IES',
        'Nome da Região da IES',
        'Status Jurídico da IES',
        'Nota do PPG'
    ]
    
    # Verificar quais colunas existem no DataFrame
    existing_columns = [col for col in ies_columns if col in df.columns]
    
    if not existing_columns:
        print("❌ Nenhuma coluna de IES encontrada no arquivo")
        return
    
    # Criar DataFrame com dados únicos de IES
    df_ies = df[existing_columns].drop_duplicates().reset_index(drop=True)
    
    # Remover colunas com valores todos nulos
    df_ies = df_ies.dropna(axis=1, how='all')
    
    # Remover duplicatas por codigo_capes_da_ies (manter o melhor conceito)
    df_ies.columns = df_ies.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('-', '_')
    
    # Limpar strings
    for col in df_ies.columns:
        if df_ies[col].dtype == 'object':
            df_ies[col] = df_ies[col].astype(str).str.strip()
    
    if 'nota_do_ppg' in df_ies.columns:
        df_ies['conceito_num'] = pd.to_numeric(df_ies['nota_do_ppg'], errors='coerce')
        df_ies = df_ies.sort_values(['codigo_capes_da_ies', 'conceito_num'], ascending=[True, False], na_position='last')
        df_ies = df_ies.drop_duplicates(subset=['codigo_capes_da_ies'], keep='first')
        df_ies = df_ies.drop(columns=['conceito_num'])
    else:
        df_ies = df_ies.drop_duplicates()
    
    print(f"📊 Dados processados: {len(df_ies)} IES únicas")
    
    # Salvar no banco usando pandas to_sql
    print("💾 Salvando no PostgreSQL...")
    
    # Criar conexão SQLAlchemy para usar com to_sql
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    
    # Usar to_sql do pandas
    df_ies.to_sql('raw_ies', engine, if_exists='replace', index=False, method='multi')
    
    print(f"✅ Tabela raw_ies criada com {len(df_ies)} registros")
    print(f"🎉 Processo concluído!")

if __name__ == "__main__":
    main()
