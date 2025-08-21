#!/usr/bin/env python3
"""
Script simplificado para criar tabela raw_docente.
Lê dad    print(f"Dados processados: {len(df)} registros")
    
    # Salvar no banco usando pandas to_sql
    print("Salvando no PostgreSQL...")rutos do CSV de docentes e salva no PostgreSQL.
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
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(script_dir, '..', 'data', 'br-capes-colsucup-docente-2021-2025-03-31.csv')

def main():
    print("CRIANDO TABELA RAW_DOCENTE")
    print("=" * 50)
    
    # Carregar dados
    print("📥 Carregando dados do CSV...")
    
    # Tentar diferentes encodings
    encodings = ['latin1', 'iso-8859-1', 'cp1252', 'utf-8']
    df = None
    
    for encoding in encodings:
        try:
            df = pd.read_csv(CSV_PATH, sep=';', encoding=encoding)
            print(f"✅ CSV carregado com encoding: {encoding}")
            break
        except (UnicodeDecodeError, FileNotFoundError):
            continue
    
    if df is None:
        print(f"❌ Não foi possível carregar o CSV: {CSV_PATH}")
        print(f"📁 Arquivo existe? {os.path.exists(CSV_PATH)}")
        return
    
    # Remover colunas com valores todos nulos
    df = df.dropna(axis=1, how='all')
    
    # Ajustar nomes das colunas para padrão snake_case e aplicar regras de padronização
    df.columns = df.columns.str.strip().str.lower().str.replace('-', '_').str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('.', '').str.replace('/', '_').str.replace('ã', 'a').str.replace('ç', 'c').str.replace('á', 'a').str.replace('é', 'e').str.replace('í', 'i').str.replace('ó', 'o').str.replace('ú', 'u')
    
    # Aplicar regras de padronização: cod para códigos, des_ para nomes, qtd para quantidade
    colunas_renomeadas = {}
    for col in df.columns:
        nova_col = col
        # Trocar codigo por cod
        if 'codigo' in nova_col:
            nova_col = nova_col.replace('codigo', 'cod')
        # Trocar nome_ por des_
        if nova_col.startswith('nome_'):
            nova_col = nova_col.replace('nome_', 'des_')
        elif '_nome' in nova_col:
            nova_col = nova_col.replace('_nome', '')
        # Trocar quantidade por qtd
        if 'quantidade' in nova_col:
            nova_col = nova_col.replace('quantidade', 'qtd')
        if nova_col != col:
            colunas_renomeadas[col] = nova_col
    
    if colunas_renomeadas:
        df = df.rename(columns=colunas_renomeadas)
        print(f"Colunas renomeadas: {len(colunas_renomeadas)} alterações")
    
    # Limpar strings
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.strip()
    
    # Remover duplicatas
    df = df.drop_duplicates()
    
    print(f"📊 Dados processados: {len(df)} registros")
    
    # Salvar no banco usando pandas to_sql
    print("� Salvando no PostgreSQL...")
    
    # Criar conexão SQLAlchemy para usar com to_sql
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    
    # Usar to_sql do pandas
    df.to_sql('raw_docente', engine, if_exists='replace', index=False, method='multi')
    
    print(f"✅ Tabela raw_docente criada com {len(df)} registros")
    print(f"🎉 Processo concluído!")

if __name__ == "__main__":
    main()
