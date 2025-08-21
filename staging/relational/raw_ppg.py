#!/usr/bin/env python3
"""
Script simplificado para criar tabela raw_ppg.
Lê dados brutos do CSV de programas de pós-graduação e salva no PostgreSQL.
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
CSV_PATH = os.path.join(script_dir, '..', 'data', 'ppg_2024.csv')

def main():
    print("CRIANDO TABELA RAW_PPG")
    print("=" * 50)
    
    # Carregar dados
    print("Carregando dados do CSV...")
    
    if not os.path.exists(CSV_PATH):
        print(f"❌ Arquivo CSV não encontrado: {CSV_PATH}")
        return
    
    # Tentar diferentes encodings e separadores
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
    separators = [',', ';', '\t']
    df = None
    
    for encoding in encodings:
        for sep in separators:
            try:
                df = pd.read_csv(CSV_PATH, sep=sep, encoding=encoding)
                # Verificar se carregou dados válidos (mais de 1 coluna)
                if len(df.columns) > 1 and len(df) > 0:
                    print(f"✅ CSV carregado com encoding: {encoding}, separador: '{sep}'")
                    break
            except (UnicodeDecodeError, pd.errors.EmptyDataError, pd.errors.ParserError):
                continue
        if df is not None and len(df.columns) > 1:
            break
    
    if df is None or len(df.columns) <= 1:
        print("❌ Não foi possível carregar o CSV com nenhum encoding/separador testado")
        return
    
    print(f"Dados brutos carregados: {len(df)} registros")
    
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
            nova_col = nova_col.replace('_nome', '_des')
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
    print(f"Registros antes de remover duplicatas: {len(df)}")
    df = df.drop_duplicates()
    print(f"Registros após remover duplicatas: {len(df)}")
    
    # Salvar no PostgreSQL
    print("Salvando no PostgreSQL...")
    
    try:
        engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        df.to_sql('raw_ppg', engine, if_exists='replace', index=False, method='multi')
        print(f"✅ Tabela raw_ppg criada com {len(df)} registros")
        
        # Mostrar primeiras colunas
        print(f"Colunas principais: {list(df.columns)[:10]}")
        print(f"Total de colunas: {len(df.columns)}")
        
    except Exception as e:
        print(f"❌ Erro ao salvar no PostgreSQL: {e}")
        return

if __name__ == "__main__":
    main()
