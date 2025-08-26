#!/usr/bin/env python3
"""
Módulo base para scripts de extração de dados raw.
Contém funcionalidades comuns reutilizáveis.
"""

import pandas as pd
from sqlalchemy import create_engine
import os
import requests
import time
from dotenv import load_dotenv
from datetime import datetime

# Configurações globais
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME") 
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

class DatabaseManager:
    """Gerenciador de conexão com o banco de dados"""
    
    def __init__(self):
        self.engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    
    def save_dataframe(self, df, table_name, if_exists='replace'):
        """Salva DataFrame no PostgreSQL"""
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False, method='multi')
            return True
        except Exception as e:
            print(f"❌ Erro ao salvar na tabela {table_name}: {e}")
            return False
    
    def execute_query(self, query):
        """Executa query SQL"""
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            print(f"❌ Erro ao executar query: {e}")
            return pd.DataFrame()

class CAPESApiExtractor:
    """Extrator de dados da API CAPES"""
    
    BASE_URL = 'https://dadosabertos.capes.gov.br/api/3/action/datastore_search'
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'DataExtractor/1.0'
        })
    
    def fetch_all_data(self, resource_id, limit=1000):
        """
        Busca todos os dados de um resource_id da API CAPES
        """
        all_data = []
        offset = 0
        total = None
        
        print(f"Extraindo dados do resource_id: {resource_id}")
        
        while True:
            params = {
                'resource_id': resource_id,
                'limit': limit,
                'offset': offset
            }
            
            try:
                response = self.session.get(self.BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                if not data.get('success'):
                    print(f"❌ API retornou erro: {data.get('error', 'Erro desconhecido')}")
                    break
                
                result = data.get('result', {})
                records = result.get('records', [])
                
                if not records:
                    break
                
                all_data.extend(records)
                
                if total is None:
                    total = result.get('total', 0)
                    print(f"Total de registros disponíveis: {total:,}")
                
                offset += len(records)
                print(f"Progresso: {len(all_data):,}/{total:,} registros ({(len(all_data)/total)*100:.1f}%)")
                
                if len(records) < limit:
                    break
                
                time.sleep(0.1)  # Rate limiting
                
            except Exception as e:
                print(f"❌ Erro na requisição (offset {offset}): {e}")
                time.sleep(5)
                continue
        
        if all_data:
            df = pd.DataFrame(all_data)
            print(f"✅ Total extraído: {len(df):,} registros")
            return df
        else:
            print("⚠️ Nenhum dado extraído")
            return pd.DataFrame()

class DataQualityAnalyzer:
    """Analisador de qualidade de dados"""
    
    @staticmethod
    def analyze_dataframe(df, name="Dados"):
        """Analisa a qualidade de um DataFrame"""
        if df.empty:
            print(f"⚠️ {name}: DataFrame vazio")
            return
        
        print(f"\nANÁLISE DE QUALIDADE - {name.upper()}")
        print("=" * 50)
        
        # Estatísticas básicas
        print(f"Registros: {len(df):,}")
        print(f"Colunas: {len(df.columns)}")
        print(f"Memória: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        # Campos com mais dados únicos
        print(f"\nCampos com mais valores únicos:")
        unique_counts = df.nunique().sort_values(ascending=False).head(5)
        for col, count in unique_counts.items():
            percentage = (count / len(df)) * 100
            print(f"  • {col:<30}: {count:>8} únicos ({percentage:5.1f}%)")
        
        # Campos com mais valores nulos
        print(f"\nCampos com mais valores nulos:")
        null_counts = df.isnull().sum().sort_values(ascending=False).head(5)
        for col, count in null_counts.items():
            if count > 0:
                percentage = (count / len(df)) * 100
                print(f"  • {col:<30}: {count:>8} nulos ({percentage:5.1f}%)")
        
        # Duplicatas
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            print(f"\n⚠️ Duplicatas encontradas: {duplicates:,}")
        else:
            print(f"\n✅ Sem duplicatas")

class DataCleaner:
    """Limpador de dados comum"""
    
    @staticmethod
    def clean_dataframe(df):
        """Aplica limpezas básicas no DataFrame"""
        print("Aplicando limpezas básicas...")
        
        # Padronizar nomes das colunas
        df.columns = df.columns.str.lower().str.strip()
        df.columns = df.columns.str.replace(r'[^\w]', '_', regex=True)
        df.columns = df.columns.str.replace(r'_+', '_', regex=True)
        df.columns = df.columns.str.strip('_')
        
        # Limpar strings
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
                df.loc[df[col].isin(['', 'nan', 'None', 'null']), col] = None
        
        # Remover colunas completamente vazias
        df = df.dropna(axis=1, how='all')
        
        return df

def print_header(title):
    """Imprime cabeçalho padronizado"""
    print(f"\n{title.upper()}")
    print("=" * 50)

def print_status(message, status="info"):
    """Imprime mensagem com status"""
    icons = {
        "success": "✅",
        "warning": "⚠️", 
        "error": "❌",
        "info": "ℹ️"
    }
    icon = icons.get(status, "•")
    print(f"{icon} {message}")

def print_summary(total_records, table_name):
    """Imprime resumo final"""
    print(f"\nRESUMO FINAL")
    print("=" * 30)
    print_status(f"Tabela {table_name} criada com {total_records:,} registros", "success")
    print_status(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "info")
