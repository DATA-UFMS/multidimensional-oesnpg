import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from models.utils import fetch_all_from_api, salvar_df_bd

# Carregar variáveis de ambiente
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

def extrair_dados_ies():
    """
    Extrai dados das Instituições de Ensino Superior da API da CAPES.
    """
    print("🏛️ Extraindo dados das IES da API CAPES...")
    
    # Resource ID para IES na API da CAPES
    RESOURCE_ID = '62f82787-3f45-4b9e-8457-3366f60c264b'
    API_URL = 'https://dadosabertos.capes.gov.br/api/3/action/datastore_search'
    
    try:
        # Buscar dados da API - só passar o resource_id
        df_raw = fetch_all_from_api(RESOURCE_ID)
        
        if df_raw.empty:
            print("⚠️ Nenhum dado retornado da API. Criando dados de exemplo...")
            return pd.DataFrame()  # Retorna DataFrame vazio se não houver dados
        
        # Normalizar nomes das colunas
        df_raw.columns = df_raw.columns.str.lower()
        
        # Selecionar e renomear colunas relevantes
        colunas_mapeamento = {
            'nm_entidade_ensino': 'nome_ies',
            'sg_entidade_ensino': 'sigla',
            'ds_dependencia_administrativa': 'categoria_administrativa',
            'sg_uf_programa': 'sigla_uf',
            'nm_municipio_programa_ies': 'municipio',
            'nm_regiao': 'regiao',
            'cd_entidade_emec': 'codigo_emec'
        }
        
        # Verificar quais colunas existem
        colunas_existentes = {}
        for col_original, col_nova in colunas_mapeamento.items():
            if col_original in df_raw.columns:
                colunas_existentes[col_original] = col_nova
        
        # Selecionar apenas colunas existentes
        df_ies = df_raw[list(colunas_existentes.keys())].copy()
        df_ies = df_ies.rename(columns=colunas_existentes)
        
        # Normalizar colunas específicas para uppercase
        if 'sigla_uf' in df_ies.columns:
            df_ies['sigla_uf'] = df_ies['sigla_uf'].astype(str).str.upper().str.strip()
        if 'regiao' in df_ies.columns:
            df_ies['regiao'] = df_ies['regiao'].astype(str).str.upper().str.strip()
        
        
        # Remover duplicatas baseado no nome da IES
        if 'nome_ies' in df_ies.columns:
            df_ies = df_ies.drop_duplicates(subset=['nome_ies'], keep='first')
        
        # Adicionar registro 0 (desconhecido/não aplicável)
        registro_desconhecido = pd.DataFrame({
            'nome_ies': ['DESCONHECIDO'],
            'sigla': ['XX'],
            'categoria_administrativa': ['DESCONHECIDO'],
            'sigla_uf': ['XX'],
            'municipio': ['DESCONHECIDO'],
            'regiao': ['DESCONHECIDO'],
            'codigo_emec': [0]
        })
        
        # Concatenar registro desconhecido com dados reais
        df_ies = pd.concat([registro_desconhecido, df_ies], ignore_index=True)
        
        # Adicionar surrogate key (começando do 0)
        df_ies.insert(0, 'ies_sk', range(0, len(df_ies)))
        
        print(f"✅ Dados das IES extraídos: {len(df_ies)} registros")
        
        return df_ies
        
    except Exception as e:
        print(f"❌ Erro ao extrair dados da API: {e}")
        return pd.DataFrame()

def salvar_dimensao_ies(df_ies):
    """
    Salva a dimensão IES no banco de dados PostgreSQL.
    """
    try:
        # Usar a nova função de salvar
        salvar_df_bd(df_ies, 'dim_ies')
        print(f"✅ Dimensão IES salva no PostgreSQL com {len(df_ies)} registros")
            
    except Exception as e:
        print(f"❌ Erro ao salvar dimensão IES: {e}")

if __name__ == "__main__":
    # Extrair dados das IES
    df_ies = extrair_dados_ies()
    
    # Salvar no banco
    salvar_dimensao_ies(df_ies)
    
    # Mostrar algumas estatísticas
    print("\n📊 Estatísticas da dimensão IES:")
    print(f"Total de IES: {len(df_ies)}")
    if 'regiao' in df_ies.columns:
        print(f"IES por região:")
        for regiao in df_ies['regiao'].unique():
            count = len(df_ies[df_ies['regiao'] == regiao])
            print(f"  {regiao}: {count} IES")
    if 'categoria_administrativa' in df_ies.columns:
        print(f"IES por categoria administrativa:")
        for categoria in df_ies['categoria_administrativa'].unique():
            count = len(df_ies[df_ies['categoria_administrativa'] == categoria])
            print(f"  {categoria}: {count} IES")

