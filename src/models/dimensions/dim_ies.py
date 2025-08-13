import pandas as pd
from sqlalchemy import create_engine
import os
import sys
from dotenv import load_dotenv

# Adicionar diretório raiz ao path para imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from src.core.core import fetch_all_from_api, salvar_df_bd

# Carregar variáveis de ambiente
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

def extrair_dados_ies():
    """
    Extrai dados das Instituições de Ensino Superior EXCLUSIVAMENTE da API da CAPES.
    """
    print("🏛️ Extraindo dados das IES APENAS da API CAPES...")
    
    # Resource ID para IES na API da CAPES
    RESOURCE_ID = '62f82787-3f45-4b9e-8457-3366f60c264b'
    
    try:
        # Buscar dados da API - ÚNICA fonte de dados
        df_raw = fetch_all_from_api(RESOURCE_ID)
        
        if df_raw.empty:
            print("❌ Nenhum dado retornado da API CAPES!")
            return pd.DataFrame()
        
        print(f"📊 Dados brutos da API: {len(df_raw)} registros")
        
        # Normalizar nomes das colunas
        df_raw.columns = df_raw.columns.str.lower()
        
        # Mapear colunas da API CAPES para nossa estrutura
        colunas_mapeamento = {
            'nm_entidade_ensino': 'nome_ies',
            'sg_entidade_ensino': 'sigla',
            'ds_dependencia_administrativa': 'categoria_administrativa',
            'sg_uf_programa': 'sigla_uf',
            'nm_municipio_programa_ies': 'municipio',
            'nm_regiao': 'regiao',
            'cd_entidade_emec': 'codigo_emec'
        }
        
        # Verificar quais colunas existem na API
        colunas_existentes = {}
        for col_original, col_nova in colunas_mapeamento.items():
            if col_original in df_raw.columns:
                colunas_existentes[col_original] = col_nova
            else:
                print(f"⚠️ Coluna não encontrada na API: {col_original}")
        
        if not colunas_existentes:
            print("❌ Nenhuma coluna esperada encontrada na API!")
            return pd.DataFrame()
        
        # Selecionar apenas colunas existentes da API
        df_ies = df_raw[list(colunas_existentes.keys())].copy()
        df_ies = df_ies.rename(columns=colunas_existentes)
        
        # Tratar e limpar dados da API
        df_ies = tratar_dados_ies_api(df_ies)
        
        # Adicionar registro SK=0 (desconhecido/não aplicável)
        registro_sk0 = criar_registro_sk0_ies()
        df_ies = pd.concat([registro_sk0, df_ies], ignore_index=True)
        
        # Adicionar surrogate key (começando do 0)
        df_ies.insert(0, 'ies_sk', range(0, len(df_ies)))
        
        print(f"✅ IES processadas da API CAPES: {len(df_ies)} registros")
        
        return df_ies
        
    except Exception as e:
        print(f"❌ Erro ao extrair dados da API CAPES: {e}")
        return pd.DataFrame()

def tratar_dados_ies_api(df_ies):
    """
    Trata e limpa os dados das IES vindos da API CAPES.
    """
    try:
        print("🔧 Tratando dados das IES da API...")
        
        # Remover linhas com valores nulos nos campos essenciais
        if 'nome_ies' in df_ies.columns:
            antes = len(df_ies)
            df_ies = df_ies.dropna(subset=['nome_ies'])
            depois = len(df_ies)
            print(f"📊 Removidos {antes - depois} registros sem nome da IES")
        
        # Normalizar textos para uppercase
        colunas_texto = ['nome_ies', 'sigla', 'categoria_administrativa', 'sigla_uf', 'municipio', 'regiao']
        for col in colunas_texto:
            if col in df_ies.columns:
                df_ies[col] = df_ies[col].astype(str).str.strip().str.upper()
        
        # Tratar campos numéricos
        if 'codigo_emec' in df_ies.columns:
            df_ies['codigo_emec'] = pd.to_numeric(df_ies['codigo_emec'], errors='coerce').fillna(0).astype(int)
        
        # Preencher valores nulos com padrões
        df_ies = df_ies.fillna({
            'sigla': 'SEM SIGLA',
            'categoria_administrativa': 'NÃO INFORMADO',
            'sigla_uf': 'XX',
            'municipio': 'NÃO INFORMADO',
            'regiao': 'NÃO INFORMADO',
            'codigo_emec': 0
        })
        
        # Remover duplicatas baseado no nome da IES e UF
        campos_dedup = ['nome_ies']
        if 'sigla_uf' in df_ies.columns:
            campos_dedup.append('sigla_uf')
            
        antes = len(df_ies)
        df_ies = df_ies.drop_duplicates(subset=campos_dedup, keep='first')
        depois = len(df_ies)
        print(f"📊 Removidas {antes - depois} IES duplicadas")
        
        # Normalizar categoria administrativa
        if 'categoria_administrativa' in df_ies.columns:
            # Mapear variações para padrões
            mapeamento_categoria = {
                'PUBLICA': 'PÚBLICA',
                'PÚBLICO': 'PÚBLICA',
                'FEDERAL': 'PÚBLICA',
                'ESTADUAL': 'PÚBLICA',
                'MUNICIPAL': 'PÚBLICA',
                'PRIVADA': 'PRIVADA',
                'PRIVADO': 'PRIVADA',
                'PARTICULAR': 'PRIVADA'
            }
            
            df_ies['categoria_administrativa'] = df_ies['categoria_administrativa'].replace(mapeamento_categoria)
        
        print(f"✅ Dados das IES tratados: {len(df_ies)} registros finais")
        
        return df_ies
        
    except Exception as e:
        print(f"❌ Erro ao tratar dados das IES: {e}")
        return df_ies

def criar_registro_sk0_ies():
    """
    Cria o registro SK=0 para valores desconhecidos/não aplicáveis.
    """
    return pd.DataFrame({
        'nome_ies': ['DESCONHECIDO'],
        'sigla': ['XX'],
        'categoria_administrativa': ['DESCONHECIDO'],
        'sigla_uf': ['XX'],
        'municipio': ['DESCONHECIDO'],
        'regiao': ['DESCONHECIDO'],
        'codigo_emec': [0]
    })

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
    print("🚀 Iniciando processo de criação da dimensão IES")
    print("📡 Fonte de dados: API CAPES (EXCLUSIVAMENTE)")
    
    # Extrair dados das IES APENAS da API
    df_ies = extrair_dados_ies()
    
    if df_ies.empty:
        print("❌ Nenhum dado foi retornado da API CAPES. Encerrando o script.")
        exit(1)
    
    # Salvar no banco
    salvar_dimensao_ies(df_ies)
    
    # Mostrar estatísticas detalhadas
    print("\n📊 Estatísticas da dimensão IES (API CAPES):")
    print(f"Total de IES: {len(df_ies):,}")
    
    if 'regiao' in df_ies.columns:
        print(f"\n🗺️ IES por região:")
        for regiao in sorted(df_ies['regiao'].unique()):
            count = len(df_ies[df_ies['regiao'] == regiao])
            print(f"  {regiao}: {count:,} IES")
    
    if 'categoria_administrativa' in df_ies.columns:
        print(f"\n🏛️ IES por categoria administrativa:")
        for categoria in sorted(df_ies['categoria_administrativa'].unique()):
            count = len(df_ies[df_ies['categoria_administrativa'] == categoria])
            print(f"  {categoria}: {count:,} IES")
    
    if 'sigla_uf' in df_ies.columns:
        print(f"\n📍 IES por UF (top 10):")
        uf_counts = df_ies['sigla_uf'].value_counts().head(10)
        for uf, count in uf_counts.items():
            print(f"  {uf}: {count:,} IES")
    
    # Verificar qualidade dos dados
    print(f"\n🔍 Qualidade dos dados:")
    if 'nome_ies' in df_ies.columns:
        sem_nome = len(df_ies[df_ies['nome_ies'].isin(['DESCONHECIDO', 'NÃO INFORMADO'])])
        print(f"  IES sem nome válido: {sem_nome:,}")
    
    if 'codigo_emec' in df_ies.columns:
        sem_codigo = len(df_ies[df_ies['codigo_emec'] == 0])
        print(f"  IES sem código EMEC: {sem_codigo:,}")
    
    print(f"\n✅ Processo concluído! Dimensão IES criada exclusivamente da API CAPES.")

