#!/usr/bin/env python3
"""
Script simplificado para criar tabela raw_ies_api.
Lê dados brutos da API CAPES de IES/programas e salva no PostgreSQL.
"""

import pandas as pd
from sqlalchemy import create_engine
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Adicionar diretório raiz ao path para imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

from src.core.core import fetch_all_from_api

# Configuração do banco via .env
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME") 
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

# Carregar variáveis de ambiente
load_dotenv()

def extrair_dados_brutos_ies_api():
    """
    Extrai dados brutos das IES diretamente da API CAPES.
    Mantém estrutura original sem transformações.
    """
    print("📡 Extraindo dados BRUTOS das IES da API CAPES...")
    print("🔍 Mantendo estrutura original da API")
    
    # Resource ID para IES na API da CAPES
    RESOURCE_ID = '62f82787-3f45-4b9e-8457-3366f60c264b'
    API_URL = 'https://dadosabertos.capes.gov.br/api/3/action/datastore_search'
    
    try:
        # Buscar TODOS os dados da API
        print(f"🚀 Iniciando extração da API: {API_URL}")
        print(f"📋 Resource ID: {RESOURCE_ID}")
        
        df_raw = fetch_all_from_api(RESOURCE_ID)
        
        if df_raw.empty:
            print("❌ Nenhum dado retornado da API CAPES!")
            return pd.DataFrame()
        
        print(f"📊 Dados brutos extraídos: {len(df_raw):,} registros")
        print(f"🔢 Colunas encontradas: {len(df_raw.columns)}")
        
        # Padronizar nomes das colunas (snake_case minúsculo)
        print("🔄 Padronizando nomes das colunas...")
        df_raw.columns = df_raw.columns.str.lower().str.strip()
        
        # Mostrar estrutura dos dados padronizados
        print(f"\n📋 Estrutura dos dados padronizados:")
        for i, col in enumerate(df_raw.columns, 1):
            tipo = str(df_raw[col].dtype)
            nulos = df_raw[col].isnull().sum()
            únicos = df_raw[col].nunique()
            print(f"  {i:2d}. {col:<40} | {tipo:<10} | {nulos:>6} nulos | {únicos:>8} únicos")
        
        # Remover colunas completamente vazias
        df_raw = df_raw.dropna(axis=1, how='all')
        
        print(f"\n✅ Dados preparados: {len(df_raw):,} registros")
        print(f"🔢 Colunas finais: {len(df_raw.columns)}")
        
        return df_raw
        
    except Exception as e:
        print(f"❌ Erro ao extrair dados da API CAPES: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def analisar_dados_brutos(df_raw):
    """
    Análise rápida dos dados brutos para entender a estrutura.
    """
    if df_raw.empty:
        print("⚠️ Nenhum dado para analisar")
        return
    
    print(f"\n🔍 ANÁLISE DOS DADOS BRUTOS:")
    print(f"=" * 50)
    
    # Estatísticas gerais
    print(f"📊 Estatísticas gerais:")
    print(f"  • Total de registros: {len(df_raw):,}")
    print(f"  • Total de colunas: {len(df_raw.columns)}")
    print(f"  • Memória utilizada: {df_raw.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Campos com mais dados únicos (provavelmente chaves)
    print(f"\n🔑 Campos com mais valores únicos (possíveis chaves):")
    campos_unicos = df_raw.select_dtypes(exclude=['datetime64[ns]']).nunique().sort_values(ascending=False).head(10)
    for campo, unicos in campos_unicos.items():
        pct = (unicos / len(df_raw)) * 100
        print(f"  • {campo:<35}: {unicos:>6} únicos ({pct:5.1f}%)")
    
    # Campos com mais valores nulos
    print(f"\n❓ Campos com mais valores nulos:")
    campos_nulos = df_raw.isnull().sum().sort_values(ascending=False).head(10)
    for campo, nulos in campos_nulos.items():
        if nulos > 0:
            pct = (nulos / len(df_raw)) * 100
            print(f"  • {campo:<35}: {nulos:>6} nulos ({pct:5.1f}%)")
    
    # Amostra dos dados
    print(f"\n📋 Amostra dos dados (5 primeiros registros):")
    # Mostrar apenas algumas colunas principais para não sobrecarregar
    colunas_principais = []
    for col in df_raw.columns:
        if any(palavra in col.lower() for palavra in ['nome', 'sigla', 'uf', 'regiao', 'municipio', 'codigo']):
            colunas_principais.append(col)
    
    if colunas_principais:
        print(df_raw[colunas_principais[:8]].head().to_string(index=False))
    else:
        print(df_raw.head(3).to_string(index=False))

def salvar_dados_brutos_ies(df_raw):
    """
    Salva os dados brutos das IES no banco de dados.
    """
    if df_raw.empty:
        print("⚠️ Nenhum dado para salvar")
        return
    
    try:
        print("💾 Salvando no PostgreSQL...")
        
        # Criar conexão SQLAlchemy para usar com to_sql
        engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        
        # Usar to_sql do pandas (seguindo padrão dos outros raw_)
        df_raw.to_sql('raw_ies_ppg_api', engine, if_exists='replace', index=False, method='multi')
        
        print(f"✅ Tabela raw_ies_api criada com {len(df_raw):,} registros")
            
    except Exception as e:
        print(f"❌ Erro ao salvar dados brutos: {e}")
        import traceback
        traceback.print_exc()

def verificar_qualidade_dados(df_raw):
    """
    Verifica a qualidade dos dados extraídos.
    """
    print(f"\n🔍 VERIFICAÇÃO DE QUALIDADE:")
    print(f"=" * 50)
    
    # Verificar campos essenciais (nomes padronizados)
    campos_essenciais = ['nm_entidade_ensino', 'sg_uf_programa', 'ds_dependencia_administrativa']
    for campo in campos_essenciais:
        if campo in df_raw.columns:
            total = len(df_raw)
            validos = df_raw[campo].notna().sum()
            vazios = total - validos
            pct_valido = (validos / total) * 100
            
            status = "✅" if pct_valido >= 90 else "⚠️" if pct_valido >= 70 else "❌"
            print(f"  {status} {campo:<35}: {validos:>6}/{total} válidos ({pct_valido:5.1f}%)")
        else:
            print(f"  ❌ {campo:<35}: Campo não encontrado")
    
    # Verificar duplicatas
    print(f"\n🔄 Verificação de duplicatas:")
    if 'nm_entidade_ensino' in df_raw.columns:
        duplicatas = df_raw.duplicated(subset=['nm_entidade_ensino']).sum()
        pct_dup = (duplicatas / len(df_raw)) * 100
        status = "✅" if pct_dup < 5 else "⚠️" if pct_dup < 10 else "❌"
        print(f"  {status} Por nome da IES: {duplicatas:>6} duplicatas ({pct_dup:5.1f}%)")
    
    # Verificar cobertura geográfica
    print(f"\n🗺️ Cobertura geográfica:")
    if 'sg_uf_programa' in df_raw.columns:
        ufs_unicas = df_raw['sg_uf_programa'].nunique()
        print(f"  📍 Estados únicos: {ufs_unicas} (esperado: ~27)")
        
        if ufs_unicas > 0:
            print(f"  🏆 Top 5 estados com mais registros:")
            top_ufs = df_raw['sg_uf_programa'].value_counts().head(5)
            for uf, count in top_ufs.items():
                print(f"    • {uf}: {count:,}")

def main():
    print("🏗️ CRIANDO TABELA RAW_IES_API")
    print("=" * 50)
    
    # 1. Extrair dados brutos da API
    df_raw = extrair_dados_brutos_ies_api()
    
    if df_raw.empty:
        print("❌ Nenhum dado foi extraído da API. Encerrando o processo.")
        return
    
    # 2. Analisar estrutura dos dados
    analisar_dados_brutos(df_raw)
    
    # 3. Verificar qualidade dos dados
    verificar_qualidade_dados(df_raw)
    
    # 4. Salvar dados brutos no banco
    salvar_dados_brutos_ies(df_raw)
    
    # 5. Relatório final
    print(f"\n📋 RELATÓRIO FINAL:")
    print(f"=" * 30)
    print(f"✅ Tabela raw_ies_api criada com sucesso!")
    print(f"📊 Total de registros: {len(df_raw):,}")
    print(f"🔢 Total de colunas: {len(df_raw.columns)}")
    print(f"📅 Data de criação: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Status: PRONTO PARA USO")

if __name__ == "__main__":
    main()
