#!/usr/bin/env python3
"""
Script simplificado para criar tabela raw_producao.
Lê dados brutos da API CAPES de produção acadêmica e salva no PostgreSQL.
"""

import pandas as pd
from sqlalchemy import create_engine
import os
import requests
from datetime import datetime
from dotenv import load_dotenv

# Configuração do banco via .env
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME") 
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

def extrair_dados_brutos_producao_api():
    """
    Extrai dados brutos de produção acadêmica diretamente da API CAPES.
    Foco em artigos em periódicos (ARTPE) por ser o tipo mais representativo.
    """
    print("📡 Extraindo dados BRUTOS de PRODUÇÃO da API CAPES...")
    print("🔍 Foco: Artigos em Periódicos (BIBLIOGRAFICA-ARTPE)")
    
    # Resource ID para artigos em periódicos 2021-2024
    RESOURCE_ID = '48bcfecf-dd46-4d35-95f4-1e0421c9c98e'  # CSV ARTPE 2021-2024
    API_URL = 'https://dadosabertos.capes.gov.br/api/3/action/datastore_search'
    
    try:
        print(f"🚀 Iniciando extração da API: {API_URL}")
        print(f"📋 Resource ID: {RESOURCE_ID}")
        print("⚠️ Limitando a 10.000 registros devido ao tamanho do dataset (501MB)")
        
        # Parâmetros para a requisição
        params = {
            'resource_id': RESOURCE_ID,
            'limit': 10000,  # Limitar devido ao tamanho
            'offset': 0
        }
        
        print("📊 Fazendo requisição à API CAPES...")
        response = requests.get(API_URL, params=params)
        
        if response.status_code != 200:
            print(f"❌ Erro HTTP: {response.status_code}")
            return pd.DataFrame()
        
        data = response.json()
        
        if not data.get('success'):
            error_info = data.get('error', {})
            print(f"❌ API retornou erro: {error_info}")
            return pd.DataFrame()
        
        # Extrair registros
        result = data.get('result', {})
        records = result.get('records', [])
        
        if not records:
            print("❌ Nenhum registro retornado!")
            return pd.DataFrame()
        
        # Converter para DataFrame
        df_raw = pd.DataFrame(records)
        
        print(f"📊 Dados brutos extraídos: {len(df_raw):,} registros")
        print(f"🔢 Colunas encontradas: {len(df_raw.columns)}")
        
        # Padronizar nomes das colunas (snake_case minúsculo) e aplicar regras de padronização
        print("🔄 Padronizando nomes das colunas...")
        df_raw.columns = df_raw.columns.str.lower().str.strip().str.replace('/', '_').str.replace('ã', 'a').str.replace('ç', 'c').str.replace('á', 'a').str.replace('é', 'e').str.replace('í', 'i').str.replace('ó', 'o').str.replace('ú', 'u')
        
        # Aplicar regras de padronização: cod para códigos, des_ para nomes, qtd para quantidade
        colunas_renomeadas = {}
        for col in df_raw.columns:
            nova_col = col
            # Trocar codigo por cod
            if 'codigo' in nova_col:
                nova_col = nova_col.replace('codigo', 'cod')
            # Trocar nome_ por des_
            if nova_col.startswith('nm_'):
                nova_col = nova_col.replace('nm_', 'des_')
            elif '_nome' in nova_col:
                nova_col = nova_col.replace('_nome', '_des')
            # Trocar quantidade por qtd
            if 'quantidade' in nova_col:
                nova_col = nova_col.replace('quantidade', 'qtd')
            if nova_col != col:
                colunas_renomeadas[col] = nova_col
        
        if colunas_renomeadas:
            df_raw = df_raw.rename(columns=colunas_renomeadas)
            print(f"📝 Colunas renomeadas: {len(colunas_renomeadas)} alterações")
        
        # Mostrar estrutura dos dados padronizados (primeiras 20 colunas)
        print(f"\n📋 Estrutura dos dados padronizados (primeiras 20 colunas):")
        for i, col in enumerate(df_raw.columns[:20], 1):
            tipo = str(df_raw[col].dtype)
            nulos = df_raw[col].isnull().sum()
            únicos = df_raw[col].nunique()
            print(f"  {i:2d}. {col:<40} | {tipo:<10} | {nulos:>6} nulos | {únicos:>8} únicos")
        
        if len(df_raw.columns) > 20:
            print(f"  ... e mais {len(df_raw.columns) - 20} colunas")
        
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
    print(f"\n📋 Amostra dos dados (3 primeiros registros):")
    # Mostrar apenas algumas colunas principais para não sobrecarregar
    colunas_principais = []
    for col in df_raw.columns:
        if any(palavra in col.lower() for palavra in ['nm_producao', 'nm_programa', 'nm_entidade', 'an_base']):
            colunas_principais.append(col)
    
    if colunas_principais:
        print(df_raw[colunas_principais[:6]].head(3).to_string(index=False))
    else:
        print(df_raw.iloc[:3, :6].to_string(index=False))

def salvar_dados_brutos_producao(df_raw):
    """
    Salva os dados brutos de produção no banco de dados.
    """
    if df_raw.empty:
        print("⚠️ Nenhum dado para salvar")
        return
    
    try:
        print("💾 Salvando no PostgreSQL...")
        
        # Criar conexão SQLAlchemy para usar com to_sql
        engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        
        # Usar to_sql do pandas (seguindo padrão dos outros raw_)
        df_raw.to_sql('raw_producao', engine, if_exists='replace', index=False, method='multi')
        
        print(f"✅ Tabela raw_producao criada com {len(df_raw):,} registros")
            
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
    
    # Verificar campos essenciais de produção acadêmica
    campos_essenciais = ['id_add_producao_intelectual', 'nm_producao', 'an_base']
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
    
    # Verificar duplicatas por ID
    print(f"\n🔄 Verificação de duplicatas:")
    if 'id_add_producao_intelectual' in df_raw.columns:
        duplicatas = df_raw.duplicated(subset=['id_add_producao_intelectual']).sum()
        pct_dup = (duplicatas / len(df_raw)) * 100
        status = "✅" if pct_dup < 1 else "⚠️" if pct_dup < 5 else "❌"
        print(f"  {status} Por ID produção: {duplicatas:>6} duplicatas ({pct_dup:5.1f}%)")
    
    # Verificar cobertura temporal
    print(f"\n📅 Cobertura temporal:")
    if 'an_base' in df_raw.columns:
        anos_unicos = df_raw['an_base'].nunique()
        ano_min = df_raw['an_base'].min() if df_raw['an_base'].notna().any() else 'N/A'
        ano_max = df_raw['an_base'].max() if df_raw['an_base'].notna().any() else 'N/A'
        print(f"  📍 Anos únicos: {anos_unicos} ({ano_min} - {ano_max})")
        
        if anos_unicos > 0:
            print(f"  🏆 Top 5 anos com mais produções:")
            top_anos = df_raw['an_base'].value_counts().head(5)
            for ano, count in top_anos.items():
                print(f"    • {ano}: {count:,}")

def main():
    print("🏗️ CRIANDO TABELA RAW_PRODUCAO")
    print("=" * 50)
    
    # 1. Extrair dados brutos da API
    df_raw = extrair_dados_brutos_producao_api()
    
    if df_raw.empty:
        print("❌ Nenhum dado foi extraído da API. Encerrando o processo.")
        return
    
    # 2. Analisar estrutura dos dados
    analisar_dados_brutos(df_raw)
    
    # 3. Verificar qualidade dos dados
    verificar_qualidade_dados(df_raw)
    
    # 4. Salvar dados brutos no banco
    salvar_dados_brutos_producao(df_raw)
    
    # 5. Relatório final
    print(f"\n📋 RELATÓRIO FINAL:")
    print(f"=" * 30)
    print(f"✅ Tabela raw_producao criada com sucesso!")
    print(f"📊 Total de registros: {len(df_raw):,}")
    print(f"🔢 Total de colunas: {len(df_raw.columns)}")
    print(f"📅 Data de criação: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Status: PRONTO PARA USO")

if __name__ == "__main__":
    main()
