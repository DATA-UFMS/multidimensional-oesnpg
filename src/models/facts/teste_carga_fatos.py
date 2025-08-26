#!/usr/bin/env python3
"""
Script para testar carga de tabelas fato
Versão corrigida com verificações e tratamento de erros
"""

import pandas as pd
from sqlalchemy import create_engine, text, inspect
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def get_engine():
    """Função para criar a conexão com o banco de dados com validação."""
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASS')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT', '5432')  # Valor padrão se não definido
    db_name = os.getenv('DB_NAME')
    
    # Validar se todas as variáveis estão definidas
    if not all([user, password, host, db_name]):
        raise ValueError("Variáveis de ambiente do banco não estão todas definidas!")
    
    # Garantir que port seja válido
    try:
        port = int(port) if port and port.lower() != 'none' else 5432
    except ValueError:
        port = 5432
        print(f"⚠️ Porta inválida, usando padrão: {port}")
    
    db_uri = f'postgresql://{user}:{password}@{host}:{port}/{db_name}'
    print(f"Conectando em: postgresql://{user}:***@{host}:{port}/{db_name}")
    return create_engine(db_uri)

def verificar_estrutura_banco(engine):
    """Verifica a estrutura do banco e mostra tabelas disponíveis."""
    print("Verificando estrutura do banco...\n")
    
    inspector = inspect(engine)
    schemas = inspector.get_schema_names()
    
    # Listar todas as tabelas encontradas
    todas_tabelas = []
    for schema in schemas:
        tabelas = inspector.get_table_names(schema=schema)
        for tabela in tabelas:
            todas_tabelas.append(f"{schema}.{tabela}")
    
    print("Tabelas encontradas:")
    for tabela in sorted(todas_tabelas):
        print(f"  {tabela}")
    
    # Mostrar estrutura das dimensões principais
    tabelas_interesse = ['dim_docente', 'dim_ies', 'dim_localidade', 'dim_ods', 
                        'dim_ppg', 'dim_producao', 'dim_tema', 'dim_tempo']
    
    for tabela in tabelas_interesse:
        if f'public.{tabela}' in todas_tabelas:
            print(f"\nColunas de public.{tabela}:")
            colunas = inspector.get_columns(tabela, schema='public')
            for col in colunas:
                print(f"  - {col['name']} ({col['type']})")
    
    return todas_tabelas

def ler_tabelas(engine):
    """Lê tabelas disponíveis e retorna em um dicionário."""
    print("\nLendo tabelas disponíveis...")
    tabelas = {}
    
    # Lista de tabelas para tentar ler
    tabelas_para_ler = {
        'raw_docente': "SELECT * FROM public.raw_docente LIMIT 100",
        'raw_ppg': "SELECT * FROM public.raw_ppg LIMIT 100", 
        'raw_tema': "SELECT * FROM public.raw_tema LIMIT 100",
        'dim_docente': "SELECT * FROM public.dim_docente LIMIT 10",
        'dim_ppg': "SELECT * FROM public.dim_ppg LIMIT 10",
        'dim_tempo': "SELECT * FROM public.dim_tempo LIMIT 10",
        'dim_ies': "SELECT * FROM public.dim_ies LIMIT 10",
        'dim_tema': "SELECT * FROM public.dim_tema LIMIT 10"
    }
    
    for nome, query in tabelas_para_ler.items():
        try:
            df = pd.read_sql(query, engine)
            tabelas[nome] = df
            print(f"✅ {nome}: {len(df)} registros")
            if len(df) > 0:
                print(f"    Colunas: {list(df.columns)}")
        except Exception as e:
            print(f"❌ {nome}: Erro - {e}")
    
    print(f"\nTotal de tabelas carregadas: {len(tabelas)}")
    return tabelas

def construir_fato_simples(tabelas):
    """Constrói uma tabela fato simples baseada nos dados disponíveis."""
    print("\nConstruindo fato simples...")
    
    # Mostrar o que temos disponível
    print("Tabelas disponíveis para construção:")
    for nome, df in tabelas.items():
        if not df.empty:
            print(f"  {nome}: {len(df)} registros, {len(df.columns)} colunas")
    
    # Começar com a tabela que tem mais dados
    if 'raw_docente' in tabelas and not tabelas['raw_docente'].empty:
        df_base = tabelas['raw_docente'].copy()
        print(f"Usando raw_docente como base: {list(df_base.columns[:10])}...")
        
        # Tentar complementar com raw_ppg se disponível
        if 'raw_ppg' in tabelas and not tabelas['raw_ppg'].empty:
            df_ppg = tabelas['raw_ppg'].copy()
            print(f"Complementando com raw_ppg: {list(df_ppg.columns[:10])}...")
        
        # Criar uma fato básica agregada
        df_fato = pd.DataFrame({
            'ano_referencia': [df_base['an_base'].mode().iloc[0] if 'an_base' in df_base.columns else 2024],
            'total_docentes': [len(df_base)],
            'data_carga': [datetime.now()],
            'total_ppgs': [tabelas['raw_ppg']['código_do_ppg'].nunique() if 'raw_ppg' in tabelas and 'código_do_ppg' in tabelas['raw_ppg'].columns else 0]
        })
        
    else:
        # Fato mínima se não temos dados suficientes
        df_fato = pd.DataFrame({
            'ano_referencia': [2024],
            'total_registros': [sum(len(df) for df in tabelas.values())],
            'data_carga': [datetime.now()]
        })
    
    print(f"✅ Fato construída: {len(df_fato)} registros")
    print(f"    Colunas: {list(df_fato.columns)}")
    return df_fato

def carregar_dados_teste(engine, nome_tabela, df_dados):
    """Cria e carrega dados na tabela de teste."""
    try:
        print("\nCriando estrutura de teste...")
        
        # Tentar criar usando pandas to_sql
        df_dados.to_sql(
            name=f'teste_{nome_tabela}',
            con=engine,
            schema='public', 
            if_exists='replace',
            index=False
        )
        print(f"✅ Tabela teste_{nome_tabela} criada com {len(df_dados)} registros")
        
    except Exception as e:
        print(f"❌ Erro ao criar/carregar: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("=== TESTE DE CARGA DE FATOS ===")
    
    try:
        # 1. Conectar ao banco
        engine = get_engine()
        
        # 2. Verificar estrutura
        todas_tabelas = verificar_estrutura_banco(engine)
        
        # 3. Ler dados disponíveis
        tabelas_em_memoria = ler_tabelas(engine)
        
        if not tabelas_em_memoria:
            print("❌ Nenhuma tabela foi carregada. Verifique a conexão com o banco.")
        else:
            # 4. Construir fato simples
            df_fato_teste = construir_fato_simples(tabelas_em_memoria)
            
            # 5. Tentar carregar
            sucesso = carregar_dados_teste(engine, 'fato_simples', df_fato_teste)
            
        print("\n✅ Processo concluído com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro geral: {e}")
        import traceback
        traceback.print_exc()