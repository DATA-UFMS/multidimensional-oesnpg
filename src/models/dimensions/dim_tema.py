#!/usr/bin/env python3
"""
Geração da Dimensão Tema (dim_tema) para Data Warehouse

Este script processa dados da tabela raw_tema no PostgreSQL para criar uma dimensão 
desnormalizada de temas. Realiza as seguintes transformações:

1. Extrai registros únicos da tabela raw_tema (eliminando duplicatas por tema_id)
2. Converte nomes de UF para siglas (ex: "SÃO PAULO" -> "SP")
3. Adiciona registro SK=0 para valores desconhecidos/não aplicáveis
4. Gera surrogate keys sequenciais (tema_sk) começando do 0
5. Cria estrutura dimensional com tema_sk, tema_id, tema_nome, macrotema_id, macrotema_nome, sigla_uf
6. Adiciona metadados de controle (created_at, updated_at)
7. Salva resultado no PostgreSQL como tabela dim_tema

Entrada: Tabela raw_tema (5.991 registros)
Saída: Dimensão dim_tema (449 temas únicos, 20 macro temas, 27 UFs)

Uso:
    python dim_tema.py                    # Criar dimensão no PostgreSQL
    python dim_tema.py --table custom     # Tabela customizada
"""

import pandas as pd
import os
from pathlib import Path
from sqlalchemy import create_engine, text
import argparse

def get_uf_mapping():
    """Cria mapeamento de nome UF para sigla UF"""
    uf_mapping = {
        'ACRE': 'AC',
        'ALAGOAS': 'AL', 
        'AMAPÁ': 'AP',
        'AMAZONAS': 'AM',
        'BAHIA': 'BA',
        'CEARÁ': 'CE',
        'DISTRITO FEDERAL': 'DF',
        'ESPÍRITO SANTO': 'ES',
        'GOIÁS': 'GO',
        'MARANHÃO': 'MA',
        'MATO GROSSO': 'MT',
        'MATO GROSSO DO SUL': 'MS',
        'MINAS GERAIS': 'MG',
        'PARANÁ': 'PR',
        'PARAÍBA': 'PB',
        'PARÁ': 'PA',
        'PERNAMBUCO': 'PE',
        'PIAUÍ': 'PI',
        'RIO DE JANEIRO': 'RJ',
        'RIO GRANDE DO NORTE': 'RN',
        'RIO GRANDE DO SUL': 'RS',
        'RONDÔNIA': 'RO',
        'RORAIMA': 'RR',
        'SANTA CATARINA': 'SC',
        'SERGIPE': 'SE',
        'SÃO PAULO': 'SP',
        'TOCANTINS': 'TO'
    }
    return uf_mapping

def connect_postgres():
    """Conecta ao PostgreSQL"""
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5433')
    database = os.getenv('POSTGRES_DB', 'dw_oesnpg')
    username = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    
    connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    
    try:
        print(f"Conectando ao PostgreSQL: {host}:{port}/{database}")
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        print(f"ERRO: Erro ao conectar ao PostgreSQL: {e}")
        return None

def extract_raw_tema(engine):
    """Extrai dados da tabela raw_tema"""
    print("Extraindo dados da tabela raw_tema...")
    
    query = """
    SELECT DISTINCT
        macrotema_id,
        macrotema_nome,
        tema_id, 
        tema_nome,
        uf
    FROM raw_tema
    WHERE tema_nome IS NOT NULL 
    AND tema_nome != ''
    ORDER BY macrotema_id, tema_id
    """
    
    try:
        df = pd.read_sql(query, engine)
        print(f"Extraidos {len(df)} registros unicos")
        return df
    except Exception as e:
        print(f"ERRO: Erro ao extrair dados: {e}")
        return None

def create_sk0_record():
    """Cria o registro SK=0 para valores desconhecidos/não aplicáveis"""
    registro_sk0 = {
        'tema_sk': 0,
        'tema_id': 0,
        'tema_nome': 'DESCONHECIDO',
        'macrotema_id': 0,
        'macrotema_nome': 'DESCONHECIDO',
        'sigla_uf': 'XX',
        'created_at': pd.Timestamp.now(),
        'updated_at': pd.Timestamp.now()
    }
    
    return pd.DataFrame([registro_sk0])

def create_dim_tema(df_raw):
    """Cria a dimensão tema"""
    print("Criando dimensao tema...")
    
    # Obter mapeamento UF
    uf_mapping = get_uf_mapping()
    
    # Aplicar mapeamento UF
    df_raw['sigla_uf'] = df_raw['uf'].map(uf_mapping)
    
    # Criar dimensão tema
    dim_tema = df_raw[['tema_id', 'tema_nome', 'macrotema_id', 'macrotema_nome', 'sigla_uf']].copy()
    
    # Remover duplicatas se houver
    dim_tema = dim_tema.drop_duplicates(subset=['tema_id']).reset_index(drop=True)
    
    # Adicionar metadados
    dim_tema['created_at'] = pd.Timestamp.now()
    dim_tema['updated_at'] = pd.Timestamp.now()
    
    # Adicionar registro SK=0 para valores desconhecidos/não aplicáveis
    registro_sk0 = create_sk0_record()
    dim_tema = pd.concat([registro_sk0, dim_tema], ignore_index=True)
    
    # Gerar surrogate keys (começando do 0)
    dim_tema['tema_sk'] = range(0, len(dim_tema))
    
    # Reordenar colunas
    dim_tema = dim_tema[['tema_sk', 'tema_id', 'tema_nome', 'macrotema_id', 'macrotema_nome', 'sigla_uf', 'created_at', 'updated_at']]
    
    print(f"Dimensao criada com {len(dim_tema)} temas (incluindo SK=0)")
    return dim_tema

def save_dim_tema(df, engine, table_name='dim_tema'):
    """Salva a dimensão no PostgreSQL"""
    print(f"Salvando dimensao na tabela {table_name}...")
    
    try:
        # Salvar dados
        df.to_sql(
            table_name,
            engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"Dimensao salva com sucesso!")
        return True
        
    except Exception as e:
        print(f"ERRO: Erro ao salvar dimensao: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Criar dimensão tema a partir de raw_tema')
    parser.add_argument('--table', default='dim_tema', help='Nome da tabela destino')
    parser.add_argument('--test', action='store_true', help='Modo teste com dados simulados')
    args = parser.parse_args()
    
    print("CRIANDO DIMENSAO TEMA")
    print("=" * 50)
    
    if args.test:
        print("MODO TESTE: Usando dados simulados")
        # Criar dados fictícios para teste
        df_raw = pd.DataFrame({
            'tema_id': [1, 2, 3],
            'tema_nome': ['Tema A', 'Tema B', 'Tema C'],
            'macrotema_id': [1, 1, 2], 
            'macrotema_nome': ['Macro A', 'Macro A', 'Macro B'],
            'uf': ['SÃO PAULO', 'RIO DE JANEIRO', 'MINAS GERAIS']
        })
    else:
        # Conectar ao banco
        engine = connect_postgres()
        if not engine:
            print("ERRO: Nao foi possivel conectar ao PostgreSQL")
            return
        
        # Extrair dados raw
        df_raw = extract_raw_tema(engine)
        if df_raw is None:
            return
    
    # Criar dimensão
    dim_tema = create_dim_tema(df_raw)
    
    # Mostrar preview
    print(f"\nPreview da dimensao:")
    print(dim_tema.head(10))
    
    # Estatísticas
    print(f"\nEstatisticas:")
    print(f"   Total de registros: {len(dim_tema)}")
    print(f"   Temas unicos (excluindo SK=0): {len(dim_tema[dim_tema['tema_sk'] != 0])}")
    print(f"   Macro temas unicos: {dim_tema[dim_tema['tema_sk'] != 0]['macrotema_id'].nunique()}")
    print(f"   UFs unicas: {dim_tema[dim_tema['tema_sk'] != 0]['sigla_uf'].nunique()}")
    
    # Salvar apenas se não for teste
    if not args.test:
        save_dim_tema(dim_tema, engine, args.table)
    else:
        print("\nMODO TESTE: Dimensao nao foi salva no banco")
    
    print("\nProcesso concluido!")

if __name__ == "__main__":
    main()
