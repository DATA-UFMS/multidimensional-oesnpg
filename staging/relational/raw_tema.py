#!/usr/bin/env python3
"""
Script para gerar raw_tema e salvar no PostgreSQL
Uma única tabela com: macrotema_id, macrotema_nome, tema_id, tema_nome, palavrachave_id, palavrachave_nome
"""

import pandas as pd
from pathlib import Path
import argparse
from sqlalchemy import create_engine, text
import os

def save_to_postgres(df, table_name='raw_tema'):
    """Salva DataFrame no PostgreSQL"""
    # Configurações do banco (via variáveis de ambiente ou padrão)
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5433')
    database = os.getenv('POSTGRES_DB', 'dw_oesnpg')
    username = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    
    # String de conexão
    connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    
    try:
        print(f"🔗 Conectando ao PostgreSQL: {host}:{port}/{database}")
        engine = create_engine(connection_string)
        
        # Testar conexão
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            row = result.fetchone()
            if row:
                version = row[0]
                print(f"✅ Conectado! Versão: {version[:50]}...")
            else:
                print(f"✅ Conectado ao PostgreSQL!")
        
        # Salvar dados
        print(f"💾 Salvando tabela: {table_name}")
        df.to_sql(
            table_name, 
            engine, 
            if_exists='replace',  # Substitui a tabela se existir
            index=False,
            method='multi',  # Mais rápido para grandes volumes
            chunksize=1000
        )
        
        print(f"✅ Dados salvos no PostgreSQL!")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao conectar/salvar no PostgreSQL: {e}")
        print(f"💡 Verifique se o PostgreSQL está rodando e as credenciais estão corretas")
        print(f"💡 Variáveis de ambiente disponíveis:")
        print(f"   POSTGRES_HOST={host}")
        print(f"   POSTGRES_PORT={port}")
        print(f"   POSTGRES_DB={database}")
        print(f"   POSTGRES_USER={username}")
        return False

def main():
    # Argumentos da linha de comando
    parser = argparse.ArgumentParser(description='Processar macro temas e salvar no PostgreSQL')
    parser.add_argument('--postgres', action='store_true', help='Salvar no PostgreSQL')
    parser.add_argument('--table', default='raw_tema', help='Nome da tabela no PostgreSQL')
    args = parser.parse_args()
    
    # Caminhos
    base_dir = Path(__file__).resolve().parent
    excel_path = (base_dir / ".." / "data" / "macro_temas_oesnpg_v2.xlsx").resolve()
    
    print(f"📖 Lendo planilha: {excel_path}")
    
    # Ler planilha
    try:
        df = pd.read_excel(excel_path, sheet_name='macro-temas-v2')
    except FileNotFoundError:
        print(f"❌ Arquivo não encontrado: {excel_path}")
        return
    except Exception as e:
        print(f"❌ Erro ao ler planilha: {e}")
        return
    
    print(f"📊 Dados carregados: {len(df)} linhas")
    print(f"🔍 Colunas disponíveis: {list(df.columns)}")
    
    # Detectar colunas principais
    def find_column(df, keywords):
        for col in df.columns:
            col_lower = col.lower()
            if any(keyword.lower() in col_lower for keyword in keywords):
                return col
        return None
    
    # Usar macro_tema_1_label como macro tema principal
    macro_col = 'macro_tema_1_label' if 'macro_tema_1_label' in df.columns else find_column(df, ['macro'])
    tema_col = 'TEMA' if 'TEMA' in df.columns else find_column(df, ['tema'])
    palavras_col = 'PALAVRA-CHAVE' if 'PALAVRA-CHAVE' in df.columns else find_column(df, ['palavra', 'chave'])
    uf_col = 'UF' if 'UF' in df.columns else find_column(df, ['uf', 'estado'])
    
    if not macro_col or not tema_col:
        print(f"❌ Não foi possível detectar colunas necessárias")
        print(f"   Macro tema encontrado: {macro_col}")
        print(f"   Tema encontrado: {tema_col}")
        return
    
    print(f"✅ Usando colunas:")
    print(f"   Macro tema: {macro_col}")
    print(f"   Tema: {tema_col}")
    print(f"   Palavras-chave: {palavras_col}")
    print(f"   UF: {uf_col}")
    
    # Preparar DataFrame
    df_work = df.copy()
    df_work['macrotema_nome'] = df_work[macro_col].fillna('').astype(str).str.strip()
    df_work['tema_nome'] = df_work[tema_col].fillna('').astype(str).str.strip()
    
    # Adicionar UF
    if uf_col and uf_col in df_work.columns:
        df_work['uf'] = df_work[uf_col].fillna('').astype(str).str.strip().str.upper()
    else:
        df_work['uf'] = ''
    
    # Tratar palavras-chave
    if palavras_col in df_work.columns:
        df_work['palavras_chave'] = df_work[palavras_col].fillna('').astype(str)
    else:
        df_work['palavras_chave'] = ''
    
    # Desnormalizar palavras-chave (explode)
    df_work['palavrachave_nome'] = (
        df_work['palavras_chave']
        .str.replace(r'[\n\r|/;,]', ';', regex=True)
        .str.split(';')
    )
    df_work = df_work.explode('palavrachave_nome').reset_index(drop=True)
    df_work['palavrachave_nome'] = df_work['palavrachave_nome'].fillna('').str.strip()
    
    # Gerar IDs
    df_work['macrotema_id'], _ = pd.factorize(df_work['macrotema_nome'], sort=True)
    df_work['macrotema_id'] += 1
    
    tema_key = df_work['macrotema_nome'] + '||' + df_work['tema_nome']
    df_work['tema_id'], _ = pd.factorize(tema_key, sort=True)
    df_work['tema_id'] += 1
    
    df_work['palavrachave_id'], _ = pd.factorize(df_work['palavrachave_nome'], sort=True)
    df_work['palavrachave_id'] += 1
    
    # Selecionar colunas finais
    final_cols = [
        'macrotema_id',
        'macrotema_nome', 
        'tema_id',
        'tema_nome',
        'palavrachave_id',
        'palavrachave_nome',
        'uf'
    ]
    
    df_final = df_work[final_cols].copy()
    
    # Adicionar metadados
    df_final['fonte_arquivo'] = excel_path.name
    df_final['created_at'] = pd.Timestamp.now()
    
    # Salvar no PostgreSQL se solicitado
    if args.postgres:
        save_to_postgres(df_final, args.table)
    else:
        print("💡 Use --postgres para enviar a tabela ao banco.")
        print("💡 Dados processados apenas em memória (sem geração de arquivos).")
    
    print(f"✅ Concluído!")
    print(f"    Linhas: {len(df_final)}")
    print(f"   🔗 Colunas: {list(df_final.columns)}")
    
    if args.postgres:
        print(f"   🗄️  Tabela PostgreSQL: {args.table}")
    else:
        print(f"   💡 Use --postgres para salvar no banco também")
    
    # Mostrar preview
    print(f"\n📋 Preview dos dados:")
    print(df_final.head())
    
    print(f"\n📈 Resumo:")
    print(f"   Macro temas únicos: {df_final['macrotema_id'].nunique()}")
    print(f"   Temas únicos: {df_final['tema_id'].nunique()}")
    print(f"   Palavras-chave únicas: {df_final['palavrachave_id'].nunique()}")
    print(f"   UFs únicas: {df_final['uf'].nunique()}")
    
    # Mostrar UFs disponíveis
    ufs_unicas = sorted(df_final['uf'].unique())
    print(f"   UFs: {', '.join(ufs_unicas)}")

if __name__ == "__main__":
    main()
