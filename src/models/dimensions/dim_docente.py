#!/usr/bin/env python3
"""
Dimensão de Docentes (dim_docente)
Integra dados de raw_docente e raw_fomentopq para criar uma dimensão unificada.
Cada docente único recebe uma surrogate key (sk), iniciando com sk=0 para "Desconhecido".
"""

import argparse
import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text

# Carregar variáveis do arquivo .env
def load_env_file():
    """Carrega variáveis do arquivo .env"""
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent.parent
    env_file = project_root / ".env"
    
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value

# Carregar .env no início
load_env_file()


DEFAULT_TABLE = "dim_docente"


def save_to_postgres(df: pd.DataFrame, table_name: str) -> bool:
    """Salva o DataFrame na tabela indicada do PostgreSQL."""
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5433")
    database = os.getenv("DB_NAME", "dw_oesnpg")
    username = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASS", "postgres")

    conn_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"

    try:
        print(f"🔗 Conectando ao PostgreSQL: {host}:{port}/{database}")
        engine = create_engine(conn_string)

        with engine.connect() as conn:
            row = conn.execute(text("SELECT version()")).fetchone()
            if row:
                print(f"✅ Conectado (versão: {row[0][:50]}...)")
            else:
                print("✅ Conectado ao PostgreSQL.")

        print(f"💾 Gravando tabela {table_name}...")
        df.to_sql(
            table_name,
            engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )
        print("✅ Dados enviados ao PostgreSQL.")
        return True

    except Exception as exc:
        print(f"❌ Erro ao conectar/salvar no PostgreSQL: {exc}")
        print("💡 Verifique se o banco está disponível e as variáveis de ambiente foram definidas.")
        return False


def get_database_connection():
    """Cria conexão com PostgreSQL."""
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5433")
    database = os.getenv("DB_NAME", "dw_oesnpg")
    username = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASS", "postgres")

    conn_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    
    try:
        engine = create_engine(conn_string)
        print(f"🔗 Conectando ao PostgreSQL: {host}:{port}/{database}")
        
        with engine.connect() as conn:
            row = conn.execute(text("SELECT version()")).fetchone()
            if row:
                print(f"✅ Conectado (versão: {row[0][:50]}...)")
        
        return engine
        
    except Exception as exc:
        raise Exception(f"Erro ao conectar ao PostgreSQL: {exc}")


def load_raw_docente_data() -> pd.DataFrame:
    """Carrega dados da tabela raw_docente."""
    print("� Carregando dados da tabela raw_docente...")
    
    engine = get_database_connection()
    
    query = """
    SELECT 
        id_pessoa,
        nm_docente,
        nr_documento_docente,
        an_nascimento_docente,
        ds_faixa_etaria,
        ds_tipo_nacionalidade_docente,
        ds_categoria_docente,
        ds_tipo_vinculo_docente_ies,
        ds_regime_trabalho,
        in_doutor,
        an_titulacao,
        nm_grau_titulacao,
        nm_area_basica_titulacao,
        nm_ies_titulacao,
        nm_pais_ies_titulacao,
        cd_cat_bolsa_produtividade,
        ano_base,
        created_at
    FROM raw_docente
    ORDER BY id_pessoa, ano_base DESC
    """
    
    try:
        df = pd.read_sql_query(query, engine)
        print(f"   ✔ {len(df):,} registros carregados da tabela raw_docente")
        
        # Normaliza campos de texto
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].fillna("").astype(str).str.strip()
        
        return df
        
    except Exception as exc:
        print(f"   ❌ Erro ao carregar raw_docente: {exc}")
        raise


def load_raw_fomentopq_data() -> pd.DataFrame:
    """Carrega dados da tabela raw_fomentopq."""
    print("💰 Carregando dados da tabela raw_fomentopq...")
    
    engine = get_database_connection()
    
    query = """
    SELECT 
        id_lattes,
        des_beneficiario as nome_beneficiario,
        des_pais as nome_pais,
        des_regiao as nome_regiao,
        des_uf as nome_uf,
        des_cidade as nome_cidade,
        des_grande_area as nome_grande_area,
        des_area as nome_area,
        des_subarea as nome_subarea,
        cod_modalidade,
        cod_categoria_nivel,
        des_instituto as nome_instituto,
        data_inicio_processo,
        data_termino_processo,
        created_at
    FROM raw_fomentopq
    ORDER BY des_beneficiario, data_inicio_processo
    """
    
    try:
        df = pd.read_sql_query(query, engine)
        print(f"   ✔ {len(df):,} registros carregados da tabela raw_fomentopq")
        
        # Normaliza campos de texto
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].fillna("").astype(str).str.strip()
        
        return df
        
    except Exception as exc:
        print(f"   ⚠️ Erro ao carregar raw_fomentopq: {exc}")
        print("   📝 Continuando sem dados de fomento PQ...")
        return pd.DataFrame()


def create_docente_dimension(df_docente: pd.DataFrame, df_fomento: pd.DataFrame) -> pd.DataFrame:
    """Cria a dimensão de docentes integrando dados de docentes e fomento PQ."""
    
    print("🔄 Criando dimensão de docentes...")
    
    # 1. Agrupa docentes por id_pessoa para obter dados únicos por docente
    print("   📋 Agregando dados por docente...")
    
    # Seleciona o registro mais recente por docente
    df_docente_sorted = df_docente.sort_values(['id_pessoa', 'ano_base'], ascending=[True, False])
    df_docentes_unicos = df_docente_sorted.drop_duplicates(subset=['id_pessoa'], keep='first')
    
    print(f"   ✔ {len(df_docentes_unicos):,} docentes únicos identificados")
    
    # 2. Preparar dados de fomento PQ se disponíveis
    fomento_dict = {}
    if not df_fomento.empty:
        print("   💰 Processando dados de fomento PQ...")
        
        # Agrupa fomento por beneficiário (pode ter múltiplas bolsas)
        fomento_agregado = df_fomento.groupby('nome_beneficiario').agg({
            'id_lattes': 'first',
            'cod_categoria_nivel': lambda x: ', '.join(sorted(set(x))),
            'cod_modalidade': 'first',
            'nome_grande_area': 'first',
            'nome_area': 'first',
            'data_inicio_processo': 'min',  # Primeira bolsa
            'data_termino_processo': 'max',  # Última bolsa
        }).reset_index()
        
        # Cria dicionário para lookup por nome
        for _, row in fomento_agregado.iterrows():
            nome_norm = row['nome_beneficiario'].upper().strip()
            fomento_dict[nome_norm] = {
                'id_lattes': row['id_lattes'],
                'pq_categoria_nivel': row['cod_categoria_nivel'],
                'pq_modalidade': row['cod_modalidade'],
                'pq_grande_area': row['nome_grande_area'],
                'pq_area': row['nome_area'],
                'pq_data_inicio': row['data_inicio_processo'],
                'pq_data_termino': row['data_termino_processo'],
                'tem_bolsa_pq': 'S'
            }
        
        print(f"   ✔ {len(fomento_dict):,} beneficiários PQ processados")
    
    # 3. Criar registros da dimensão
    print("   🏗️ Construindo dimensão...")
    
    dim_records = []
    
    # Registro 0: Desconhecido
    dim_records.append({
        'sk_docente': 0,
        'id_pessoa': None,
        'nome_docente': 'Desconhecido',
        'documento_docente': None,
        'ano_nascimento': None,
        'faixa_etaria': 'Desconhecido',
        'nacionalidade': 'Desconhecido',
        'categoria_docente': 'Desconhecido',
        'vinculo_ies': 'Desconhecido',
        'regime_trabalho': 'Desconhecido',
        'bl_doutor': False,
        'ano_titulacao': None,
        'grau_titulacao': 'Desconhecido',
        'area_titulacao': 'Desconhecido',
        'ies_titulacao': 'Desconhecido',
        'pais_titulacao': 'Desconhecido',
        'tem_bolsa_produtividade': 'Desconhecido',
        'categoria_bolsa_produtividade': 'Desconhecido',
        # Campos de fomento PQ
        'id_lattes': None,
        'pq_categoria_nivel': None,
        'pq_modalidade': None,
        'pq_grande_area': None,
        'pq_area': None,
        'pq_data_inicio': None,
        'pq_data_termino': None,
        'tem_bolsa_pq': 'N',
        'ano_base_mais_recente': None
    })
    
    # Registros dos docentes
    sk_counter = 1
    for idx, row in df_docentes_unicos.iterrows():
        
        # Busca dados de fomento PQ por nome
        nome_norm = str(row['nm_docente']).upper().strip()
        dados_pq = fomento_dict.get(nome_norm, {})
        
        # Verifica se tem bolsa de produtividade nos dados de docentes
        tem_bolsa_capes = 'S' if (pd.notna(row['cd_cat_bolsa_produtividade']) and 
                                  str(row['cd_cat_bolsa_produtividade']).strip() != '') else 'N'
        
        # Converte campos numéricos
        try:
            ano_nascimento = int(row['an_nascimento_docente']) if pd.notna(row['an_nascimento_docente']) and str(row['an_nascimento_docente']).strip() != '' else None
        except:
            ano_nascimento = None
            
        try:
            ano_titulacao = int(row['an_titulacao']) if pd.notna(row['an_titulacao']) and str(row['an_titulacao']).strip() != '' else None
        except:
            ano_titulacao = None
            
        try:
            ano_base = int(row['ano_base']) if pd.notna(row['ano_base']) and str(row['ano_base']).strip() != '' else None
        except:
            ano_base = None
        
        # Converte campo booleano de doutor
        bl_doutor = True if str(row['in_doutor']).upper().strip() == 'S' else False
        
        dim_record = {
            'sk_docente': sk_counter,  # SK inicia em 1 (0 é para desconhecido)
            'id_pessoa': row['id_pessoa'],
            'nome_docente': row['nm_docente'],
            'documento_docente': row['nr_documento_docente'],
            'ano_nascimento': ano_nascimento,
            'faixa_etaria': row['ds_faixa_etaria'],
            'nacionalidade': row['ds_tipo_nacionalidade_docente'],
            'categoria_docente': row['ds_categoria_docente'],
            'vinculo_ies': row['ds_tipo_vinculo_docente_ies'],
            'regime_trabalho': row['ds_regime_trabalho'],
            'bl_doutor': bl_doutor,
            'ano_titulacao': ano_titulacao,
            'grau_titulacao': row['nm_grau_titulacao'],
            'area_titulacao': row['nm_area_basica_titulacao'],
            'ies_titulacao': row['nm_ies_titulacao'],
            'pais_titulacao': row['nm_pais_ies_titulacao'],
            'tem_bolsa_produtividade': tem_bolsa_capes,
            'categoria_bolsa_produtividade': row['cd_cat_bolsa_produtividade'],
            # Campos de fomento PQ
            'id_lattes': dados_pq.get('id_lattes'),
            'pq_categoria_nivel': dados_pq.get('pq_categoria_nivel'),
            'pq_modalidade': dados_pq.get('pq_modalidade'),
            'pq_grande_area': dados_pq.get('pq_grande_area'),
            'pq_area': dados_pq.get('pq_area'),
            'pq_data_inicio': dados_pq.get('pq_data_inicio'),
            'pq_data_termino': dados_pq.get('pq_data_termino'),
            'tem_bolsa_pq': dados_pq.get('tem_bolsa_pq', 'N'),
            'ano_base_mais_recente': ano_base
        }
        
        dim_records.append(dim_record)
        sk_counter += 1
    
    df_dim = pd.DataFrame(dim_records)
    
    print(f"   ✔ Dimensão criada com {len(df_dim):,} registros")
    
    return df_dim


def main():
    parser = argparse.ArgumentParser(description="Gera a dimensão de docentes (dim_docente)")
    parser.add_argument("--postgres", action="store_true", 
                       help="Envia a tabela também para o PostgreSQL")
    parser.add_argument("--table", default=DEFAULT_TABLE,
                       help=f"Nome da tabela destino no PostgreSQL (default: {DEFAULT_TABLE})")
    
    args = parser.parse_args()
    
    print("👨‍🏫 === GERAÇÃO DA DIMENSÃO DOCENTES ===")
    
    try:
        # 1. Carregar dados de docentes
        df_docente = load_raw_docente_data()
        
        # 2. Carregar dados de fomento PQ
        df_fomento = load_raw_fomentopq_data()
        
        # 3. Criar dimensão
        df_dim_docente = create_docente_dimension(df_docente, df_fomento)
        
        # 4. Relatório final
        print("\n📊 Estatísticas da dimensão:")
        print(f"   • Total de registros: {len(df_dim_docente):,}")
        print(f"   • Docentes com bolsa CAPES: {len(df_dim_docente[df_dim_docente['tem_bolsa_produtividade'] == 'S']):,}")
        print(f"   • Docentes com bolsa PQ: {len(df_dim_docente[df_dim_docente['tem_bolsa_pq'] == 'S']):,}")
        print(f"   • Doutores: {len(df_dim_docente[df_dim_docente['bl_doutor'] == True]):,}")
        
        # 5. Amostra dos dados
        print("\n📋 Amostra dos dados:")
        sample_cols = ['sk_docente', 'nome_docente', 'categoria_docente', 'bl_doutor', 'tem_bolsa_pq']
        print(df_dim_docente[sample_cols].head())
        
        # 6. Salvar no PostgreSQL se solicitado
        if args.postgres:
            save_to_postgres(df_dim_docente, args.table)
        else:
            print("💡 Use --postgres para enviar a tabela ao banco.")
            print("💡 Dados processados apenas em memória (sem geração de arquivos).")
        
    except Exception as exc:
        print(f"❌ Erro durante o processamento: {exc}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
