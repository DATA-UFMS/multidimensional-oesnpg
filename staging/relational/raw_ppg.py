#!/usr/bin/env python3
"""
🎓 RAW PPG - Carregamento de Dados de Programas de Pós-Graduação
================================================================
Carrega dados do arquivo ppg_2024.csv para a tabela raw_ppg no PostgreSQL
Fonte: staging/data/ppg_2024.csv
Destino: raw_ppg (PostgreSQL)
Data: 18/09/2025
"""

import pandas as pd
import numpy as np
import os
import sys
import argparse
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging

def get_project_root() -> Path:
    """Encontra o diretório raiz do projeto de forma robusta."""
    current_path = Path(__file__).resolve()
    while not (current_path / '.env').exists() and not (current_path / '.git').exists() and current_path.parent != current_path:
        current_path = current_path.parent
    return current_path

# Configurar paths e logging
project_root = get_project_root()
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_engine():
    """Cria engine de conexão com PostgreSQL."""
    load_dotenv(dotenv_path=project_root / '.env')
    
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    if not all([db_user, db_pass, db_host, db_port, db_name]):
        raise ValueError("Variáveis de ambiente do banco não configuradas")

    db_uri = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    return create_engine(db_uri)

def load_ppg_csv():
    """Carrega e processa o arquivo ppg_2024.csv."""
    logger.info("📂 Carregando arquivo ppg_2024.csv...")
    
    csv_path = project_root / "staging" / "data" / "ppg_2024.csv"
    
    if not csv_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")
    
    try:
        # Carregar CSV com encoding correto
        df = pd.read_csv(csv_path, sep=';', encoding='latin1')
        logger.info(f"✅ Carregados {len(df):,} registros de PPG")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Erro ao carregar CSV: {e}")
        raise

def clean_and_transform_data(df):
    """Limpa e transforma os dados para inserção no banco."""
    logger.info("🧹 Limpando e transformando dados...")
    
    df_clean = df.copy()
    
    # Mapear nomes de colunas para padrão snake_case
    column_mapping = {
        'Ano Base': 'ano_base',
        'Codigo capes da IES': 'codigo_capes_da_ies',
        'Nome da IES': 'nome_da_ies',
        'Nome da Região da IES': 'nome_da_regiao_da_ies',
        'Sigla da Região da IES': 'sigla_da_regiao_da_ies',
        'CD_REGIAO_IBGE': 'cd_regiao_ibge',
        'UF da IES': 'uf_da_ies',
        'Status Jurídico da IES': 'status_juridico_da_ies',
        'Código do PPG': 'codigo_do_ppg',
        'Nome PPG': 'nome_ppg',
        'Nota do PPG': 'nota_do_ppg',
        'Modalidade do PPG \x96 Acadêmico ou Profissional': 'modalidade_do_ppg',
        'Situação do PPG': 'situacao_do_ppg',
        'Programa em rede (Sim/Não)': 'programa_em_rede',
        'Código Grande Area do PPG': 'codigo_grande_area_do_ppg',
        'Grande Area do PPG': 'grande_area_do_ppg',
        'Código Area de Conhecimento do PPG': 'codigo_area_de_conhecimento_do_ppg',
        'Area de Conhecimento do PPG': 'area_de_conhecimento_do_ppg',
        'Id Area de Avaliação do PPG': 'id_area_de_avaliacao_do_ppg',
        'Area de Avaliação do PPG': 'area_de_avaliacao_do_ppg',
        'Total de Cursos do PPG': 'total_de_cursos_do_ppg',
        'Quantidade de docentes no PPG': 'quantidade_de_docentes_no_ppg',
        'Quantidade de discentes matriculados no PPG': 'quantidade_de_discentes_matriculados_no_ppg'
    }
    
    # Renomear colunas
    df_clean = df_clean.rename(columns=column_mapping)
    
    # Limpar campos de texto
    text_columns = [
        'nome_da_ies', 'nome_da_regiao_da_ies', 'sigla_da_regiao_da_ies',
        'uf_da_ies', 'status_juridico_da_ies', 'nome_ppg', 'modalidade_do_ppg',
        'situacao_do_ppg', 'programa_em_rede', 'grande_area_do_ppg',
        'area_de_conhecimento_do_ppg', 'area_de_avaliacao_do_ppg'
    ]
    
    for col in text_columns:
        if col in df_clean.columns:
            # Remover espaços extras e padronizar
            df_clean[col] = df_clean[col].astype(str).str.strip()
            df_clean[col] = df_clean[col].replace(['nan', 'None', ''], None)
    
    # Tratar campos numéricos
    numeric_columns = [
        'ano_base', 'codigo_capes_da_ies', 'cd_regiao_ibge', 'nota_do_ppg',
        'codigo_grande_area_do_ppg', 'codigo_area_de_conhecimento_do_ppg',
        'id_area_de_avaliacao_do_ppg', 'total_de_cursos_do_ppg',
        'quantidade_de_docentes_no_ppg', 'quantidade_de_discentes_matriculados_no_ppg'
    ]
    
    for col in numeric_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # Tratar campo código do PPG (texto)
    if 'codigo_do_ppg' in df_clean.columns:
        df_clean['codigo_do_ppg'] = df_clean['codigo_do_ppg'].astype(str).str.strip()
    
    # Adicionar metadados
    df_clean['fonte_arquivo'] = 'ppg_2024.csv'
    df_clean['created_at'] = pd.Timestamp.now()
    
    logger.info(f"✅ Dados transformados: {len(df_clean):,} registros, {len(df_clean.columns)} colunas")
    
    return df_clean

def create_raw_ppg_table(engine):
    """Cria a tabela raw_ppg no PostgreSQL."""
    logger.info("🏗️ Criando tabela raw_ppg...")
    
    statements = [
        "DROP TABLE IF EXISTS raw_ppg CASCADE",
        """
        CREATE TABLE raw_ppg (
            id SERIAL PRIMARY KEY,
            ano_base INTEGER,
            codigo_capes_da_ies INTEGER,
            nome_da_ies VARCHAR(500),
            nome_da_regiao_da_ies VARCHAR(50),
            sigla_da_regiao_da_ies VARCHAR(10),
            cd_regiao_ibge INTEGER,
            uf_da_ies VARCHAR(10),
            status_juridico_da_ies VARCHAR(100),
            codigo_do_ppg VARCHAR(50),
            nome_ppg VARCHAR(500),
            nota_do_ppg DECIMAL(3,1),
            modalidade_do_ppg VARCHAR(50),
            situacao_do_ppg VARCHAR(100),
            programa_em_rede VARCHAR(10),
            codigo_grande_area_do_ppg INTEGER,
            grande_area_do_ppg VARCHAR(200),
            codigo_area_de_conhecimento_do_ppg INTEGER,
            area_de_conhecimento_do_ppg VARCHAR(300),
            id_area_de_avaliacao_do_ppg INTEGER,
            area_de_avaliacao_do_ppg VARCHAR(200),
            total_de_cursos_do_ppg INTEGER,
            quantidade_de_docentes_no_ppg INTEGER,
            quantidade_de_discentes_matriculados_no_ppg INTEGER,
            fonte_arquivo VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        "CREATE INDEX idx_raw_ppg_codigo_ppg ON raw_ppg(codigo_do_ppg)",
        "CREATE INDEX idx_raw_ppg_codigo_ies ON raw_ppg(codigo_capes_da_ies)",
        "CREATE INDEX idx_raw_ppg_uf ON raw_ppg(uf_da_ies)",
        "CREATE INDEX idx_raw_ppg_modalidade ON raw_ppg(modalidade_do_ppg)",
        "CREATE INDEX idx_raw_ppg_grande_area ON raw_ppg(codigo_grande_area_do_ppg)",
        "CREATE INDEX idx_raw_ppg_area_conhecimento ON raw_ppg(codigo_area_de_conhecimento_do_ppg)"
    ]
    
    try:
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))
        logger.info("✅ Tabela raw_ppg criada com sucesso")
        
    except Exception as e:
        logger.error(f"❌ Erro ao criar tabela: {e}")
        raise

def save_to_postgres(df, engine, table_name='raw_ppg'):
    """Salva DataFrame no PostgreSQL."""
    logger.info(f"💾 Salvando dados na tabela {table_name}...")
    
    try:
        # Salvar dados
        df.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        # Verificar inserção
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.scalar()
        
        logger.info(f"✅ {count:,} registros inseridos na tabela {table_name}")
        
    except Exception as e:
        logger.error(f"❌ Erro ao salvar no PostgreSQL: {e}")
        raise

def validate_data(engine):
    """Valida os dados inseridos."""
    logger.info("🔍 Validando dados inseridos...")
    
    validation_queries = [
        ("Total de registros", "SELECT COUNT(*) FROM raw_ppg"),
        ("PPGs únicos", "SELECT COUNT(DISTINCT codigo_do_ppg) FROM raw_ppg"),
        ("IES únicas", "SELECT COUNT(DISTINCT codigo_capes_da_ies) FROM raw_ppg"),
        ("UFs únicas", "SELECT COUNT(DISTINCT uf_da_ies) FROM raw_ppg"),
        ("Modalidades", "SELECT modalidade_do_ppg, COUNT(*) FROM raw_ppg GROUP BY modalidade_do_ppg ORDER BY COUNT(*) DESC"),
        ("Regiões", "SELECT nome_da_regiao_da_ies, COUNT(*) FROM raw_ppg GROUP BY nome_da_regiao_da_ies ORDER BY COUNT(*) DESC")
    ]
    
    try:
        with engine.connect() as conn:
            for description, query in validation_queries:
                result = conn.execute(text(query))
                
                if "GROUP BY" in query:
                    logger.info(f"📊 {description}:")
                    for row in result:
                        logger.info(f"   {row[0]}: {row[1]:,}")
                else:
                    count = result.scalar()
                    logger.info(f"📊 {description}: {count:,}")
        
        logger.info("✅ Validação concluída")
        
    except Exception as e:
        logger.error(f"❌ Erro na validação: {e}")

def main():
    """Função principal."""
    parser = argparse.ArgumentParser(description='Carrega dados de PPG para PostgreSQL')
    parser.add_argument('--validate-only', action='store_true', help='Apenas validar dados existentes')
    args = parser.parse_args()
    
    try:
        logger.info("🎓 Iniciando carregamento de dados PPG")
        
        # Conectar ao banco
        engine = get_db_engine()
        logger.info("✅ Conexão com PostgreSQL estabelecida")
        
        if args.validate_only:
            validate_data(engine)
            return
        
        # Carregar dados do CSV
        df_raw = load_ppg_csv()
        
        # Transformar dados
        df_clean = clean_and_transform_data(df_raw)
        
        # Criar tabela
        create_raw_ppg_table(engine)
        
        # Salvar no PostgreSQL
        save_to_postgres(df_clean, engine)
        
        # Validar dados
        validate_data(engine)
        
        print("\n" + "="*60)
        print("🎉 CARREGAMENTO DE PPG CONCLUÍDO COM SUCESSO!")
        print("="*60)
        print("✅ Tabela: raw_ppg")
        print("✅ Fonte: ppg_2024.csv")
        print(f"✅ Registros: {len(df_clean):,}")
        print("✅ Índices: Criados para performance")
        print("✅ Validação: Dados verificados")
        print("="*60)
        
    except Exception as e:
        logger.error(f"❌ Erro durante execução: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
