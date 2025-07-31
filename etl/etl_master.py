#!/usr/bin/env python3
"""
Script Master de ETL para o Data Warehouse do Observatório CAPES
Este script coordena todo o processo de ETL (Extract, Transform, Load)
"""

import os
import sys
import subprocess
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Carregar variáveis de ambiente
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

def log_message(message, level="INFO"):
    """
    Função para logging com timestamp.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")

def executar_sql_script(script_path):
    """
    Executa um script SQL no banco de dados.
    """
    try:
        log_message(f"Executando script SQL: {script_path}")
        
        # Criar conexão com o banco
        url = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(url)
        
        # Ler e executar o script
        with open(script_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()
        
        with engine.begin() as conn:
            # Dividir o script em comandos individuais
            commands = [cmd.strip() for cmd in sql_content.split(';') if cmd.strip()]
            
            for command in commands:
                if command:
                    conn.execute(text(command))
        
        log_message(f"Script SQL executado com sucesso: {script_path}")
        return True
        
    except Exception as e:
        log_message(f"Erro ao executar script SQL {script_path}: {e}", "ERROR")
        return False

def executar_python_script(script_path):
    """
    Executa um script Python.
    """
    try:
        log_message(f"Executando script Python: {script_path}")
        
        # Adicionar o diretório raiz ao PYTHONPATH para resolver imports
        root_dir = os.path.dirname(os.path.dirname(__file__))
        core_dir = os.path.join(root_dir, 'core')
        env = os.environ.copy()
        if 'PYTHONPATH' in env:
            env['PYTHONPATH'] = f"{root_dir}:{core_dir}:{env['PYTHONPATH']}"
        else:
            env['PYTHONPATH'] = f"{root_dir}:{core_dir}"
        
        result = subprocess.run([sys.executable, script_path], 
                              capture_output=True, text=True, 
                              cwd=os.path.dirname(script_path), env=env)
        
        if result.returncode == 0:
            log_message(f"Script Python executado com sucesso: {script_path}")
            if result.stdout:
                print(result.stdout)
            return True
        else:
            log_message(f"Erro ao executar script Python {script_path}: {result.stderr}", "ERROR")
            return False
            
    except Exception as e:
        log_message(f"Erro ao executar script Python {script_path}: {e}", "ERROR")
        return False

def verificar_conexao_banco():
    """
    Verifica se é possível conectar ao banco de dados.
    """
    try:
        log_message("Verificando conexão com o banco de dados...")
        
        url = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(url)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            
        log_message("Conexão com o banco de dados estabelecida com sucesso")
        return True
        
    except Exception as e:
        log_message(f"Erro ao conectar com o banco de dados: {e}", "ERROR")
        return False

def criar_banco_dados():
    """
    Cria o banco de dados se não existir.
    """
    try:
        log_message(f"Verificando se o banco '{DB_NAME}' existe...")
        
        # Conectar ao banco postgres padrão para criar o banco do projeto
        url_postgres = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/postgres'
        engine_postgres = create_engine(url_postgres, isolation_level='AUTOCOMMIT')
        
        with engine_postgres.connect() as conn:
            # Verificar se o banco existe
            result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'"))
            if result.fetchone():
                log_message(f"Banco '{DB_NAME}' já existe")
                return True
            
            # Criar o banco
            log_message(f"Criando banco de dados '{DB_NAME}'...")
            conn.execute(text(f'CREATE DATABASE "{DB_NAME}"'))
            log_message(f"Banco '{DB_NAME}' criado com sucesso")
            
        return True
        
    except Exception as e:
        log_message(f"Erro ao criar banco de dados: {e}", "ERROR")
        return False

def executar_etl_completo():
    """
    Executa o processo completo de ETL.
    """
    log_message("=== INICIANDO PROCESSO DE ETL COMPLETO ===")
    
    # Criar banco de dados se não existir
    if not criar_banco_dados():
        log_message("Falha ao criar/verificar banco de dados. Abortando ETL.", "ERROR")
        return False
    
    # Verificar conexão com o banco
    if not verificar_conexao_banco():
        log_message("Falha na conexão com o banco. Abortando ETL.", "ERROR")
        return False
    
    # Lista de scripts a serem executados em ordem
    scripts_etl = [
        # 1. Criar estrutura das dimensões
        # ("SQL", "../sql/ddl/create_all_dimensions.sql", "Criação das tabelas de dimensões"),
        
        # 2. Popular dimensões
        ("Python", "../models/dimensions/dim_tempo.py", "População da dimensão tempo"),
        ("Python", "../models/dimensions/dim_localidade.py", "População da dimensão localidade"),
        ("Python", "../models/dimensions/dim_tema.py", "População da dimensão tema"),
        ("Python", "../models/dimensions/dim_ods.py", "População da dimensão ODS"),
        ("Python", "../models/dimensions/dim_ies.py", "População da dimensão IES"),
        ("Python", "../models/dimensions/dim_ppg.py", "População da dimensão PPG"),
        ("Python", "../models/dimensions/dim_producao.py", "População da dimensão produção"),
        ("Python", "../models/dimensions/dim_docente.py", "População da dimensão docente"),
        
        # 3. Popular tabela fato
        ("Python", "../models/facts/create_fact_table.py", "População da tabela fato"),
    ]
    
    # Executar scripts em sequência
    sucesso_total = True
    
    for tipo, script, descricao in scripts_etl:
        log_message(f"--- {descricao} ---")
        
        script_path = os.path.join(os.path.dirname(__file__), script)
        
        if not os.path.exists(script_path):
            log_message(f"Script não encontrado: {script_path}", "ERROR")
            sucesso_total = False
            continue
        
        if tipo == "SQL":
            sucesso = executar_sql_script(script_path)
        elif tipo == "Python":
            sucesso = executar_python_script(script_path)
        else:
            log_message(f"Tipo de script desconhecido: {tipo}", "ERROR")
            sucesso = False
        
        if not sucesso:
            log_message(f"Falha na execução de: {script}", "ERROR")
            sucesso_total = False
            # Continuar com os próximos scripts mesmo em caso de erro
    
    # Resultado final
    if sucesso_total:
        log_message("=== ETL COMPLETO EXECUTADO COM SUCESSO ===")
    else:
        log_message("=== ETL COMPLETO FINALIZADO COM ERROS ===", "ERROR")
    
    return sucesso_total

def executar_etl_incremental():
    """
    Executa apenas a atualização incremental dos dados (sem recriar estruturas).
    """
    log_message("=== INICIANDO ETL INCREMENTAL ===")
    
    # Scripts para atualização incremental
    scripts_incrementais = [
        ("Python", "../models/dimensions/dim_ies.py", "Atualização da dimensão IES"),
        ("Python", "../models/dimensions/dim_ppg.py", "Atualização da dimensão PPG"),
        ("Python", "../models/facts/fact_table.py", "Atualização da tabela fato"),
    ]
    
    sucesso_total = True
    
    for tipo, script, descricao in scripts_incrementais:
        log_message(f"--- {descricao} ---")
        
        script_path = os.path.join(os.path.dirname(__file__), script)
        
        if tipo == "Python":
            sucesso = executar_python_script(script_path)
        else:
            sucesso = False
        
        if not sucesso:
            sucesso_total = False
    
    if sucesso_total:
        log_message("=== ETL INCREMENTAL EXECUTADO COM SUCESSO ===")
    else:
        log_message("=== ETL INCREMENTAL FINALIZADO COM ERROS ===", "ERROR")
    
    return sucesso_total

if __name__ == "__main__":
    # Verificar argumentos da linha de comando
    if len(sys.argv) > 1:
        modo = sys.argv[1].lower()
        
        if modo == "incremental":
            executar_etl_incremental()
        elif modo == "completo":
            executar_etl_completo()
        else:
            print("Uso: python etl_master.py [completo|incremental]")
            print("  completo    - Executa ETL completo (recria tudo)")
            print("  incremental - Executa apenas atualização dos dados")
    else:
        # Por padrão, executar ETL completo
        executar_etl_completo()

