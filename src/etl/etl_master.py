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

def verificar_integridade_schema():
    """
    Verifica se as PKs e FKs foram criadas corretamente.
    """
    try:
        log_message("Verificando integridade do schema...")
        
        url = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(url)
        
        with engine.connect() as conn:
            # Verificar Primary Keys
            pk_query = text("""
                SELECT 
                    tc.table_name,
                    tc.constraint_name,
                    kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_name LIKE 'dim_%'
                ORDER BY tc.table_name
            """)
            
            pk_result = conn.execute(pk_query)
            pks = pk_result.fetchall()
            
            # Verificar Foreign Keys
            fk_query = text("""
                SELECT 
                    tc.constraint_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY' 
                  AND tc.table_name = 'fato_pos_graduacao'
                ORDER BY tc.constraint_name
            """)
            
            fk_result = conn.execute(fk_query)
            fks = fk_result.fetchall()
            
        log_message(f"✅ Primary Keys encontradas: {len(pks)}")
        for pk in pks:
            log_message(f"   📋 {pk.table_name}.{pk.column_name} ({pk.constraint_name})")
            
        log_message(f"✅ Foreign Keys encontradas: {len(fks)}")
        for fk in fks:
            log_message(f"   🔗 {fk.column_name} -> {fk.foreign_table_name}.{fk.foreign_column_name}")
        
        # Verificação ajustada: espera-se 8 dimensões com PKs e 4 FKs na tabela fato simplificada
        # Fato simplificada usa apenas: tempo_sk, localidade_sk, tema_sk, ies_sk
        pks_ok = len(pks) >= 8  # 8 dimensões devem ter PKs
        fks_ok = len(fks) >= 4  # 4 FKs na tabela fato simplificada
        
        if pks_ok and fks_ok:
            log_message("✅ Schema íntegro: PKs e FKs criadas conforme modelo simplificado")
            return True
        else:
            missing_pks = max(0, 8 - len(pks))
            missing_fks = max(0, 4 - len(fks))
            if missing_pks > 0:
                log_message(f"⚠️  Faltam {missing_pks} Primary Keys", "WARNING")
            if missing_fks > 0:
                log_message(f"⚠️  Faltam {missing_fks} Foreign Keys na tabela fato", "WARNING")
            return False
        
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
        
        # 4. Definir chaves primárias e estrangeiras
        ("SQL", "../../sql/ddl/add_pk.sql", "Definição de chaves primárias"),
        ("SQL", "../../sql/ddl/add_fk.sql", "Definição de chaves estrangeiras"),
    ]
    
    # Executar scripts em sequência
    sucesso_total = True
    scripts_executados = []
    scripts_com_erro = []
    scripts_nao_executados = []
    
    for i, (tipo, script, descricao) in enumerate(scripts_etl):
        log_message(f"--- {descricao} ---")
        
        script_path = os.path.join(os.path.dirname(__file__), script)
        
        if not os.path.exists(script_path):
            log_message(f"Script não encontrado: {script_path}", "ERROR")
            scripts_com_erro.append((descricao, f"Arquivo não encontrado: {script_path}"))
            sucesso_total = False
            # Adicionar scripts restantes à lista de não executados
            for j in range(i+1, len(scripts_etl)):
                scripts_nao_executados.append(scripts_etl[j][2])
            break
        
        if tipo == "SQL":
            sucesso = executar_sql_script(script_path)
        elif tipo == "Python":
            sucesso = executar_python_script(script_path)
        else:
            log_message(f"Tipo de script desconhecido: {tipo}", "ERROR")
            sucesso = False
        
        if sucesso:
            scripts_executados.append(descricao)
        else:
            scripts_com_erro.append((descricao, f"Falha na execução de: {script}"))
            sucesso_total = False
            log_message(f"❌ ERRO CRÍTICO: Falha na execução de: {script}", "ERROR")
            log_message(f"🚨 ABORTANDO PROCESSO ETL", "ERROR")
            # Adicionar scripts restantes à lista de não executados
            for j in range(i+1, len(scripts_etl)):
                scripts_nao_executados.append(scripts_etl[j][2])
            break
    
    # Resultado final com relatório detalhado
    print("\n" + "="*80)
    if sucesso_total:
        log_message("=== ✅ ETL COMPLETO EXECUTADO COM SUCESSO ===")
        print(f"🎉 Todos os {len(scripts_executados)} scripts foram executados com sucesso!")
        print("\n📋 Scripts executados:")
        for i, script in enumerate(scripts_executados, 1):
            print(f"   {i:2d}. ✅ {script}")
        
        # Verificar integridade do schema
        print("\n" + "="*40)
        log_message("=== VERIFICAÇÃO DE INTEGRIDADE DO SCHEMA ===")
        if verificar_integridade_schema():
            log_message("✅ Schema íntegro: PKs e FKs criadas corretamente")
        else:
            log_message("⚠️  Possíveis problemas de integridade detectados", "WARNING")
    else:
        log_message("=== ❌ ETL COMPLETO FINALIZADO COM ERROS ===", "ERROR")
        print("\n📊 RELATÓRIO DE EXECUÇÃO:")
        
        if scripts_executados:
            print(f"\n✅ Scripts executados com SUCESSO ({len(scripts_executados)}):")
            for i, script in enumerate(scripts_executados, 1):
                print(f"   {i:2d}. ✅ {script}")
        
        if scripts_com_erro:
            print(f"\n❌ Scripts com ERRO ({len(scripts_com_erro)}):")
            for i, (script, erro) in enumerate(scripts_com_erro, 1):
                print(f"   {i:2d}. ❌ {script}")
                print(f"       🔍 {erro}")
        
        if scripts_nao_executados:
            print(f"\n⏸️  Scripts NÃO EXECUTADOS devido ao erro ({len(scripts_nao_executados)}):")
            for i, script in enumerate(scripts_nao_executados, 1):
                print(f"   {i:2d}. ⏸️  {script}")
        
        print(f"\n🚨 PROCESSO ABORTADO APÓS PRIMEIRO ERRO")
        print(f"💡 Corrija os erros acima e execute novamente")
    
    print("="*80)
    
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

