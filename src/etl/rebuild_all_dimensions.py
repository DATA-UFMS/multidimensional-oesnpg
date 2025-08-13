#!/usr/bin/env python3
"""
Script para recriar todas as dimensões do Data Warehouse com registros SK=0
"""

import subprocess
import sys
import os
from datetime import datetime

def run_script(script_name):
    """
    Executa um script de população de dimensão e captura o resultado.
    """
    print(f"\n{'='*60}")
    print(f"🔄 Executando {script_name}...")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run([sys.executable, script_name], 
                              capture_output=True, text=True, check=True)
        print(result.stdout)
        if result.stderr:
            print(f"⚠️ Avisos: {result.stderr}")
        print(f"✅ {script_name} executado com sucesso!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Erro ao executar {script_name}:")
        print(f"Código de saída: {e.returncode}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        return False

def main():
    """
    Executa todos os scripts de população de dimensões na ordem correta.
    """
    print("🏗️ RECRIANDO TODAS AS DIMENSÕES DO DATA WAREHOUSE")
    print(f"📅 Iniciado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Obter diretório atual e definir caminho para as dimensões
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    dimensions_dir = os.path.join(project_root, 'models', 'dimensions')
    
    # Lista de scripts na ordem de execução (independentes primeiro)
    scripts = [
        "dim_tempo.py",       # Independente
        "dim_localidade.py",  # Independente  
        "dim_tema.py",        # Independente
        "dim_ods.py",         # Independente (ODS da ONU)
        "dim_ies.py",         # Usa API CAPES
        "dim_ppg.py",         # Usa CSV ou API CAPES
        "dim_producao.py",    # Usa API CAPES
        "dim_docente.py",     # Usa CSV CAPES
    ]
    
    resultados = {}
    
    for script in scripts:
        script_path = os.path.join(dimensions_dir, script)
        if os.path.exists(script_path):
            resultados[script] = run_script(script_path)
        else:
            print(f"⚠️ Arquivo {script} não encontrado em {dimensions_dir}!")
            resultados[script] = False
    
    # Resumo final
    print(f"\n{'='*60}")
    print("📊 RESUMO DA EXECUÇÃO")
    print(f"{'='*60}")
    
    sucessos = 0
    falhas = 0
    
    for script, sucesso in resultados.items():
        status = "✅ SUCESSO" if sucesso else "❌ FALHA"
        print(f"  {script:<30} {status}")
        if sucesso:
            sucessos += 1
        else:
            falhas += 1
    
    print(f"\n📈 Total: {sucessos} sucessos, {falhas} falhas")
    print(f"📅 Finalizado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if falhas == 0:
        print("\n🎉 Todas as dimensões foram criadas com sucesso!")
        print("💡 Todas as dimensões agora possuem registro SK=0 para valores desconhecidos.")
        
        # Verificar registros SK=0
        print("\n🔍 Verificando registros SK=0...")
        verify_sk_zero()
    else:
        print(f"\n⚠️ {falhas} script(s) falharam. Verifique os logs acima.")
        return 1
    
    return 0

def verify_sk_zero():
    """
    Verifica se todas as dimensões têm registros SK=0.
    """
    try:
        from sqlalchemy import create_engine, text
        import os
        from dotenv import load_dotenv
        
        load_dotenv()
        
        DB_HOST = os.getenv('DB_HOST')
        DB_NAME = os.getenv('DB_NAME')
        DB_USER = os.getenv('DB_USER')
        DB_PASS = os.getenv('DB_PASS')
        DB_PORT = os.getenv('DB_PORT')
        
        url = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(url)
        
        tables = ['dim_tempo', 'dim_localidade', 'dim_tema', 'dim_ies', 'dim_ppg', 'dim_producao']
        
        with engine.connect() as conn:
            print("Registros SK=0 por dimensão:")
            for table in tables:
                try:
                    sk_col = table.replace('dim_', '') + '_sk'
                    result = conn.execute(text(f'SELECT COUNT(*) FROM {table} WHERE {sk_col} = 0')).fetchone()
                    count = result[0] if result else 0
                    status = "✅" if count > 0 else "❌"
                    print(f"  {status} {table}: {count} registro(s)")
                except Exception as e:
                    print(f"  ⚠️ {table}: Erro - {e}")
    
    except Exception as e:
        print(f"⚠️ Erro ao verificar registros SK=0: {e}")

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
