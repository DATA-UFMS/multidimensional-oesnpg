#!/usr/bin/env python3
"""
Script de Instalação e Verificação de Dependências
Data Warehouse Observatório CAPES
30/07/2025
"""

import subprocess
import sys
import importlib
import os

def print_header(title):
    """Imprime cabeçalho formatado"""
    print("=" * 60)
    print(f"🔧 {title}")
    print("=" * 60)

def check_python_version():
    """Verifica versão do Python"""
    version = sys.version_info
    print(f"🐍 Python: {version.major}.{version.minor}.{version.micro}")
    
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("❌ Python 3.8+ é necessário!")
        return False
    
    print("✅ Versão do Python adequada")
    return True

def install_requirements():
    """Instala dependências do requirements.txt"""
    try:
        print("\n📦 Instalando dependências do requirements.txt...")
        
        # Verificar se requirements.txt existe
        if not os.path.exists('requirements.txt'):
            print("❌ Arquivo requirements.txt não encontrado!")
            return False
        
        # Instalar dependências
        result = subprocess.run([
            sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Dependências instaladas com sucesso!")
            return True
        else:
            print(f"❌ Erro na instalação: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Erro durante instalação: {e}")
        return False

def verify_dependencies():
    """Verifica se as dependências principais estão instaladas"""
    print("\n🔍 Verificando dependências principais...")
    
    core_deps = {
        'pandas': 'Manipulação de dados',
        'numpy': 'Computação numérica',
        'sqlalchemy': 'ORM e conexão BD',
        'psycopg2': 'Driver PostgreSQL',
        'requests': 'Requisições HTTP',
        'dotenv': 'Variáveis de ambiente'
    }
    
    failed = []
    
    for package, description in core_deps.items():
        try:
            if package == 'psycopg2':
                import psycopg2
            elif package == 'dotenv':
                from dotenv import load_dotenv
            else:
                importlib.import_module(package)
            
            print(f"   ✅ {package:12} - {description}")
            
        except ImportError:
            print(f"   ❌ {package:12} - {description} (FALTANDO)")
            failed.append(package)
    
    return len(failed) == 0, failed

def test_database_connection():
    """Testa configuração do Data Warehouse"""
    print("\n🗄️ Testando utils do Data Warehouse...")
    
    try:
        # Testar import dos utils
        from models.utils import Config, get_db_manager
        print("   ✅ Utils importados com sucesso")
        
        # Testar configuração
        config = Config()
        print(f"   ✅ Configurado para: {config.DB_NAME}")
        
        # Testar conexão (apenas instanciação)
        db = get_db_manager()
        print("   ✅ DatabaseManager instanciado")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Erro nos utils: {e}")
        return False

def main():
    """Função principal"""
    print_header("INSTALAÇÃO E VERIFICAÇÃO DE DEPENDÊNCIAS")
    
    # 1. Verificar Python
    if not check_python_version():
        sys.exit(1)
    
    # 2. Instalar dependências
    if not install_requirements():
        print("\n⚠️ Tentando continuar com verificação...")
    
    # 3. Verificar dependências
    deps_ok, failed = verify_dependencies()
    
    # 4. Testar Data Warehouse
    dw_ok = test_database_connection()
    
    # 5. Resultado final
    print_header("RESULTADO")
    
    if deps_ok and dw_ok:
        print("🎉 AMBIENTE CONFIGURADO COM SUCESSO!")
        print("\n📋 Próximos passos:")
        print("   1. Configurar arquivo .env com credenciais do banco")
        print("   2. Executar: python etl/etl_master.py completo")
        print("   3. Verificar dados: python QUICKSTART.py")
        
    else:
        print("⚠️ CONFIGURAÇÃO INCOMPLETA")
        
        if failed:
            print(f"\n❌ Dependências em falta: {', '.join(failed)}")
            print("   Solução: pip install -r requirements.txt")
        
        if not dw_ok:
            print("\n❌ Problemas com utils do Data Warehouse")
            print("   Verifique a estrutura do projeto")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()
