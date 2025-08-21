#!/usr/bin/env python3
"""
Script master para executar todos os scripts raw em sequência.
Executa a extração de dados brutos de todas as fontes (CSV, Excel, API) 
seguindo a ordem correta de dependências.
"""

import os
import sys
import subprocess
import time
from datetime import datetime
from pathlib import Path

# Adicionar diretório raiz ao path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))  # Dois níveis acima para chegar na raiz
sys.path.insert(0, project_root)

def executar_script(script_path, nome_script):
    """
    Executa um script Python e retorna o resultado.
    """
    print(f"\n{'='*80}")
    print(f"🚀 EXECUTANDO: {nome_script}")
    print(f"📁 Caminho: {script_path}")
    print(f"{'='*80}")
    
    inicio = time.time()
    
    try:
        # Executar o script usando subprocess
        result = subprocess.run([
            sys.executable, script_path
        ], capture_output=True, text=True, cwd=os.path.dirname(script_path))
        
        fim = time.time()
        duracao = fim - inicio
        
        if result.returncode == 0:
            print(f"✅ {nome_script} executado com SUCESSO!")
            print(f"⏱️ Tempo de execução: {duracao:.2f} segundos")
            if result.stdout:
                print(f"\n📋 OUTPUT:")
                print(result.stdout[-1000:])  # Últimas 1000 chars para não sobrecarregar
        else:
            print(f"❌ {nome_script} FALHOU!")
            print(f"⏱️ Tempo de execução: {duracao:.2f} segundos")
            print(f"🔍 Código de saída: {result.returncode}")
            if result.stderr:
                print(f"\n❌ ERRO:")
                print(result.stderr)
            if result.stdout:
                print(f"\n📋 OUTPUT:")
                print(result.stdout)
        
        return result.returncode == 0, duracao
        
    except Exception as e:
        fim = time.time()
        duracao = fim - inicio
        print(f"❌ ERRO ao executar {nome_script}: {e}")
        print(f"⏱️ Tempo de execução: {duracao:.2f} segundos")
        return False, duracao

def main():
    """
    Executa todos os scripts raw em sequência.
    """
    print("🏗️ EXECUTOR MASTER - TODOS OS SCRIPTS RAW")
    print("=" * 80)
    print(f"📅 Início da execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Diretório do projeto: {project_root}")
    print("=" * 80)
    
    # Definir scripts na ordem de execução
    scripts_raw = [
        {
            'path': 'staging/relational/raw_tema.py',
            'nome': 'RAW_TEMA',
            'descricao': 'Dados de temas/palavras-chave do Excel'
        },
        {
            'path': 'staging/relational/raw_docente.py', 
            'nome': 'RAW_DOCENTE',
            'descricao': 'Dados de docentes do CSV CAPES'
        },
        {
            'path': 'staging/relational/raw_ppg.py',
            'nome': 'RAW_PPG', 
            'descricao': 'Dados de programas de pós-graduação do CSV'
        },
        {
            'path': 'staging/relational/raw_ies_api.py',
            'nome': 'RAW_IES_API',
            'descricao': 'Dados de IES da API CAPES'
        },
        {
            'path': 'staging/relational/raw_producao.py',
            'nome': 'RAW_PRODUCAO',
            'descricao': 'Dados de produção acadêmica da API CAPES'
        }
    ]
    
    # Verificar se todos os scripts existem
    print("🔍 VERIFICANDO EXISTÊNCIA DOS SCRIPTS:")
    print("-" * 50)
    
    scripts_validos = []
    for script in scripts_raw:
        script_path = os.path.join(project_root, script['path'])
        if os.path.exists(script_path):
            print(f"✅ {script['nome']}: {script['path']}")
            scripts_validos.append({**script, 'full_path': script_path})
        else:
            print(f"❌ {script['nome']}: {script['path']} (NÃO ENCONTRADO)")
    
    if not scripts_validos:
        print("\n❌ Nenhum script válido encontrado! Encerrando.")
        return
    
    print(f"\n📊 Total de scripts encontrados: {len(scripts_validos)}/{len(scripts_raw)}")
    
    # Executar scripts em sequência
    resultados = []
    tempo_total_inicio = time.time()
    
    for i, script in enumerate(scripts_validos, 1):
        print(f"\n🔄 PROGRESSO: {i}/{len(scripts_validos)} scripts")
        print(f"📝 {script['nome']}: {script['descricao']}")
        
        sucesso, duracao = executar_script(script['full_path'], script['nome'])
        
        resultados.append({
            'nome': script['nome'],
            'descricao': script['descricao'],
            'sucesso': sucesso,
            'duracao': duracao
        })
        
        # Pequena pausa entre execuções
        if i < len(scripts_validos):
            print(f"\n⏳ Pausando 2 segundos antes do próximo script...")
            time.sleep(2)
    
    tempo_total_fim = time.time()
    duracao_total = tempo_total_fim - tempo_total_inicio
    
    # Relatório final
    print(f"\n{'='*80}")
    print("📋 RELATÓRIO FINAL DA EXECUÇÃO")
    print(f"{'='*80}")
    
    sucessos = 0
    falhas = 0
    
    print("📊 RESULTADOS POR SCRIPT:")
    print("-" * 80)
    for resultado in resultados:
        status = "✅ SUCESSO" if resultado['sucesso'] else "❌ FALHA"
        duracao_min = resultado['duracao'] / 60
        print(f"{status:12} | {resultado['nome']:15} | {duracao_min:6.2f}min | {resultado['descricao']}")
        
        if resultado['sucesso']:
            sucessos += 1
        else:
            falhas += 1
    
    print(f"\n📈 ESTATÍSTICAS FINAIS:")
    print(f"  • Scripts executados: {len(resultados)}")
    print(f"  • Sucessos: {sucessos}")
    print(f"  • Falhas: {falhas}")
    print(f"  • Taxa de sucesso: {(sucessos/len(resultados)*100):.1f}%")
    print(f"  • Tempo total: {duracao_total/60:.2f} minutos")
    print(f"  • Tempo médio por script: {duracao_total/len(resultados):.2f} segundos")
    
    # Status final
    if falhas == 0:
        print(f"\n🎉 EXECUÇÃO CONCLUÍDA COM SUCESSO!")
        print(f"✅ Todos os {sucessos} scripts raw foram executados sem erros")
        print(f"💾 Todas as tabelas raw estão disponíveis no PostgreSQL")
        print(f"🎯 Sistema pronto para execução das dimensões")
    else:
        print(f"\n⚠️ EXECUÇÃO CONCLUÍDA COM PROBLEMAS!")
        print(f"✅ {sucessos} scripts executados com sucesso")
        print(f"❌ {falhas} scripts falharam")
        print(f"🔧 Revise os logs acima para identificar e corrigir os problemas")
    
    print(f"\n📅 Fim da execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    
    # Retornar código de saída baseado no resultado
    return 0 if falhas == 0 else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
