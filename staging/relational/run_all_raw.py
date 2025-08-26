#!/usr/bin/env python3
"""
Script coordenador para executar todos os scripts raw.
Executa os scripts de extração em ordem otimizada.
"""

import subprocess
import time
import os
from datetime import datetime
from base_raw import print_header, print_status

# Scripts disponíveis em ordem de prioridade
RAW_SCRIPTS = {
    'raw_tema.py': {
        'descricao': 'Temas e ODS',
        'prioridade': 1,
        'dependencias': []
    },
    'raw_ies.py': {
        'descricao': 'Instituições de Ensino Superior',
        'prioridade': 2,
        'dependencias': []
    },
    'raw_ppg.py': {
        'descricao': 'Programas de Pós-Graduação',
        'prioridade': 3,
        'dependencias': ['raw_ies.py']
    },
    'raw_docente.py': {
        'descricao': 'Docentes e Pesquisadores',
        'prioridade': 4,
        'dependencias': ['raw_ppg.py']
    },
    'raw_pq.py': {
        'descricao': 'Bolsas de Produtividade em Pesquisa',
        'prioridade': 5,
        'dependencias': ['raw_docente.py']
    },
    'raw_producao.py': {
        'descricao': 'Produção Acadêmica',
        'prioridade': 6,
        'dependencias': ['raw_docente.py']
    }
}

def execute_script(script_name, timeout=300):
    """Executa um script Python e retorna resultado"""
    script_path = os.path.join(os.path.dirname(__file__), script_name)
    
    if not os.path.exists(script_path):
        print_status(f"Script não encontrado: {script_name}", "error")
        return False, f"Arquivo {script_name} não existe"
    
    print_status(f"Iniciando execução de {script_name}...")
    start_time = time.time()
    
    try:
        result = subprocess.run(
            ['python3', script_path],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=os.path.dirname(script_path)
        )
        
        execution_time = time.time() - start_time
        
        if result.returncode == 0:
            print_status(f"{script_name} concluído em {execution_time:.1f}s", "success")
            return True, result.stdout
        else:
            print_status(f"{script_name} falhou (código: {result.returncode})", "error")
            return False, result.stderr
            
    except subprocess.TimeoutExpired:
        print_status(f"{script_name} excedeu tempo limite ({timeout}s)", "error")
        return False, f"Timeout após {timeout}s"
    except Exception as e:
        print_status(f"Erro ao executar {script_name}: {e}", "error")
        return False, str(e)

def get_execution_order():
    """Determina ordem de execução baseada em dependências"""
    executed = set()
    execution_order = []
    
    # Ordenar por prioridade
    scripts_by_priority = sorted(RAW_SCRIPTS.items(), key=lambda x: x[1]['prioridade'])
    
    for script, config in scripts_by_priority:
        # Verificar se dependências foram satisfeitas
        deps_satisfied = all(dep in executed for dep in config['dependencias'])
        
        if deps_satisfied:
            execution_order.append(script)
            executed.add(script)
    
    return execution_order

def main():
    print_header("Executando Todos os Scripts RAW")
    
    start_time = datetime.now()
    results = {}
    
    # Obter ordem de execução
    execution_order = get_execution_order()
    
    print_status(f"Scripts a executar: {len(execution_order)}")
    for i, script in enumerate(execution_order, 1):
        config = RAW_SCRIPTS[script]
        print_status(f"  {i}. {script} - {config['descricao']}")
    
    print()
    
    # Executar scripts em ordem
    for script in execution_order:
        config = RAW_SCRIPTS[script]
        
        print(f"\n{'='*60}")
        print(f"EXECUTANDO: {script.upper()}")
        print(f"Descrição: {config['descricao']}")
        print(f"{'='*60}")
        
        success, output = execute_script(script)
        results[script] = {
            'success': success,
            'output': output,
            'descricao': config['descricao']
        }
        
        if not success:
            print_status(f"Erro em {script}. Continuando com próximo script...", "warning")
            print(f"Detalhes do erro:\n{output}")
        
        # Pausa entre execuções
        time.sleep(2)
    
    # Relatório final
    end_time = datetime.now()
    total_time = (end_time - start_time).total_seconds()
    
    print(f"\n{'='*60}")
    print("RELATÓRIO FINAL DE EXECUÇÃO")
    print(f"{'='*60}")
    
    successful = sum(1 for r in results.values() if r['success'])
    failed = len(results) - successful
    
    print_status(f"Tempo total de execução: {total_time:.1f}s")
    print_status(f"Scripts executados: {len(results)}")
    print_status(f"Sucessos: {successful}", "success" if successful > 0 else "info")
    print_status(f"Falhas: {failed}", "error" if failed > 0 else "info")
    
    print(f"\nDetalhamento por script:")
    for script, result in results.items():
        status_icon = "✅" if result['success'] else "❌"
        print(f"  {status_icon} {script:<20} - {result['descricao']}")
    
    if failed == 0:
        print_status("Todos os scripts executados com sucesso!", "success")
    else:
        print_status(f"{failed} script(s) falharam. Verificar logs acima.", "warning")

if __name__ == "__main__":
    main()
