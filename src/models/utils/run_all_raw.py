#!/usr/bin/env python3
"""
Script utilitário: executa pipelines da camada RAW.

Permite rodar todos os scripts ou um subconjunto informado via CLI.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List

# Adicionar diretório raiz ao path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent.parent
sys.path.insert(0, str(project_root))

RAW_PIPELINES: List[Dict] = [
    {
        "key": "raw_docente",
        "nome": "RAW_DOCENTE",
        "caminho": "staging/relational/raw_docente.py",
        "descricao": "Carga de dados de docentes",
        "ordem": 1,
    },
    {
        "key": "raw_ppg",
        "nome": "RAW_PPG",
        "caminho": "staging/relational/raw_ppg.py",
        "descricao": "Carga de dados de programas de pós-graduação",
        "ordem": 2,
    },
    {
        "key": "raw_tema",
        "nome": "RAW_TEMA",
        "caminho": "staging/relational/raw_tema.py",
        "descricao": "Carga de dados de temas/palavras-chave",
        "ordem": 3,
    },
]

RAW_MAP = {item["key"]: item for item in RAW_PIPELINES}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa scripts da camada RAW do DW OESNPG."
    )
    parser.add_argument(
        "-r",
        "--raws",
        nargs="+",
        help="Lista de pipelines RAW (ex.: raw_docente raw_ppg). "
        "Se omitido, executa todos.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Apenas lista os pipelines disponíveis e encerra.",
    )
    parser.add_argument(
        "--show-stdout",
        action="store_true",
        help="Mostra a saída completa (stdout) dos scripts invocados.",
    )
    return parser.parse_args()


def listar_raws() -> None:
    print("Pipelines RAW disponíveis:")
    for item in sorted(RAW_PIPELINES, key=lambda s: s["ordem"]):
        print(f"  - {item['key']:12s} | {item['descricao']}")


def selecionar_scripts(raws: Iterable[str] | None) -> List[Dict]:
    if not raws:
        return sorted(RAW_PIPELINES, key=lambda s: s["ordem"])

    selecionados = []
    for key in raws:
        key_norm = key.strip().lower()
        if key_norm not in RAW_MAP:
            raise ValueError(
                f"Pipeline RAW desconhecido: '{key}'. Utilize --list para consultar as opções."
            )
        selecionados.append(RAW_MAP[key_norm])
    return selecionados


def executar_raw(raws: Iterable[str] | None = None, *, show_stdout: bool = False) -> bool:
    scripts = selecionar_scripts(raws)

    print("=" * 80)
    print("EXECUTANDO CAMADA RAW")
    print("=" * 80)
    print(f"Início: {datetime.now():%Y-%m-%d %H:%M:%S}\n")

    resultados = []

    for script in scripts:
        print("-" * 80)
        print(f"Executando: {script['nome']}")
        print(f"Descrição: {script['descricao']}")
        print(f"Arquivo: {script['caminho']}")
        print("-" * 80)
        try:
            inicio = datetime.now()
            script_path = project_root / script["caminho"]
            if not script_path.exists():
                raise FileNotFoundError(f"Script não encontrado: {script_path}")

            resultado = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=str(project_root),
                capture_output=not show_stdout,
                text=True,
            )

            if show_stdout and resultado.stdout:
                print(resultado.stdout)
            elif resultado.stdout:
                print(resultado.stdout.strip())

            if resultado.returncode != 0:
                if resultado.stderr:
                    print(resultado.stderr)
                raise RuntimeError(f"Código de saída: {resultado.returncode}")

            fim = datetime.now()
            tempo = (fim - inicio).total_seconds()

            resultados.append(
                {
                    "script": script["nome"],
                    "status": "SUCESSO ✅",
                    "tempo": f"{tempo:.2f}s",
                }
            )
            print(f"✅ {script['nome']} concluído com sucesso em {tempo:.2f}s\n")

        except Exception as exc:  # pylint: disable=broad-except
            resultados.append(
                {
                    "script": script["nome"],
                    "status": f"ERRO ❌: {str(exc)[:60]}",
                    "tempo": "-",
                }
            )
            print(f"❌ Erro ao executar {script['nome']}: {exc}\n")

    _imprimir_resumo(resultados)
    erros = len([r for r in resultados if "ERRO" in r["status"]])
    return erros == 0


def _imprimir_resumo(resultados: List[Dict]) -> None:
    print("=" * 80)
    print("RESUMO DA EXECUÇÃO - CAMADA RAW")
    print("=" * 80)
    for resultado in resultados:
        print(
            f"{resultado['script']:<30} {resultado['status']:<50} {resultado['tempo']:>10}"
        )

    total = len(resultados)
    sucessos = len([r for r in resultados if "SUCESSO" in r["status"]])
    erros = total - sucessos

    print("\n" + "=" * 80)
    print(f"Fim: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 80)

    print(f"\n📊 Estatísticas:")
    print(f"   Total de scripts RAW: {total}")
    print(f"   Sucesso: {sucessos} ✅")
    print(f"   Erros: {erros} ❌")

    if erros > 0:
        print(f"\n⚠️  {erros} script(s) com erro. Verifique os logs acima.")
    else:
        print(f"\n✅ Todos os {total} scripts RAW executados com sucesso!")


def main() -> None:
    args = parse_args()
    if args.list:
        listar_raws()
        sys.exit(0)

    try:
        sucesso = executar_raw(args.raws, show_stdout=args.show_stdout)
        sys.exit(0 if sucesso else 1)
    except ValueError as err:
        print(f"❌ {err}")
        listar_raws()
        sys.exit(1)


if __name__ == "__main__":
    main()
