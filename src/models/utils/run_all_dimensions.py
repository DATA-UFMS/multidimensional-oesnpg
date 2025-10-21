#!/usr/bin/env python3
"""
Script utilitário: executa pipelines das dimensões do DW OESNPG.

Permite rodar todas as dimensões em sequência ou um subconjunto informado via CLI.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List

# Adicionar diretório raiz ao path para manter compatibilidade com imports relativos
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent.parent
sys.path.insert(0, str(project_root))

DIMENSION_PIPELINES: List[Dict] = [
    {
        "key": "dim_tempo",
        "nome": "DIM_TEMPO",
        "caminho": "src/models/dimensions/dim_tempo.py",
        "descricao": "Dimensão temporal (datas, anos, trimestres)",
        "prioridade": 1,
        "ordem": 1,
    },
    {
        "key": "dim_localidade",
        "nome": "DIM_LOCALIDADE",
        "caminho": "src/models/dimensions/dim_localidade.py",
        "descricao": "Dimensão de localidades (estados, municípios)",
        "prioridade": 1,
        "ordem": 2,
    },
    {
        "key": "dim_tema",
        "nome": "DIM_TEMA",
        "caminho": "src/models/dimensions/dim_tema.py",
        "descricao": "Dimensão de temas e palavras-chave",
        "prioridade": 1,
        "ordem": 3,
    },
    {
        "key": "dim_ods",
        "nome": "DIM_ODS",
        "caminho": "src/models/dimensions/dim_ods.py",
        "descricao": "Dimensão dos Objetivos de Desenvolvimento Sustentável",
        "prioridade": 1,
        "ordem": 4,
    },
    {
        "key": "dim_ies",
        "nome": "DIM_IES",
        "caminho": "src/models/dimensions/dim_ies.py",
        "descricao": "Dimensão de Instituições de Ensino Superior",
        "prioridade": 2,
        "ordem": 5,
    },
    {
        "key": "dim_ppg",
        "nome": "DIM_PPG",
        "caminho": "src/models/dimensions/dim_ppg.py",
        "descricao": "Dimensão de Programas de Pós-Graduação",
        "prioridade": 2,
        "ordem": 6,
    },
    {
        "key": "dim_docente",
        "nome": "DIM_DOCENTE",
        "caminho": "src/models/dimensions/dim_docente.py",
        "descricao": "Dimensão de docentes",
        "prioridade": 3,
        "ordem": 7,
    },
    {
        "key": "dim_discente",
        "nome": "DIM_DISCENTE",
        "caminho": "src/models/dimensions/dim_discente.py",
        "descricao": "Dimensão de discentes",
        "prioridade": 3,
        "ordem": 8,
    },
    {
        "key": "dim_titulado",
        "nome": "DIM_TITULADO",
        "caminho": "src/models/dimensions/dim_titulado.py",
        "descricao": "Dimensão de titulados",
        "prioridade": 3,
        "ordem": 9,
    },
    {
        "key": "dim_posdoc",
        "nome": "DIM_POSDOC",
        "caminho": "src/models/dimensions/dim_posdoc.py",
        "descricao": "Dimensão de pós-doutorandos",
        "prioridade": 3,
        "ordem": 10,
    },
    {
        "key": "dim_producao",
        "nome": "DIM_PRODUCAO",
        "caminho": "src/models/dimensions/dim_producao.py",
        "descricao": "Dimensão de produções (2023-2024)",
        "prioridade": 4,
        "ordem": 11,
    },
]

PIPELINE_MAP = {item["key"]: item for item in DIMENSION_PIPELINES}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa pipelines de dimensões do DW OESNPG."
    )
    parser.add_argument(
        "-d",
        "--dimensions",
        nargs="+",
        help="Lista de dimensões para executar (ex.: dim_tempo dim_tema). "
        "Se omitido, executa todas conforme a ordem padrão.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Apenas lista as dimensões disponíveis e encerra.",
    )
    parser.add_argument(
        "--show-stdout",
        action="store_true",
        help="Mostra a saída completa (stdout) dos scripts executados.",
    )
    return parser.parse_args()


def listar_dimensoes() -> None:
    print("Dimensões disponíveis:")
    for item in sorted(DIMENSION_PIPELINES, key=lambda s: (s["prioridade"], s["ordem"])):
        print(
            f"  - {item['key']:12s} | prioridade {item['prioridade']} | {item['descricao']}"
        )


def selecionar_scripts(dimensions: Iterable[str] | None) -> List[Dict]:
    if not dimensions:
        return sorted(DIMENSION_PIPELINES, key=lambda s: (s["prioridade"], s["ordem"]))

    selecionados = []
    for key in dimensions:
        key_norm = key.strip().lower()
        if key_norm not in PIPELINE_MAP:
            raise ValueError(
                f"Dimensão desconhecida: '{key}'. Utilize --list para consultar as opções."
            )
        selecionados.append(PIPELINE_MAP[key_norm])
    return selecionados


def executar_dimensoes(
    dimensions: Iterable[str] | None = None, *, show_stdout: bool = False
) -> bool:
    scripts = selecionar_scripts(dimensions)
    agrupar_por_prioridade = dimensions is None

    print("=" * 80)
    print("EXECUTANDO DIMENSÕES")
    print("=" * 80)
    print(f"Início: {datetime.now():%Y-%m-%d %H:%M:%S}\n")

    resultados: List[Dict] = []

    if agrupar_por_prioridade:
        prioridades = sorted({s["prioridade"] for s in scripts})
        for prioridade in prioridades:
            grupo = [s for s in scripts if s["prioridade"] == prioridade]
            if not grupo:
                continue

            print("=" * 80)
            print(f"GRUPO DE PRIORIDADE {prioridade}")
            print("=" * 80)
            _executar_grupo(grupo, resultados, show_stdout)
    else:
        _executar_grupo(scripts, resultados, show_stdout)

    _imprimir_resumo(resultados)
    erros = len([r for r in resultados if "ERRO" in r["status"]])
    return erros == 0


def _executar_grupo(
    scripts: Iterable[Dict], resultados: List[Dict], show_stdout: bool
) -> None:
    for script in scripts:
        print("\n" + "-" * 80)
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
                    "dimensao": script["nome"],
                    "status": "SUCESSO ✅",
                    "tempo": f"{tempo:.2f}s",
                    "prioridade": script["prioridade"],
                }
            )
            print(f"\n✅ {script['nome']} concluída com sucesso em {tempo:.2f}s")

        except Exception as exc:  # pylint: disable=broad-except
            resultados.append(
                {
                    "dimensao": script["nome"],
                    "status": f"ERRO ❌: {str(exc)[:60]}",
                    "tempo": "-",
                    "prioridade": script.get("prioridade"),
                }
            )
            print(f"\n❌ Erro ao executar {script['nome']}: {exc}")


def _imprimir_resumo(resultados: List[Dict]) -> None:
    print("\n" + "=" * 80)
    print("RESUMO DA EXECUÇÃO DAS DIMENSÕES")
    print("=" * 80)
    print(f"\n{'Dimensão':<30} {'Status':<50} {'Tempo':>10}")
    print("-" * 80)

    for resultado in resultados:
        print(
            f"{resultado['dimensao']:<30} "
            f"{resultado['status']:<50} "
            f"{resultado['tempo']:>10}"
        )

    total = len(resultados)
    sucessos = len([r for r in resultados if "SUCESSO" in r["status"]])
    erros = total - sucessos

    print("\n" + "=" * 80)
    print(f"Fim: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 80)

    print(f"\n📊 Estatísticas:")
    print(f"   Total de dimensões: {total}")
    print(f"   Sucesso: {sucessos} ✅")
    print(f"   Erros: {erros} ❌")

    prioridades = sorted({r["prioridade"] for r in resultados if r["prioridade"]})
    if prioridades:
        print(f"\n📊 Por grupo de prioridade:")
        for prioridade in prioridades:
            grupo = [r for r in resultados if r["prioridade"] == prioridade]
            sucessos_grupo = len([r for r in grupo if "SUCESSO" in r["status"]])
            print(f"   Prioridade {prioridade}: {sucessos_grupo}/{len(grupo)} sucesso(s)")


def main() -> None:
    args = parse_args()
    if args.list:
        listar_dimensoes()
        sys.exit(0)

    try:
        sucesso = executar_dimensoes(args.dimensions, show_stdout=args.show_stdout)
        sys.exit(0 if sucesso else 1)
    except ValueError as err:
        print(f"❌ {err}")
        listar_dimensoes()
        sys.exit(1)


if __name__ == "__main__":
    main()
