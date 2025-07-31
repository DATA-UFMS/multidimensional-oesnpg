#!/usr/bin/env python3
import os
import subprocess

def main():
    print("🏛️ Data Warehouse Observatório CAPES v2.0")
    print("=" * 70)
    
    print("\n ESTRUTURA ATUAL (Pós-Reorganização):")
    print("┌─ MULTIDIMENSIONAL-OESNPG/")
    print("├── etl/                     Pipeline ETL Principal")
    print("│   ├── etl_master.py        Orquestrador principal")
    print("│   ├── demo_ppg_config.py   Configurações de demo")
    print("│   ├── example_new_etl.py   Exemplo da nova arquitetura")
    print("│   ├── rebuild_all_dimensions.py  Rebuild completo")
    print("│   └── test_new_architecture.py   Testes da arquitetura")
    print("│")
    print("├── models/                  Modelos e Lógica de Negócio")
    print("│   ├── dimensions/          8 Scripts de Dimensões")
    print("│   │   ├── dim_tempo.py     Calendário 2000-2030")
    print("│   │   ├── dim_localidade.py Estados e regiões")
    print("│   │   ├── dim_tema.py      Temas estratégicos")
    print("│   │   ├── dim_ods.py       17 ODS da ONU")
    print("│   │   ├── dim_ies.py       Instituições")
    print("│   │   ├── dim_ppg.py       Programas de pós-grad")
    print("│   │   ├── dim_producao.py  Produção científica")
    print("│   │   └── dim_docente.py   Corpo docente")
    print("│   ├── facts/               Tabela Fato")
    print("│   │   └── create_fact_table.py ⭐ SCRIPT PRINCIPAL ÚNICO")
    print("│   └── utils/               Utilitários Core")
    print("│       ├── core.py          Todas as funcionalidades")
    print("│       └── __init__.py      Exports e configuração")
    print("│")
    print("├── persistence/             Migração e Compatibilidade")
    print("│   ├── migration_tool.py    Ferramenta de migração")
    print("│   ├── README.md           Guia de migração")
    print("│   └── dim_*_migrated.py   Templates migrados")
    print("│")
    print("├── sql/                     Scripts SQL ORGANIZADOS")
    print("│   └── ddl/                 DDL Simples e Diretos")
    print("│       ├── add_primary_keys_dimensoes.sql  PKs das dimensões")
    print("│       └── add_fks_simples_fato.sql        FKs da tabela fato")
    print("│")
    print("├── executar_fks.py          Script Python para executar FKs")
    print("├── QUICKSTART.py            Guia rápido e status atual")
    print("├── setup_environment.py     Setup automático")
    print("│")
    print("├── .env                     Configurações de ambiente")
    print("├── requirements.txt         Dependências Python")
    print("├── setup_environment.py     Setup automático")
    print("└── README.md               Documentação principal")

    print("\n🎯 COMANDOS DE EXECUÇÃO ATUALIZADOS:")
    print("\n1️⃣ Criar Tabela Fato (PRINCIPAL):")
    print("   conda activate base")
    print("   python models/facts/create_fact_table.py")
    
    print("\n2️⃣ Adicionar Primary Keys (OPCIONAL):")
    print("   psql -f sql/ddl/add_primary_keys_dimensoes.sql") 
    
    print("\n3️⃣ Adicionar Foreign Keys (OPCIONAL):")
    print("   python executar_fks.py")
    print("   # OU: psql -f sql/ddl/add_fks_simples_fato.sql")
    
    print("\n4️⃣ Executar Dimensão Individual:")
    print("   python models/dimensions/dim_tempo.py")
    print("   python models/dimensions/dim_localidade.py")
    
    print("\n5️⃣ Verificar Status:")
    print("   python QUICKSTART.py")


if __name__ == "__main__":
    main()
