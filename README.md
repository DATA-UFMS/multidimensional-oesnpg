# Data Warehouse Observatório CAPES

> Sistema de análise multidimensional da pós-graduação brasileira  
> Star Schema com PostgreSQL 

```bash
# 1. Instalar dependências
pip install -r requirements.txt

# 2. Configurar banco (.env)
cp .env.example .env
# Editar com suas credenciais PostgreSQL

# 3. Executar etl
python etl/etl_master.py [completo|incremental]
python sql/utils/executar_fks.py

# 4. Verificar
python QUICKSTART.py
```

## O que é?

Sistema analítico em **Star Schema** para dados da pós-graduação brasileira:
- **8 dimensões** (tempo, localidade, IES, PPG, tema, produção, ODS, docente)
- **1 tabela fato** com 160+ registros (2021-2024)
- **Métricas**: acadêmicas, produção, RH e financeiras


## Como Usar

### 1. Criar Tabela Fato
```bash
python models/facts/create_fact_table.py
# Gera 160+ registros com dados realistas
```

### 2. Verificar
```bash
python QUICKSTART.py
```

## Visão Geral do Sistema

### O que é?
O Data Warehouse Observatório CAPES é um sistema analítico que organiza dados da pós-graduação brasileira em uma estrutura **Star Schema** otimizada para consultas OLAP e relatórios estratégicos.

### Para que serve?
- **Análises temporais** da evolução da pós-graduação (2021-2024)
- **Mapeamento geográfico** por estados e regiões
- **Comparação institucional** entre IES e PPGs
- **Alinhamento com ODS** da ONU
- **Métricas de produção** científica e acadêmica

### Como funciona?
```
DADOS DE ENTRADA          PROCESSAMENTO           SAÍDA ANALÍTICA
├─ Dimensões (8 tabelas)  ➜  ├─ Star Schema          ➜  ├─ Consultas OLAP
├─ Tempo (2021-2024)      ➜  ├─ Integridade Ref.    ➜  ├─ Dashboards
├─ Geografia (27 UFs)     ➜  ├─ Métricas agregadas  ➜  ├─ Relatórios
└─ Instituições (PPGs)    ➜  └─ 160+ registros      ➜  └─ Análises ad-hoc
```

## � Estrutura Atual

```
MULTIDIMENSIONAL-OESNPG/
├── models/                      # Modelos e lógica de negócio
│   ├── dimensions/              # 8 Scripts de dimensões
│   │   ├── dim_tempo.py         # Calendário 2000-2030
│   │   ├── dim_localidade.py    # Estados e regiões
│   │   ├── dim_tema.py          # Temas estratégicos
│   │   ├── dim_ods.py           # 17 ODS da ONU
│   │   ├── dim_ies.py           # Instituições
│   │   ├── dim_ppg.py           # Programas de pós-graduação
│   │   ├── dim_producao.py      # Produção científica
│   │   └── dim_docente.py       # Corpo docente
│   ├── facts/                   # Tabela fato
│   │   └── create_fact_table.py # Script principal único
│   └── utils/                   # Utilitários core
│       ├── core.py              # Todas as funcionalidades
│       └── __init__.py          # Exports e configuração
├── sql/                         # Scripts SQL organizados
│   ├── ddl/                     # DDL simples e diretos
│   │   ├── add_primary_keys_dimensoes.sql  # PKs das dimensões
│   │   └── add_fks_simples_fato.sql        # FKs da tabela fato
│   └── utils/                   # Utilitários SQL
│       └── executar_fks.py      # Script Python para PKs + FKs
├──  QUICKSTART.py             # Guia rápido e status
├── 🔧 setup_environment.py      # Setup automático
├── 📄 requirements.txt          # Dependências Python
└── � migration/             # Migração e compatibilidade
    ├── migration_tool.py       # Ferramenta de migração ETL
    ├── README.md              # Guia de migração
    └── *_migrated.py          # Templates migrados
```

## Scripts Principais

### 1️⃣ **Tabela Fato (ÚNICO)**
```bash
python models/facts/create_fact_table.py
```
- Cria estrutura completa da tabela fato
- Gera dados realistas baseados nas dimensões
- 160+ registros com crescimento ano a ano
- Funciona com psycopg2 (sem problemas SQLAlchemy)

### 2. Primary Keys e Foreign Keys
```bash
python sql/utils/executar_fks.py
```
- Executa PKs nas 8 dimensões automaticamente
- Cria FKs na tabela fato com integridade referencial
- Script único que resolve todas as constraints

## Exemplos de Uso

### Análise por Região
```sql
SELECT l.regiao, SUM(f.num_cursos) as cursos
FROM fato_pos_graduacao f
JOIN dim_localidade l ON f.localidade_sk = l.localidade_sk
GROUP BY l.regiao;
```

### Evolução Temporal
```sql
SELECT t.ano, COUNT(*) as programas, AVG(f.nota_avaliacao_capes) as nota
FROM fato_pos_graduacao f
JOIN dim_tempo t ON f.tempo_sk = t.tempo_sk
GROUP BY t.ano;
```

## Requisitos

- **Python 3.8+**
- **PostgreSQL 12+**
- **Dependências**: pandas, psycopg2-binary, python-dotenv

### .env
```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=dw_oesnpg
DB_USER=postgres
DB_PASSWORD=sua_senha
```

## Performance

- **Pipeline completo**: ~20 segundos
- **160 registros**: ~15 segundos
- **Constraints**: ~5 segundos
- **Uso RAM**: ~150MB

---

**Data Warehouse CAPES v1.0** | UFMS | 2025