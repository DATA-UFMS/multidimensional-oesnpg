# 🎓 Data Warehouse Observatório CAPES

> Sistema de análise multidimensional da pós-graduação brasileira  
> **Star Schema** com PostgreSQL | 160+ registros | 8 dimensões  
> [![Status](https://img.shields.io/badge/Status-Produção-green)](.) [![Python](https://img.shields.io/badge/Python-3.8+-blue)](.) [![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-blue)](.)

## 🚀 Início Rápido 

```bash
# 1. 📦 Instalar dependências
pip install -r requirements.txt

# 2. 🔧 Configurar banco (.env)
cp .env.example .env
# Editar com suas credenciais PostgreSQL

# 3. 🚀 Executar ETL completo
python src/etl/etl_master.py

# 4. ✅ Verificar funcionamento
psql -d dw_oesnpg -c "\dt"
psql -d dw_oesnpg -c "SELECT COUNT(*) FROM fato_pos_graduacao;"
```

## O que é?

Sistema analítico em **Star Schema** para dados da pós-graduação brasileira:
- **8 dimensões** (tempo, localidade, IES, PPG, tema, produção, ODS, docente)
- **1 tabela fato** com 160+ registros (2021-2024)
- **Métricas**: acadêmicas, produção, RH e financeiras


## 🎯 Como Usar

### 1️⃣ Criar Tabela Fato (PRINCIPAL)
```bash
python src/models/facts/create_fact_table.py
# Gera 88,816 registros com dados realistas
```

### 2️⃣ Adicionar Primary Keys (OPCIONAL)
```bash
psql -f sql/ddl/add_primary_keys_dimensoes.sql
# Adiciona chaves primárias em todas as dimensões
```

### 3️⃣ Adicionar Foreign Keys (OPCIONAL)
```bash
python sql/utils/executar_fks.py
# OU: psql -f sql/ddl/add_fks_fato.sql
# Adiciona integridade referencial
```

### 4️⃣ Executar Dimensão Individual
```bash
python src/models/dimensions/dim_tempo.py
python src/models/dimensions/dim_localidade.py
# Executar dimensões específicas conforme necessário
```

### 5️⃣ Verificar Funcionamento
```bash
# Listar tabelas criadas
psql -d dw_oesnpg -c "\dt"

# Contar registros da fato
psql -d dw_oesnpg -c "SELECT COUNT(*) FROM fato_pos_graduacao;"
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

## 🏗️ Estrutura do Projeto

```
MULTIDIMENSIONAL-OESNPG/
├── 📄 .env                       # Configurações de ambiente
├── 📄 .env.example               # Template de configuração
├──  README.md                  # Documentação principal
│
├── 📁 src/                       # 🎯 CÓDIGO FONTE PRINCIPAL
│   ├── core/                     # 🧠 Funcionalidades centrais
│   │   ├── __init__.py           # Configuração do módulo core
│   │   └── core.py               # Classes e funções principais do sistema
│   ├── etl/                      # ⚙️ Pipelines ETL
│   │   ├── etl_master.py         # ETL principal e orquestrador
│   │   └── rebuild_all_dimensions.py  # Rebuilder completo de dimensões
│   └── models/                   # 🎲 MODELOS DE DADOS
│       ├── dimensions/           # 8 Dimensões do Star Schema
│       │   ├── dim_tempo.py      # Calendário 2000-2030
│       │   ├── dim_localidade.py # Estados e regiões (27 UFs)
│       │   ├── dim_tema.py       # Temas estratégicos (5,977)
│       │   ├── dim_ods.py        # 17 ODS da ONU
│       │   ├── dim_ies.py        # Instituições (377 IES)
│       │   ├── dim_ppg.py        # Programas de pós-graduação
│       │   ├── dim_producao.py   # Produção científica
│       │   └── dim_docente.py    # Corpo docente
│       ├── facts/                # 📊 TABELA FATO
│       │   ├── create_fact_table.py      # Criador da fato principal
│       │   └── README_FATO.md  # Documentação detalhada
│       └── utils/                # Utilitários dos modelos
│
├── 📁 sql/                       # 🗃️ SCRIPTS SQL ORGANIZADOS
│   ├── ddl/                      # Data Definition Language
│   │   ├── add_primary_keys_dimensoes.sql   # PKs das dimensões
│   │   ├── add_fks_fato.sql        # FKs da tabela fato
│   │   └── create_fato.sql    # DDL da fato
│   ├── dml/                      # Data Manipulation Language
│   │   ├── consulta_temas_por_uf_dw.sql    # Query UF otimizada
│   │   └── validacao_fato_vs_raw.sql       # Validação de dados
│   └── utils/                    # Utilitários SQL
│       └── executar_fks.py       # Executor de constraints
│
├── 📁 seeds/                     # 🌱 DADOS BASE E SEEDS
│   ├── curadoria_temas.xlsx      # Temas curatorados (Excel)
│   ├── ppg_2024.csv              # Programas 2024
│   ├── municipios.csv            # Municípios brasileiros
│   └── relational/               # Dados relacionais processados
│       ├── raw_ies.py            # IES da API CAPES
│       ├── raw_tema.py           # Temas por UF
│       └── raw_*.py              # Outros dados brutos
│
├── 📁 migration/                 # 🔄 FERRAMENTAS DE MIGRAÇÃO
│   ├── migration_tool.py         # Ferramenta principal
│   ├── README.md                 # Guia de migração
│   └── *_migrated.py             # Templates migrados
│
└── 📁 diagrams/                  # 📈 DIAGRAMAS E VISUALIZAÇÕES
    └── star_schema_diagram.mmd   # Diagrama do Star Schema
```

## 🎯 Comandos de Execução

### Fluxo Principal (Recomendado)
```bash
# 1. Executar ETL completo (dimensões + fato + constraints)
python src/etl/etl_master.py

# 2. Verificar criação
psql -d dw_oesnpg -c "\dt"
```

### Execução Manual (Alternativa)
```bash
# 1. Criar todas as dimensões e tabela fato
python src/models/facts/create_fact_table.py

# 2. Adicionar chaves primárias (opcional)
psql -d dw_oesnpg -f sql/ddl/add_primary_keys_dimensoes.sql

# 3. Adicionar chaves estrangeiras (opcional)
python sql/utils/executar_fks.py
```

### Execução de Dimensões Individuais
```bash
# Executar dimensões específicas
python src/models/dimensions/dim_tempo.py      # Calendário 2000-2030
python src/models/dimensions/dim_localidade.py # Estados e regiões
python src/models/dimensions/dim_tema.py       # Temas estratégicos
python src/models/dimensions/dim_ods.py        # 17 ODS da ONU
python src/models/dimensions/dim_ies.py        # Instituições
python src/models/dimensions/dim_ppg.py        # Programas de pós-grad
python src/models/dimensions/dim_producao.py   # Produção científica
python src/models/dimensions/dim_docente.py    # Corpo docente
```

### Comandos de Verificação
```bash
# Listar todas as tabelas criadas
psql -d dw_oesnpg -c "\dt"

# Contar registros da tabela fato
psql -d dw_oesnpg -c "SELECT COUNT(*) FROM fato_pos_graduacao;"

# Verificar dimensões criadas
psql -d dw_oesnpg -c "SELECT COUNT(*) FROM dim_tempo;"
psql -d dw_oesnpg -c "SELECT COUNT(*) FROM dim_localidade;"
```

## 🧠 Módulos Principais

### 📦 **src/core/** - Funcionalidades Centrais
- **`core.py`**: Classes e funções base do sistema
  - Gerenciamento de conexões de banco
  - Utilitários de transformação de dados
  - Logging e configuração central
  - Classes abstratas para ETL

### ⚙️ **src/etl/** - Pipelines de ETL
- **`etl_master.py`**: Orquestrador principal do ETL
  - Execução coordenada de todas as dimensões
  - Controle de dependências entre tabelas
  - Logs detalhados de execução
- **`rebuild_all_dimensions.py`**: Rebuilder completo
  - Reconstrução de todas as 8 dimensões
  - Validação de integridade
  - Recuperação de falhas

### 🎲 **src/models/** - Modelos de Dados
- **`dimensions/`**: 8 dimensões do Star Schema
- **`facts/`**: Tabela fato principal
- **`utils/`**: Utilitários específicos dos modelos

## 🚀 Scripts Principais

### 1️⃣ **Tabela Fato (PRINCIPAL)**
```bash
python src/models/facts/create_fact_table.py
```
- Cria estrutura completa da tabela fato otimizada
- Gera 88,816 registros baseados em dados reais
- Associações tema-IES-localidade com crescimento temporal
- Compatível com psycopg2 (sem dependências SQLAlchemy)

### 2️⃣ **Constraints e Integridade**
```bash
python sql/utils/executar_fks.py
```
- Aplica Primary Keys nas 8 dimensões automaticamente
- Cria Foreign Keys na tabela fato com integridade referencial
- Script único que resolve todas as constraints do Star Schema

### 3️⃣ **ETL Completo (Alternativo)**
```bash
python src/etl/etl_master.py
```
- Execução coordenada de todo o pipeline ETL
- Reconstrução completa de todas as dimensões
- Ideal para atualizações completas do data warehouse

### 4️⃣ **Rebuild de Dimensões**
```bash
python src/etl/rebuild_all_dimensions.py
```
- Reconstrução específica das 8 dimensões
- Validação de integridade entre dimensões
- Útil para correções e atualizações parciais

### 5️⃣ **Verificação e Status**
```bash
python QUICKSTART.py
```
- Valida conexão com banco de dados
- Verifica existência de todas as dimensões
- Testa integridade da tabela fato
- Gera relatório de status completo

### 🏗️ **Criação Manual da Fato (Alternativa)**
```bash
python src/models/facts/create_fact_table.py
```
- Cria estrutura completa da tabela fato otimizada
- Gera 88,816 registros baseados nas dimensões reais
- 160+ registros com crescimento ano a ano
- Funciona com psycopg2 (sem problemas SQLAlchemy)

### 2. Primary Keys e Foreign Keys
```bash
python sql/utils/executar_fks.py
```
- Executa PKs nas 8 dimensões automaticamente
- Cria FKs na tabela fato com integridade referencial
- Script único que resolve todas as constraints

## 📊 Métricas Disponíveis

### 🎯 **4 Análises Principais**
1. **📍 Quantidade de temas por UF** - Distribuição geográfica
2. **🏛️ Quantidade por categoria administrativa** - Público vs Privado  
3. **🏫 Quantidade de temas por IES** - Ranking institucional
4. **🗺️ Quantidade de temas por região** - Visão macro-regional

### 🔍 **Exemplos de Consultas**

### 📈 Evolução Temporal por Ano
```sql
SELECT 
    t.ano, 
    COUNT(DISTINCT f.tema_sk) as temas_ativos,
    COUNT(DISTINCT f.ies_sk) as ies_participantes
FROM fato_pos_graduacao f
JOIN dim_tempo t ON f.tempo_sk = t.tempo_sk
GROUP BY t.ano
ORDER BY t.ano;
```

### 🌎 Distribuição por Região
```sql
SELECT 
    l.regiao, 
    COUNT(DISTINCT f.tema_sk) as total_temas,
    COUNT(DISTINCT f.ies_sk) as total_ies
FROM fato_pos_graduacao f
JOIN dim_localidade l ON f.localidade_sk = l.localidade_sk
GROUP BY l.regiao
ORDER BY total_temas DESC;
```

## ⚙️ Configuração

### 🐍 Requisitos

- **Python 3.8+**
- **PostgreSQL 12+**
- **Dependências**: pandas, psycopg2-binary, python-dotenv

### 🔐 Variáveis de Ambiente (.env)
```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=dw_oesnpg
DB_USER=postgres
DB_PASSWORD=sua_senha
```

## 📊 Performance e Estatísticas

### ⚡ Benchmarks
- **Pipeline completo**: ~20 segundos
- **Criação da fato**: ~15 segundos  
- **Aplicação de constraints**: ~5 segundos
- **Uso de memória**: ~150MB
- **Total de registros**: 88,816 associações

### 📈 Dados Processados  
- **🎯 5,977 temas únicos** mapeados
- **🏛️ 377 IES** da API oficial CAPES
- **📍 27 UFs** e 5 regiões cobertas
- **📅 Período**: 2021-2024

---

💼 **Data Warehouse CAPES v2.0** | 🎓 UFMS | 📅 2025