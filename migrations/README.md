# Sistema de Migrations - Data Warehouse OES-NPG

Este diretório contém o sistema de migrations para geração automática de scripts DDL do Data Warehouse OES-NPG, suportando PostgreSQL e Oracle.

## 📁 Estrutura

```
migrations/
├── README.md                    # Esta documentação
├── migration_generator.py       # Gerador principal de migrations
└── output/                      # Diretório de saída (ignorado pelo Git)
    ├── docs/                    # Documentação do schema
    │   ├── SCHEMA_DOCUMENTATION.md
    │   └── schema_dw_oesnpg.json
    ├── postgresql/              # Scripts PostgreSQL
    │   └── [timestamp]_create_dw_oesnpg.sql
    └── oracle/                  # Scripts Oracle
        └── [timestamp]_create_dw_oesnpg.sql
```

## 🚀 Uso Básico

### Executar o Gerador

```bash
# No diretório raiz do projeto
python migrations/migration_generator.py
```

### Saída Esperada

```
=== Sistema de Migrations DW OES-NPG ===

Gerando migration para POSTGRESQL...
✅ Migration salva: /path/to/migrations/output/postgresql/20250818_145435_create_dw_oesnpg.sql

Gerando migration para ORACLE...
✅ Migration salva: /path/to/migrations/output/oracle/20250818_145435_create_dw_oesnpg.sql

Gerando documentação do schema...
✅ Documentação salva em: /path/to/migrations/output/docs/schema_dw_oesnpg.json, /path/to/migrations/output/docs/SCHEMA_DOCUMENTATION.md

=== Migrations geradas com sucesso! ===
```

## ⚙️ Configuração

O sistema usa variáveis de ambiente definidas no arquivo `.env`:

```properties
# 🏗️ CONFIGURAÇÕES DE MIGRATIONS
BASE_DIR=/caminho/para/o/projeto
OUTPUT_DIR=migrations/output
```

### Variáveis Importantes

- **BASE_DIR**: Diretório base do projeto
- **OUTPUT_DIR**: Diretório de saída relativo ao BASE_DIR

## 📊 Schema do Data Warehouse

O sistema gera automaticamente um **Star Schema** completo com:

### 8 Dimensões

1. **dim_tempo** - Dimensão temporal (ano/mês/dia/trimestre)
2. **dim_localidade** - Dimensão geográfica (região/UF/município)
3. **dim_tema** - Áreas de conhecimento CAPES
4. **dim_ods** - Objetivos de Desenvolvimento Sustentável
5. **dim_ies** - Instituições de Ensino Superior
6. **dim_ppg** - Programas de Pós-Graduação
7. **dim_producao** - Tipos de produção acadêmica
8. **dim_docente** - Docentes permanentes

### 1 Tabela Fato

- **fato_pos_graduacao** - Métricas consolidadas da pós-graduação brasileira

## 🔧 Funcionalidades

### Scripts Gerados

Para cada banco de dados, o sistema gera:

1. **SEQUENCES** (quando necessário)
2. **CREATE TABLE** para todas as dimensões e fato
3. **PRIMARY KEYS** para todas as tabelas
4. **FOREIGN KEYS** para relacionamentos
5. **ADDITIONAL INDEXES** para performance

### Diferenças entre Dialetos

#### PostgreSQL
- Tipos nativos: `SERIAL`, `VARCHAR`, `BOOLEAN`
- Sequences automáticas para SERIAL
- Naming convention: `pk_tabela`, `fk_tabela_coluna`

#### Oracle
- Tipos mapeados: `NUMBER(10)`, `VARCHAR2`, `NUMBER(1)`
- Sequences explícitas para auto-increment
- Naming convention: `PK_TABELA`, `FK_TABELA_COLUNA`

## 📝 Documentação Automática

O sistema gera automaticamente:

### JSON Schema
```json
{
  "name": "dw_oesnpg",
  "tables": [
    {
      "name": "dim_tempo",
      "type": "dimension",
      "comment": "Dimensão temporal...",
      "columns": [...]
    }
  ]
}
```

### Markdown Documentation
- Visão geral do DW
- Documentação detalhada de cada dimensão
- Especificação da tabela fato
- Estratégia de indexação

## 🏗️ Arquitetura do Código

### Classes Principais

```python
@dataclass
class Column:          # Representa uma coluna
class Table:           # Representa uma tabela
class Schema:          # Schema completo do DW

class DatabaseDialect:     # Classe base para dialetos
class PostgreSQLDialect:   # Implementação PostgreSQL
class OracleDialect:       # Implementação Oracle

class MigrationGenerator:  # Gerador principal
```

### Fluxo de Execução

1. **Definição do Schema** - Define todas as tabelas e colunas
2. **Seleção do Dialeto** - PostgreSQL ou Oracle
3. **Geração do DDL** - CREATE, ALTER, INDEX statements
4. **Salvamento** - Arquivos SQL organizados por banco
5. **Documentação** - JSON e Markdown automáticos

## 🎯 Patterns Utilizados

### Strategy Pattern
- `DatabaseDialect` como interface
- `PostgreSQLDialect` e `OracleDialect` como estratégias

### Builder Pattern
- Construção progressiva do schema
- Separação entre definição e geração

### Template Method
- Fluxo comum de geração
- Implementações específicas por dialeto

## 🔍 Índices de Performance

### Automáticos
- Primary Keys (todas as tabelas)
- Foreign Keys (tabela fato)

### Adicionais
- Índices nas FKs da tabela fato
- Índices compostos (ano, mês)
- Índices em colunas de filtro frequente

## 📋 Requisitos

### Python Packages
```txt
python-dotenv    # Configurações via .env
```

### Estrutura de Arquivos
```
projeto/
├── .env                    # Configurações
├── migrations/
│   └── migration_generator.py
```

## 🚨 Observações Importantes

### Arquivos Ignorados
O diretório `migrations/output/` está no `.gitignore` para evitar commit de arquivos gerados.

### Timestamps
Cada execução gera arquivos com timestamp único no formato: `YYYYMMDD_HHMMSS_create_dw_oesnpg.sql`

### Múltiplas Execuções
É seguro executar o gerador múltiplas vezes - cada execução cria novos arquivos sem sobrescrever.

## 🔧 Manutenção

### Adicionar Nova Dimensão
1. Criar `Table` na função `_define_schema()`
2. Adicionar à lista `tables` do `Schema`
3. Executar o gerador

### Modificar Coluna
1. Alterar definição na dimensão correspondente
2. Regenerar migrations
3. Criar migration de ALTER se necessário

### Novo Banco de Dados
1. Criar nova classe `NovoDialect(DatabaseDialect)`
2. Implementar métodos abstratos
3. Adicionar ao `generate_migration()`

## 📚 Referências

- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Oracle Database Documentation](https://docs.oracle.com/database/)
- [Data Warehouse Design Patterns](https://en.wikipedia.org/wiki/Star_schema)

---

**Desenvolvido para o Observatório OES-NPG**  
*Sistema de geração automática de migrations para Data Warehouse acadêmico*