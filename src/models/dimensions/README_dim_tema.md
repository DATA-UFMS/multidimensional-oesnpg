# Dimensão Tema (dim_tema)

Script para criar a dimensão `dim_tema` a partir da tabela `raw_tema` no PostgreSQL.

## Funcionalidades

- ✅ **Extração**: Dados únicos da tabela `raw_tema` 
- ✅ **Transformação**: Nome UF → Sigla UF (ex: "SÃO PAULO" → "SP")
- ✅ **Carregamento**: Salva no PostgreSQL como `dim_tema`
- ✅ **Exportação**: Gera arquivos Parquet e CSV locais

## Estrutura da Dimensão

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `tema_id` | int | Chave primária - ID único do tema |
| `tema_nome` | str | Nome completo do tema |
| `macrotema_id` | int | ID do macro tema pai |
| `macrotema_nome` | str | Nome do macro tema |
| `sigla_uf` | str | Sigla da UF (AC, AL, AM, etc.) |
| `created_at` | timestamp | Data de criação do registro |
| `updated_at` | timestamp | Data da última atualização |

## Como Usar

### 1. Pré-requisito: Carregar dados raw

Primeiro carregue os dados na tabela `raw_tema`:

```bash
cd ../../../staging/relational
python raw_tema.py --postgres --table raw_tema
```

### 2. Criar dimensão completa

```bash
# Criar dimensão no PostgreSQL + arquivos locais
python dim_tema.py

# Especificar nome da tabela
python dim_tema.py --table dim_tema_v2
```

### 3. Apenas arquivos (sem PostgreSQL)

```bash
# Apenas gerar arquivos CSV/Parquet
python dim_tema.py --export-only

# Especificar diretório de saída
python dim_tema.py --export-only --output-dir /path/to/output
```

## Configuração PostgreSQL

Configure as variáveis de ambiente:

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=dw_oesnpg
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=sua_senha
```

## Mapeamento UF → Sigla

O script inclui mapeamento completo de todos os estados brasileiros:

```python
'SÃO PAULO' → 'SP'
'RIO DE JANEIRO' → 'RJ'
'MINAS GERAIS' → 'MG'
# ... todos os 27 estados/DF
```

## Arquivos Gerados

- **dim_tema.parquet** (~24KB) - Formato otimizado
- **dim_tema.csv** (~81KB) - Formato legível

## Dados Esperados

- **Registros**: 449 temas únicos
- **Macro Temas**: 20 categorias
- **UFs**: 27 unidades federativas
- **Origem**: Baseado em 5,991 registros de `raw_tema`

## Query SQL Equivalente

A transformação equivale a esta query SQL:

```sql
SELECT DISTINCT
    tema_id,
    tema_nome,
    macrotema_id,
    macrotema_nome,
    CASE uf
        WHEN 'SÃO PAULO' THEN 'SP'
        WHEN 'RIO DE JANEIRO' THEN 'RJ'
        -- ... outros mapeamentos
    END as sigla_uf,
    NOW() as created_at,
    NOW() as updated_at
FROM raw_tema
WHERE tema_nome IS NOT NULL 
AND tema_nome != ''
ORDER BY macrotema_id, tema_id;
```

## Exemplo de Uso

```bash
# Setup completo
cd staging/relational
python raw_tema.py --postgres

cd ../../src/models/dimensions  
python dim_tema.py

# Verificar resultado
ls -la dim_tema.*
```