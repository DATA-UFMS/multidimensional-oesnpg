# Scripts DDL Simplificados

Esta pasta contém apenas os scripts DDL essenciais para o Data Warehouse:

## 📁 Arquivos

1. **`add_pk.sql`** - Adiciona chaves primárias em todas as 8 dimensões
2. **`add_fk.sql`** - Adiciona chaves estrangeiras na tabela fato
3. **`verify_integrity.sql`** - Verifica a integridade das PKs e FKs criadas

## 🚀 Uso

Execute os scripts na seguinte ordem:

```bash
# 1. Criar primary keys
psql -d dw_oesnpg -f add_pk.sql

# 2. Criar foreign keys
psql -d dw_oesnpg -f add_fk.sql

# 3. Verificar integridade
psql -d dw_oesnpg -f verify_integrity.sql
```

## ✅ Validação

O script `verify_integrity.sql` deve retornar:
- 8 Primary Keys criadas
- 4 Foreign Keys criadas
- 0 violações de integridade

## 🔧 ETL Master

Estes scripts são executados automaticamente pelo `etl_master.py` ao final do processo ETL.
