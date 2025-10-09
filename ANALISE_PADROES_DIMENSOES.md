# Análise de Padrões - Dimensões do Data Warehouse

## Data da Análise
09/10/2025

## Objetivo
Comparar as implementações das dimensões `dim_localidade` e `dim_docente` para identificar padrões, inconsistências e oportunidades de melhoria.

---

## 1. Comparação Estrutural

### 1.1 Documentação

| Aspecto | dim_localidade | dim_docente | Status |
|---------|----------------|-------------|---------|
| **Docstring inicial** | ✅ Completa e detalhada | ✅ Completa e detalhada | ✅ PADRONIZADO |
| **Descrição do módulo** | ✅ Clara | ✅ Clara | ✅ CONSISTENTE |
| **Fontes de dados** | ✅ Especificadas | ✅ Especificadas | ✅ CONSISTENTE |
| **Estrutura da dimensão** | ✅ Todas as colunas listadas | ✅ Todas as colunas listadas | ✅ CONSISTENTE |
| **Processo ETL** | ⚠️ Não explicitado | ✅ Detalhado em 3 etapas | ⚠️ MELHORAR dim_localidade |
| **Validações** | ✅ Descritas | ✅ Descritas | ✅ CONSISTENTE |
| **Autor e data** | ✅ Presentes | ✅ Presentes | ✅ CONSISTENTE |

---

## 2. Arquitetura de Código

### 2.1 Organização de Funções

#### dim_localidade (Mais Simples)
```
1. criar_dimensao_localidade() - Função principal
2. salvar_dimensao_localidade() - Persistência
3. __main__ - Execução
```

#### dim_docente (Mais Modular)
```
1. get_project_root() - Utilitário de path
2. get_db_engine() - Conexão DB
3. load_parquet_from_minio() - Carga MinIO
4. load_raw_data_from_postgres() - Carga PostgreSQL
5. create_enriched_docente_dimension() - Transformação
6. save_to_postgres() - Persistência
7. main() - Orquestração
8. __main__ - Execução
```

**Análise**: dim_docente é mais modular e testável. dim_localidade poderia se beneficiar de decomposição similar.

---

### 2.2 Gestão de Configuração

| Aspecto | dim_localidade | dim_docente | Recomendação |
|---------|----------------|-------------|--------------|
| **Carregamento .env** | ✅ load_dotenv() no topo | ✅ load_dotenv() em get_db_engine() | ✅ Ambas válidas |
| **Variáveis de ambiente** | ✅ DB_HOST, DB_NAME, etc. | ✅ DB_* + MINIO_* | ✅ Completas |
| **Validação de config** | ❌ Não valida | ✅ Valida com all([...]) | ⚠️ ADICIONAR em dim_localidade |
| **Mensagens de erro** | ⚠️ Genéricas | ✅ Específicas | ⚠️ MELHORAR dim_localidade |

---

### 2.3 Carga de Dados

#### dim_localidade
- **Fontes**: CSV local (estados) + CSV GitHub (municípios)
- **Método**: pd.read_csv() direto
- **Tratamento de erros**: Básico (try/except)

#### dim_docente
- **Fontes**: Parquet MinIO + PostgreSQL (raw_docente + raw_fomentopq)
- **Método**: Funções dedicadas (load_parquet_from_minio, load_raw_data_from_postgres)
- **Tratamento de erros**: Detalhado com mensagens específicas

**Análise**: dim_docente tem melhor separação de responsabilidades e tratamento de erros mais robusto.

---

## 3. Transformação de Dados

### 3.1 Estratégia de Mapeamento de Colunas

#### dim_localidade
- Mapeamento manual inline (if/else)
- Verificação de existência de colunas com `if col in df.columns`
- Uso de `reindex()` para padronizar colunas

#### dim_docente
- Dicionário de mapeamento (`column_mapping`)
- Filtragem de colunas disponíveis: `[col for col in mapping.keys() if col in df.columns]`
- Rename com dicionário filtrado

**Recomendação**: Abordagem de dim_docente é mais limpa e manutenível.

---

### 3.2 Deduplicação

| Aspecto | dim_localidade | dim_docente | Comentário |
|---------|----------------|-------------|------------|
| **Necessária?** | ❌ Não (dados únicos por design) | ✅ Sim (múltiplas fontes) | - |
| **Método** | N/A | drop_duplicates() + sort by data | ✅ Correto |
| **Critério** | N/A | Registro mais recente | ✅ Lógica clara |

---

### 3.3 Criação de Campos Derivados

#### dim_localidade
- `sigla_regiao`: Deriva de `regiao` (primeiros 2 chars)
- `uf`: Alias de `sigla_uf` (para validação)
- Mapeamento manual `codigo_uf` → `sigla_uf` via dicionário

#### dim_docente
- `bl_doutor`: Converte 'SIM'/'NÃO' → True/False
- `bl_coordenador_ppg`: Converte 'SIM'/'NÃO' → True/False
- `bl_bolsa_pq`: Consolida múltiplas fontes com operador OR

**Análise**: Ambas corretas, mas dim_docente usa conversões mais robustas (map com fillna).

---

### 3.4 Enriquecimento de Dados

#### dim_localidade
- **Método**: Join entre estados e municípios (ambos CSV)
- **Estratégia**: Mapeamento por `codigo_uf`
- **Fallback**: Dicionário hardcoded quando join falha

#### dim_docente
- **Método**: Merges sequenciais (base → raw_docente → raw_fomentopq)
- **Estratégia**: Join por `id_pessoa` e `id_lattes`
- **Fallback**: Match por nome normalizado quando id_lattes ausente

**Recomendação**: dim_docente tem estratégia mais sofisticada com fallback inteligente.

---

## 4. Validação de Dados

### 4.1 Uso do Framework de Validação

| Aspecto | dim_localidade | dim_docente | Status |
|---------|----------------|-------------|---------|
| **Importa validator** | ✅ Sim | ✅ Sim | ✅ PADRONIZADO |
| **Executa validação** | ✅ Sim | ❌ NÃO! | ⚠️ **PROBLEMA CRÍTICO** |
| **Trata erros** | ✅ Levanta DataValidationError | N/A | ⚠️ ADICIONAR em dim_docente |
| **Mostra warnings** | ✅ Sim | N/A | ⚠️ ADICIONAR em dim_docente |

**⚠️ DESCOBERTA IMPORTANTE**: `dim_docente` importa o framework de validação mas **NÃO O USA**!

---

### 4.2 Validações Manuais

#### dim_localidade
```python
# Valida formato de UF antes de salvar
df['uf'].str.match(r'^[A-Z]{2}$', na=False)
```

#### dim_docente
```python
# Valida apenas na função save_to_postgres
if df.empty:
    raise DimensionCreationError("DataFrame está vazio")
```

**Análise**: dim_localidade tem validações mais rigorosas.

---

## 5. Registro "Desconhecido" (SK=0)

### 5.1 Criação

#### dim_localidade
```python
registro_desconhecido = NamingConventions.get_standard_unknown_record('localidade')
# Atualiza campos específicos
registro_desconhecido.update({...})
# Cria DataFrame e concatena
df_localidade = pd.concat([pd.DataFrame([registro_desconhecido]), df_localidade])
```

#### dim_docente
```python
sk0_record = pd.DataFrame([{
    'docente_sk': 0,
    'id_pessoa': 0,
    'des_docente': 'Desconhecido',
    ...
}])
final_dim = pd.concat([sk0_record, df_enriched])
```

**Análise**: 
- dim_localidade usa utilitário `NamingConventions` (MELHOR)
- dim_docente hardcoda valores (menos manutenível)

---

## 6. Surrogate Key

### 6.1 Estratégia de Criação

| Aspecto | dim_localidade | dim_docente | Recomendação |
|---------|----------------|-------------|--------------|
| **Método** | `np.arange(len(df))` | `range(1, len(df)+1)` | Ambas válidas |
| **Início** | 0 | 1 → 0 após concat SK=0 | dim_localidade mais direto |
| **Ordem** | Após concat final | Antes de adicionar SK=0 | dim_localidade mais lógico |
| **Biblioteca** | numpy | built-in range | dim_docente mais leve |

**Recomendação**: Padronizar para `range(len(df))` após adicionar SK=0.

---

## 7. Persistência no PostgreSQL

### 7.1 Estratégia de Salvamento

#### dim_localidade
```python
# Remove coluna auxiliar 'uf' (usada só para validação)
df_to_save = df_localidade.drop(columns=['uf'], errors='ignore')

# Trunca e insere
conn.exec_driver_sql("DELETE FROM dim_localidade;")
df_to_save.to_sql('dim_localidade', conn, if_exists='append', index=False)
```

#### dim_docente
```python
# Cria tabela com DDL explícito
create_table_sql = """CREATE TABLE IF NOT EXISTS dim_docente (...)"""
conn.exec_driver_sql(create_table_sql)

# Trunca e insere
conn.exec_driver_sql(f"DELETE FROM {table_name};")
df.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
```

**Análise**:
- dim_docente cria schema explícito (MELHOR para controle de tipos)
- dim_docente usa `method='multi'` (mais eficiente)
- dim_localidade usa `DELETE` (igual a dim_docente)

---

### 7.2 Tratamento de Erros

| Aspecto | dim_localidade | dim_docente | Status |
|---------|----------------|-------------|---------|
| **Try/except** | ✅ Sim | ✅ Sim | ✅ CONSISTENTE |
| **Mensagens específicas** | ✅ Boas | ✅ Boas | ✅ CONSISTENTE |
| **Levanta exceção customizada** | ✅ DimensionCreationError | ✅ Print genérico | ⚠️ MELHORAR dim_docente |

---

## 8. Logging e Feedback

### 8.1 Mensagens Informativas

#### dim_localidade
- ✅ Etapas claramente identificadas
- ✅ Contadores de registros
- ✅ Debug detalhado (antes da validação)
- ✅ Estatísticas finais

#### dim_docente
- ✅ Etapas claramente identificadas
- ✅ Contadores com formatação (`:,`)
- ✅ Feedback de enriquecimento (matches)
- ❌ Sem estatísticas finais

**Recomendação**: Ambas boas, dim_docente poderia adicionar estatísticas finais.

---

## 9. Descobertas e Recomendações

### 9.1 ⚠️ Problemas Críticos

1. **dim_docente NÃO EXECUTA VALIDAÇÃO**
   - Importa `validate_dimension_data` mas nunca chama
   - Recomendação: Adicionar validação antes de salvar

2. **dim_docente não usa NamingConventions para SK=0**
   - Hardcoda valores em vez de usar utilitário
   - Recomendação: Usar `NamingConventions.get_standard_unknown_record('docente')`

3. **Inconsistência na criação de tabelas**
   - dim_docente define DDL explícito
   - dim_localidade deixa pandas inferir (pode gerar tipos incorretos)
   - Recomendação: Sempre usar DDL explícito

---

### 9.2 Oportunidades de Melhoria

#### Para dim_localidade:
1. ✅ Adicionar validação de configuração (all([...]))
2. ✅ Considerar modularização (funções separadas para carga)
3. ✅ Adicionar DDL explícito na criação da tabela
4. ✅ Usar `method='multi'` no to_sql para melhor performance
5. ✅ Adicionar processo ETL detalhado na documentação

#### Para dim_docente:
1. 🔴 **URGENTE**: Adicionar chamada ao framework de validação
2. ✅ Usar NamingConventions para registro SK=0
3. ✅ Levantar DimensionCreationError em save_to_postgres
4. ✅ Adicionar estatísticas finais no main()
5. ✅ Considerar adicionar validação de integridade referencial

---

### 9.3 Padrões a Adotar

| Padrão | Implementação Recomendada | Justificativa |
|--------|---------------------------|---------------|
| **Documentação** | Seguir template dim_localidade | Completa e clara |
| **Modularização** | Seguir template dim_docente | Testável e manutenível |
| **Validação** | Framework obrigatório em todas | Qualidade de dados |
| **SK=0** | NamingConventions sempre | Consistência |
| **DDL** | Explícito sempre | Controle de tipos |
| **Salvamento** | `method='multi'` sempre | Performance |
| **Logging** | Etapas + contadores + estatísticas | Monitoramento |

---

## 10. Checklist de Qualidade para Dimensões

Use este checklist para revisar qualquer dimensão nova ou existente:

### Documentação
- [ ] Docstring completa no topo do arquivo
- [ ] Descrição clara do propósito
- [ ] Fontes de dados listadas
- [ ] Estrutura da dimensão documentada (todas as colunas)
- [ ] Processo ETL detalhado (Extract, Transform, Load)
- [ ] Validações descritas
- [ ] Dependências listadas
- [ ] Autor e data de atualização

### Configuração
- [ ] Usa load_dotenv() corretamente
- [ ] Valida variáveis de ambiente obrigatórias
- [ ] Mensagens de erro específicas para config inválida

### Carga de Dados
- [ ] Funções separadas por fonte de dados
- [ ] Tratamento robusto de erros
- [ ] Logging de progresso (registros carregados)

### Transformação
- [ ] Mapeamento de colunas com dicionário
- [ ] Deduplicação quando necessário (manter mais recente)
- [ ] Campos derivados documentados
- [ ] Enriquecimento com estratégia de fallback

### Validação
- [ ] Usa framework de validação (validate_dimension_data)
- [ ] Trata erros de validação (levanta DataValidationError)
- [ ] Mostra warnings quando apropriado
- [ ] Validações críticas antes de salvar

### Registro Desconhecido
- [ ] Usa NamingConventions.get_standard_unknown_record()
- [ ] SK=0 sempre
- [ ] Todos os campos obrigatórios preenchidos

### Surrogate Key
- [ ] Sequencial iniciando em 0
- [ ] Criada APÓS adicionar SK=0
- [ ] Sem gaps ou duplicatas

### Persistência
- [ ] DDL explícito (CREATE TABLE)
- [ ] Remove colunas auxiliares antes de salvar
- [ ] Usa method='multi' no to_sql
- [ ] Tratamento de erros com exceções customizadas
- [ ] Transação atômica (with engine.begin())

### Logging
- [ ] Mensagens em cada etapa do ETL
- [ ] Contadores de registros formatados
- [ ] Estatísticas finais (por nível, categoria, etc.)
- [ ] Feedback de enriquecimento (matches)

---

## 11. Próximos Passos

### Prioridade ALTA
1. ⚠️ Adicionar validação em dim_docente (CRÍTICO)
2. ⚠️ Padronizar uso de NamingConventions em todas as dimensões

### Prioridade MÉDIA
3. Criar template padrão para novas dimensões
4. Adicionar DDL explícito em dim_localidade
5. Refatorar dim_localidade para maior modularização

### Prioridade BAIXA
6. Criar suite de testes unitários
7. Adicionar métricas de qualidade de dados
8. Documentar guia de desenvolvimento de dimensões

---

**Conclusão**: Ambas as implementações têm qualidades, mas dim_docente tem um problema crítico (ausência de validação). O padrão ideal combina a modularização de dim_docente com a rigorosidade de validação de dim_localidade, além de adotar DDL explícito e uso consistente de utilitários como NamingConventions.
