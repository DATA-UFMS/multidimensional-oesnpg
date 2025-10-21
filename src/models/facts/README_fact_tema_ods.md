# Tabela Fato: fact_tema_ods

## 📋 Descrição

A tabela `fact_tema_ods` é uma **tabela fato sem medidas** (factless fact table) que registra os relacionamentos entre temas de pesquisa e os Objetivos de Desenvolvimento Sustentável (ODS).

Esta tabela permite análises de alinhamento entre a pesquisa acadêmica (representada por temas/palavras-chave) e os 20 ODS (17 oficiais da ONU + 3 expandidos).

## 🏗️ Estrutura da Tabela

```sql
CREATE TABLE fact_tema_ods (
    tema_ods_id SERIAL PRIMARY KEY,           -- ID único da associação
    tema_sk INTEGER NOT NULL,                 -- FK para dim_tema
    ods_sk INTEGER NOT NULL,                  -- FK para dim_ods
    tipo_associacao VARCHAR(50),              -- Manual, Automática, Validada
    nivel_confianca DECIMAL(5,2),             -- 0-100%
    data_associacao DATE,                     -- Data do mapeamento
    usuario_associacao VARCHAR(100),          -- Quem criou
    observacao TEXT,                          -- Notas adicionais
    ativo BOOLEAN DEFAULT TRUE,               -- Se está ativo
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    
    CONSTRAINT uk_tema_ods UNIQUE (tema_sk, ods_sk)
);
```

## 🎯 Propósito

### Casos de Uso

1. **Análise de Alinhamento ODS**
   - Identificar quais áreas de pesquisa contribuem para cada ODS
   - Medir a cobertura dos ODS pela pesquisa acadêmica
   - Encontrar gaps de pesquisa em ODS específicos

2. **Classificação Automática**
   - Associar produções acadêmicas aos ODS através de seus temas
   - Criar dashboards de impacto por ODS
   - Gerar relatórios de contribuição para Agenda 2030

3. **Recomendações**
   - Sugerir ODS relevantes para novos projetos
   - Recomendar temas para pesquisadores interessados em ODS específicos

## 📊 Tipos de Associação

| Tipo | Descrição | Nível de Confiança |
|------|-----------|-------------------|
| **Automática** | Criada por matching de palavras-chave | 60-80% |
| **Manual** | Criada por curador humano | 90-100% |
| **Validada** | Automática revisada e aprovada | 95-100% |
| **IA** | Criada por modelo de ML/IA | 70-90% |

## 🔄 Processo de Criação

### 1. Criação da Estrutura
```bash
python3 src/models/facts/fact_tema_ods.py
```

### 2. Mapeamento Automático
O script automaticamente:
- Busca palavras-chave de `dim_tema`
- Compara com descritores de `dim_ods`
- Cria associações quando há match
- Define nível de confiança baseado no tipo de match

### 3. Validação Manual
Após criar os mapeamentos automáticos, recomenda-se:
- Revisar associações com baixa confiança
- Adicionar mapeamentos manuais importantes
- Desativar associações incorretas

## 📈 Exemplos de Consultas

### 1. Top 10 ODS mais pesquisados
```sql
SELECT 
    o.numero_ods,
    o.nome_ods,
    COUNT(DISTINCT f.tema_sk) as total_temas,
    COUNT(*) as total_associacoes
FROM fact_tema_ods f
JOIN dim_ods o ON f.ods_sk = o.ods_sk
WHERE f.ativo = TRUE
GROUP BY o.numero_ods, o.nome_ods
ORDER BY total_temas DESC
LIMIT 10;
```

### 2. Temas associados a um ODS específico
```sql
SELECT 
    t.palavra_chave,
    f.tipo_associacao,
    f.nivel_confianca,
    f.observacao
FROM fact_tema_ods f
JOIN dim_tema t ON f.tema_sk = t.tema_sk
JOIN dim_ods o ON f.ods_sk = o.ods_sk
WHERE o.numero_ods = 4  -- ODS 4: Educação de Qualidade
  AND f.ativo = TRUE
ORDER BY f.nivel_confianca DESC;
```

### 3. Cobertura de ODS por programa de pós-graduação
```sql
SELECT 
    p.nome_programa,
    COUNT(DISTINCT f.ods_sk) as ods_cobertos,
    STRING_AGG(DISTINCT o.numero_ods::TEXT, ', ' ORDER BY o.numero_ods) as lista_ods
FROM fact_producao fp
JOIN dim_tema t ON fp.tema_sk = t.tema_sk
JOIN fact_tema_ods f ON t.tema_sk = f.tema_sk
JOIN dim_ods o ON f.ods_sk = o.ods_sk
JOIN dim_ppg p ON fp.ppg_sk = p.ppg_sk
WHERE f.ativo = TRUE
GROUP BY p.nome_programa
ORDER BY ods_cobertos DESC;
```

### 4. ODS não cobertos ou pouco pesquisados
```sql
SELECT 
    o.numero_ods,
    o.nome_ods,
    o.ods_tipo,
    COALESCE(COUNT(f.tema_sk), 0) as total_temas
FROM dim_ods o
LEFT JOIN fact_tema_ods f ON o.ods_sk = f.ods_sk AND f.ativo = TRUE
WHERE o.ods_sk > 0
GROUP BY o.numero_ods, o.nome_ods, o.ods_tipo
HAVING COUNT(f.tema_sk) < 5
ORDER BY total_temas, o.numero_ods;
```

## 🔧 Manutenção

### Adicionar Associação Manual
```sql
INSERT INTO fact_tema_ods (tema_sk, ods_sk, tipo_associacao, nivel_confianca, usuario_associacao, observacao)
VALUES (
    123,  -- tema_sk
    5,    -- ods_sk (ODS 5: Igualdade de Gênero)
    'Manual',
    100.0,
    'João Silva',
    'Tema claramente relacionado a questões de gênero'
);
```

### Desativar Associação Incorreta
```sql
UPDATE fact_tema_ods
SET ativo = FALSE,
    updated_at = CURRENT_TIMESTAMP,
    observacao = COALESCE(observacao || ' | ', '') || 'Desativada por revisão manual em ' || CURRENT_DATE
WHERE tema_sk = 456 AND ods_sk = 3;
```

### Validar Associações Automáticas
```sql
UPDATE fact_tema_ods
SET tipo_associacao = 'Validada',
    nivel_confianca = 95.0,
    updated_at = CURRENT_TIMESTAMP
WHERE tipo_associacao = 'Automática'
  AND nivel_confianca >= 80
  AND tema_sk IN (SELECT tema_sk FROM validacao_manual);
```

## 📊 Estatísticas Recomendadas

Execute periodicamente para monitorar a qualidade:

```sql
-- Resumo geral
SELECT 
    COUNT(*) as total_associacoes,
    COUNT(DISTINCT tema_sk) as temas_unicos,
    COUNT(DISTINCT ods_sk) as ods_unicos,
    tipo_associacao,
    AVG(nivel_confianca) as media_confianca
FROM fact_tema_ods
WHERE ativo = TRUE
GROUP BY tipo_associacao;
```

## 🚀 Próximos Passos

1. **Implementar ML para Associações**
   - Treinar modelo para prever ODS baseado em texto completo
   - Usar embeddings de temas e descritores ODS
   - Melhorar precisão das associações automáticas

2. **Dashboard Analítico**
   - Criar visualizações de rede tema-ODS
   - Mapas de calor de cobertura ODS
   - Timeline de evolução da pesquisa por ODS

3. **API de Recomendação**
   - Endpoint para sugerir ODS dado um tema
   - Endpoint para sugerir temas dado um ODS
   - Sistema de pontuação de relevância

4. **Integração com Fact Tables**
   - Enriquecer `fact_producao` com tags ODS
   - Criar views materializadas para performance
   - Reports automáticos de impacto ODS

## 📝 Notas Técnicas

- **Factless Fact Table**: Esta tabela não tem medidas numéricas, apenas registra a ocorrência de relacionamentos
- **Granularidade**: Uma linha por relacionamento tema-ODS
- **SCD Type**: Não usa Slowly Changing Dimension, mas tem flag `ativo` para soft delete
- **Performance**: Índices em todas as FKs e campos de filtro comuns

## 👥 Responsáveis

- **Criação**: Sistema ETL
- **Manutenção**: Equipe de Curadoria de Dados
- **Validação**: Especialistas em ODS e Pesquisadores

---

**Última Atualização**: 15 de outubro de 2025
**Versão**: 1.0.0
