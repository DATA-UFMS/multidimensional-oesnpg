# 📊 FATO - Data Warehouse CAPES

> **Versão otimizada** da tabela fato focada nas 4 métricas essenciais  
> ⚡ 88,816 registros | 🎯 Performance superior | 🔗 Integridade garantida

## 🎯 Visão Geral

A **FATO** foi criada para substituir a estrutura complexa anterior com uma abordagem otimizada focada nas 4 métricas essenciais solicitadas:

1. **📍 Quantidade de temas por UF**
2. **🏛️ Quantidade de temas por categoria administrativa** (público/privado)
3. **🏫 Quantidade de temas por IES**
4. **🗺️ Quantidade de temas por região**

## 🚀 Como Executar

### Criação da FATO
```bash
# Navegar para o diretório do projeto
cd /path/to/multidimensional-oesnpg

# Executar o script principal
python src/models/facts/create_fact_table.py
```

### Estrutura da Nova FATO

**Tabela:** `fato_temas_`

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `fato_id` | SERIAL PRIMARY KEY | Identificador único |
| `tema_sk` | INTEGER | Chave estrangeira para dim_tema |
| `ies_sk` | INTEGER | Chave estrangeira para dim_ies |
| `localidade_sk` | INTEGER | Chave estrangeira para dim_localidade |
| `tempo_sk` | INTEGER | Chave estrangeira para dim_tempo |
| `presente_na_uf` | INTEGER | 1 se tema está presente na UF |
| `presente_na_categoria` | INTEGER | 1 se tema está na categoria (público/privado) |
| `presente_na_ies` | INTEGER | 1 se tema está presente na IES |
| `presente_na_regiao` | INTEGER | 1 se tema está presente na região |
| `qtd_registros` | INTEGER | Para contar registros nas agregações |
| `created_at` | TIMESTAMP | Data de criação |
| `updated_at` | TIMESTAMP | Data de atualização |

## 📈 Métricas Disponíveis

### 1. Quantidade de Temas por UF
```sql
SELECT 
    dl.sigla_uf,
    COUNT(DISTINCT dt.tema_id) as qtd_temas_uf,
    COUNT(DISTINCT di.ies_sk) as qtd_ies,
    SUM(f.qtd_registros) as total_associacoes
FROM fato_temas_ f
    JOIN dim_tema dt ON f.tema_sk = dt.tema_sk
    JOIN dim_localidade dl ON f.localidade_sk = dl.localidade_sk
    JOIN dim_ies di ON f.ies_sk = di.ies_sk
GROUP BY dl.sigla_uf
ORDER BY qtd_temas_uf DESC;
```

### 2. Quantidade de Temas por Categoria Administrativa
```sql
SELECT 
    di.categoria_administrativa,
    COUNT(DISTINCT dt.tema_id) as qtd_temas_categoria,
    COUNT(DISTINCT di.ies_sk) as qtd_ies,
    SUM(f.qtd_registros) as total_associacoes
FROM fato_temas_ f
    JOIN dim_tema dt ON f.tema_sk = dt.tema_sk
    JOIN dim_ies di ON f.ies_sk = di.ies_sk
GROUP BY di.categoria_administrativa
ORDER BY qtd_temas_categoria DESC;
```

### 3. Quantidade de Temas por Região
```sql
SELECT 
    dl.regiao,
    COUNT(DISTINCT dt.tema_id) as qtd_temas_regiao,
    COUNT(DISTINCT dl.localidade_sk) as qtd_ufs,
    SUM(f.qtd_registros) as total_associacoes
FROM fato_temas_ f
    JOIN dim_tema dt ON f.tema_sk = dt.tema_sk
    JOIN dim_localidade dl ON f.localidade_sk = dl.localidade_sk
GROUP BY dl.regiao
ORDER BY qtd_temas_regiao DESC;
```

### 4. Top IES por Quantidade de Temas
```sql
SELECT 
    di.nome_ies,
    di.categoria_administrativa,
    dl.sigla_uf,
    COUNT(DISTINCT dt.tema_id) as qtd_temas_ies
FROM fato_temas_ f
    JOIN dim_tema dt ON f.tema_sk = dt.tema_sk
    JOIN dim_ies di ON f.ies_sk = di.ies_sk
    JOIN dim_localidade dl ON f.localidade_sk = dl.localidade_sk
GROUP BY di.nome_ies, di.categoria_administrativa, dl.sigla_uf
ORDER BY qtd_temas_ies DESC;
```

## 🔧 Características Técnicas

### ✅ Otimizações Implementadas

1. **Estrutura Simplificada**: Apenas 12 campos vs 26+ da versão anterior
2. **Índices Automáticos**: Criados automaticamente para todas as chaves estrangeiras
3. **Dados Reais**: Baseada em associações reais tema-IES-UF dos dados brutos
4. **Performance**: Otimizada para agregações rápidas
5. **Integridade**: Relacionamentos corretos entre todas as dimensões

### 🎯 Resultados Obtidos

- **📊 88,816 registros** de associações tema-IES-localidade
- **🔢 5,977 temas únicos** representados
- **🏛️ 377 IES** da API CAPES oficial
- **📍 27 UFs** cobertas
- **⚡ Performance otimizada** para as 4 métricas solicitadas

## 📦 Dependências

### Dependências Requeridas
- `src/models/dimensions/dim_tema.py` (com sigla_uf)
- `src/models/dimensions/dim_ies.py` (da API CAPES)
- `src/models/dimensions/dim_localidade.py`
- `src/models/dimensions/dim_tempo.py`
- `seeds/relational/raw_tema.py` (dados brutos)

### Fontes de Dados
- **DIM_IES**: 100% API CAPES oficial
- **DIM_TEMA**: Excel curadoria_temas.xlsx (com sigla UF)
- **RAW_TEMA**: Dados brutos de temas por UF
- **DIM_LOCALIDADE**: Mapeamento UF-Região

## 🔄 Processo de Atualização

### Backup Automático
O script cria automaticamente backup da tabela anterior como `fato_pos_graduacao_backup`.

### Substituição Manual
Para substituir a FATO antiga:
```sql
-- Renomear tabela antiga
ALTER TABLE fato_pos_graduacao RENAME TO fato_pos_graduacao_old;

-- Renomear nova tabela
ALTER TABLE fato_temas_ RENAME TO fato_pos_graduacao;
```

## 📋 Logs e Monitoramento

O script gera logs detalhados incluindo:
- ✅ Status de backup
- 📊 Estatísticas de criação
- 🔍 Validação de métricas
- ⚡ Tempos de execução
- 📈 Contadores de registros

## 🎉 Benefícios Alcançados

1. **📊 Métricas Precisas**: Todas as 4 métricas solicitadas funcionando
2. **⚡ Performance Superior**: Consultas muito mais rápidas
3. **🔧 Manutenção Simples**: Estrutura limpa e documentada
4. **📡 Dados Confiáveis**: IES direto da API oficial CAPES
5. **🗺️ Mapeamento Correto**: UFs alinhadas entre todas as dimensões
6. **🔗 Integridade Total**: Relacionamentos consistentes

---

📅 **Data de Criação:** 05/08/2025  
🔄 **Versão:** 2.0  
✅ **Status:** Produção  
🎓 **Projeto:** UFMS Data Warehouse CAPES
