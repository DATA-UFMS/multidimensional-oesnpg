# Scripts RAW Unificados

Este diretório contém scripts padronizados e unificados para extração de dados raw das diferentes fontes do projeto.

## Estrutura

### Módulo Base
- **`base_raw.py`**: Módulo comum com funcionalidades reutilizáveis
  - `DatabaseManager`: Gerenciamento de conexões e operações do banco
  - `CAPESApiExtractor`: Extração de dados da API CAPES
  - `DataQualityAnalyzer`: Análise de qualidade dos dados
  - `DataCleaner`: Limpeza e padronização básica

### Scripts de Extração

#### 1. `raw_tema.py`
- **Fonte**: `seeds/curadoria_temas.xlsx`
- **Descrição**: Processa dados de temas e ODS
- **Tabela**: `raw_tema`
- **Características**:
  - Desnormalização de temas múltiplos
  - Padronização de campos ODS

#### 2. `raw_ies.py`
- **Fontes**: 
  - API CAPES (Resource ID: `62f82787-3f45-4b9e-8457-3366f60c264b`)
  - `seeds/municipios.csv`
  - `seeds/tabela de codigos UF e Regiao IBGE.xlsx`
- **Descrição**: Dados de Instituições de Ensino Superior
- **Tabela**: `raw_ies`
- **Características**:
  - Consolidação de múltiplas fontes
  - Identificação da fonte dos dados

#### 3. `raw_ppg.py`
- **Fonte**: `seeds/ppg_2024.csv`
- **Descrição**: Programas de Pós-Graduação
- **Tabela**: `raw_ppg`
- **Características**:
  - Conversão automática de encoding
  - Padronização de notas CAPES

#### 4. `raw_docente.py`
- **Fontes**:
  - API CAPES (Resource ID: `7d9547c8-9a0d-433a-b2c8-ee9fbbdc5b3a`)
  - `seeds/br-capes-colsucup-docente-2021-2025-03-31.csv`
- **Descrição**: Docentes e pesquisadores
- **Tabela**: `raw_docente`
- **Características**:
  - Limpeza de IDs Lattes
  - Consolidação API + CSV

#### 5. `raw_pq.py`
- **Fonte**: `data/Planilha_Mapa_Fomento_PQ.xlsx - Sheet 1.csv`
- **Descrição**: Bolsas de Produtividade em Pesquisa
- **Tabela**: `raw_pq`
- **Características**:
  - Extração de informações institucionais
  - Conversão de datas
  - Normalização de strings

#### 6. `raw_producao.py`
- **Fonte**: API CAPES (múltiplos datasets)
  - ARTPE: `85e0faae-9db0-4d0d-9bfe-38281c666b13`
  - LIVRO: `b953a29f-e9a8-41af-af25-ffb25be51cf6`
  - APTRA: `284f0f5b-319f-4c2d-bba6-ddff45b69c28`
  - ANAIS: `31e59def-5a18-459d-b8c0-f7befeb62400`
- **Descrição**: Produção acadêmica
- **Tabela**: `raw_producao`
- **Características**:
  - Consolidação de múltiplos tipos de produção
  - Rate limiting para API
  - Modo de teste com limitação de datasets

### Script Coordenador

#### `run_all_raw.py`
- **Descrição**: Executa todos os scripts em ordem otimizada
- **Características**:
  - Gerenciamento de dependências
  - Timeout por script
  - Relatório consolidado de execução
  - Tratamento de erros individual

## Padrões de Código

### Convenções
- ✅ **Emojis permitidos**: `✅`, `⚠️`, `❌` apenas
- 🚫 **Emojis removidos**: Todos os outros emojis decorativos
- 📝 **Mensagens**: Claras e objetivas
- 🔧 **Funções**: Pequenas e focadas em uma responsabilidade

### Estrutura Padrão
```python
#!/usr/bin/env python3
\"\"\"Docstring descritiva\"\"\"

from base_raw import DatabaseManager, DataQualityAnalyzer, ...

def load_data():
    \"\"\"Carrega dados da fonte\"\"\"
    pass

def transform_data(df):
    \"\"\"Transforma e limpa dados\"\"\"
    pass

def main():
    \"\"\"Função principal\"\"\"
    print_header("Título")
    # 1. Carregar dados
    # 2. Transformar dados
    # 3. Analisar qualidade
    # 4. Salvar no banco
    # 5. Relatório final
```

### Tratamento de Erros
- Uso de `try/except` com mensagens informativas
- Continuidade de execução quando possível
- Status claros: `success`, `warning`, `error`, `info`

### Análise de Qualidade
- Estatísticas básicas automáticas
- Campos com mais valores únicos
- Detecção de valores nulos
- Verificação de duplicatas

## Uso

### Executar Script Individual
```bash
python3 raw_tema.py
python3 raw_ies.py
# etc.
```

### Executar Todos os Scripts
```bash
python3 run_all_raw.py
```

### Configuração
- Variáveis de ambiente no arquivo `.env`
- Caminhos relativos para portabilidade
- Configurações centralizadas no `base_raw.py`

## Dependências
- pandas
- sqlalchemy
- python-dotenv
- requests
- openpyxl (para arquivos Excel)

## Logs
- Saída padronizada com status claros
- Progressos de extração da API
- Relatórios de qualidade de dados
- Resumos finais com estatísticas
