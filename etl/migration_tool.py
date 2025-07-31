"""
Script de migração para usar as novas classes ETL
Migra dim_*.py para usar etl_utils.py e persists.py
Data Warehouse Observatório CAPES v2.0
"""

import os
import re
import shutil
from datetime import datetime
from pathlib import Path

class ETLMigrationTool:
    """Ferramenta para migrar arquivos para nova arquitetura ETL"""
    
    def __init__(self, source_dir="/Users/vanessaborges/Library/CloudStorage/OneDrive-Pessoal/UFMS/aulas-van/repo-github/capes/dw"):
        self.source_dir = Path(source_dir)
        self.backup_dir = self.source_dir / "backup_migration"
        self.migration_log = []
    
    def create_backup(self):
        """Cria backup dos arquivos originais"""
        print("📋 Criando backup dos arquivos originais...")
        
        if not self.backup_dir.exists():
            self.backup_dir.mkdir()
        
        # Arquivos para backup
        files_to_backup = list(self.source_dir.glob("dim_*.py"))
        files_to_backup.extend(list(self.source_dir.glob("etl_*.py")))
        files_to_backup.extend(list(self.source_dir.glob("functions.py")))
        
        for file_path in files_to_backup:
            if file_path.is_file():
                backup_path = self.backup_dir / f"{file_path.name}.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                shutil.copy2(file_path, backup_path)
                print(f"   ✅ Backup: {file_path.name} → {backup_path.name}")
        
        return len(files_to_backup)
    
    def analyze_populate_files(self):
        """Analisa arquivos dim_* para identificar padrões"""
        print("\n🔍 Analisando arquivos dim_*...")
        
        populate_files = list(self.source_dir.glob("dim_*.py"))
        analysis = {}
        
        for file_path in populate_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                analysis[file_path.name] = {
                    'has_api_calls': 'requests.get' in content or 'capes_api' in content.lower(),
                    'has_csv_read': 'pd.read_csv' in content or '.csv' in content,
                    'has_xlsx_read': 'pd.read_excel' in content or '.xlsx' in content,
                    'has_json_read': 'pd.read_json' in content or '.json' in content,
                    'has_database_write': 'to_sql' in content,
                    'has_logging': 'logging' in content or 'print(' in content,
                    'lines_count': len(content.split('\n')),
                    'functions_count': len(re.findall(r'^def\s+\w+', content, re.MULTILINE))
                }
                
                print(f"   📄 {file_path.name}:")
                print(f"      - Linhas: {analysis[file_path.name]['lines_count']}")
                print(f"      - Funções: {analysis[file_path.name]['functions_count']}")
                print(f"      - API: {'✅' if analysis[file_path.name]['has_api_calls'] else '❌'}")
                print(f"      - CSV: {'✅' if analysis[file_path.name]['has_csv_read'] else '❌'}")
                print(f"      - DB Write: {'✅' if analysis[file_path.name]['has_database_write'] else '❌'}")
                
            except Exception as e:
                print(f"   ❌ Erro ao analisar {file_path.name}: {e}")
                analysis[file_path.name] = {'error': str(e)}
        
        return analysis
    
    def generate_migration_template(self, dimension_name, analysis_data):
        """Gera template migrado para uma dimensão específica"""
        template = f'''"""
Migração automática: dim_{dimension_name}.py
Usando nova arquitetura ETL Utils e Persists
Data Warehouse Observatório CAPES v2.0
Migrado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

import pandas as pd
from etl_utils import (
    DimensionBuilder, DataExtractor, DataTransformer, 
    DataValidator, ETLConfig, log_execution
)
from persists import DataManager
from functions import conectar_bd  # Compatibilidade


@log_execution
def extrair_dados_{dimension_name}():
    """
    Extrai dados para dimensão {dimension_name}
    """
    extractor = DataExtractor()
    
    try:'''
        
        # Adicionar código baseado na análise
        if analysis_data.get('has_api_calls'):
            template += f'''
        # Extrair dados da API CAPES
        dados_api = extractor.extract_from_capes_api(
            endpoint="{dimension_name}",
            params={{'limit': 1000}}
        )
        return dados_api'''
        
        elif analysis_data.get('has_csv_read'):
            template += f'''
        # Extrair dados de CSV
        dados_csv = extractor.extract_from_csv(
            file_path="data-explorer/oracle-data/datasets.nosync/{dimension_name}.csv"
        )
        return dados_csv'''
        
        elif analysis_data.get('has_xlsx_read'):
            template += f'''
        # Extrair dados de Excel
        dados_excel = extractor.extract_from_excel(
            file_path="data-explorer/oracle-data/datasets.nosync/{dimension_name}.xlsx"
        )
        return dados_excel'''
        
        else:
            template += f'''
        # TODO: Implementar extração específica para {dimension_name}
        # Baseado no arquivo original
        dados = pd.DataFrame()  # Implementar extração
        return dados'''
        
        template += f'''
        
    except Exception as e:
        print(f"❌ Erro na extração de {dimension_name}: {{e}}")
        return pd.DataFrame()


@log_execution
def transformar_dados_{dimension_name}(df_origem):
    """
    Transforma dados para dimensão {dimension_name}
    """
    if df_origem.empty:
        return df_origem
    
    transformer = DataTransformer()
    validator = DataValidator()
    
    try:
        # 1. Limpar dados
        df_limpo = transformer.clean_dataframe(df_origem)
        
        # 2. Padronizar colunas (implementar conforme necessário)
        # df_padronizado = transformer.standardize_columns(df_limpo, mapping_dict)
        
        # 3. Validar dados
        if not validator.validate_required_columns(df_limpo, []):  # TODO: adicionar colunas obrigatórias
            print("⚠️ Validação de colunas falhou")
        
        # 4. Remover duplicatas
        df_final = transformer.remove_duplicates(df_limpo)
        
        return df_final
        
    except Exception as e:
        print(f"❌ Erro na transformação de {dimension_name}: {{e}}")
        return pd.DataFrame()


@log_execution 
def construir_dimensao_{dimension_name}(df_transformado):
    """
    Constrói dimensão {dimension_name} com chave surrogate
    """
    if df_transformado.empty:
        return df_transformado
    
    builder = DimensionBuilder('dim_{dimension_name}')
    
    try:
        # Construir dimensão com SK automática
        df_dimensao = builder.build_dimension(df_transformado)
        
        print(f"✅ Dimensão {dimension_name} construída: {{len(df_dimensao)}} registros")
        return df_dimensao
        
    except Exception as e:
        print(f"❌ Erro na construção de dimensão {dimension_name}: {{e}}")
        return pd.DataFrame()


@log_execution
def salvar_dimensao_{dimension_name}(df_dimensao):
    """
    Salva dimensão {dimension_name} no banco
    """
    if df_dimensao.empty:
        print(f"⚠️ DataFrame vazio para {dimension_name}")
        return False
    
    data_manager = DataManager()
    
    try:
        # Salvar dimensão
        success = data_manager.writer.write_dimension(
            df_dimensao, 
            'dim_{dimension_name}',
            validate_sk=True
        )
        
        if success:
            print(f"✅ Dimensão {dimension_name} salva com {{len(df_dimensao)}} registros")
        else:
            print(f"❌ Erro ao salvar dimensão {dimension_name}")
        
        return success
        
    except Exception as e:
        print(f"❌ Erro ao salvar {dimension_name}: {{e}}")
        return False


@log_execution
def executar_etl_{dimension_name}():
    """
    Executa ETL completo para dimensão {dimension_name}
    """
    print(f"🚀 Iniciando ETL para dimensão {dimension_name}...")
    
    try:
        # 1. Extrair
        df_origem = extrair_dados_{dimension_name}()
        if df_origem.empty:
            print(f"⚠️ Nenhum dado extraído para {dimension_name}")
            return False
        
        # 2. Transformar
        df_transformado = transformar_dados_{dimension_name}(df_origem)
        if df_transformado.empty:
            print(f"⚠️ Erro na transformação para {dimension_name}")
            return False
        
        # 3. Construir dimensão
        df_dimensao = construir_dimensao_{dimension_name}(df_transformado)
        if df_dimensao.empty:
            print(f"⚠️ Erro na construção para {dimension_name}")
            return False
        
        # 4. Salvar
        success = salvar_dimensao_{dimension_name}(df_dimensao)
        
        if success:
            print(f"✅ ETL {dimension_name} concluído com sucesso!")
        else:
            print(f"❌ ETL {dimension_name} falhou na etapa de salvamento")
        
        return success
        
    except Exception as e:
        print(f"❌ Erro no ETL {dimension_name}: {{e}}")
        import traceback
        traceback.print_exc()
        return False


# Manter compatibilidade com código antigo
def {dimension_name}():
    """Função de compatibilidade - usar executar_etl_{dimension_name}()"""
    return executar_etl_{dimension_name}()


if __name__ == "__main__":
    # Executar ETL
    executar_etl_{dimension_name}()
'''
        
        return template
    
    def create_migration_guide(self):
        """Cria guia de migração"""
        guide_content = f"""# Guia de Migração ETL
## Data Warehouse Observatório CAPES v2.0

Migração realizada em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 🎯 Objetivo
Migrar de arquivos isolados para arquitetura modular com:
- `etl_utils.py`: Classes utilitárias para ETL
- `persists.py`: Camada de persistência de dados
- `functions.py`: Funções de compatibilidade

## 📋 Passos da Migração

### 1. Backup Criado
Arquivos originais salvos em `backup_migration/`

### 2. Nova Arquitetura
- **etl_utils.py**: Classes ETLConfig, DataExtractor, DataTransformer, DataValidator, DimensionBuilder, TimeUtils
- **persists.py**: Classes DatabaseConnection, DataReader, DataWriter, DataManager  
- **functions.py**: Funções de compatibilidade e wrappers

### 3. Padrão de Migração
Cada `dim_*.py` agora segue o padrão:

```python
# 1. Extrair dados
df_origem = extrair_dados_dimensao()

# 2. Transformar dados  
df_transformado = transformar_dados_dimensao(df_origem)

# 3. Construir dimensão com SK
df_dimensao = construir_dimensao_dimensao(df_transformado)

# 4. Salvar no banco
success = salvar_dimensao_dimensao(df_dimensao)
```

### 4. Vantagens da Nova Arquitetura
- ✅ Reutilização de código
- ✅ Logging padronizado
- ✅ Tratamento de erros consistente
- ✅ Validação automática
- ✅ Gestão de conexões otimizada
- ✅ Construção automática de chaves surrogate

### 5. Como Usar

#### Exemplo básico:
```python
from etl_utils import DimensionBuilder, DataExtractor
from persists import DataManager

# Extrair dados
extractor = DataExtractor()
dados = extractor.extract_from_capes_api("docentes")

# Construir dimensão
builder = DimensionBuilder('dim_docente')
df_dimensao = builder.build_dimension(dados)

# Salvar
data_manager = DataManager()
data_manager.writer.write_dimension(df_dimensao, 'dim_docente')
```

### 6. Arquivos Migrados
Os arquivos foram analisados e templates gerados baseados nos padrões identificados.

### 7. Próximos Passos
1. Revisar templates gerados
2. Implementar extrações específicas nos TODO's
3. Testar cada dimensão individualmente
4. Executar ETL completo
5. Validar integridade dos dados

### 8. Troubleshooting
- Verificar conexões de banco com `data_manager.db.test_connection()`
- Usar logs para debug: imports têm decorador `@log_execution`
- Consultar `example_new_etl.py` para exemplos práticos

## 🆘 Suporte
Para dúvidas, consulte:
- `example_new_etl.py`: Exemplos práticos
- `etl_utils.py`: Documentação das classes
- `persists.py`: Documentação da camada de dados
"""
        
        guide_path = self.source_dir / "MIGRATION_GUIDE.md"
        with open(guide_path, 'w', encoding='utf-8') as f:
            f.write(guide_content)
        
        print(f"📖 Guia de migração criado: {guide_path}")
        return guide_path
    
    def run_migration(self):
        """Executa migração completa"""
        print("🔄 Iniciando migração para nova arquitetura ETL...")
        
        # 1. Criar backup
        backup_count = self.create_backup()
        print(f"✅ {backup_count} arquivos copiados para backup")
        
        # 2. Analisar arquivos
        analysis = self.analyze_populate_files()
        
        # 3. Gerar templates migrados
        print("\n🏗️ Gerando templates migrados...")
        templates_created = 0
        
        for filename, data in analysis.items():
            if 'error' not in data and 'dim_' in filename:
                # Extrair nome da dimensão
                dimension_name = filename.replace('dim_', '').replace('.py', '')
                
                # Gerar template
                template_content = self.generate_migration_template(dimension_name, data)
                
                # Salvar template
                template_path = self.source_dir / f"dim_{dimension_name}_migrated.py"
                with open(template_path, 'w', encoding='utf-8') as f:
                    f.write(template_content)
                
                print(f"   ✅ Template criado: dim_{dimension_name}_migrated.py")
                templates_created += 1
        
        # 4. Criar guia de migração
        self.create_migration_guide()
        
        print(f"\n🎉 Migração concluída!")
        print(f"   - {backup_count} arquivos em backup")
        print(f"   - {templates_created} templates migrados criados")
        print(f"   - Guia de migração disponível")
        print(f"\n📋 Próximos passos:")
        print(f"   1. Revisar templates *_migrated.py")
        print(f"   2. Implementar TODOs específicos")
        print(f"   3. Testar com example_new_etl.py")
        print(f"   4. Executar ETL migrado")
        
        return {
            'backup_count': backup_count,
            'templates_created': templates_created,
            'analysis': analysis
        }


if __name__ == "__main__":
    # Executar migração
    migration_tool = ETLMigrationTool()
    result = migration_tool.run_migration()
    
    print(f"\n📊 Resumo da migração:")
    print(f"   - Arquivos analisados: {len(result['analysis'])}")
    print(f"   - Templates criados: {result['templates_created']}")
    print(f"   - Backups realizados: {result['backup_count']}")
