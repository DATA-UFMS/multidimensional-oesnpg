#!/usr/bin/env python3
"""
🎓 DIMENSÃO PPG - Data Warehouse Observatório CAPES
=======================================================
Cria a dimensão dim_ppg baseada nos dados da raw_ppg
Estrutura: sk, informações dos Programas de Pós-Graduação
Data: 19/09/2025 - Atualizada para usar raw_ppg como fonte
"""

import pandas as pd
import numpy as np
import os
import sys
from dotenv import load_dotenv
import logging
from pathlib import Path
# Adicionar o diretório raiz ao path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from src.utils.naming_conventions import NamingConventions
from src.validation.data_validator import validate_dimension_data, get_validation_summary
from src.core.exceptions import DimensionCreationError, DataValidationError


# Adicionar path para imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from src.core.core import get_db_manager

def get_project_root() -> Path:
    """Encontra o diretório raiz do projeto de forma robusta."""
    current_path = Path(__file__).resolve()
    while not (current_path / '.env').exists() and not (current_path / '.git').exists() and current_path.parent != current_path:
        current_path = current_path.parent
    return current_path

# Carregar variáveis de ambiente
project_root_path = get_project_root()
load_dotenv(dotenv_path=project_root_path / '.env')

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def carregar_dados_raw_ppg():
    """
    Carrega os dados da tabela raw_ppg para DataFrame.
    """
    logger.info("📥 Carregando dados da raw_ppg...")
    db = get_db_manager()
    
    try:
        query = """
        SELECT 
            ano_base,
            codigo_capes_da_ies,
            nome_da_ies,
            nome_da_regiao_da_ies,
            sigla_da_regiao_da_ies,
            cd_regiao_ibge,
            uf_da_ies,
            status_juridico_da_ies,
            codigo_do_ppg,
            nome_ppg,
            nota_do_ppg,
            modalidade_do_ppg,
            situacao_do_ppg,
            programa_em_rede,
            codigo_grande_area_do_ppg,
            grande_area_do_ppg,
            codigo_area_de_conhecimento_do_ppg,
            area_de_conhecimento_do_ppg,
            id_area_de_avaliacao_do_ppg,
            area_de_avaliacao_do_ppg,
            total_de_cursos_do_ppg,
            quantidade_de_docentes_no_ppg,
            quantidade_de_discentes_matriculados_no_ppg
        FROM raw_ppg
        WHERE codigo_do_ppg IS NOT NULL
        ORDER BY codigo_do_ppg;
        """
        
        df = db.execute_query(query)
        logger.info(f"✅ Carregados {len(df):,} registros da raw_ppg")
        return df
        
    except Exception as e:
        logger.error(f"❌ Erro ao carregar dados da raw_ppg: {str(e)}")
        return None

def processar_dataframe_ppg(df):
    """
    Processa o DataFrame de PPG aplicando transformações e limpezas.
    """
    if df is None or df.empty:
        logger.error("❌ DataFrame vazio ou None para processamento")
        return None
        
    logger.info(f"🔄 Processando {len(df):,} registros de PPG...")
    
    try:
        # Fazer cópia para não alterar o original
        df_processed = df.copy()
        
        # 1. Remover duplicatas baseado no código do PPG
        df_processed = df_processed.drop_duplicates(subset=['codigo_do_ppg'], keep='first')
        logger.info(f"📊 Processando {len(df_processed):,} PPGs únicos (removidas {len(df) - len(df_processed):,} duplicatas)")
        
        # 2. Limpar e padronizar campos de texto
        logger.info("🧹 Limpando e padronizando dados...")
        
        colunas_texto = [
            'nome_da_ies', 'nome_da_regiao_da_ies', 'sigla_da_regiao_da_ies', 'uf_da_ies',
            'status_juridico_da_ies', 'nome_ppg', 'modalidade_do_ppg', 'situacao_do_ppg',
            'programa_em_rede', 'grande_area_do_ppg', 'area_de_conhecimento_do_ppg',
            'area_de_avaliacao_do_ppg'
        ]
        
        for col in colunas_texto:
            if col in df_processed.columns:
                df_processed[col] = df_processed[col].fillna('Não informado')
                df_processed[col] = df_processed[col].astype(str).str.strip()
                df_processed[col] = df_processed[col].replace(['', 'nan', 'None'], 'Não informado')
        
        # 3. Tratar campos numéricos
        colunas_numericas = [
            'ano_base', 'codigo_capes_da_ies', 'cd_regiao_ibge', 'codigo_grande_area_do_ppg',
            'codigo_area_de_conhecimento_do_ppg', 'id_area_de_avaliacao_do_ppg', 'total_de_cursos_do_ppg',
            'quantidade_de_docentes_no_ppg', 'quantidade_de_discentes_matriculados_no_ppg'
        ]
        
        for col in colunas_numericas:
            if col in df_processed.columns:
                df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').fillna(0).astype(int)
        
        # 4. Tratar campo nota_do_ppg (decimal)
        if 'nota_do_ppg' in df_processed.columns:
            df_processed['nota_do_ppg'] = pd.to_numeric(df_processed['nota_do_ppg'], errors='coerce').fillna(0.0)
        
        # 5. Padronizar valores categóricos
        if 'modalidade_do_ppg' in df_processed.columns:
            df_processed['modalidade_do_ppg'] = df_processed['modalidade_do_ppg'].replace({
                'ACADÊMICO': 'Acadêmico',
                'PROFISSIONAL': 'Profissional',
                'ACADEMICO': 'Acadêmico'
            })
        
        if 'programa_em_rede' in df_processed.columns:
            df_processed['programa_em_rede'] = df_processed['programa_em_rede'].replace({
                'Não': 'Não',
                'Sim': 'Sim',
                'N': 'Não',
                'S': 'Sim'
            })
        
        logger.info(f"✅ Processamento concluído: {len(df_processed):,} PPGs processados")
        return df_processed
        
    except Exception as e:
        logger.error(f"❌ Erro durante processamento: {str(e)}")
        return None
        
        # Tratar nota do PPG (pode ser decimal)
        if 'nota_do_ppg' in df_processed.columns:
            df_processed['nota_do_ppg'] = pd.to_numeric(df_processed['nota_do_ppg'], errors='coerce').fillna(0.0)
        
        # 4. Padronizar modalidade
        if 'modalidade_ppg' in df_processed.columns:
            modalidade_map = {
                'ACADEMICO': 'Acadêmico',
                'PROFISSIONAL': 'Profissional',
                'ACADÊMICO': 'Acadêmico'
            }
            df_processed['modalidade_ppg'] = df_processed['modalidade_ppg'].str.upper()
            df_processed['modalidade_ppg'] = df_processed['modalidade_ppg'].map(modalidade_map).fillna('Não informado')
        
        # 5. Padronizar situação
        if 'situacao_ppg' in df_processed.columns:
            situacao_map = {
                'EM_FUNCIONAMENTO': 'Em Funcionamento',
                'EM FUNCIONAMENTO': 'Em Funcionamento',
                'DESATIVADO': 'Desativado',
                'SUSPENSO': 'Suspenso'
            }
            df_processed['situacao_ppg'] = df_processed['situacao_ppg'].str.upper()
            df_processed['situacao_ppg'] = df_processed['situacao_ppg'].map(situacao_map).fillna('Não informado')
        
        # 6. Padronizar programa em rede
        if 'programa_em_rede' in df_processed.columns:
            rede_map = {
                'SIM': 'Sim',
                'NÃO': 'Não',
                'NAO': 'Não'
            }
            df_processed['programa_em_rede'] = df_processed['programa_em_rede'].str.upper()
            df_processed['programa_em_rede'] = df_processed['programa_em_rede'].map(rede_map).fillna('Não')
        
        # 7. Padronizar regiões
        if 'nome_regiao_ies' in df_processed.columns:
            regiao_map = {
                'NORTE': 'Norte',
                'NORDESTE': 'Nordeste',
                'CENTRO-OESTE': 'Centro-Oeste',
                'SUDESTE': 'Sudeste',
                'SUL': 'Sul'
            }
            df_processed['nome_regiao_ies'] = df_processed['nome_regiao_ies'].str.upper()
            df_processed['nome_regiao_ies'] = df_processed['nome_regiao_ies'].map(regiao_map).fillna('Não informado')
        
        # 8. Padronizar UF
        if 'uf_da_ies' in df_processed.columns:
            df_processed['uf_da_ies'] = df_processed['uf_da_ies'].str.upper().str.strip()
        
        logger.info(f"✅ Processamento concluído: {len(df_processed):,} registros processados")
        return df_processed
        
    except Exception as e:
        logger.error(f"❌ Erro durante processamento: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def criar_dimensao_ppg():
    """
    Cria a dimensão PPG usando pandas para manipular os dados.
    """
    logger.info("🎓 Criando dimensão PPG com pandas...")
    db = get_db_manager()
    
    try:
        # 1. Remover tabela existente se houver
        logger.info("🗑️  Removendo dim_ppg existente...")
        drop_sql = "DROP TABLE IF EXISTS dim_ppg CASCADE;"
        db.execute_sql(drop_sql)
        
        # 2. Criar tabela dim_ppg
        logger.info("🏗️  Criando nova estrutura dim_ppg...")
        create_sql = """
        CREATE TABLE dim_ppg (
            sk SERIAL PRIMARY KEY,
            codigo_programa VARCHAR(50),
            nome_programa VARCHAR(500),
            nota_programa DECIMAL(3,1),
            modalidade VARCHAR(50),
            situacao VARCHAR(50),
            programa_em_rede VARCHAR(10),
            ies_vinculada VARCHAR(500),
            codigo_ies INTEGER,
            uf VARCHAR(10),
            regiao VARCHAR(50),
            area_conhecimento VARCHAR(200),
            grande_area VARCHAR(200),
            area_avaliacao VARCHAR(200),
            total_cursos INTEGER,
            quantidade_docentes INTEGER,
            quantidade_discentes INTEGER,
            ano_base INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        if not db.execute_sql(create_sql):
            logger.error("❌ Erro ao criar tabela dim_ppg")
            return False
            
        # 3. Inserir registro SK=0 (desconhecido)
        logger.info("🔧 Inserindo registro DESCONHECIDO (SK=0)...")
        sk0_data = pd.DataFrame({
            'ppg_sk': [0],
            'codigo_programa': ['DESCONHECIDO'],
            'nome_programa': ['NÃO INFORMADO'],
            'nota_programa': [0.0],
            'modalidade': ['NÃO INFORMADO'],
            'situacao': ['NÃO INFORMADO'],
            'programa_em_rede': ['Não'],
            'ies_vinculada': ['NÃO INFORMADO'],
            'codigo_ies': [0],
            'uf': ['XX'],
            'regiao': ['NÃO INFORMADO'],
            'area_conhecimento': ['NÃO INFORMADO'],
            'grande_area': ['NÃO INFORMADO'],
            'area_avaliacao': ['NÃO INFORMADO'],
            'total_cursos': [0],
            'quantidade_docentes': [0],
            'quantidade_discentes': [0],
            'ano_base': [0]
        })
        
        # Inserir registro SK=0
        db.save_dataframe(sk0_data, 'dim_ppg', if_exists='append')
        
        # 4. Carregar e processar dados de PPG da raw_ppg
        df_raw = carregar_dados_raw_ppg()
        if df_raw is None:
            logger.error("❌ Falha ao carregar dados da raw_ppg")
            return False
            
        df_ppg = processar_dataframe_ppg(df_raw)
        if df_ppg is None:
            logger.error("❌ Falha ao processar DataFrame de PPG")
            return False
        
        # 5. Mapear colunas para a estrutura da dimensão
        logger.info("🔄 Mapeando colunas para estrutura da dimensão...")
        df_final = pd.DataFrame()
        
        # Mapeamento de colunas
        mapeamento = {
            'codigo_programa': 'codigo_do_ppg',
            'nome_programa': 'nome_ppg',
            'nota_programa': 'nota_do_ppg',
            'modalidade': 'modalidade_do_ppg',
            'situacao': 'situacao_do_ppg',
            'programa_em_rede': 'programa_em_rede',
            'ies_vinculada': 'nome_da_ies',
            'codigo_ies': 'codigo_capes_da_ies',
            'uf': 'uf_da_ies',
            'regiao': 'nome_da_regiao_da_ies',
            'area_conhecimento': 'area_de_conhecimento_do_ppg',
            'grande_area': 'grande_area_do_ppg',
            'area_avaliacao': 'area_de_avaliacao_do_ppg',
            'total_cursos': 'total_de_cursos_do_ppg',
            'quantidade_docentes': 'quantidade_de_docentes_no_ppg',
            'quantidade_discentes': 'quantidade_de_discentes_matriculados_no_ppg',
            'ano_base': 'ano_base'
        }
        
        # Aplicar mapeamento
        for col_destino, col_origem in mapeamento.items():
            if col_origem in df_ppg.columns:
                df_final[col_destino] = df_ppg[col_origem]
            else:
                # Valores padrão baseado no tipo
                if col_destino in ['codigo_ies', 'total_cursos', 'quantidade_docentes', 'quantidade_discentes', 'ano_base']:
                    df_final[col_destino] = 0
                elif col_destino == 'nota_programa':
                    df_final[col_destino] = 0.0
                elif col_destino == 'programa_em_rede':
                    df_final[col_destino] = 'Não'
                else:
                    df_final[col_destino] = 'Não informado'
        
        # 6. Inserir dados processados no banco
        logger.info("💾 Inserindo dados processados no banco...")
        db.save_dataframe(df_final, 'dim_ppg', if_exists='append')
        
        # 7. Verificar inserção
        count_query = "SELECT COUNT(*) as total FROM dim_ppg;"
        result = db.execute_query(count_query)
        total = result.iloc[0]['total']
        
        logger.info(f"✅ dim_ppg criada com {total:,} registros")
        
        # 8. Criar índices para performance
        logger.info("🔍 Criando índices...")
        indices_sql = [
            "CREATE INDEX IF NOT EXISTS idx_dim_ppg_codigo ON dim_ppg(codigo_programa);",
            "CREATE INDEX IF NOT EXISTS idx_dim_ppg_uf ON dim_ppg(uf);",
            "CREATE INDEX IF NOT EXISTS idx_dim_ppg_regiao ON dim_ppg(regiao);",
            "CREATE INDEX IF NOT EXISTS idx_dim_ppg_modalidade ON dim_ppg(modalidade);",
            "CREATE INDEX IF NOT EXISTS idx_dim_ppg_area ON dim_ppg(area_conhecimento);"
        ]
        
        for idx_sql in indices_sql:
            db.execute_sql(idx_sql)
        
        logger.info("✅ Índices criados")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao criar dimensão PPG: {str(e)}")
        return False

def validar_dimensao_ppg():
    """Valida os dados da dimensão PPG."""
    logger.info("🔍 Validando dimensão PPG...")
    db = get_db_manager()
    
    try:
        print("\n" + "="*60)
        print("📊 VALIDAÇÃO DA DIMENSÃO PPG")
        print("="*60)
        
        # 1. Contagem total
        query_total = "SELECT COUNT(*) as total FROM dim_ppg;"
        result = db.execute_query(query_total)
        total = result.iloc[0]['total']
        print(f"📊 Total de registros: {total:,}")
        
        # 2. PPG por região
        print("\n🌎 PPG por região:")
        query_regiao = """
        SELECT 
            regiao,
            COUNT(*) as qtd_ppg
        FROM dim_ppg 
        WHERE sk > 0
        GROUP BY regiao
        ORDER BY qtd_ppg DESC;
        """
        result = db.execute_query(query_regiao)
        print(result.to_string(index=False))
        
        # 3. PPG por modalidade
        print("\n📚 PPG por modalidade:")
        query_modalidade = """
        SELECT 
            modalidade,
            COUNT(*) as qtd_ppg
        FROM dim_ppg 
        WHERE sk > 0
        GROUP BY modalidade
        ORDER BY qtd_ppg DESC;
        """
        result = db.execute_query(query_modalidade)
        print(result.to_string(index=False))
        
        # 4. PPG por situação
        print("\n📈 PPG por situação:")
        query_situacao = """
        SELECT 
            situacao,
            COUNT(*) as qtd_ppg
        FROM dim_ppg 
        WHERE sk > 0
        GROUP BY situacao
        ORDER BY qtd_ppg DESC;
        """
        result = db.execute_query(query_situacao)
        print(result.to_string(index=False))
        
        # 5. Top 10 grandes áreas
        print("\n🎓 Top 10 grandes áreas:")
        query_grande_area = """
        SELECT 
            grande_area,
            COUNT(*) as qtd_ppg
        FROM dim_ppg 
        WHERE sk > 0
        GROUP BY grande_area
        ORDER BY qtd_ppg DESC
        LIMIT 10;
        """
        result = db.execute_query(query_grande_area)
        print(result.to_string(index=False))
        
        # 6. Distribuição de notas
        print("\n⭐ Distribuição de notas:")
        query_notas = """
        SELECT 
            nota_programa,
            COUNT(*) as qtd_ppg
        FROM dim_ppg 
        WHERE sk > 0 AND nota_programa > 0
        GROUP BY nota_programa
        ORDER BY nota_programa DESC;
        """
        result = db.execute_query(query_notas)
        print(result.to_string(index=False))
        
        print("\n✅ Validação concluída!")
        
    except Exception as e:
        logger.error(f"❌ Erro na validação: {str(e)}")

def main():
    """Função principal."""
    try:
        logger.info("🚀 Iniciando criação da dimensão PPG")
        
        # 1. Criar dimensão
        if not criar_dimensao_ppg():
            logger.error("❌ Falha na criação da dimensão PPG")
            return
            
        # 2. Validar dimensão
        validar_dimensao_ppg()
        
        print("\n" + "="*70)
        print("🎉 DIMENSÃO PPG CRIADA COM SUCESSO!")
        print("="*70)
        print("✅ Tabela: dim_ppg")
        print("✅ Fonte: raw_ppg")
        print("✅ Índices: Performance otimizada")
        print("✅ Dados: Tratados e normalizados")
        print("="*70)
        
    except Exception as e:
        logger.error(f"❌ Erro durante criação da dimensão PPG: {str(e)}")

if __name__ == "__main__":
    main()
