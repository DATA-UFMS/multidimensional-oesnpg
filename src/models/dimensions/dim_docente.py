#!/usr/bin/env python3
"""
🎓 DIMENSÃO DOCENTE - Data Warehouse Observatório CAPE        # Fazer cópia para não alterar o original
        df_processed = df.copy()
        
        # 1. Remover duplicatas baseado no id_pessoa
        df_processed = df_processed.drop_duplicates(subset=['id_pessoa'], keep='first')
        logger.info(f"📊 Processando {len(df_processed):,} docentes únicos (removidas {len(df) - len(df_processed):,} duplicatas)")
        
        # 2. Limpar e padronizar campos de texto
        logger.info("🧹 Limpando e padronizando dados...")===============================================
Cria a dimensão dim_docente baseada nos dados da raw_docente
Estrutura: docente_sk, informações pessoais, titulação, vinculação
Data: 18/08/2025
"""

import pandas as pd
import numpy as np
import os
import sys
from dotenv import load_dotenv
import logging

# Adicionar diretório raiz ao path para imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from src.core.core import get_db_manager

# Carregar variáveis de ambiente
load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def carregar_dados_raw_docente():
    """
    Carrega os dados da tabela raw_docente para DataFrame.
    """
    logger.info("📊 Carregando dados da raw_docente...")
    db = get_db_manager()
    
    try:
        # Query para buscar todos os dados da raw_docente
        query = """
        SELECT 
            id_pessoa,
            nm_docente,
            tp_documento_docente,
            nr_documento_docente,
            an_nascimento_docente,
            ds_faixa_etaria,
            ds_tipo_nacionalidade_docente,
            nm_pais_nacionalidade_docente,
            ds_categoria_docente,
            ds_tipo_vinculo_docente_ies,
            ds_regime_trabalho,
            cd_cat_bolsa_produtividade,
            in_doutor,
            an_titulacao,
            nm_grau_titulacao,
            cd_area_basica_titulacao,
            nm_area_basica_titulacao,
            sg_ies_titulacao,
            nm_ies_titulacao,
            nm_pais_ies_titulacao,
            an_base
        FROM raw_docente
        ORDER BY id_pessoa;
        """
        
        df = db.execute_query(query)
        logger.info(f"✅ Carregados {len(df):,} registros da raw_docente")
        return df
        
    except Exception as e:
        logger.error(f"❌ Erro ao carregar dados: {str(e)}")
        return None

def processar_dataframe_docente(df):
    """
    Processa o DataFrame dos docentes aplicando transformações e limpezas.
    """
    logger.info("🔄 Processando DataFrame dos docentes...")
    
    try:
        # Fazer cópia para não alterar o original
        df_processed = df.copy()
        
        # 1. Remover duplicatas baseado no id_pessoa
        logger.info(f"📊 Registros antes da remoção de duplicatas: {len(df_processed):,}")
        df_processed = df_processed.drop_duplicates(subset=['id_pessoa'], keep='first')
        logger.info(f"� Registros após remoção de duplicatas: {len(df_processed):,}")
        
        # 2. Limpar e padronizar campos de texto
        logger.info("🧹 Limpando campos de texto...")
        
        # Função para limpar texto
        def limpar_texto(valor):
            if pd.isna(valor) or str(valor).strip() == '' or str(valor).upper() == 'NAN':
                return 'DESCONHECIDO'
            return str(valor).strip().upper()
        
        # Aplicar limpeza nos campos de texto
        campos_texto = [
            'nm_docente', 'tp_documento_docente', 'ds_faixa_etaria',
            'ds_tipo_nacionalidade_docente', 'nm_pais_nacionalidade_docente',
            'ds_categoria_docente', 'ds_tipo_vinculo_docente_ies', 'ds_regime_trabalho',
            'nm_grau_titulacao', 'nm_area_basica_titulacao', 'sg_ies_titulacao',
            'nm_ies_titulacao', 'nm_pais_ies_titulacao'
        ]
        
        for campo in campos_texto:
            df_processed[campo] = df_processed[campo].apply(limpar_texto)
        
        # 3. Tratar campo de bolsa produtividade
        def tratar_bolsa_produtividade(valor):
            if pd.isna(valor) or str(valor).upper() in ['NAN', '']:
                return 'DESCONHECIDO'
            return str(valor).strip().upper()
        
        df_processed['cd_cat_bolsa_produtividade'] = df_processed['cd_cat_bolsa_produtividade'].apply(tratar_bolsa_produtividade)
        
        # 4. Tratar campo eh_doutor
        def tratar_eh_doutor(valor):
            if pd.isna(valor):
                return 'DESCONHECIDO'
            valor_str = str(valor).strip().upper()
            if valor_str == 'S':
                return 'Sim'
            elif valor_str == 'N':
                return 'Não'
            else:
                return 'DESCONHECIDO'
        
        df_processed['in_doutor'] = df_processed['in_doutor'].apply(tratar_eh_doutor)
        
        # 5. Tratar campos numéricos
        campos_numericos = [
            'id_pessoa', 'an_nascimento_docente', 'an_titulacao',
            'cd_area_basica_titulacao', 'an_base'
        ]
        
        for campo in campos_numericos:
            df_processed[campo] = pd.to_numeric(df_processed[campo], errors='coerce').fillna(0).astype(int)
        
        # 6. Anonimizar número do documento
        def anonimizar_documento(numero):
            if pd.isna(numero) or str(numero).strip() == '' or len(str(numero).strip()) < 5:
                return '***'
            num_str = str(numero).strip()
            return f"{num_str[:3]}***{num_str[-2:]}"
        
        df_processed['numero_documento_anonimo'] = df_processed['nr_documento_docente'].apply(anonimizar_documento)
        
        # 7. Calcular campos derivados
        ano_atual = 2025  # Ano atual
        
        # Idade aproximada
        df_processed['idade_aproximada'] = np.where(
            df_processed['an_nascimento_docente'] > 0,
            ano_atual - df_processed['an_nascimento_docente'],
            0
        )
        
        # Tempo desde titulação
        df_processed['tempo_titulacao'] = np.where(
            df_processed['an_titulacao'] > 0,
            ano_atual - df_processed['an_titulacao'],
            0
        )
        
        # 8. Criar DataFrame final com as colunas da dimensão
        df_final = pd.DataFrame({
            'id_pessoa': df_processed['id_pessoa'],
            'nome_docente': df_processed['nm_docente'],
            'tipo_documento': df_processed['tp_documento_docente'],
            'numero_documento': df_processed['numero_documento_anonimo'],
            'ano_nascimento': df_processed['an_nascimento_docente'],
            'faixa_etaria': df_processed['ds_faixa_etaria'],
            'nacionalidade': df_processed['ds_tipo_nacionalidade_docente'],
            'pais_nacionalidade': df_processed['nm_pais_nacionalidade_docente'],
            'categoria_docente': df_processed['ds_categoria_docente'],
            'tipo_vinculo': df_processed['ds_tipo_vinculo_docente_ies'],
            'regime_trabalho': df_processed['ds_regime_trabalho'],
            'categoria_bolsa_produtividade': df_processed['cd_cat_bolsa_produtividade'],
            'eh_doutor': df_processed['in_doutor'],
            'ano_titulacao': df_processed['an_titulacao'],
            'grau_titulacao': df_processed['nm_grau_titulacao'],
            'codigo_area_titulacao': df_processed['cd_area_basica_titulacao'],
            'area_titulacao': df_processed['nm_area_basica_titulacao'],
            'sigla_ies_titulacao': df_processed['sg_ies_titulacao'],
            'nome_ies_titulacao': df_processed['nm_ies_titulacao'],
            'pais_ies_titulacao': df_processed['nm_pais_ies_titulacao'],
            'ano_base': df_processed['an_base'],
            'idade_aproximada': df_processed['idade_aproximada'],
            'tempo_titulacao': df_processed['tempo_titulacao']
        })
        
        logger.info(f"✅ DataFrame processado: {len(df_final):,} registros")
        logger.info(f"📊 Colunas: {list(df_final.columns)}")
        
        # Estatísticas rápidas
        logger.info("📈 Estatísticas:")
        logger.info(f"  • Doutores: {len(df_final[df_final['eh_doutor'] == 'Sim']):,}")
        logger.info(f"  • Permanentes: {len(df_final[df_final['categoria_docente'] == 'PERMANENTE']):,}")
        logger.info(f"  • Idade média: {df_final[df_final['idade_aproximada'] > 0]['idade_aproximada'].mean():.1f} anos")
        
        return df_final
        
    except Exception as e:
        logger.error(f"❌ Erro ao processar DataFrame: {str(e)}")
        return None

def criar_dimensao_docente():
    """
    Cria a dimensão docente usando pandas para manipular os dados.
    """
    logger.info("🎓 Criando dimensão DOCENTE com pandas...")
    db = get_db_manager()
    
    try:
        # 1. Remover tabela existente se houver
        logger.info("🗑️  Removendo dim_docente existente...")
        drop_sql = "DROP TABLE IF EXISTS dim_docente CASCADE;"
        db.execute_sql(drop_sql)
        
        # 2. Criar tabela dim_docente
        logger.info("🏗️  Criando nova estrutura dim_docente...")
        create_sql = """
        CREATE TABLE dim_docente (
            docente_sk SERIAL PRIMARY KEY,
            id_pessoa INTEGER,
            nome_docente VARCHAR(500),
            tipo_documento VARCHAR(50),
            numero_documento VARCHAR(50),
            ano_nascimento INTEGER,
            faixa_etaria VARCHAR(50),
            nacionalidade VARCHAR(100),
            pais_nacionalidade VARCHAR(100),
            categoria_docente VARCHAR(50),
            tipo_vinculo VARCHAR(100),
            regime_trabalho VARCHAR(50),
            categoria_bolsa_produtividade VARCHAR(20),
            eh_doutor VARCHAR(20),
            ano_titulacao INTEGER,
            grau_titulacao VARCHAR(50),
            codigo_area_titulacao INTEGER,
            area_titulacao VARCHAR(200),
            sigla_ies_titulacao VARCHAR(20),
            nome_ies_titulacao VARCHAR(500),
            pais_ies_titulacao VARCHAR(100),
            ano_base INTEGER,
            idade_aproximada INTEGER,
            tempo_titulacao INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        if not db.execute_sql(create_sql):
            logger.error("❌ Erro ao criar tabela dim_docente")
            return False
            
        # 3. Inserir registro SK=0 (desconhecido)
        logger.info("🔧 Inserindo registro DESCONHECIDO (SK=0)...")
        sk0_data = pd.DataFrame({
            'docente_sk': [0],
            'id_pessoa': [0],
            'nome_docente': ['NÃO INFORMADO'],
            'tipo_documento': ['NÃO INFORMADO'],
            'numero_documento': ['***'],
            'ano_nascimento': [0],
            'faixa_etaria': ['NÃO INFORMADO'],
            'nacionalidade': ['NÃO INFORMADO'],
            'pais_nacionalidade': ['NÃO INFORMADO'],
            'categoria_docente': ['NÃO INFORMADO'],
            'tipo_vinculo': ['NÃO INFORMADO'],
            'regime_trabalho': ['NÃO INFORMADO'],
            'categoria_bolsa_produtividade': ['NÃO INFORMADO'],
            'eh_doutor': ['NÃO INFORMADO'],
            'ano_titulacao': [0],
            'grau_titulacao': ['NÃO INFORMADO'],
            'codigo_area_titulacao': [0],
            'area_titulacao': ['NÃO INFORMADO'],
            'sigla_ies_titulacao': ['NÃO INFORMADO'],
            'nome_ies_titulacao': ['NÃO INFORMADO'],
            'pais_ies_titulacao': ['NÃO INFORMADO'],
            'ano_base': [0],
            'idade_aproximada': [0],
            'tempo_titulacao': [0]
        })
        
        # Inserir registro SK=0
        db.save_dataframe(sk0_data, 'dim_docente', if_exists='append')
        
        # 4. Carregar e processar dados dos docentes
        df_raw = carregar_dados_raw_docente()
        if df_raw is None:
            logger.error("❌ Falha ao carregar dados da raw_docente")
            return False
            
        df_docentes = processar_dataframe_docente(df_raw)
        if df_docentes is None:
            logger.error("❌ Falha ao processar DataFrame dos docentes")
            return False
            
        # 5. Inserir dados processados no banco
        logger.info("💾 Inserindo dados processados no banco...")
        db.save_dataframe(df_docentes, 'dim_docente', if_exists='append')
        
        # 6. Verificar inserção
        count_query = "SELECT COUNT(*) as total FROM dim_docente;"
        result = db.execute_query(count_query)
        total = result.iloc[0]['total']
        
        logger.info(f"✅ dim_docente criada com {total:,} registros")
        
        # 7. Criar índices para performance
        logger.info("🔍 Criando índices...")
        indices_sql = [
            "CREATE INDEX IF NOT EXISTS idx_dim_docente_id_pessoa ON dim_docente(id_pessoa);",
            "CREATE INDEX IF NOT EXISTS idx_dim_docente_categoria ON dim_docente(categoria_docente);",
            "CREATE INDEX IF NOT EXISTS idx_dim_docente_titulacao ON dim_docente(grau_titulacao);",
            "CREATE INDEX IF NOT EXISTS idx_dim_docente_doutor ON dim_docente(eh_doutor);"
        ]
        
        for idx_sql in indices_sql:
            db.execute_sql(idx_sql)
        
        logger.info("✅ Índices criados")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao criar dimensão docente: {str(e)}")
        return False

def validar_dimensao_docente():
    """Valida os dados da dimensão docente."""
    logger.info("🔍 Validando dimensão DOCENTE...")
    db = get_db_manager()
    
    try:
        print("\n" + "="*60)
        print("📊 VALIDAÇÃO DA DIMENSÃO DOCENTE")
        print("="*60)
        
        # 1. Contagem total
        query_total = "SELECT COUNT(*) as total FROM dim_docente;"
        result = db.execute_query(query_total)
        total = result.iloc[0]['total']
        print(f"📊 Total de registros: {total:,}")
        
        # 2. Docentes por categoria
        print("\n🎓 Docentes por categoria:")
        query_categoria = """
        SELECT 
            categoria_docente,
            COUNT(*) as qtd_docentes
        FROM dim_docente 
        WHERE docente_sk > 0
        GROUP BY categoria_docente
        ORDER BY qtd_docentes DESC;
        """
        result = db.execute_query(query_categoria)
        print(result.to_string(index=False))
        
        # 3. Regime de trabalho
        print("\n💼 Regime de trabalho:")
        query_regime = """
        SELECT 
            regime_trabalho,
            COUNT(*) as qtd_docentes
        FROM dim_docente 
        WHERE docente_sk > 0
        GROUP BY regime_trabalho
        ORDER BY qtd_docentes DESC;
        """
        result = db.execute_query(query_regime)
        print(result.to_string(index=False))
        
        # 4. Doutores
        print("\n🎓 Docentes doutores:")
        query_doutor = """
        SELECT 
            eh_doutor,
            COUNT(*) as qtd_docentes
        FROM dim_docente 
        WHERE docente_sk > 0
        GROUP BY eh_doutor
        ORDER BY qtd_docentes DESC;
        """
        result = db.execute_query(query_doutor)
        print(result.to_string(index=False))
        
        # 5. Faixa etária
        print("\n📅 Faixa etária:")
        query_idade = """
        SELECT 
            faixa_etaria,
            COUNT(*) as qtd_docentes
        FROM dim_docente 
        WHERE docente_sk > 0
        GROUP BY faixa_etaria
        ORDER BY qtd_docentes DESC
        LIMIT 10;
        """
        result = db.execute_query(query_idade)
        print(result.to_string(index=False))
        
        print("\n✅ Validação concluída!")
        
    except Exception as e:
        logger.error(f"❌ Erro na validação: {str(e)}")

def main():
    """Função principal."""
    try:
        logger.info("🚀 Iniciando criação da dimensão DOCENTE")
        
        # 1. Criar dimensão
        if not criar_dimensao_docente():
            logger.error("❌ Falha na criação da dimensão docente")
            return
            
        # 2. Validar dimensão
        validar_dimensao_docente()
        
        print("\n" + "="*70)
        print("🎉 DIMENSÃO DOCENTE CRIADA COM SUCESSO!")
        print("="*70)
        print("✅ Tabela: dim_docente")
        print("✅ Fonte: raw_docente")
        print("✅ Índices: Performance otimizada")
        print("✅ Dados: Tratados e normalizados")
        print("="*70)
        
    except Exception as e:
        logger.error(f"❌ Erro durante criação da dimensão DOCENTE: {str(e)}")

if __name__ == "__main__":
    main()
