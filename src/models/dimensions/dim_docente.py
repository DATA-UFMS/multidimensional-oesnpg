#!/usr/bin/env python3
"""
DIMENSÃO DOCENTE - Data Warehouse Observatório CAPES
===============================================
Cria a dimensão dim_docente baseada nos dados da raw_docente
associados com raw_pq para enriquecimento de bolsistas PQ.
Estrutura: d        # Estatísticas finais
        total_matches = match1_count + match2_count + match3_count
        taxa_matching = (total_matches / len(df_docentes_clean)) * 100
        
        logger.info(f"Resultado final do matching multi-critério:")
        logger.info(f"   • Total docentes: {len(df_docentes_clean):,}")
        logger.info(f"   • Total matches: {total_matches:,} ({taxa_matching:.1f}%)")
        logger.info(f"   • Matches precisos (Nome+UF+Cidade): {match1_count:,}")
        logger.info(f"   • Matches intermediários (Nome+UF): {match2_count:,}")
        logger.info(f"   • Matches básicos (Nome): {match3_count:,}") informações pessoais, titulação, vinculação, bolsa PQ
Data: 21/08/2025
"""

import pandas as pd
import numpy as np
import os
import sys
from dotenv import load_dotenv
import logging
import unicodedata
import re

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

def normalize_name(name):
    """Normaliza nomes para comparação"""
    if pd.isna(name):
        return ""
    
    # Remove acentos
    name = unicodedata.normalize('NFKD', str(name))
    name = ''.join(c for c in name if not unicodedata.combining(c))
    
    # Limpa e normaliza
    name = re.sub(r'[^\w\s]', ' ', name)  # Remove pontuação
    name = re.sub(r'\s+', ' ', name.strip())  # Normaliza espaços
    return name.upper()

def carregar_dados_raw_docente():
    """
    Carrega os dados da tabela raw_docente para DataFrame.
    """
    logger.info("Carregando dados da raw_docente...")
    db = get_db_manager()
    
    try:
        query = """
        SELECT 
            id_pessoa_original as id_pessoa,
            des_docente,
            tp_documento_docente,
            nr_documento_docente,
            an_nascimento_docente,
            des_faixa_etaria,
            des_tipo_nacionalidade_docente,
            des_pais_nacionalidade_docente,
            des_categoria_docente,
            des_tipo_vinculo_docente_ies,
            des_regime_trabalho,
            cod_cat_bolsa_produtividade,
            in_doutor,
            an_titulacao,
            des_grau_titulacao,
            cod_area_basica_titulacao,
            des_area_basica_titulacao,
            sg_ies_titulacao,
            des_ies_titulacao,
            des_pais_ies_titulacao,
            des_entidade_ensino,
            des_municipio_programa_ies,
            sg_uf_programa,
            des_regiao,
            des_grande_area_conhecimento,
            an_base
        FROM raw_docente
        ORDER BY id_pessoa_original;
        """
        
        df = db.execute_query(query)
        logger.info(f"✅ Carregados {len(df):,} registros da raw_docente")
        return df
        
    except Exception as e:
        logger.error(f"❌ Erro ao carregar dados: {str(e)}")
        return None

def carregar_dados_raw_pq():
    """
    Carrega os dados da tabela raw_pq para DataFrame.
    """
    logger.info("Carregando dados da raw_pq...")
    db = get_db_manager()
    
    try:
        query = """
        SELECT 
            des_beneficiario,
            id_lattes,
            des_grande_area,
            des_area,
            des_subarea,
            cod_modalidade,
            cod_categoria_nivel,
            des_instituto,
            des_uf,
            des_cidade,
            des_regiao,
            data_inicio_processo,
            data_termino_processo
        FROM raw_pq
        ORDER BY des_beneficiario;
        """
        
        df = db.execute_query(query)
        logger.info(f"✅ Carregados {len(df):,} registros da raw_pq")
        return df
        
    except Exception as e:
        logger.error(f"❌ Erro ao carregar dados da raw_pq: {str(e)}")
        return None

def associar_docentes_com_pq(df_docentes, df_pq):
    """
    Associa docentes com bolsistas PQ usando estratégia multi-critério:
    1. Nome + UF + Cidade (mais preciso)
    2. Nome + UF (intermediário) 
    3. Apenas Nome (menos preciso, para casos restantes)
    """
    logger.info("Associando docentes com bolsistas PQ (estratégia multi-critério)...")
    
    try:
        # Normalizar nomes para match
        df_docentes['nome_normalizado'] = df_docentes['des_docente'].apply(normalize_name)
        df_pq['nome_normalizado'] = df_pq['des_beneficiario'].apply(normalize_name)
        
        # Normalizar campos de localização
        df_docentes['uf_normalizada'] = df_docentes['sg_uf_programa'].astype(str).str.upper().str.strip()
        df_pq['uf_normalizada'] = df_pq['des_uf'].astype(str).str.upper().str.strip()
        
        df_docentes['cidade_normalizada'] = df_docentes['des_municipio_programa_ies'].astype(str).str.upper().str.strip()
        df_pq['cidade_normalizada'] = df_pq['des_cidade'].astype(str).str.upper().str.strip()
        
        # Remover registros com nomes vazios
        df_docentes_clean = df_docentes[df_docentes['nome_normalizado'] != ''].copy()
        df_pq_clean = df_pq[df_pq['nome_normalizado'] != ''].copy()
        
        logger.info(f"Docentes para matching: {len(df_docentes_clean):,}")
        logger.info(f"Bolsistas PQ para matching: {len(df_pq_clean):,}")
        
        # ESTRATÉGIA 1: Nome + UF + Cidade (mais restritivo)
        logger.info("Estratégia 1: Nome + UF + Cidade...")
        df_match1 = df_docentes_clean.merge(
            df_pq_clean,
            left_on=['nome_normalizado', 'uf_normalizada', 'cidade_normalizada'],
            right_on=['nome_normalizado', 'uf_normalizada', 'cidade_normalizada'],
            how='inner',
            suffixes=('', '_pq')
        )
        match1_count = len(df_match1)
        logger.info(f"   ✅ Matches Nome+UF+Cidade: {match1_count:,}")
        
        # ESTRATÉGIA 2: Nome + UF (para não encontrados na estratégia 1)
        logger.info("Estratégia 2: Nome + UF...")
        docentes_restantes = df_docentes_clean[~df_docentes_clean.index.isin(df_match1.index)]
        pq_restantes = df_pq_clean[~df_pq_clean['nome_normalizado'].isin(df_match1['nome_normalizado'])]
        
        df_match2 = docentes_restantes.merge(
            pq_restantes,
            left_on=['nome_normalizado', 'uf_normalizada'],
            right_on=['nome_normalizado', 'uf_normalizada'],
            how='inner',
            suffixes=('', '_pq')
        )
        match2_count = len(df_match2)
        logger.info(f"   ✅ Matches Nome+UF: {match2_count:,}")
        
        # ESTRATÉGIA 3: Apenas Nome (para ainda restantes)
        logger.info("Estratégia 3: Apenas Nome...")
        docentes_ainda_restantes = df_docentes_clean[
            ~df_docentes_clean.index.isin(df_match1.index.union(df_match2.index))
        ]
        pq_ainda_restantes = df_pq_clean[
            ~df_pq_clean['nome_normalizado'].isin(
                df_match1['nome_normalizado'].tolist() + df_match2['nome_normalizado'].tolist()
            )
        ]
        
        df_match3 = docentes_ainda_restantes.merge(
            pq_ainda_restantes,
            on='nome_normalizado',
            how='inner',
            suffixes=('', '_pq')
        )
        match3_count = len(df_match3)
        logger.info(f"   ✅ Matches apenas Nome: {match3_count:,}")
        
        # Combinar todos os matches
        df_com_pq = pd.concat([df_match1, df_match2, df_match3], ignore_index=True)
        
        # Docentes sem match (sem bolsa PQ)
        df_sem_pq = df_docentes_clean[
            ~df_docentes_clean.index.isin(
                df_match1.index.union(df_match2.index).union(df_match3.index)
            )
        ].copy()
        
        # Adicionar colunas PQ com valores padrão para não-bolsistas
        pq_columns = ['des_beneficiario', 'id_lattes', 'des_grande_area', 'des_area', 
                     'des_subarea', 'cod_modalidade', 'cod_categoria_nivel', 'des_instituto',
                     'des_uf', 'des_regiao', 'data_inicio_processo', 'data_termino_processo']
        
        for col in pq_columns:
            if col not in df_sem_pq.columns:
                if 'data_' in col:
                    df_sem_pq[col] = pd.NaT
                else:
                    df_sem_pq[col] = 'NÃO INFORMADO'
        
        # Garantir que df_com_pq tenha as mesmas colunas
        for col in df_sem_pq.columns:
            if col not in df_com_pq.columns:
                df_com_pq[col] = 'NÃO INFORMADO'
        
        # Combinar docentes com e sem PQ
        df_final = pd.concat([df_com_pq, df_sem_pq], ignore_index=True)
        
        # Preencher valores vazios nos matches
        fill_values = {
            'des_beneficiario': 'NÃO BOLSISTA PQ',
            'id_lattes': 'NÃO INFORMADO',
            'des_grande_area': 'NÃO INFORMADO',
            'des_area': 'NÃO INFORMADO', 
            'des_subarea': 'NÃO INFORMADO',
            'cod_modalidade': 'NÃO INFORMADO',
            'cod_categoria_nivel': 'NÃO INFORMADO',
            'des_instituto': 'NÃO INFORMADO',
            'des_uf': 'NÃO INFORMADO',
            'des_regiao': 'NÃO INFORMADO'
        }
        
        for col, default_val in fill_values.items():
            if col in df_final.columns:
                df_final[col] = df_final[col].fillna(default_val)
        
        # Adicionar flag de bolsista PQ
        df_final['bl_bolsista_pq'] = np.where(
            df_final['des_beneficiario'] != 'NÃO BOLSISTA PQ',
            'Sim',
            'Não'
        )
        
        # Estatísticas finais
        total_matches = match1_count + match2_count + match3_count
        taxa_matching = (total_matches / len(df_docentes_clean)) * 100
        
        logger.info(f"� Resultado final do matching multi-critério:")
        logger.info(f"   • Total docentes: {len(df_docentes_clean):,}")
        logger.info(f"   • Total matches: {total_matches:,} ({taxa_matching:.1f}%)")
        logger.info(f"   • Matches precisos (Nome+UF+Cidade): {match1_count:,}")
        logger.info(f"   • Matches intermediários (Nome+UF): {match2_count:,}")
        logger.info(f"   • Matches básicos (Nome): {match3_count:,}")
        
        # Remover colunas temporárias
        temp_cols = ['nome_normalizado', 'uf_normalizada', 'cidade_normalizada']
        df_final.drop(temp_cols, axis=1, inplace=True, errors='ignore')
        
        return df_final
        
    except Exception as e:
        logger.error(f"❌ Erro na associação: {str(e)}")
        return df_docentes  # Retorna original se falhar

def processar_dataframe_docente(df):
    """
    Processa o DataFrame dos docentes aplicando transformações e limpezas.
    """
    logger.info("Processando DataFrame dos docentes...")
    
    try:
        # Fazer cópia para não alterar o original
        df_processed = df.copy()
        
        # 1. Remover duplicatas baseado no id_pessoa
        logger.info(f"Registros antes da remoção de duplicatas: {len(df_processed):,}")
        df_processed = df_processed.drop_duplicates(subset=['id_pessoa'], keep='first')
        logger.info(f"Registros após remoção de duplicatas: {len(df_processed):,}")
        
        # 2. Limpar e padronizar campos de texto
        logger.info("Limpando campos de texto...")
        
        # Função para limpar texto
        def limpar_texto(valor):
            if pd.isna(valor) or str(valor).strip() == '' or str(valor).upper() == 'NAN':
                return 'DESCONHECIDO'
            return str(valor).strip().upper()
        
        # Aplicar limpeza nos campos de texto
        campos_texto = [
            'des_docente', 'tp_documento_docente', 'des_faixa_etaria',
            'des_tipo_nacionalidade_docente', 'des_pais_nacionalidade_docente',
            'des_categoria_docente', 'des_tipo_vinculo_docente_ies', 'des_regime_trabalho',
            'des_grau_titulacao', 'des_area_basica_titulacao', 'sg_ies_titulacao',
            'des_ies_titulacao', 'des_pais_ies_titulacao', 'des_entidade_ensino',
            'sg_uf_programa', 'des_grande_area_conhecimento',
            # Campos PQ
            'des_beneficiario', 'id_lattes', 'des_grande_area', 'des_area',
            'des_subarea', 'cod_modalidade', 'cod_categoria_nivel', 'des_instituto',
            'des_uf', 'des_regiao'
        ]
        
        for campo in campos_texto:
            if campo in df_processed.columns:
                df_processed[campo] = df_processed[campo].apply(limpar_texto)
        
        # 3. Tratar campo de bolsa produtividade (tanto da raw_docente quanto da raw_pq)
        def tratar_bolsa_produtividade(valor):
            if pd.isna(valor) or str(valor).upper() in ['NAN', '', 'NÃO INFORMADO']:
                return 'NÃO INFORMADO'
            return str(valor).strip().upper()
        
        if 'cod_cat_bolsa_produtividade' in df_processed.columns:
            df_processed['cod_cat_bolsa_produtividade'] = df_processed['cod_cat_bolsa_produtividade'].apply(tratar_bolsa_produtividade)
        if 'cod_categoria_nivel' in df_processed.columns:
            df_processed['cod_categoria_nivel'] = df_processed['cod_categoria_nivel'].apply(tratar_bolsa_produtividade)
        
        # 4. Tratar campo bl_doutor
        def tratar_bl_doutor(valor):
            if pd.isna(valor):
                return 'DESCONHECIDO'
            valor_str = str(valor).strip().upper()
            if valor_str == 'S':
                return 'Sim'
            elif valor_str == 'N':
                return 'Não'
            else:
                return 'DESCONHECIDO'
        
        df_processed['in_doutor'] = df_processed['in_doutor'].apply(tratar_bl_doutor)
        
        # Tratar campos numéricos
        campos_numericos = [
            'id_pessoa', 'an_nascimento_docente', 'an_titulacao',
            'cod_area_basica_titulacao', 'an_base'
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
        
        # 8. Criar DataFrame final com as colunas da dimensão (incluindo campos PQ)
        df_final = pd.DataFrame({
            'id_pessoa': df_processed['id_pessoa'],
            'nome_docente': df_processed['des_docente'],
            'tipo_documento': df_processed['tp_documento_docente'],
            'numero_documento': df_processed['numero_documento_anonimo'],
            'ano_nascimento': df_processed['an_nascimento_docente'],
            'faixa_etaria': df_processed['des_faixa_etaria'],
            'nacionalidade': df_processed['des_tipo_nacionalidade_docente'],
            'pais_nacionalidade': df_processed['des_pais_nacionalidade_docente'],
            'categoria_docente': df_processed['des_categoria_docente'],
            'tipo_vinculo': df_processed['des_tipo_vinculo_docente_ies'],
            'regime_trabalho': df_processed['des_regime_trabalho'],
            'categoria_bolsa_produtividade': df_processed.get('cod_cat_bolsa_produtividade', 'NÃO INFORMADO'),
            'bl_doutor': df_processed['in_doutor'],
            'ano_titulacao': df_processed['an_titulacao'],
            'grau_titulacao': df_processed['des_grau_titulacao'],
            'codigo_area_titulacao': df_processed['cod_area_basica_titulacao'],
            'area_titulacao': df_processed['des_area_basica_titulacao'],
            'sigla_ies_titulacao': df_processed['sg_ies_titulacao'],
            'nome_ies_titulacao': df_processed['des_ies_titulacao'],
            'pais_ies_titulacao': df_processed['des_pais_ies_titulacao'],
            'instituicao_atual': df_processed.get('des_entidade_ensino', 'NÃO INFORMADO'),
            'uf_programa': df_processed.get('sg_uf_programa', 'NÃO INFORMADO'),
            'grande_area_conhecimento': df_processed.get('des_grande_area_conhecimento', 'NÃO INFORMADO'),
            'ano_base': df_processed['an_base'],
            'idade_aproximada': df_processed['idade_aproximada'],
            'tempo_titulacao': df_processed['tempo_titulacao'],
            # Campos PQ
            'bl_bolsista_pq': df_processed.get('bl_bolsista_pq', 'Não'),
            'id_lattes': df_processed.get('id_lattes', 'NÃO INFORMADO'),
            'grande_area_pq': df_processed.get('des_grande_area', 'NÃO INFORMADO'),
            'area_pq': df_processed.get('des_area', 'NÃO INFORMADO'),
            'subarea_pq': df_processed.get('des_subarea', 'NÃO INFORMADO'),
            'modalidade_pq': df_processed.get('cod_modalidade', 'NÃO INFORMADO'),
            'categoria_pq': df_processed.get('cod_categoria_nivel', 'NÃO INFORMADO'),
            'instituto_pq': df_processed.get('des_instituto', 'NÃO INFORMADO'),
            'uf_pq': df_processed.get('des_uf', 'NÃO INFORMADO'),
            'regiao_pq': df_processed.get('des_regiao', 'NÃO INFORMADO'),
            'inicio_bolsa_pq': df_processed.get('data_inicio_processo', pd.NaT),
            'termino_bolsa_pq': df_processed.get('data_termino_processo', pd.NaT)
        })
        
        # Contar bolsistas PQ
        bolsistas_count = len(df_final[df_final['bl_bolsista_pq'] == 'Sim'])
        
        logger.info(f"✅ DataFrame processado: {len(df_final):,} registros")
        logger.info(f"Bolsistas PQ identificados: {bolsistas_count:,} ({bolsistas_count/len(df_final)*100:.1f}%)")
        logger.info(f"Colunas: {len(df_final.columns)} (incluindo campos PQ)")
        
        # Estatísticas rápidas
        logger.info("Estatísticas:")
        logger.info(f"  • Doutores: {len(df_final[df_final['bl_doutor'] == 'Sim']):,}")
        logger.info(f"  • Permanentes: {len(df_final[df_final['categoria_docente'] == 'PERMANENTE']):,}")
        logger.info(f"  • Bolsistas PQ: {bolsistas_count:,}")
        logger.info(f"  • Idade média: {df_final[df_final['idade_aproximada'] > 0]['idade_aproximada'].mean():.1f} anos")
        
        return df_final
        
    except Exception as e:
        logger.error(f"❌ Erro ao processar DataFrame: {str(e)}")
        return None

def criar_dimensao_docente():
    """
    Cria a dimensão docente associando dados da raw_docente com raw_pq.
    """
    logger.info("Criando dimensão DOCENTE com associação PQ...")
    db = get_db_manager()
    
    try:
        # 1. Remover tabela existente se houver
        logger.info("Removendo dim_docente existente...")
        drop_sql = "DROP TABLE IF EXISTS dim_docente CASCADE;"
        db.execute_sql(drop_sql)
        
        # 2. Criar tabela dim_docente expandida (com campos PQ)
        logger.info("Criando nova estrutura dim_docente...")
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
            bl_doutor VARCHAR(20),
            ano_titulacao INTEGER,
            grau_titulacao VARCHAR(50),
            codigo_area_titulacao INTEGER,
            area_titulacao VARCHAR(200),
            sigla_ies_titulacao VARCHAR(20),
            nome_ies_titulacao VARCHAR(500),
            pais_ies_titulacao VARCHAR(100),
            instituicao_atual VARCHAR(500),
            uf_programa VARCHAR(5),
            grande_area_conhecimento VARCHAR(200),
            ano_base INTEGER,
            idade_aproximada INTEGER,
            tempo_titulacao INTEGER,
            -- Campos PQ
            bl_bolsista_pq VARCHAR(10),
            id_lattes VARCHAR(50),
            grande_area_pq VARCHAR(200),
            area_pq VARCHAR(200),
            subarea_pq VARCHAR(200),
            modalidade_pq VARCHAR(20),
            categoria_pq VARCHAR(20),
            instituto_pq VARCHAR(500),
            uf_pq VARCHAR(50),
            regiao_pq VARCHAR(50),
            inicio_bolsa_pq DATE,
            termino_bolsa_pq DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        if not db.execute_sql(create_sql):
            logger.error("❌ Erro ao criar tabela dim_docente")
            return False
            
        # 3. Inserir registro SK=0 (desconhecido)
        logger.info("Inserindo registro DESCONHECIDO (SK=0)...")
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
            'bl_doutor': ['NÃO INFORMADO'],
            'ano_titulacao': [0],
            'grau_titulacao': ['NÃO INFORMADO'],
            'codigo_area_titulacao': [0],
            'area_titulacao': ['NÃO INFORMADO'],
            'sigla_ies_titulacao': ['NÃO INFORMADO'],
            'nome_ies_titulacao': ['NÃO INFORMADO'],
            'pais_ies_titulacao': ['NÃO INFORMADO'],
            'instituicao_atual': ['NÃO INFORMADO'],
            'uf_programa': ['XX'],
            'grande_area_conhecimento': ['NÃO INFORMADO'],
            'ano_base': [0],
            'idade_aproximada': [0],
            'tempo_titulacao': [0],
            'bl_bolsista_pq': ['Não'],
            'id_lattes': ['NÃO INFORMADO'],
            'grande_area_pq': ['NÃO INFORMADO'],
            'area_pq': ['NÃO INFORMADO'],
            'subarea_pq': ['NÃO INFORMADO'],
            'modalidade_pq': ['NÃO INFORMADO'],
            'categoria_pq': ['NÃO INFORMADO'],
            'instituto_pq': ['NÃO INFORMADO'],
            'uf_pq': ['XX'],
            'regiao_pq': ['NÃO INFORMADO'],
            'inicio_bolsa_pq': [pd.NaT],
            'termino_bolsa_pq': [pd.NaT]
        })
        
        # Inserir registro SK=0
        db.save_dataframe(sk0_data, 'dim_docente', if_exists='append')
        
        # 4. Carregar dados das tabelas raw
        df_docentes = carregar_dados_raw_docente()
        if df_docentes is None:
            logger.error("❌ Falha ao carregar dados da raw_docente")
            return False
            
        df_pq = carregar_dados_raw_pq()
        if df_pq is None:
            logger.error("❌ Falha ao carregar dados da raw_pq")
            return False
        
        # 5. Associar docentes com bolsistas PQ
        df_associado = associar_docentes_com_pq(df_docentes, df_pq)
        
        # 6. Processar dados associados
        df_processado = processar_dataframe_docente(df_associado)
        if df_processado is None:
            logger.error("❌ Falha ao processar DataFrame dos docentes")
            return False
            
        # 7. Inserir dados processados no banco em lotes menores
        batch_size = 100  # Reduzir tamanho do lote ainda mais para evitar problemas
        total_registros = len(df_processado)
        logger.info(f"Inserindo {total_registros:,} docentes na dimensão em lotes de {batch_size:,}...")
        
        sucesso_total = 0
        for i in range(0, total_registros, batch_size):
            batch = df_processado.iloc[i:i+batch_size]
            logger.info(f"Processando lote {i//batch_size + 1}/{(total_registros-1)//batch_size + 1} ({len(batch):,} registros)")
            
            if db.save_dataframe(batch, 'dim_docente', if_exists='append'):
                sucesso_total += len(batch)
            else:
                logger.error(f"❌ Erro ao inserir lote {i//batch_size + 1}")
                return False
        
        logger.info(f"✅ {sucesso_total:,} docentes inseridos com sucesso")
        
        # 8. Verificar inserção
        count_query = "SELECT COUNT(*) as total FROM dim_docente;"
        result = db.execute_query(count_query)
        total = result.iloc[0]['total']
        
        logger.info(f"✅ dim_docente criada com {total:,} registros")
        
        # 9. Criar índices para performance
        logger.info("Criando índices...")
        indices_sql = [
            "CREATE INDEX IF NOT EXISTS idx_dim_docente_id_pessoa ON dim_docente(id_pessoa);",
            "CREATE INDEX IF NOT EXISTS idx_dim_docente_categoria ON dim_docente(categoria_docente);",
            "CREATE INDEX IF NOT EXISTS idx_dim_docente_titulacao ON dim_docente(grau_titulacao);",
            "CREATE INDEX IF NOT EXISTS idx_dim_docente_doutor ON dim_docente(bl_doutor);",
            "CREATE INDEX IF NOT EXISTS idx_dim_docente_bolsista_pq ON dim_docente(bl_bolsista_pq);",
            "CREATE INDEX IF NOT EXISTS idx_dim_docente_categoria_pq ON dim_docente(categoria_pq);",
            "CREATE INDEX IF NOT EXISTS idx_dim_docente_uf ON dim_docente(uf_programa);"
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
    logger.info("Validando dimensão DOCENTE...")
    db = get_db_manager()
    
    try:
        print("\n" + "="*60)
        print("VALIDAÇÃO DA DIMENSÃO DOCENTE")
        print("="*60)
        
        # 1. Contagem total
        query_total = "SELECT COUNT(*) as total FROM dim_docente;"
        result = db.execute_query(query_total)
        total = result.iloc[0]['total']
        print(f"Total de registros: {total:,}")
        
        # 2. Docentes por categoria
        print("\nDocentes por categoria:")
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
        print("\nRegime de trabalho:")
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
        print("\nDocentes doutores:")
        query_doutor = """
        SELECT 
            bl_doutor,
            COUNT(*) as qtd_docentes
        FROM dim_docente 
        WHERE docente_sk > 0
        GROUP BY bl_doutor
        ORDER BY qtd_docentes DESC;
        """
        result = db.execute_query(query_doutor)
        print(result.to_string(index=False))
        
        # 5. Faixa etária
        print("\nFaixa etária:")
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
        logger.info("Iniciando criação da dimensão DOCENTE")
        
        # 1. Criar dimensão
        if not criar_dimensao_docente():
            logger.error("❌ Falha na criação da dimensão docente")
            return
            
        # 2. Validar dimensão
        validar_dimensao_docente()
        
        print("\n" + "="*70)
        print("DIMENSÃO DOCENTE CRIADA COM SUCESSO!")
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
