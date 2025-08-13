#!/usr/bin/env python3
"""
Populador da Dimensão PPG (Programas de Pós-Graduação)
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from src.core.core import get_db_manager, get_capes_api, Config, log_execution
import logging

logger = logging.getLogger(__name__)

@log_execution
def extrair_dados_ppg_api():
    """
    Extrai dados dos PPGs da API CAPES.
    """
    logger.info("🏛️ Extraindo dados dos PPGs da API CAPES...")
    
    try:
        api = get_capes_api()
        config = Config()
        
        # Buscar dados de PPG
        resource_id = config.RESOURCE_IDS.get('ppg', '21be9dd6-d4fa-470e-a5b9-b59c20879f10')
        
        df_raw = api.fetch_all_data(resource_id)
        
        if df_raw.empty:
            logger.error("❌ Nenhum dado de PPG encontrado na API")
            return pd.DataFrame()
        
        logger.info(f"✅ Dados dos PPGs extraídos: {len(df_raw)} registros")
        return df_raw
        
    except Exception as e:
        logger.error(f"❌ Erro ao extrair dados da API: {e}")
        return pd.DataFrame()

@log_execution
def processar_dados_ppg(df_raw):
    """
    Processa e limpa os dados dos PPGs.
    """
    logger.info("🔄 Processando dados dos PPGs...")
    
    try:
        if df_raw.empty:
            logger.error("❌ DataFrame vazio para processamento")
            return pd.DataFrame()
        
        # Mapear colunas baseado nos dados reais da API
        colunas_mapeamento = {
            'codigo_programa': 'CD_PROGRAMA_IES',
            'nome_programa': 'NM_PROGRAMA_IES', 
            'nivel_programa': 'CD_NIVEL_PROGRAMA',
            'ies_vinculada': 'NM_ENTIDADE_ENSINO',
            'codigo_ies': 'CD_ENTIDADE_ENSINO',
            'sigla_ies': 'SG_ENTIDADE_ENSINO',
            'uf': 'SG_UF_IES',
            'regiao': 'NM_REGIAO',
            'area_conhecimento': 'NM_AREA_CONHECIMENTO',
            'area_avaliacao': 'NM_AREA_AVALIACAO',
            'modalidade': 'NM_MODALIDADE_PROGRAMA',
            'nota_capes': 'NR_NOTA_PROGRAMA',
            'situacao': 'DS_SITUACAO_PROGRAMA',
            'ano_base': 'AN_BASE'
        }
        
        # Verificar quais colunas existem no DataFrame
        colunas_existentes = {}
        for col_nova, col_original in colunas_mapeamento.items():
            colunas_disponiveis = [c for c in df_raw.columns if col_original.upper() in c.upper()]
            if colunas_disponiveis:
                colunas_existentes[col_nova] = colunas_disponiveis[0]
            else:
                logger.warning(f"⚠️ Coluna {col_original} não encontrada")
        
        # Criar DataFrame processado
        df_processado = pd.DataFrame()
        
        for col_nova, col_original in colunas_existentes.items():
            df_processado[col_nova] = df_raw[col_original]
        
        # Tratar valores nulos e padronizar
        df_processado = df_processado.fillna('Desconhecido')
        
        # Normalizar strings
        string_cols = ['nome_programa', 'ies_vinculada', 'sigla_ies', 'uf', 'regiao', 
                      'area_conhecimento', 'area_avaliacao', 'modalidade', 'situacao']
        
        for col in string_cols:
            if col in df_processado.columns:
                df_processado[col] = df_processado[col].astype(str).str.strip().str.upper()
        
        # Padronizar região
        if 'regiao' in df_processado.columns:
            regiao_map = {
                'NORTE': 'Norte',
                'NORDESTE': 'Nordeste', 
                'CENTRO-OESTE': 'Centro-Oeste',
                'SUDESTE': 'Sudeste',
                'SUL': 'Sul'
            }
            df_processado['regiao'] = df_processado['regiao'].map(regiao_map).fillna('Desconhecido')
        
        # Tratar ano base
        if 'ano_base' in df_processado.columns:
            df_processado['ano_base'] = pd.to_numeric(df_processado['ano_base'], errors='coerce').fillna(2024).astype(int)
        else:
            df_processado['ano_base'] = 2024
        
        # Tratar nota CAPES
        if 'nota_capes' in df_processado.columns:
            df_processado['nota_capes'] = pd.to_numeric(df_processado['nota_capes'], errors='coerce').fillna(0)
        else:
            df_processado['nota_capes'] = 0
        
        # Remover duplicatas
        colunas_duplicata = ['codigo_programa', 'nome_programa', 'ies_vinculada']
        colunas_existentes_dup = [col for col in colunas_duplicata if col in df_processado.columns]
        
        if colunas_existentes_dup:
            registros_antes = len(df_processado)
            df_processado = df_processado.drop_duplicates(subset=colunas_existentes_dup)
            registros_depois = len(df_processado)
            logger.info(f"📊 Duplicatas removidas: {registros_antes - registros_depois}")
        
        logger.info(f"✅ Dados dos PPGs processados: {len(df_processado)} registros")
        return df_processado
        
    except Exception as e:
        logger.error(f"❌ Erro ao processar dados: {e}")
        return pd.DataFrame()

@log_execution
def criar_dimensao_ppg(df_processado):
    """
    Cria a dimensão PPG com SK e estrutura adequada.
    """
    logger.info("🏗️ Criando dimensão PPG...")
    
    try:
        if df_processado.empty:
            logger.warning("⚠️ DataFrame vazio - criando apenas registro SK=0")
            df_ppg = pd.DataFrame([{
                'ppg_sk': 0,
                'codigo_programa': 'DESCONHECIDO',
                'nome_programa': 'Desconhecido',
                'nivel_programa': 'DESCONHECIDO',
                'ies_vinculada': 'Desconhecido',
                'codigo_ies': 'DESCONHECIDO',
                'sigla_ies': 'DESCONHECIDO',
                'uf': 'DESCONHECIDO',
                'regiao': 'Desconhecido',
                'area_conhecimento': 'Desconhecido',
                'area_avaliacao': 'Desconhecido',
                'modalidade': 'Desconhecido',
                'nota_capes': 0.0,
                'situacao': 'DESCONHECIDO',
                'ano_base': 2024
            }])
        else:
            # Criar registro SK=0 (desconhecido)
            registro_desconhecido = {
                'ppg_sk': 0,
                'codigo_programa': 'DESCONHECIDO',
                'nome_programa': 'Desconhecido',
                'nivel_programa': 'DESCONHECIDO',
                'ies_vinculada': 'Desconhecido',
                'codigo_ies': 'DESCONHECIDO',
                'sigla_ies': 'DESCONHECIDO',
                'uf': 'DESCONHECIDO',
                'regiao': 'Desconhecido',
                'area_conhecimento': 'Desconhecido',
                'area_avaliacao': 'Desconhecido',
                'modalidade': 'Desconhecido',
                'nota_capes': 0.0,
                'situacao': 'DESCONHECIDO',
                'ano_base': 2024
            }
            
            # Criar dimensão com dados processados
            df_ppg = df_processado.copy()
            
            # Adicionar surrogate key sequencial
            df_ppg['ppg_sk'] = range(1, len(df_ppg) + 1)
            
            # Garantir que todas as colunas existam
            colunas_obrigatorias = [
                'codigo_programa', 'nome_programa', 'nivel_programa', 'ies_vinculada',
                'codigo_ies', 'sigla_ies', 'uf', 'regiao', 'area_conhecimento',
                'area_avaliacao', 'modalidade', 'nota_capes', 'situacao', 'ano_base'
            ]
            
            for col in colunas_obrigatorias:
                if col not in df_ppg.columns:
                    df_ppg[col] = 'Desconhecido' if col != 'nota_capes' and col != 'ano_base' else (0.0 if col == 'nota_capes' else 2024)
            
            # Adicionar registro SK=0 no início
            df_ppg = pd.concat([pd.DataFrame([registro_desconhecido]), df_ppg], ignore_index=True)
        
        # Reordenar colunas
        colunas_finais = [
            'ppg_sk', 'codigo_programa', 'nome_programa', 'nivel_programa',
            'ies_vinculada', 'codigo_ies', 'sigla_ies', 'uf', 'regiao',
            'area_conhecimento', 'area_avaliacao', 'modalidade', 
            'nota_capes', 'situacao', 'ano_base'
        ]
        
        df_ppg = df_ppg[colunas_finais]
        
        logger.info(f"✅ Dimensão PPG criada com {len(df_ppg)} registros")
        return df_ppg
        
    except Exception as e:
        logger.error(f"❌ Erro ao criar dimensão: {e}")
        return pd.DataFrame()

@log_execution
def salvar_dimensao_ppg(df_ppg):
    """
    Salva a dimensão PPG no banco de dados.
    """
    try:
        if df_ppg.empty:
            logger.error("❌ DataFrame vazio - não há dados para salvar")
            return False
            
        db = get_db_manager()
        success = db.save_dataframe(df_ppg, 'dim_ppg', if_exists='replace')
        
        if success:
            logger.info(f"✅ Dimensão PPG salva no PostgreSQL com {len(df_ppg)} registros")
            return True
        else:
            logger.error("❌ Falha ao salvar dimensão PPG")
            return False
            
    except Exception as e:
        logger.error(f"❌ Erro ao salvar dimensão: {e}")
        return False

@log_execution
def gerar_estatisticas_ppg(df_ppg):
    """
    Gera estatísticas da dimensão PPG.
    """
    if df_ppg.empty:
        logger.info("❌ Não há dados para gerar estatísticas")
        return
        
    logger.info("\n📊 Estatísticas da dimensão PPG:")
    logger.info(f"Total de PPGs: {len(df_ppg)}")
    
    # Estatísticas por região
    if 'regiao' in df_ppg.columns:
        logger.info("\nPPGs por região:")
        regiao_stats = df_ppg['regiao'].value_counts()
        for regiao, count in regiao_stats.items():
            logger.info(f"  {regiao}: {count} PPGs")
    
    # Estatísticas por UF
    if 'uf' in df_ppg.columns:
        logger.info(f"\nUFs únicas: {df_ppg['uf'].nunique()}")
    
    # Estatísticas por modalidade
    if 'modalidade' in df_ppg.columns:
        logger.info("\nPPGs por modalidade:")
        modalidade_stats = df_ppg['modalidade'].value_counts()
        for modalidade, count in modalidade_stats.items():
            logger.info(f"  {modalidade}: {count} PPGs")
    
    # Notas CAPES
    if 'nota_capes' in df_ppg.columns:
        notas_validas = df_ppg[df_ppg['nota_capes'] > 0]
        if not notas_validas.empty:
            logger.info(f"\nNota CAPES média: {notas_validas['nota_capes'].mean():.1f}")
            logger.info(f"Nota CAPES mediana: {notas_validas['nota_capes'].median():.1f}")

def main():
    """
    Função principal para execução do script.
    """
    try:
        logger.info("🚀 Iniciando processo de criação da dimensão PPG")
        logger.info("🏛️ Fonte de dados: API CAPES")
        
        # 1. Extrair dados da API
        df_raw = extrair_dados_ppg_api()
        
        # 2. Processar dados
        df_processado = processar_dados_ppg(df_raw)
        
        # 3. Criar dimensão
        df_ppg = criar_dimensao_ppg(df_processado)
        
        if df_ppg.empty:
            logger.error("❌ Falha ao criar dimensão PPG")
            return False
        
        # 4. Salvar no banco
        if not salvar_dimensao_ppg(df_ppg):
            return False
        
        # 5. Gerar estatísticas
        gerar_estatisticas_ppg(df_ppg)
        
        logger.info("✅ Processo concluído! Dimensão PPG criada com sucesso.")
        logger.info("💡 A dimensão inclui informações sobre programas de pós-graduação.")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro na execução principal: {e}")
        return False

if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    success = main()
    sys.exit(0 if success else 1)
