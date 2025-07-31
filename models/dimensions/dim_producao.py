#!/usr/bin/env python3
"""
Populador da Dimensão Produção (Produção Acadêmica)
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime

# Adicionar path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from models.utils.core import get_db_manager, get_capes_api, Config, log_execution
import logging

logger = logging.getLogger(__name__)

@log_execution
def extrair_dados_producao_api():
    """
    Extrai dados de produção acadêmica da API CAPES.
    """
    logger.info("📊 Extraindo dados de produção da API CAPES...")
    
    try:
        api = get_capes_api()
        config = Config()
        
        # Buscar dados de produção
        resource_id = config.RESOURCE_IDS.get('producoes', '7cd574be-7a3d-4750-a246-2ed0a7573073')
        
        df_raw = api.fetch_all_data(resource_id)
        
        if df_raw.empty:
            logger.error("❌ Nenhum dado de produção encontrado na API")
            return pd.DataFrame()
        
        logger.info(f"✅ Dados de produção extraídos: {len(df_raw)} registros")
        return df_raw
        
    except Exception as e:
        logger.error(f"❌ Erro ao extrair dados da API: {e}")
        return pd.DataFrame()

@log_execution
def processar_dados_producao(df_raw):
    """
    Processa e limpa os dados de produção.
    """
    logger.info("🔄 Processando dados de produção...")
    
    try:
        if df_raw.empty:
            logger.error("❌ DataFrame vazio para processamento")
            return pd.DataFrame()
        
        # Mapear colunas baseado nos dados reais da API
        colunas_mapeamento = {
            'ano_referencia': 'AN_BASE',
            'uf_sigla': 'SG_UF_IES',
            'regiao': 'NM_REGIAO',
            'codigo_ies': 'CD_IES',
            'nome_ies': 'NM_IES',
            'tipo_producao': 'DS_TIPO_PRODUCAO',
            'subtipo_producao': 'DS_SUBTIPO_PRODUCAO',
            'quantidade': 'QT_PRODUCAO',
            'area_conhecimento': 'NM_AREA_CONHECIMENTO',
            'programa': 'NM_PROGRAMA'
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
        string_cols = ['uf_sigla', 'regiao', 'nome_ies', 'tipo_producao', 
                      'subtipo_producao', 'area_conhecimento', 'programa']
        
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
        
        # Tratar valores numéricos
        numeric_cols = ['ano_referencia', 'codigo_ies', 'quantidade']
        for col in numeric_cols:
            if col in df_processado.columns:
                df_processado[col] = pd.to_numeric(df_processado[col], errors='coerce').fillna(0).astype(int)
        
        # Filtrar anos válidos
        if 'ano_referencia' in df_processado.columns:
            df_processado = df_processado[df_processado['ano_referencia'] >= 2010]
        
        # Agrupar dados por combinações relevantes para reduzir volume
        if len(df_processado) > 10000:  # Se há muitos registros, agrupar
            colunas_agrupamento = ['ano_referencia', 'uf_sigla', 'regiao', 'tipo_producao']
            colunas_agrupamento = [col for col in colunas_agrupamento if col in df_processado.columns]
            
            if colunas_agrupamento and 'quantidade' in df_processado.columns:
                df_processado = df_processado.groupby(colunas_agrupamento).agg({
                    'quantidade': 'sum',
                    'codigo_ies': 'first',
                    'nome_ies': 'first',
                    'subtipo_producao': 'first',
                    'area_conhecimento': 'first',
                    'programa': 'first'
                }).reset_index()
                
                logger.info(f"📊 Dados agrupados para {len(df_processado)} registros")
        
        logger.info(f"✅ Dados de produção processados: {len(df_processado)} registros")
        return df_processado
        
    except Exception as e:
        logger.error(f"❌ Erro ao processar dados: {e}")
        return pd.DataFrame()

@log_execution
def criar_dimensao_producao(df_processado):
    """
    Cria a dimensão Produção com SK e estrutura adequada.
    """
    logger.info("🏗️ Criando dimensão Produção...")
    
    try:
        if df_processado.empty:
            logger.warning("⚠️ DataFrame vazio - criando apenas registro SK=0")
            df_producao = pd.DataFrame([{
                'producao_sk': 0,
                'ano_referencia': 0,
                'uf_sigla': 'DESCONHECIDO',
                'regiao': 'Desconhecido',
                'codigo_ies': 0,
                'nome_ies': 'Desconhecido',
                'tipo_producao': 'DESCONHECIDO',
                'subtipo_producao': 'DESCONHECIDO',
                'quantidade': 0,
                'area_conhecimento': 'Desconhecido',
                'programa': 'Desconhecido'
            }])
        else:
            # Criar registro SK=0 (desconhecido)
            registro_desconhecido = {
                'producao_sk': 0,
                'ano_referencia': 0,
                'uf_sigla': 'DESCONHECIDO',
                'regiao': 'Desconhecido',
                'codigo_ies': 0,
                'nome_ies': 'Desconhecido',
                'tipo_producao': 'DESCONHECIDO',
                'subtipo_producao': 'DESCONHECIDO',
                'quantidade': 0,
                'area_conhecimento': 'Desconhecido',
                'programa': 'Desconhecido'
            }
            
            # Criar dimensão com dados processados
            df_producao = df_processado.copy()
            
            # Adicionar surrogate key sequencial
            df_producao['producao_sk'] = range(1, len(df_producao) + 1)
            
            # Garantir que todas as colunas existam
            colunas_obrigatorias = [
                'ano_referencia', 'uf_sigla', 'regiao', 'codigo_ies', 'nome_ies',
                'tipo_producao', 'subtipo_producao', 'quantidade', 'area_conhecimento', 'programa'
            ]
            
            for col in colunas_obrigatorias:
                if col not in df_producao.columns:
                    if col in ['ano_referencia', 'codigo_ies', 'quantidade']:
                        df_producao[col] = 0
                    else:
                        df_producao[col] = 'Desconhecido'
            
            # Adicionar registro SK=0 no início
            df_producao = pd.concat([pd.DataFrame([registro_desconhecido]), df_producao], ignore_index=True)
        
        # Reordenar colunas
        colunas_finais = [
            'producao_sk', 'ano_referencia', 'uf_sigla', 'regiao', 'codigo_ies',
            'nome_ies', 'tipo_producao', 'subtipo_producao', 'quantidade',
            'area_conhecimento', 'programa'
        ]
        
        df_producao = df_producao[colunas_finais]
        
        logger.info(f"✅ Dimensão Produção criada com {len(df_producao)} registros")
        return df_producao
        
    except Exception as e:
        logger.error(f"❌ Erro ao criar dimensão: {e}")
        return pd.DataFrame()

@log_execution
def salvar_dimensao_producao(df_producao):
    """
    Salva a dimensão Produção no banco de dados.
    """
    try:
        if df_producao.empty:
            logger.error("❌ DataFrame vazio - não há dados para salvar")
            return False
            
        db = get_db_manager()
        success = db.save_dataframe(df_producao, 'dim_producao', if_exists='replace')
        
        if success:
            logger.info(f"✅ Dimensão Produção salva no PostgreSQL com {len(df_producao)} registros")
            return True
        else:
            logger.error("❌ Falha ao salvar dimensão Produção")
            return False
            
    except Exception as e:
        logger.error(f"❌ Erro ao salvar dimensão: {e}")
        return False

@log_execution
def gerar_estatisticas_producao(df_producao):
    """
    Gera estatísticas da dimensão Produção.
    """
    if df_producao.empty:
        logger.info("❌ Não há dados para gerar estatísticas")
        return
        
    logger.info("\n📊 Estatísticas da dimensão Produção:")
    logger.info(f"Total de registros: {len(df_producao)}")
    
    # Estatísticas por região
    if 'regiao' in df_producao.columns:
        logger.info("\nProdução por região:")
        regiao_stats = df_producao['regiao'].value_counts()
        for regiao, count in regiao_stats.items():
            logger.info(f"  {regiao}: {count} registros")
    
    # Estatísticas por tipo de produção
    if 'tipo_producao' in df_producao.columns:
        logger.info("\nProdução por tipo:")
        tipo_stats = df_producao['tipo_producao'].value_counts().head(10)
        for tipo, count in tipo_stats.items():
            logger.info(f"  {tipo}: {count} registros")
    
    # Anos disponíveis
    if 'ano_referencia' in df_producao.columns:
        anos_validos = df_producao[df_producao['ano_referencia'] > 0]
        if not anos_validos.empty:
            logger.info(f"\nAnos disponíveis: {anos_validos['ano_referencia'].min()}-{anos_validos['ano_referencia'].max()}")
    
    # Quantidade total
    if 'quantidade' in df_producao.columns:
        total_producao = df_producao[df_producao['producao_sk'] != 0]['quantidade'].sum()
        logger.info(f"Total de produções: {total_producao:,}")

def main():
    """
    Função principal para execução do script.
    """
    try:
        logger.info("🚀 Iniciando processo de criação da dimensão Produção")
        logger.info("📊 Fonte de dados: API CAPES")
        
        # 1. Extrair dados da API
        df_raw = extrair_dados_producao_api()
        
        # 2. Processar dados
        df_processado = processar_dados_producao(df_raw)
        
        # 3. Criar dimensão
        df_producao = criar_dimensao_producao(df_processado)
        
        if df_producao.empty:
            logger.error("❌ Falha ao criar dimensão Produção")
            return False
        
        # 4. Salvar no banco
        if not salvar_dimensao_producao(df_producao):
            return False
        
        # 5. Gerar estatísticas
        gerar_estatisticas_producao(df_producao)
        
        logger.info("✅ Processo concluído! Dimensão Produção criada com sucesso.")
        logger.info("💡 A dimensão inclui informações sobre produção acadêmica.")
        
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
