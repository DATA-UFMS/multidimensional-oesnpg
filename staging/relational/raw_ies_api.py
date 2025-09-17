#!/usr/bin/env python3
"""
Módulo para extração de dados de IES (Instituições de Ensino Superior) 
via API da CAPES - Dados Abertos
Prioriza uso de APIs REST ao invés de downloads de CSV
"""

import os
import sys
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging
import time
from typing import Dict, List, Optional
import json

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dw_etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CAPESAPIClient:
    """Cliente para interação com APIs da CAPES"""
    
    def __init__(self):
        self.base_url = "https://dadosabertos.capes.gov.br"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'UFMS-Research-Bot/1.0',
            'Accept': 'application/json,text/csv,application/xml'
        })
        
    def get_dataset_info(self, dataset_id: str) -> Optional[Dict]:
        """Obtém informações sobre um dataset específico"""
        try:
            url = f"{self.base_url}/api/3/action/package_show"
            params = {'id': dataset_id}
            
            logger.info(f"🔍 Consultando dataset: {dataset_id}")
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get('success'):
                return data.get('result')
            else:
                logger.error(f"❌ Erro na API: {data.get('error')}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Erro ao consultar dataset {dataset_id}: {str(e)}")
            return None
    
    def list_datasets(self, search_term: str = "programa") -> List[Dict]:
        """Lista datasets relacionados a um termo de busca"""
        try:
            url = f"{self.base_url}/api/3/action/package_search"
            params = {
                'q': search_term,
                'rows': 100,
                'sort': 'metadata_modified desc'
            }
            
            logger.info(f"🔍 Buscando datasets com termo: '{search_term}'")
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get('success'):
                results = data.get('result', {})
                datasets = results.get('results', [])
                logger.info(f"✅ Encontrados {len(datasets)} datasets")
                return datasets
            else:
                logger.error(f"❌ Erro na busca: {data.get('error')}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Erro ao listar datasets: {str(e)}")
            return []
    
    def get_resource_data_via_api(self, resource_id: str) -> Optional[pd.DataFrame]:
        """Obtém dados de um resource via API CKAN (não CSV)"""
        try:
            # Usar endpoint da API CKAN para buscar dados
            url = f"{self.base_url}/api/3/action/datastore_search"
            params = {
                'resource_id': resource_id,
                'limit': 100000  # Limite alto para obter todos os dados
            }
            
            logger.info(f"📡 Consultando dados via API CKAN: {resource_id}")
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get('success'):
                result = data.get('result', {})
                records = result.get('records', [])
                
                if records:
                    df = pd.DataFrame(records)
                    logger.info(f"✅ Dados obtidos via API: {len(df)} registros")
                    return df
                else:
                    logger.warning("⚠️ Nenhum registro encontrado via API CKAN")
                    return None
            else:
                logger.error(f"❌ Erro na API CKAN: {data.get('error')}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Erro ao consultar API CKAN para {resource_id}: {str(e)}")
            return None
    
    def search_datasets_with_api_resources(self, search_term: str = "programa") -> List[Dict]:
        """Busca datasets que tenham recursos disponíveis via API"""
        try:
            url = f"{self.base_url}/api/3/action/package_search"
            params = {
                'q': search_term,
                'rows': 50,
                'sort': 'metadata_modified desc',
                'fq': 'res_format:API OR res_format:JSON'  # Filtrar por recursos API/JSON
            }
            
            logger.info(f"🔍 Buscando datasets com API para: '{search_term}'")
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get('success'):
                results = data.get('result', {})
                datasets = results.get('results', [])
                
                # Filtrar datasets que realmente têm dados via API
                api_datasets = []
                for dataset in datasets:
                    resources = dataset.get('resources', [])
                    for resource in resources:
                        # Verificar se o resource tem datastore ativo
                        if resource.get('datastore_active'):
                            api_datasets.append(dataset)
                            break
                
                logger.info(f"✅ Encontrados {len(api_datasets)} datasets com API")
                return api_datasets
            else:
                logger.error(f"❌ Erro na busca: {data.get('error')}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Erro ao buscar datasets com API: {str(e)}")
            return []

def load_database_config():
    """Carrega configuração do banco de dados"""
    load_dotenv()
    
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5433'),
        'database': os.getenv('DB_NAME', 'dw_oesnpg'),
        'username': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASS', 'postgres')
    }

def extract_ies_from_programas_api(client: CAPESAPIClient, years: List[int] = [2021, 2022, 2023]) -> pd.DataFrame:
    """Extrai dados de IES a partir dos datasets de programas via API CKAN"""
    
    logger.info("🏛️ EXTRAINDO DADOS DE IES VIA API CKAN")
    logger.info("=" * 50)
    
    # Buscar datasets de programas com API disponível
    datasets = client.search_datasets_with_api_resources("programa stricto sensu")
    
    if not datasets:
        logger.error("❌ Nenhum dataset com API encontrado")
        return pd.DataFrame()
    
    all_ies_data = []
    
    # Processar cada dataset encontrado
    for dataset in datasets:
        dataset_name = dataset.get('name', '')
        dataset_title = dataset.get('title', '')
        
        # Filtrar apenas datasets de programas
        if 'programa' not in dataset_title.lower():
            continue
            
        logger.info(f"📊 Processando dataset: {dataset_title}")
        
        # Processar recursos do dataset
        resources = dataset.get('resources', [])
        
        for resource in resources:
            # Verificar se o resource tem datastore ativo (API disponível)
            if not resource.get('datastore_active', False):
                continue
                
            resource_id = resource.get('id', '')
            resource_name = resource.get('name', '')
            
            # Filtrar por anos se especificado no nome
            if years and not any(str(year) in resource_name for year in years):
                continue
                
            logger.info(f"� Consultando resource via API: {resource_name}")
            
            # Obter dados via API CKAN
            df = client.get_resource_data_via_api(resource_id)
            
            if df is not None and len(df) > 0:
                # Procurar colunas de IES (diferentes variações de nomes)
                ies_columns_variants = [
                    # Variações comuns dos nomes das colunas
                    ['NM_ENTIDADE_ENSINO', 'nm_entidade_ensino', 'nome_entidade', 'ies_nome'],
                    ['SG_ENTIDADE_ENSINO', 'sg_entidade_ensino', 'sigla_entidade', 'ies_sigla'],
                    ['CD_ENTIDADE_ENSINO', 'cd_entidade_ensino', 'codigo_entidade', 'ies_codigo'],
                    ['CD_ENTIDADE_CAPES', 'cd_entidade_capes', 'codigo_capes'],
                    ['CD_ENTIDADE_EMEC', 'cd_entidade_emec', 'codigo_emec'],
                    ['DS_DEPENDENCIA_ADMINISTRATIVA', 'ds_dependencia_administrativa', 'dependencia'],
                    ['CS_STATUS_JURIDICO', 'cs_status_juridico', 'status_juridico'],
                    ['NM_MUNICIPIO_ENTIDADE_ENSINO', 'nm_municipio_entidade_ensino', 'municipio'],
                    ['SG_UF_ENTIDADE_ENSINO', 'sg_uf_entidade_ensino', 'uf'],
                    ['NM_REGIAO_ENTIDADE_ENSINO', 'nm_regiao_entidade_ensino', 'regiao'],
                    ['AN_BASE', 'an_base', 'ano_base']
                ]
                
                # Encontrar colunas existentes
                column_mapping = {}
                for variants in ies_columns_variants:
                    for variant in variants:
                        if variant in df.columns:
                            # Usar o primeiro nome da lista como padrão
                            standard_name = variants[0]
                            column_mapping[variant] = standard_name
                            break
                
                if column_mapping:
                    # Selecionar apenas colunas de IES encontradas
                    existing_columns = list(column_mapping.keys())
                    df_ies = df[existing_columns].copy()
                    
                    # Renomear para nomes padronizados
                    df_ies.rename(columns=column_mapping, inplace=True)
                    
                    # Remover duplicatas
                    df_ies = df_ies.drop_duplicates()
                    
                    # Extrair ano do nome do resource
                    year = None
                    for y in years:
                        if str(y) in resource_name:
                            year = y
                            break
                    
                    if year:
                        df_ies['ANO_REFERENCIA'] = year
                    
                    all_ies_data.append(df_ies)
                    logger.info(f"✅ {len(df_ies)} IES extraídas via API de {resource_name}")
                else:
                    logger.warning(f"⚠️ Nenhuma coluna de IES encontrada em {resource_name}")
                    # Mostrar colunas disponíveis para debug
                    logger.info(f"   Colunas disponíveis: {list(df.columns)[:10]}...")
            
            # Pausa entre requests
            time.sleep(1)
    
    if all_ies_data:
        # Consolidar todos os dados
        df_consolidated = pd.concat(all_ies_data, ignore_index=True)
        
        # Remover duplicatas baseado em identificadores únicos
        key_columns = ['CD_ENTIDADE_ENSINO', 'CD_ENTIDADE_CAPES', 'NM_ENTIDADE_ENSINO']
        existing_key_columns = [col for col in key_columns if col in df_consolidated.columns]
        
        if existing_key_columns:
            df_final = df_consolidated.drop_duplicates(subset=existing_key_columns)
        else:
            df_final = df_consolidated.drop_duplicates()
        
        logger.info(f"🎯 DADOS CONSOLIDADOS VIA API: {len(df_final)} IES únicas")
        return df_final
    
    else:
        logger.error("❌ Nenhum dado de IES foi extraído via API")
        return pd.DataFrame()

def save_to_postgresql(df: pd.DataFrame, db_config: Dict) -> bool:
    """Salva dados de IES no PostgreSQL"""
    try:
        logger.info("💾 Salvando dados de IES no PostgreSQL...")
        
        if df.empty:
            logger.error("❌ DataFrame vazio, nada para salvar")
            return False
        
        # Conectar ao banco
        conn_string = f"postgresql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        engine = create_engine(conn_string)
        
        # Padronizar nomes das colunas
        df_clean = df.copy()
        df_clean.columns = [col.lower() for col in df_clean.columns]
        
        # Adicionar timestamp de criação
        df_clean['created_at'] = pd.Timestamp.now()
        
        # Verificar se tabela existe e remover
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS raw_ies_api"))
            conn.commit()
        
        # Salvar dados
        df_clean.to_sql(
            'raw_ies_api',
            engine,
            if_exists='replace',
            index=False,
            dtype={
                'cd_entidade_ensino': 'BIGINT',
                'cd_entidade_capes': 'BIGINT', 
                'cd_entidade_emec': 'BIGINT',
                'nm_entidade_ensino': 'TEXT',
                'sg_entidade_ensino': 'VARCHAR(20)',
                'ds_dependencia_administrativa': 'VARCHAR(100)',
                'cs_status_juridico': 'VARCHAR(100)',
                'nm_municipio_entidade_ensino': 'VARCHAR(200)',
                'sg_uf_entidade_ensino': 'VARCHAR(2)',
                'nm_regiao_entidade_ensino': 'VARCHAR(50)',
                'ano_referencia': 'INTEGER',
                'an_base': 'INTEGER',
                'created_at': 'TIMESTAMP'
            }
        )
        
        # Adicionar chave primária
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE raw_ies_api ADD COLUMN id SERIAL PRIMARY KEY"))
            conn.commit()
        
        logger.info("✅ Dados salvos com sucesso no PostgreSQL!")
        
        # Estatísticas finais
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM raw_ies_api")).fetchone()
            total_records = result[0] if result else 0
            
            result = conn.execute(text("SELECT COUNT(DISTINCT nm_entidade_ensino) FROM raw_ies_api")).fetchone()
            unique_ies = result[0] if result else 0
        
        logger.info(f"📊 ESTATÍSTICAS FINAIS:")
        logger.info(f"   • Total de registros: {total_records}")
        logger.info(f"   • IES únicas: {unique_ies}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao salvar no PostgreSQL: {str(e)}")
        return False

def explore_capes_api():
    """Explora a API da CAPES para identificar datasets com API disponível"""
    client = CAPESAPIClient()
    
    logger.info("🔍 EXPLORANDO API CKAN DA CAPES")
    logger.info("=" * 40)
    
    # Buscar datasets com API disponível
    datasets = client.search_datasets_with_api_resources("programa")
    
    logger.info(f"\n📋 DATASETS COM API DISPONÍVEL:")
    for i, dataset in enumerate(datasets[:10]):  # Mostrar apenas os 10 primeiros
        name = dataset.get('name', 'N/A')
        title = dataset.get('title', 'N/A')
        resources = dataset.get('resources', [])
        
        api_resources = [r for r in resources if r.get('datastore_active')]
        
        logger.info(f"   {i+1}. {title}")
        logger.info(f"      ID: {name}")
        logger.info(f"      Resources com API: {len(api_resources)}")
        
        # Mostrar alguns resources com API
        for j, resource in enumerate(api_resources[:3]):
            resource_name = resource.get('name', 'N/A')
            resource_id = resource.get('id', 'N/A')
            logger.info(f"         • {resource_name} (ID: {resource_id})")
        
        if len(api_resources) > 3:
            logger.info(f"         ... e mais {len(api_resources) - 3} resources")
    
    return datasets

def main():
    """Função principal"""
    try:
        logger.info("🚀 INICIANDO EXTRAÇÃO DE IES VIA API CKAN")
        logger.info("=" * 60)
        
        # Inicializar cliente da API
        client = CAPESAPIClient()
        
        # Explorar API para encontrar datasets disponíveis
        logger.info("🔍 Explorando datasets disponíveis via API...")
        datasets = explore_capes_api()
        
        if not datasets:
            logger.error("❌ Nenhum dataset com API encontrado")
            return False
        
        # Extrair dados de IES via API CKAN
        df_ies = extract_ies_from_programas_api(client, years=[2021, 2022, 2023])
        
        if df_ies.empty:
            logger.error("❌ Nenhum dado de IES foi extraído via API")
            return False
        
        # Mostrar amostra dos dados
        logger.info("\n📋 AMOSTRA DOS DADOS EXTRAÍDOS VIA API:")
        print(df_ies.head())
        print(f"\nColunas disponíveis: {list(df_ies.columns)}")
        print(f"Total de registros: {len(df_ies)}")
        
        # Salvar no PostgreSQL
        db_config = load_database_config()
        success = save_to_postgresql(df_ies, db_config)
        
        if success:
            logger.info("🎉 Processo de extração via API concluído com sucesso!")
            return True
        else:
            logger.error("❌ Falha ao salvar dados extraídos via API")
            return False
            
    except Exception as e:
        logger.error(f"❌ Erro geral na extração via API: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
