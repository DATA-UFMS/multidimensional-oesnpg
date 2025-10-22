"""
dim_localidade.py

Módulo para criação e gerenciamento da dimensão de localidade no Data Warehouse.

Descrição:
    Este módulo implementa o processo de ETL (Extract, Transform, Load) para a dimensão
    de localidade, que inclui informações sobre estados (UF) e municípios brasileiros.
    
    A dimensão contém:
    - 27 estados brasileiros com coordenadas geográficas (latitude/longitude)
    - 5.570+ municípios com suas respectivas localizações
    - Hierarquia geográfica: Região > Estado (UF) > Município
    - Códigos IBGE para integração com outras bases de dados

Fontes de Dados:
    - Estados: Arquivo local 'tabela de codigos UF e Regiao IBGE.xlsx'
    - Municípios: CSV do GitHub (kelvins/municipios-brasileiros)
    - Coordenadas: Incluídas nos arquivos de origem

Estrutura da Dimensão:
    - localidade_sk: Surrogate key (chave substituta sequencial)
    - sigla_uf: Sigla do estado (ex: 'SP', 'RJ', 'MG')
    - nome_uf: Nome completo do estado
    - regiao: Nome da região (Norte, Nordeste, Sul, Sudeste, Centro-Oeste)
    - sigla_regiao: Sigla da região (2 caracteres)
    - latitude: Coordenada geográfica (latitude)
    - longitude: Coordenada geográfica (longitude)
    - nivel: Tipo do registro ('UF' ou 'MUNICIPIO')
    - municipio: Nome do município (quando aplicável)
    - codigo_ibge: Código IBGE do município
    - capital: Flag indicando se é capital (0 ou 1)
    - nome: Nome completo da localidade

Validações:
    - Formato de UF: Exatamente 2 letras maiúsculas
    - Completude de dados obrigatórios
    - Valores únicos para surrogate keys
    - Integridade referencial entre municípios e estados

Uso:
    python3 src/models/dimensions/dim_localidade.py

Autor: UFMS - Data Warehouse CAPES/OES/NPG
Data de Criação: 2025
Última Atualização: 09/10/2025
"""

import pandas as pd
from sqlalchemy import create_engine
import os
import sys
from dotenv import load_dotenv

# Adicionar o diretório raiz ao path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from src.utils.naming_conventions import NamingConventions
from src.validation.data_validator import validate_dimension_data, get_validation_summary
from src.core.exceptions import DimensionCreationError, DataValidationError

# Carregar variáveis de ambiente
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

def criar_dimensao_localidade():
    """
    Cria a dimensão localidade com dados atualizados de UFs e municípios (inclui lat/long).
    """
    print("Criando dimensão localidade...")

    # URLs configuráveis via ambiente
    url_municipios = os.getenv('MUNICIPIOS_CSV_URL') or 'https://raw.githubusercontent.com/kelvins/municipios-brasileiros/main/csv/municipios.csv'
    url_estados = os.getenv('ESTADOS_CSV_URL') or 'https://raw.githubusercontent.com/kelvins/municipios-brasileiros/main/csv/estados.csv'

    # Fallback de regiões por UF
    regiao_por_uf = {
        'AC': 'NORTE','AL': 'NORDESTE','AP': 'NORTE','AM': 'NORTE','BA': 'NORDESTE','CE': 'NORDESTE','DF': 'CENTRO-OESTE',
        'ES': 'SUDESTE','GO': 'CENTRO-OESTE','MA': 'NORDESTE','MT': 'CENTRO-OESTE','MS': 'CENTRO-OESTE','MG': 'SUDESTE',
        'PA': 'NORTE','PB': 'NORDESTE','PR': 'SUL','PE': 'NORDESTE','PI': 'NORDESTE','RJ': 'SUDESTE','RN': 'NORDESTE',
        'RS': 'SUL','RO': 'NORTE','RR': 'NORTE','SC': 'SUL','SP': 'SUDESTE','SE': 'NORDESTE','TO': 'NORTE'
    }
    nome_por_uf = {
        'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapá', 'AM': 'Amazonas', 'BA': 'Bahia', 'CE': 'Ceará',
        'DF': 'Distrito Federal', 'ES': 'Espírito Santo', 'GO': 'Goiás', 'MA': 'Maranhão', 'MT': 'Mato Grosso',
        'MS': 'Mato Grosso do Sul', 'MG': 'Minas Gerais', 'PA': 'Pará', 'PB': 'Paraíba', 'PR': 'Paraná',
        'PE': 'Pernambuco', 'PI': 'Piauí', 'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte',
        'RS': 'Rio Grande do Sul', 'RO': 'Rondônia', 'RR': 'Roraima', 'SC': 'Santa Catarina',
        'SP': 'São Paulo', 'SE': 'Sergipe', 'TO': 'Tocantins'
    }

    # Carregar estados (UFs)
    try:
        df_estados_raw = pd.read_csv(url_estados)
    except Exception:
        # fallback: tentar arquivo local não versionado
        df_estados_raw = pd.DataFrame([])

    # Normalizar estados
    if not df_estados_raw.empty:
        # Possíveis nomes de colunas
        col_map_estados = {}
        for cand in ['uf', 'sigla', 'sigla_uf', 'estado_sigla']:
            if cand in df_estados_raw.columns:
                col_map_estados['sigla_uf'] = cand
                break
        for cand in ['nome', 'nome_uf', 'estado', 'estado_nome']:
            if cand in df_estados_raw.columns:
                col_map_estados['nome_uf'] = cand
                break
        for cand in ['codigo_uf', 'codigo', 'id_uf', 'codigo_ibge']:
            if cand in df_estados_raw.columns:
                col_map_estados['codigo_uf'] = cand
                break
        for cand in ['regiao', 'região', 'region']:
            if cand in df_estados_raw.columns:
                col_map_estados['regiao'] = cand
                break
        for cand in ['latitude', 'lat']:
            if cand in df_estados_raw.columns:
                col_map_estados['latitude'] = cand
                break
        for cand in ['longitude', 'lon', 'lng']:
            if cand in df_estados_raw.columns:
                col_map_estados['longitude'] = cand
                break

        df_estados = df_estados_raw.rename(columns=col_map_estados)
        if 'sigla_uf' not in df_estados.columns and 'uf' in df_estados.columns:
            df_estados = df_estados.rename(columns={'uf': 'sigla_uf'})
        if 'nome_uf' not in df_estados.columns and 'nome' in df_estados.columns:
            df_estados = df_estados.rename(columns={'nome': 'nome_uf'})
        if 'regiao' not in df_estados.columns and 'sigla_uf' in df_estados.columns:
            df_estados['regiao'] = df_estados['sigla_uf'].apply(lambda uf: regiao_por_uf.get(uf))
        if 'sigla_regiao' not in df_estados.columns and 'regiao' in df_estados.columns:
            df_estados['sigla_regiao'] = df_estados['regiao'].str[:2].str.upper().str.replace('Ç','C')
        # Nome para validação
        df_estados['nome'] = df_estados.get('nome_uf', df_estados.get('sigla_uf'))
        df_estados['nivel'] = 'UF'
        df_estados['municipio'] = None
        df_estados['codigo_ibge'] = None
        df_estados['capital'] = 0
        # Alias para convenção do validador
        if 'uf' not in df_estados.columns and 'sigla_uf' in df_estados.columns:
            df_estados['uf'] = df_estados['sigla_uf']
        # Reordenar colunas (inclui alias 'uf' para validação)
        df_estados = df_estados[['sigla_uf','uf','nome_uf','regiao','sigla_regiao','latitude','longitude','nivel','municipio','codigo_ibge','capital','nome']]
    else:
        df_estados = pd.DataFrame({
            'sigla_uf': list(nome_por_uf.keys()),
            'uf': list(nome_por_uf.keys()),
            'nome_uf': list(nome_por_uf.values()),
            'regiao': [regiao_por_uf.get(sigla) for sigla in nome_por_uf.keys()],
            'sigla_regiao': [regiao_por_uf.get(sigla, '')[:2].upper() if regiao_por_uf.get(sigla) else None for sigla in nome_por_uf.keys()],
            'latitude': [None] * len(nome_por_uf),
            'longitude': [None] * len(nome_por_uf),
            'nivel': ['UF'] * len(nome_por_uf),
            'municipio': [None] * len(nome_por_uf),
            'codigo_ibge': [None] * len(nome_por_uf),
            'capital': [0] * len(nome_por_uf),
            'nome': list(nome_por_uf.values())
        })

    # Carregar municípios
    try:
        df_mun_raw = pd.read_csv(url_municipios)
    except Exception:
        # fallback para arquivo local fornecido pelo projeto
        local_path = os.path.join(project_root, 'staging', 'data', 'municipios.csv')
        df_mun_raw = pd.read_csv(local_path)

    # Normalizar municípios
    col_map_mun = {}
    for cand in ['codigo_ibge', 'codigo', 'id_municipio']:
        if cand in df_mun_raw.columns:
            col_map_mun['codigo_ibge'] = cand
            break
    for cand in ['municipio', 'nome_municipio', 'nome', 'nome_mun']:
        if cand in df_mun_raw.columns:
            col_map_mun['municipio'] = cand
            break
    for cand in ['codigo_uf', 'uf_id', 'id_uf']:
        if cand in df_mun_raw.columns:
            col_map_mun['codigo_uf'] = cand
            break
    for cand in ['latitude', 'lat']:
        if cand in df_mun_raw.columns:
            col_map_mun['latitude'] = cand
            break
    for cand in ['longitude', 'lon', 'lng']:
        if cand in df_mun_raw.columns:
            col_map_mun['longitude'] = cand
            break
    for cand in ['capital']:
        if cand in df_mun_raw.columns:
            col_map_mun['capital'] = cand
            break

    df_mun = df_mun_raw.rename(columns=col_map_mun)
    # Garantir tipos básicos
    if 'capital' in df_mun.columns:
        df_mun['capital'] = df_mun['capital'].fillna(0).astype(int)
    else:
        df_mun['capital'] = 0

    # Criar mapeamento codigo_uf -> sigla_uf usando os primeiros 2 dígitos do codigo_ibge
    # Código IBGE do município: primeiros 2 dígitos = código UF
    mapa_codigo_uf_sigla = {
        11: 'RO', 12: 'AC', 13: 'AM', 14: 'RR', 15: 'PA', 16: 'AP', 17: 'TO',
        21: 'MA', 22: 'PI', 23: 'CE', 24: 'RN', 25: 'PB', 26: 'PE', 27: 'AL', 28: 'SE', 29: 'BA',
        31: 'MG', 32: 'ES', 33: 'RJ', 35: 'SP',
        41: 'PR', 42: 'SC', 43: 'RS',
        50: 'MS', 51: 'MT', 52: 'GO', 53: 'DF'
    }
    
    # Extrair código UF do codigo_ibge se necessário
    if 'codigo_ibge' in df_mun.columns and 'sigla_uf' not in df_mun.columns:
        df_mun['codigo_uf'] = df_mun['codigo_ibge'].astype(str).str[:2].astype(int)
        df_mun['sigla_uf'] = df_mun['codigo_uf'].map(mapa_codigo_uf_sigla)
        df_mun['regiao'] = df_mun['sigla_uf'].map(regiao_por_uf)
    
    # Trazer UF sigla e região via join com estados, se disponível e ainda não temos sigla_uf
    elif not df_estados.empty and 'codigo_uf' in df_estados.columns and 'codigo_uf' in df_mun.columns:
        # Obter combinação única por codigo_uf via groupby para evitar ambiguidade tipada
        estados_keys = df_estados.groupby('codigo_uf', as_index=False).first()[['codigo_uf','sigla_uf','regiao']]
        estados_keys = pd.DataFrame(estados_keys)
        df_mun = df_mun.merge(estados_keys, on='codigo_uf', how='left', suffixes=('', '_estado'))
        # Se merge falhou, usar o mapeamento manual
        if df_mun['sigla_uf'].isna().any():
            mask_nulo = df_mun['sigla_uf'].isna()
            df_mun.loc[mask_nulo, 'sigla_uf'] = df_mun.loc[mask_nulo, 'codigo_uf'].map(mapa_codigo_uf_sigla)
            df_mun.loc[mask_nulo, 'regiao'] = df_mun.loc[mask_nulo, 'sigla_uf'].map(regiao_por_uf)

    # Garantir coluna regiao antes de derivar sigla_regiao
    if 'regiao' not in df_mun.columns:
        if 'sigla_uf' in df_mun.columns:
            df_mun['regiao'] = df_mun['sigla_uf'].apply(lambda uf: regiao_por_uf.get(uf))
        else:
            df_mun['regiao'] = None
    df_mun['sigla_regiao'] = df_mun['regiao'].astype(str).str[:2].str.upper().str.replace('Ç','C')
    df_mun['nivel'] = 'MUNICIPIO'
    if 'municipio' in df_mun.columns:
        df_mun['nome'] = df_mun['municipio']
    else:
        # fallback: tentar outras colunas comuns
        for alt in ['nome_municipio', 'nome', 'cidade']:
            if alt in df_mun.columns:
                df_mun['nome'] = df_mun[alt]
                break
        if 'nome' not in df_mun.columns:
            df_mun['nome'] = None
    # Garantir coluna municipio para o schema esperado
    if 'municipio' not in df_mun.columns:
        df_mun['municipio'] = df_mun['nome']
    # Alias esperado pelo validador
    if 'uf' not in df_mun.columns and 'sigla_uf' in df_mun.columns:
        df_mun['uf'] = df_mun['sigla_uf']

    # Preencher nome_uf a partir do mapeamento de estados quando ausente
    if 'nome_uf' not in df_mun.columns:
        df_mun['nome_uf'] = None
    if 'sigla_uf' in df_mun.columns and not df_estados.empty:
        mapa_nome_uf = (
            df_estados[['sigla_uf', 'nome_uf']]
            .dropna(subset=['sigla_uf'])
            .drop_duplicates()
            .set_index('sigla_uf')
        )
        df_mun['nome_uf'] = df_mun['nome_uf'].fillna(
            df_mun['sigla_uf'].map(mapa_nome_uf['nome_uf'])
        )

    # Selecionar colunas finais compatíveis
    cols_comuns = ['sigla_uf','uf','nome_uf','regiao','sigla_regiao','latitude','longitude','nivel','municipio','codigo_ibge','capital','nome']
    if 'nome_uf' not in df_mun.columns:
        df_mun['nome_uf'] = None
    if 'sigla_uf' not in df_mun.columns and 'uf' in df_mun.columns:
        df_mun = df_mun.rename(columns={'uf': 'sigla_uf'})
    df_mun = df_mun.reindex(columns=cols_comuns)

    # Combinar UFs e municípios
    frames = []
    if not df_estados.empty:
        frames.append(df_estados[cols_comuns])
    frames.append(df_mun[cols_comuns])
    df_localidade = pd.concat(frames, ignore_index=True)

    # Registro 0 não informado
    registro_nao_informado = NamingConventions.get_standard_unknown_record('localidade')
    registro_nao_informado.update({
        'uf': 'XX',
        'nome_uf': 'NÃO INFORMADO',
        'sigla_uf': 'XX',
        'regiao': 'NÃO INFORMADO',
        'sigla_regiao': 'XX',
        'latitude': None,
        'longitude': None,
        'nivel': 'NÃO INFORMADO',
        'municipio': None,
        'codigo_ibge': None,
        'capital': 0,
        'nome': 'NÃO INFORMADO'
    })
    df_localidade = pd.concat([pd.DataFrame([{
        'sigla_uf': registro_nao_informado['uf'],
        'uf': registro_nao_informado['uf'],
        'nome_uf': registro_nao_informado['nome_uf'],
        'regiao': registro_nao_informado['regiao'],
        'sigla_regiao': registro_nao_informado['sigla_regiao'],
        'latitude': registro_nao_informado['latitude'],
        'longitude': registro_nao_informado['longitude'],
        'nivel': registro_nao_informado['nivel'],
        'municipio': registro_nao_informado['municipio'],
        'codigo_ibge': registro_nao_informado['codigo_ibge'],
        'capital': registro_nao_informado['capital'],
        'nome': registro_nao_informado['nome']
    }]), df_localidade], ignore_index=True)
    
    # CRÍTICO: Recriar coluna 'uf' após TODOS os concats (pode ter sido perdida)
    # Garantir que TODOS os registros tenham uf = sigla_uf
    df_localidade['uf'] = df_localidade['sigla_uf']

    # Surrogate key iniciando em 0
    import numpy as np
    df_localidade.insert(0, 'localidade_sk', np.arange(len(df_localidade)))
    
    # DEBUG: Verificar estado da coluna 'uf' ANTES da validação
    print(f"\n🔍 DEBUG - Estado da coluna 'uf' antes da validação:")
    print(f"   Total de registros: {len(df_localidade)}")
    print(f"   Tipo de dados: {df_localidade['uf'].dtype}")
    print(f"   Valores nulos: {df_localidade['uf'].isna().sum()}")
    print(f"   Valores únicos: {df_localidade['uf'].nunique()}")
    print(f"   Valores únicos: {sorted(df_localidade['uf'].dropna().unique().tolist())}")
    print(f"   Registros válidos (^[A-Z]{{2}}$): {df_localidade['uf'].str.match(r'^[A-Z]{2}$', na=False).sum()}")
    print(f"   Registros inválidos: {(~df_localidade['uf'].str.match(r'^[A-Z]{2}$', na=False)).sum()}")
    
    # Mostrar exemplos de inválidos
    invalid = df_localidade[~df_localidade['uf'].str.match(r'^[A-Z]{2}$', na=False)]
    if len(invalid) > 0:
        print(f"\n   📋 Primeiros 5 registros inválidos:")
        print(invalid[['localidade_sk', 'sigla_uf', 'uf', 'nome', 'nivel']].head(5).to_string())
    
    # Validar dados usando o sistema de validação
    try:
        validation_results = validate_dimension_data(df_localidade, 'localidade')
        summary = get_validation_summary(validation_results)
        
        if summary['error_count'] > 0:
            print(f"Encontrados {summary['error_count']} erros de validação")
            for result in validation_results:
                if not result.passed and result.severity == 'ERROR':
                    print(f"  [ERROR] {result.rule_name}: {result.message}")
            raise DataValidationError(f"Validation failed with {summary['error_count']} errors")
        
        if summary['warning_count'] > 0:
            print(f"Encontrados {summary['warning_count']} avisos de validação")
            for result in validation_results:
                if not result.passed and result.severity == 'WARNING':
                    print(f"  [WARN] {result.rule_name}: {result.message}")
        
        print(f"Dados validados com sucesso (taxa de sucesso: {summary['success_rate']:.1%})")
        
    except DataValidationError as e:
        print(f"Erro de validação: {e}")
        raise
    except Exception as e:
        raise
    
    print(f"Dimensão localidade criada com {len(df_localidade)} registros")
    
    return df_localidade

def salvar_dimensao_localidade(df_localidade):
    """
    Salva a dimensão localidade no banco de dados PostgreSQL.
    """
    try:
        # Validar dados antes de salvar
        if df_localidade.empty:
            raise DimensionCreationError("DataFrame está vazio")
        
        # Verificar se tem a coluna sk
        if 'localidade_sk' not in df_localidade.columns:
            raise DimensionCreationError("DataFrame não possui coluna 'localidade_sk' obrigatória")
        
        # Remover coluna 'uf' (usada apenas para validação, não existe na tabela)
        df_to_save = df_localidade.drop(columns=['uf'], errors='ignore')
        
        # Criar conexão com o banco
        url = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(url)
        
        with engine.begin() as conn:
            # Primeiro criar a tabela com estrutura explícita (usando nomes padronizados)
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS dim_localidade (
                localidade_sk INTEGER PRIMARY KEY,
                sigla_uf VARCHAR(2),
                nome_uf VARCHAR(100),
                regiao VARCHAR(20),
                sigla_regiao VARCHAR(3),
                latitude DECIMAL(10,7),
                longitude DECIMAL(10,7),
                nivel VARCHAR(40) NOT NULL,
                municipio VARCHAR(120),
                codigo_ibge BIGINT,
                capital SMALLINT DEFAULT 0,
                nome VARCHAR(120),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            # Executar a criação da tabela
            conn.exec_driver_sql(create_table_sql)
            
            # Limpar tabela se já existir dados
            conn.exec_driver_sql("DELETE FROM dim_localidade;")
            
            # Inserir dados (sem a coluna 'uf')
            df_to_save.to_sql('dim_localidade', conn, if_exists='append', index=False)
        print(f"Dimensão localidade salva no PostgreSQL com {len(df_to_save)} registros")
            
    except DimensionCreationError as e:
        print(f"Erro de criação da dimensão: {e}")
        raise
    except Exception as e:
        print(f"Erro ao salvar dimensão localidade: {e}")
        raise DimensionCreationError(f"Falha ao salvar dimensão localidade: {str(e)}")

if __name__ == "__main__":
    # Criar dimensão localidade
    df_localidade = criar_dimensao_localidade()
    
    # Salvar no banco
    salvar_dimensao_localidade(df_localidade)
    
    # Mostrar algumas estatísticas
    print("\nEstatísticas da dimensão localidade:")
    print(f"Total de UFs: {len(df_localidade)}")
    print(f"Regiões: {df_localidade['regiao'].dropna().unique()}")
    print(f"Registros por nível:")
    for nivel in df_localidade['nivel'].unique():
        count = len(df_localidade[df_localidade['nivel'] == nivel])
        print(f"  {nivel}: {count}")
