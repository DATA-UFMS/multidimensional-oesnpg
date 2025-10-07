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
        df_estados = pd.DataFrame([])

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

    # Trazer UF sigla e região via join, se disponível
    if not df_estados.empty and 'codigo_uf' in df_estados.columns:
        # Obter combinação única por codigo_uf via groupby para evitar ambiguidade tipada
        estados_keys = df_estados.groupby('codigo_uf', as_index=False).first()[['codigo_uf','sigla_uf','regiao']]
        estados_keys = pd.DataFrame(estados_keys)
        df_mun = df_mun.merge(estados_keys, on='codigo_uf', how='left')
    else:
        # Sem código UF nos estados, tentar mapear por código_uf dos municípios usando dicionário de região
        if 'sigla_uf' not in df_mun.columns and 'codigo_uf' in df_mun.columns:
            # Não temos sigla diretamente; manterá nulo
            pass
        if 'regiao' not in df_mun.columns and 'sigla_uf' in df_mun.columns:
            df_mun['regiao'] = df_mun['sigla_uf'].apply(lambda uf: regiao_por_uf.get(uf))

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

    # Registro 0 desconhecido
    registro_desconhecido = NamingConventions.get_standard_unknown_record('localidade')
    registro_desconhecido.update({
        'uf': 'XX',
        'nome_uf': 'DESCONHECIDO',
        'sigla_uf': 'XX',
        'regiao': 'DESCONHECIDO',
        'sigla_regiao': 'XX',
        'latitude': None,
        'longitude': None,
        'nivel': 'DESCONHECIDO',
        'municipio': None,
        'codigo_ibge': None,
        'capital': 0,
        'nome': 'DESCONHECIDO'
    })
    df_localidade = pd.concat([pd.DataFrame([{
        'sigla_uf': registro_desconhecido['uf'],
        'uf': registro_desconhecido['uf'],
        'nome_uf': registro_desconhecido['nome_uf'],
        'regiao': registro_desconhecido['regiao'],
        'sigla_regiao': registro_desconhecido['sigla_regiao'],
        'latitude': registro_desconhecido['latitude'],
        'longitude': registro_desconhecido['longitude'],
        'nivel': registro_desconhecido['nivel'],
        'municipio': registro_desconhecido['municipio'],
        'codigo_ibge': registro_desconhecido['codigo_ibge'],
        'capital': registro_desconhecido['capital'],
        'nome': registro_desconhecido['nome']
    }]), df_localidade], ignore_index=True)

    # Surrogate key iniciando em 0
    import numpy as np
    df_localidade.insert(0, 'localidade_sk', np.arange(len(df_localidade)))
    
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
                nivel VARCHAR(12) NOT NULL,
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
            
            # Inserir dados
            df_localidade.to_sql('dim_localidade', conn, if_exists='append', index=False)
        print(f"Dimensão localidade salva no PostgreSQL com {len(df_localidade)} registros")
            
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

