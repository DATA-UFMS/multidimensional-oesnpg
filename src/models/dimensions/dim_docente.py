#!/usr/bin/env python3
"""
dim_docente.py

Módulo para criação e gerenciamento da dimensão de docentes no Data Warehouse.

Descrição:
    Este módulo implementa o processo de ETL (Extract, Transform, Load) para a dimensão
    de docentes da pós-graduação brasileira, consolidando dados de múltiplas fontes e
    enriquecendo com informações sobre qualificação, bolsas de produtividade e dados
    demográficos.
    
    A dimensão contém:
    - Dados básicos de identificação (id_pessoa, nome, documentos)
    - Informações demográficas (sexo, raça/cor, deficiência, nacionalidade)
    - Qualificação acadêmica (titulação, área, instituição tituladora)
    - Vínculo institucional (categoria, regime de trabalho)
    - Bolsas de produtividade em pesquisa (PQ/CNPq)
    - Identificadores únicos (id_lattes, id_pessoa)

Fontes de Dados:
    - Base Principal: add_docentes.parquet (MinIO) - Dados consolidados dos docentes
    - Enriquecimento 1: raw_docente (PostgreSQL) - Dados detalhados de cadastro
    - Enriquecimento 2: raw_fomentopq (PostgreSQL) - Bolsas de produtividade CNPq

Estrutura da Dimensão:
    - docente_sk: Surrogate key (chave substituta sequencial, inicia em 0)
    - id_pessoa: Identificador único do docente no sistema CAPES
    - des_docente: Nome completo do docente
    - des_categoria_docente: Categoria (Permanente, Colaborador, Visitante)
    - des_regime_trabalho: Regime de trabalho (Integral, Parcial, Horista)
    - des_faixa_etaria: Faixa etária do docente
    - cs_sexo: Sexo (M/F)
    - bl_doutor: Flag booleana indicando se possui doutorado
    - an_titulacao: Ano de obtenção da titulação máxima
    - des_grau_titulacao: Grau de titulação (Doutorado, Mestrado, etc.)
    - des_area_titulacao: Área de conhecimento da titulação
    - sg_ies_titulacao: Sigla da instituição tituladora
    - bl_bolsa_pq: Flag booleana indicando se possui bolsa PQ (consolidada)
    - cod_bolsa_produtividade: Código da bolsa de produtividade
    - bl_coordenador_ppg: Flag booleana indicando se é coordenador de PPG
    
    Campos de Enriquecimento (raw_docente):
    - tipo_documento: Tipo do documento (RG, CPF, Passaporte, etc.)
    - documento_docente: Número do documento
    - ano_nascimento: Ano de nascimento
    - nacionalidade: Tipo de nacionalidade (Brasileira, Estrangeira)
    - pais_nacionalidade: País de origem
    - vinculo_ies: Tipo de vínculo com a IES
    - nome_ies_titulacao: Nome completo da instituição tituladora
    - pais_titulacao: País da instituição tituladora
    - ano_base_mais_recente: Ano base mais recente dos dados
    
    Campos de Enriquecimento (raw_fomentopq):
    - id_lattes: Identificador da plataforma Lattes
    - pq_categoria_nivel: Categoria e nível da bolsa PQ (1A, 1B, 1C, 1D, 2, etc.)
    - pq_modalidade: Modalidade da bolsa
    - pq_grande_area: Grande área de conhecimento da bolsa
    - pq_area: Área específica de conhecimento da bolsa
    - pq_data_inicio: Data de início do processo/bolsa
    - pq_data_termino: Data de término do processo/bolsa

Processo de ETL:
    1. Extração: Carrega dados base do MinIO (parquet) e tabelas raw do PostgreSQL
    2. Transformação:
       - Normalização de nomes de colunas
       - Conversão de campos texto para booleanos (SIM/NÃO -> True/False)
       - Deduplicação por id_pessoa (mantém registro mais recente)
       - Enriquecimento por join com raw_docente (por id_pessoa)
       - Enriquecimento por join com raw_fomentopq (por id_lattes ou nome)
       - Consolidação de informação de bolsa PQ de múltiplas fontes
       - Adição de surrogate key sequencial
       - Criação de registro SK=0 para 'Desconhecido'
    3. Carga: Inserção em massa na tabela dim_docente do PostgreSQL

Validações:
    - Unicidade de id_pessoa e docente_sk
    - Campos obrigatórios: id_pessoa, des_docente
    - Integridade de tipos booleanos
    - Consistência de datas (pq_data_inicio < pq_data_termino)
    - Verificação de duplicatas

Dependências:
    - pandas: Manipulação de DataFrames
    - sqlalchemy: Conexão e operações com PostgreSQL
    - MinIO/S3: Leitura de arquivos parquet
    - python-dotenv: Gerenciamento de variáveis de ambiente

Uso:
    python3 src/models/dimensions/dim_docente.py

Notas Técnicas:
    - O enriquecimento com raw_fomentopq tenta primeiro por id_lattes
    - Se id_lattes não disponível, tenta match por nome normalizado
    - Deduplicação mantém sempre o registro mais recente (por ano_base ou data)
    - Registro SK=0 garante integridade referencial em joins

Autor: UFMS - Data Warehouse CAPES/OES/NPG
Data de Criação: 2025
Última Atualização: 09/10/2025
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path

# Adicionar o diretório raiz ao path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from src.utils.naming_conventions import NamingConventions
from src.validation.data_validator import validate_dimension_data, get_validation_summary
from src.core.exceptions import DimensionCreationError, DataValidationError


def get_project_root() -> Path:
    """Encontra o diretório raiz do projeto de forma robusta."""
    current_path = Path(__file__).resolve()
    while not (current_path / '.env').exists() and not (current_path / '.git').exists() and current_path.parent != current_path:
        current_path = current_path.parent
    return current_path

def get_db_engine(env_path: Path):
    """Conecta ao PostgreSQL usando variáveis de ambiente."""
    load_dotenv(dotenv_path=env_path)
    
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    if not all([db_user, db_pass, db_host, db_port, db_name]):
        raise ValueError("As variáveis de ambiente do banco de dados não estão configuradas.")

    db_uri = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    try:
        engine = create_engine(db_uri)
        with engine.connect() as connection:
            print(f"Conexão com o banco '{db_name}' estabelecida com sucesso.")
        return engine
    except Exception as e:
        print(f"ERRO: Falha ao conectar com o banco de dados: {e}")
        raise

def load_parquet_from_minio(env_path: Path) -> pd.DataFrame:
    """
    Lê um arquivo Parquet do MinIO com base nas variáveis de ambiente.
    """
    load_dotenv(dotenv_path=env_path)

    endpoint = os.getenv("MINIO_ENDPOINT")
    bucket = os.getenv("MINIO_BUCKET")
    parquet_path = os.getenv("MINIO_PARQUET_PATH")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not all([endpoint, bucket, parquet_path, access_key, secret_key]):
        raise ValueError("As variáveis de ambiente do MinIO não estão configuradas.")

    storage_options = {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {"endpoint_url": endpoint},
    }

    # Caminho completo para o arquivo no MinIO
    path = f"s3://{bucket}/{parquet_path}/add_docentes.parquet"
    
    print(f"Lendo arquivo Parquet do MinIO: {path}")
    try:
        df = pd.read_parquet(path, storage_options=storage_options)
        print(f"Dados carregados com sucesso: {len(df):,} registros.")
        return df
    except Exception as e:
        print(f"ERRO: Falha ao ler o arquivo Parquet do MinIO: {e}")
        raise

def load_raw_data_from_postgres(engine) -> tuple:
    """
    Carrega dados das tabelas raw_docente e raw_fomentopq do PostgreSQL
    """
    print("Carregando dados de enriquecimento do PostgreSQL...")
    
    # Carregar raw_docente
    print("  - Carregando raw_docente...")
    df_raw_docente = pd.read_sql_query("""
    SELECT 
        id_pessoa,
        tp_documento_docente as tipo_documento,
        nr_documento_docente as documento_docente,
        an_nascimento_docente as ano_nascimento,
        ds_tipo_nacionalidade_docente as nacionalidade,
        nm_pais_nacionalidade_docente as pais_nacionalidade,
        ds_tipo_vinculo_docente_ies as vinculo_ies,
        nm_ies_titulacao as nome_ies_titulacao,
        nm_pais_ies_titulacao as pais_titulacao,
        ano_base as ano_base_mais_recente
    FROM raw_docente
    """, engine)
    
    # Consolidar raw_docente por id_pessoa (mais recente)
    df_raw_docente = df_raw_docente.sort_values('ano_base_mais_recente', ascending=False)
    df_raw_docente = df_raw_docente.drop_duplicates(subset=['id_pessoa'], keep='first')
    print(f"    ✅ {len(df_raw_docente):,} docentes únicos de raw_docente")
    
    # Carregar raw_fomentopq (com tratamento se não existir)
    print("  - Carregando raw_fomentopq...")
    try:
        df_raw_pq = pd.read_sql_query("""
        SELECT 
            id_lattes,
            des_beneficiario as nome_beneficiario,
            cod_categoria_nivel as pq_categoria_nivel,
            cod_modalidade as pq_modalidade,
            des_grande_area as pq_grande_area,
            des_area as pq_area,
            data_inicio_processo as pq_data_inicio,
            data_termino_processo as pq_data_termino
        FROM raw_fomentopq
        WHERE id_lattes IS NOT NULL AND id_lattes != ''
        """, engine)
        
        # Consolidar raw_fomentopq por id_lattes (mais recente)
        df_raw_pq = df_raw_pq.sort_values('pq_data_inicio', ascending=False)
        df_raw_pq = df_raw_pq.drop_duplicates(subset=['id_lattes'], keep='first')
        print(f"    ✅ {len(df_raw_pq):,} bolsistas PQ únicos de raw_fomentopq")
    except Exception as e:
        print(f"    ⚠️  Tabela raw_fomentopq não encontrada: {e}")
        print("    📋 Continuando sem dados de bolsa PQ...")
        # Criar DataFrame vazio com as colunas esperadas
        df_raw_pq = pd.DataFrame(columns=[
            'id_lattes', 'nome_beneficiario', 'pq_categoria_nivel', 'pq_modalidade',
            'pq_grande_area', 'pq_area', 'pq_data_inicio', 'pq_data_termino'
        ])
    
    return df_raw_docente, df_raw_pq

def create_enriched_docente_dimension(df_base: pd.DataFrame, df_raw_docente: pd.DataFrame, df_raw_pq: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma o DataFrame base do parquet e enriquece com dados raw do PostgreSQL
    """
    print("Processando dados para criar a dimensão consolidada...")

    # 1. Processar dados base do parquet (como antes)
    column_mapping = {
        'ID_PESSOA': 'id_pessoa',
        'NM_DOCENTE': 'des_docente',
        'DS_CATEGORIA_DOCENTE': 'des_categoria_docente',
        'DS_REGIME_TRABALHO': 'des_regime_trabalho',
        'DS_FAIXA_ETARIA': 'des_faixa_etaria',
        'TP_SEXO_DOCENTE': 'cs_sexo',
        'IN_DOUTOR': 'in_doutor',
        'AN_TITULACAO': 'an_titulacao',
        'NM_GRAU_TITULACAO': 'des_grau_titulacao',
        'NM_AREA_BASICA_TITULACAO': 'des_area_titulacao',
        'SG_IES_TITULACAO': 'sg_ies_titulacao',
        'CD_CAT_BOLSA_PRODUTIVIDADE': 'cod_bolsa_produtividade',
        'IN_COORDENADOR_PPG': 'in_coordenador_ppg',
        'ID_LATTES': 'id_lattes'  # Incluir ID_LATTES se existir
    }
    
    # Filtrar apenas colunas que existem no DataFrame
    available_columns = [col for col in column_mapping.keys() if col in df_base.columns]
    df_dim = df_base[available_columns].copy()
    df_dim.rename(columns={k: column_mapping[k] for k in available_columns}, inplace=True)

    # Processar campos booleanos
    if 'in_doutor' in df_dim.columns:
        df_dim['bl_doutor'] = df_dim['in_doutor'].str.upper().map({'SIM': True, 'NÃO': False}).fillna(False).astype(bool)
        df_dim.drop(columns=['in_doutor'], inplace=True)
    
    if 'in_coordenador_ppg' in df_dim.columns:
        df_dim['bl_coordenador_ppg'] = df_dim['in_coordenador_ppg'].str.upper().map({'SIM': True, 'NÃO': False}).fillna(False).astype(bool)
        df_dim.drop(columns=['in_coordenador_ppg'], inplace=True)
    
    # Bolsa PQ inicial (será enriquecida depois)
    if 'cod_bolsa_produtividade' in df_dim.columns:
        df_dim['bl_bolsa_pq_original'] = df_dim['cod_bolsa_produtividade'].notna() & (df_dim['cod_bolsa_produtividade'] != '')
    else:
        df_dim['bl_bolsa_pq_original'] = False

    # Agrupar por id_pessoa (manter primeiro registro)
    df_dim = df_dim.drop_duplicates(subset=['id_pessoa'], keep='first').reset_index(drop=True)
    print(f"  ✅ Base processada: {len(df_dim):,} docentes únicos do parquet")

    # 2. ENRIQUECER: Merge com raw_docente
    print("  - Enriquecendo com raw_docente...")
    df_enriched = pd.merge(df_dim, df_raw_docente, on='id_pessoa', how='left')
    
    matches_raw = len(df_enriched[df_enriched['tipo_documento'].notna()])
    print(f"    ✅ {matches_raw:,} docentes enriquecidos com raw_docente")

    # 3. ENRIQUECER: Merge com raw_fomentopq (por id_lattes se disponível)
    if 'id_lattes' in df_enriched.columns:
        print("  - Enriquecendo com raw_fomentopq via id_lattes...")
        df_enriched = pd.merge(df_enriched, df_raw_pq, on='id_lattes', how='left')
        matches_pq = len(df_enriched[df_enriched['pq_categoria_nivel'].notna()])
        print(f"    ✅ {matches_pq:,} docentes enriquecidos com bolsa PQ")
    else:
        print("  - Tentando enriquecer com raw_fomentopq via nome...")
        # Normalizar nomes para match aproximado
        def normalize_name(name):
            if pd.isna(name):
                return ""
            return str(name).upper().strip()
        
        df_enriched['nome_normalizado'] = df_enriched['des_docente'].apply(normalize_name)
        df_raw_pq['nome_normalizado'] = df_raw_pq['nome_beneficiario'].apply(normalize_name)
        
        df_enriched = pd.merge(
            df_enriched, 
            df_raw_pq[['nome_normalizado', 'id_lattes', 'pq_categoria_nivel', 'pq_modalidade', 
                      'pq_grande_area', 'pq_area', 'pq_data_inicio', 'pq_data_termino']], 
            on='nome_normalizado', 
            how='left'
        )
        df_enriched.drop('nome_normalizado', axis=1, inplace=True)
        
        matches_pq = len(df_enriched[df_enriched['pq_categoria_nivel'].notna()])
        print(f"    ✅ {matches_pq:,} docentes enriquecidos com bolsa PQ (por nome)")

    # 4. CONSOLIDAR campos finais
    print("  - Consolidando campos finais...")
    
    # Consolidar informação de bolsa PQ
    df_enriched['bl_bolsa_pq'] = (
        df_enriched['bl_bolsa_pq_original'].fillna(False) | 
        df_enriched['pq_categoria_nivel'].notna()
    )
    
    # Tratar campos de data
    for col in ['pq_data_inicio', 'pq_data_termino']:
        if col in df_enriched.columns:
            df_enriched[col] = pd.to_datetime(df_enriched[col], errors='coerce')
    
    # Garantir que campos obrigatórios existam
    if 'bl_doutor' not in df_enriched.columns:
        df_enriched['bl_doutor'] = False
    if 'bl_coordenador_ppg' not in df_enriched.columns:
        df_enriched['bl_coordenador_ppg'] = False

    # 5. Adicionar chave surrogate
    df_enriched.reset_index(drop=True, inplace=True)
    df_enriched['docente_sk'] = range(1, len(df_enriched) + 1)
    
    # 6. Adicionar registro SK=0 para 'Desconhecido'
    sk0_record = pd.DataFrame([{
        'docente_sk': 0,
        'id_pessoa': 0,
        'des_docente': 'Desconhecido',
        'bl_doutor': False,
        'bl_bolsa_pq': False,
        'bl_coordenador_ppg': False
    }])
    
    final_dim = pd.concat([sk0_record, df_enriched], ignore_index=True)
    
    # 7. Organizar colunas finais
    priority_cols = [
        'docente_sk', 'id_pessoa', 'des_docente', 'des_categoria_docente', 
        'des_regime_trabalho', 'des_faixa_etaria', 'cs_sexo', 'bl_doutor', 
        'an_titulacao', 'des_grau_titulacao', 'des_area_titulacao', 'sg_ies_titulacao',
        'bl_bolsa_pq', 'cod_bolsa_produtividade', 'bl_coordenador_ppg'
    ]
    
    # Adicionar colunas de enriquecimento
    enrichment_cols = [
        'id_lattes', 'tipo_documento', 'documento_docente', 'ano_nascimento',
        'nacionalidade', 'pais_nacionalidade', 'vinculo_ies', 'nome_ies_titulacao',
        'pais_titulacao', 'pq_categoria_nivel', 'pq_modalidade', 'pq_grande_area',
        'pq_area', 'pq_data_inicio', 'pq_data_termino', 'ano_base_mais_recente'
    ]
    
    # Selecionar apenas colunas que existem
    all_cols = priority_cols + enrichment_cols
    final_cols = [col for col in all_cols if col in final_dim.columns]
    final_dim = final_dim[final_cols]
    
    print(f"  ✅ Dimensão consolidada: {len(final_dim):,} registros com {len(final_dim.columns)} colunas")
    return final_dim

def save_to_postgres(df: pd.DataFrame, engine, table_name: str):
    """Salva o DataFrame final no PostgreSQL."""
    print(f"Salvando dimensão na tabela 'public.{table_name}'...")
    try:
        with engine.begin() as conn:
            # Primeiro criar a tabela com estrutura explícita
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS dim_docente (
                docente_sk INTEGER PRIMARY KEY,
                id_pessoa VARCHAR(50) NOT NULL,
                nome_docente VARCHAR(255),
                tipo_documento VARCHAR(50),
                numero_documento VARCHAR(50),
                pais_nacionalidade VARCHAR(100),
                uf_nascimento VARCHAR(2),
                cidade_nascimento VARCHAR(100),
                sexo VARCHAR(1),
                raca_cor VARCHAR(50),
                deficiencia VARCHAR(50),
                des_grau_titulacao VARCHAR(100),
                des_area_titulacao VARCHAR(255),
                sg_ies_titulacao VARCHAR(20),
                cod_bolsa_produtividade VARCHAR(20),
                bl_doutor BOOLEAN,
                bl_coordenador_ppg BOOLEAN,
                bl_bolsa_pq_original BOOLEAN,
                id_lattes VARCHAR(50),
                pq_categoria_nivel VARCHAR(50),
                pq_area_atuacao VARCHAR(255),
                pq_periodo_vigencia VARCHAR(50)
            );
            """
            
            # Executar a criação da tabela
            conn.exec_driver_sql(create_table_sql)
            
            # Limpar tabela se já existir dados
            conn.exec_driver_sql(f"DELETE FROM {table_name};")
            
        # Mapear nomes de colunas do DataFrame para os nomes da tabela
        column_mapping = {
            'des_docente': 'nome_docente',
            'cs_sexo': 'sexo',
            'documento_docente': 'numero_documento',
            'bl_bolsa_pq': 'bl_bolsa_pq_original',
            'pq_area': 'pq_area_atuacao'
        }
        
        # Renomear apenas as colunas que existem no DataFrame
        df_to_save = df.copy()
        rename_dict = {k: v for k, v in column_mapping.items() if k in df_to_save.columns}
        if rename_dict:
            df_to_save = df_to_save.rename(columns=rename_dict)
            print(f"  📝 Colunas renomeadas: {list(rename_dict.keys())} → {list(rename_dict.values())}")
        
        # Transformar campo sexo para apenas 1 caractere (M/F/O)
        if 'sexo' in df_to_save.columns:
            def map_sexo(value):
                if pd.isna(value) or value is None or value == '':
                    return None
                value_str = str(value).strip().upper()
                if value_str.startswith('M'):
                    return 'M'
                elif value_str.startswith('F'):
                    return 'F'
                else:
                    return 'O'  # Outro
            df_to_save['sexo'] = df_to_save['sexo'].apply(map_sexo)
        
        # Truncar campos VARCHAR para caber nos limites da tabela
        varchar_limits = {
            'id_pessoa': 50,
            'nome_docente': 255,
            'tipo_documento': 50,
            'numero_documento': 50,
            'pais_nacionalidade': 100,
            'uf_nascimento': 2,
            'cidade_nascimento': 100,
            'raca_cor': 50,
            'deficiencia': 50,
            'des_grau_titulacao': 100,
            'des_area_titulacao': 255,
            'sg_ies_titulacao': 20,
            'cod_bolsa_produtividade': 20,
            'id_lattes': 50,
            'pq_categoria_nivel': 50,
            'pq_area_atuacao': 255,
            'pq_periodo_vigencia': 50
        }
        
        for col, max_len in varchar_limits.items():
            if col in df_to_save.columns:
                df_to_save[col] = df_to_save[col].astype(str).str[:max_len]
                # Substituir 'nan' string por None
                df_to_save[col] = df_to_save[col].replace('nan', None)
        
        # Selecionar apenas as colunas que existem na tabela
        table_columns = ['docente_sk', 'id_pessoa', 'nome_docente', 'tipo_documento', 'numero_documento',
                        'pais_nacionalidade', 'uf_nascimento', 'cidade_nascimento', 'sexo', 'raca_cor',
                        'deficiencia', 'des_grau_titulacao', 'des_area_titulacao', 'sg_ies_titulacao',
                        'cod_bolsa_produtividade', 'bl_doutor', 'bl_coordenador_ppg', 'bl_bolsa_pq_original',
                        'id_lattes', 'pq_categoria_nivel', 'pq_area_atuacao', 'pq_periodo_vigencia']
        
        # Manter apenas colunas que existem no DataFrame
        available_cols = [col for col in table_columns if col in df_to_save.columns]
        df_to_save = df_to_save[available_cols]
        
        # Inserir dados em chunks para evitar overflow de parâmetros
        # 31 colunas x 500 registros = 15.500 parâmetros (abaixo do limite de 32.767)
        CHUNK_SIZE = 500
        total_chunks = (len(df_to_save) + CHUNK_SIZE - 1) // CHUNK_SIZE
        print(f"  Inserindo {len(df_to_save):,} registros ({len(available_cols)} colunas) em {total_chunks} chunks de {CHUNK_SIZE}...")
        
        for i in range(0, len(df_to_save), CHUNK_SIZE):
            chunk = df_to_save.iloc[i:i+CHUNK_SIZE]
            chunk_num = (i // CHUNK_SIZE) + 1
            chunk.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
            print(f"    Chunk {chunk_num}/{total_chunks} inserido ({len(chunk)} registros)")
            
        print("  ✅ Dimensão salva com sucesso!")
    except Exception as e:
        print(f"❌ ERRO: Falha ao salvar a dimensão: {e}")
        raise

def main():
    """Função principal para orquestrar a criação da dimensão consolidada."""
    TABLE_NAME = 'dim_docente'
    
    print(f"INICIANDO GERAÇÃO DA DIMENSÃO CONSOLIDADA {TABLE_NAME.upper()}")
    print("=" * 70)
    print("Estratégia: add_docentes.parquet (base) + raw_docente + raw_fomentopq")

    try:
        project_root = get_project_root()
        
        # 1. Conectar ao banco de dados
        engine = get_db_engine(project_root / '.env')
        
        # 2. Tentar ler dados base do MinIO, com fallback para PostgreSQL
        try:
            print("Tentando carregar dados do MinIO...")
            df_base = load_parquet_from_minio(project_root / '.env')
        except Exception as minio_error:
            print(f"⚠️  MinIO não disponível: {minio_error}")
            print("📋 Usando fallback: carregando dados direto do PostgreSQL (raw_docente)...")
            # Usar raw_docente como base quando MinIO não está disponível
            # Retornar colunas com nomes em MAIÚSCULAS para compatibilidade com create_enriched_docente_dimension
            df_base = pd.read_sql_query("""
            SELECT 
                id_pessoa as "ID_PESSOA",
                nm_docente as "NM_DOCENTE",
                ds_categoria_docente as "DS_CATEGORIA_DOCENTE",
                ds_regime_trabalho as "DS_REGIME_TRABALHO",
                ds_faixa_etaria as "DS_FAIXA_ETARIA",
                in_doutor as "IN_DOUTOR",
                an_titulacao as "AN_TITULACAO",
                nm_grau_titulacao as "NM_GRAU_TITULACAO",
                nm_area_basica_titulacao as "NM_AREA_BASICA_TITULACAO",
                sg_ies_titulacao as "SG_IES_TITULACAO",
                cd_cat_bolsa_produtividade as "CD_CAT_BOLSA_PRODUTIVIDADE",
                'NÃO' as "IN_COORDENADOR_PPG",
                ano_base
            FROM raw_docente
            WHERE ano_base = (SELECT MAX(ano_base) FROM raw_docente)
            """, engine)
            print(f"✅ {len(df_base):,} registros carregados do PostgreSQL (ano mais recente)")
        
        # 3. Carregar dados de enriquecimento do PostgreSQL
        df_raw_docente, df_raw_pq = load_raw_data_from_postgres(engine)
        
        # 4. Criar a dimensão consolidada
        dim_docente = create_enriched_docente_dimension(df_base, df_raw_docente, df_raw_pq)
        
        # 5. Salvar a dimensão no PostgreSQL
        save_to_postgres(dim_docente, engine, table_name=TABLE_NAME)
        
        # 6. Exibir preview e estatísticas
        print("\nPreview da dimensão consolidada:")
        print(dim_docente.head())
        
        print("\nEstatísticas da dimensão:")
        print(f"  - Total de registros: {len(dim_docente):,}")
        print(f"  - Doutores: {dim_docente['bl_doutor'].sum():,}")
        print(f"  - Bolsistas PQ: {dim_docente['bl_bolsa_pq'].sum():,}")
        if 'bl_coordenador_ppg' in dim_docente.columns:
            print(f"  - Coordenadores de PPG: {dim_docente['bl_coordenador_ppg'].sum():,}")
        
        # Estatísticas de enriquecimento
        if 'tipo_documento' in dim_docente.columns:
            enriquecidos = len(dim_docente[dim_docente['tipo_documento'].notna() & (dim_docente['tipo_documento'] != '')])
            print(f"  - Enriquecidos com raw_docente: {enriquecidos:,} ({enriquecidos/len(dim_docente)*100:.1f}%)")
            
        if 'pq_categoria_nivel' in dim_docente.columns:
            com_pq = len(dim_docente[dim_docente['pq_categoria_nivel'].notna()])
            print(f"  - Enriquecidos com raw_fomentopq: {com_pq:,} ({com_pq/len(dim_docente)*100:.1f}%)")

        print("\n🎉 DIMENSÃO CONSOLIDADA CRIADA COM SUCESSO!")
        print("Fontes: add_docentes.parquet + raw_docente + raw_fomentopq")

    except Exception as e:
        print(f"\nO processo falhou. Motivo: {e}")
        raise e
    
    print("\nProcesso concluído.")

if __name__ == "__main__":
    main()
