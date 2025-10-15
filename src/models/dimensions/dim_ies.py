#!/usr/bin/env python3
"""
Geração da Dimensão de IES (dim_ies) a partir de add_docentes.parquet no MinIO.

Este script lê o arquivo 'add_docentes.parquet' do MinIO, extrai e desduplica
as informações INSTITUCIONAIS PURAS das Instituições de Ensino Superior (IES) 
para criar uma dimensão limpa e a salva no banco de dados PostgreSQL.

CARACTERÍSTICAS INCLUÍDAS:
- Identificação institucional (código CAPES, sigla, nome, CNPJ)
- Status jurídico e dependência administrativa
- Localização geográfica (região, UF, município, país)
- Metadados de origem (vínculo/titulação)

CARACTERÍSTICAS EXCLUÍDAS:
- Dados de programas de pós-graduação (pertencem à dim_ppg)
- Dados de titulação de docentes (pertencem à dim_docente)
- Dados de relacionamento docente-IES (pertencem à tabela fato)
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path


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

    path = f"s3://{bucket}/{parquet_path}/add_docentes.parquet"
    
    print(f"Lendo arquivo Parquet do MinIO: {path}")
    try:
        df = pd.read_parquet(path, storage_options=storage_options)
        print(f"Dados carregados com sucesso: {len(df):,} registros.")
        return df
    except Exception as e:
        print(f"ERRO: Falha ao ler o arquivo Parquet do MinIO: {e}")
        raise

def create_ies_dimension(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma o DataFrame bruto na dimensão de IES final.
    Garante que cada IES seja uma tupla única consolidando APENAS dados institucionais.
    
    🇧🇷 IMPORTANTE: 
    - Exclui dados de titulação, PPG e relacionamentos docente-IES
    - Filtra APENAS IES BRASILEIRAS (remove IES internacionais de titulação)
    - Desduplicação por NOME da IES (des_ies) priorizando registros com MENOS valores nulos
    """
    print("Processando dados para criar a dimensão de IES...")
    print(f"Registros de entrada: {len(df_raw):,}")

    # 1. Extrair IES de VÍNCULO (onde o docente trabalha atualmente)
    # APENAS características INSTITUCIONAIS PURAS da IES (sem relacionamentos docente-IES)
    ies_vinculo_cols = [
        'CD_ENTIDADE_CAPES',             # Código da entidade CAPES
        'SG_ENTIDADE_ENSINO',            # Sigla da entidade de ensino
        'NM_ENTIDADE_ENSINO',            # Nome da entidade de ensino
        'NR_CNPJ_IES',                   # CNPJ da IES
        'CS_STATUS_JURIDICO',            # Status jurídico
        'DS_DEPENDENCIA_ADMINISTRATIVA', # Dependência administrativa
        'NM_REGIAO',                     # Região
        'SG_UF_PROGRAMA',                # UF do programa
        'NM_MUNICIPIO_PROGRAMA_IES',     # Município do programa IES
        'CD_IBGE_PROGRAMA_IES'           # Código IBGE do município do programa
    ]
    
    # Verificar quais colunas existem no DataFrame
    available_vinculo_cols = [col for col in ies_vinculo_cols if col in df_raw.columns]
    print(f"Colunas de vínculo disponíveis: {available_vinculo_cols}")
    
    df_ies_vinculo = df_raw[available_vinculo_cols].copy()
    df_ies_vinculo.rename(columns={
        'CD_ENTIDADE_CAPES': 'cod_entidade_capes',
        'SG_ENTIDADE_ENSINO': 'sg_ies',
        'NM_ENTIDADE_ENSINO': 'des_ies',
        'NR_CNPJ_IES': 'nr_cnpj_ies',
        'CS_STATUS_JURIDICO': 'des_status_juridico',
        'DS_DEPENDENCIA_ADMINISTRATIVA': 'des_dependencia_adm',
        'NM_REGIAO': 'des_regiao',
        'SG_UF_PROGRAMA': 'sg_uf',
        'NM_MUNICIPIO_PROGRAMA_IES': 'des_municipio_programa',
        'CD_IBGE_PROGRAMA_IES': 'cod_ibge_municipio'
    }, inplace=True)

    # 2. Extrair IES de TITULAÇÃO (onde o docente se formou)
    # APENAS características INSTITUCIONAIS da IES de titulação
    ies_titulacao_cols = [
        'SG_IES_TITULACAO',              # Sigla da IES de titulação
        'NM_IES_TITULACAO',              # Nome da IES de titulação
        'NM_PAIS_IES_TITULACAO'          # País da IES de titulação (usado apenas para filtro)
    ]
    
    available_titulacao_cols = [col for col in ies_titulacao_cols if col in df_raw.columns]
    print(f"Colunas de titulação disponíveis: {available_titulacao_cols}")
    
    if available_titulacao_cols:
        df_ies_titulacao = df_raw[available_titulacao_cols].copy()
        
        # 🇧🇷 FILTRAR APENAS IES BRASILEIRAS ANTES DE RENOMEAR
        # Manter apenas IES onde país é nulo/vazio OU explicitamente Brasil
        print(f"IES de titulação antes do filtro brasileiro: {len(df_ies_titulacao):,}")
        df_ies_titulacao = df_ies_titulacao[
            (df_ies_titulacao['NM_PAIS_IES_TITULACAO'].isna()) | 
            (df_ies_titulacao['NM_PAIS_IES_TITULACAO'] == '') |
            (df_ies_titulacao['NM_PAIS_IES_TITULACAO'].str.upper().str.contains('BRASIL', na=False))
        ]
        print(f"IES de titulação após filtro brasileiro: {len(df_ies_titulacao):,}")
        
        # Agora renomear apenas os campos necessários (SEM des_pais)
        df_ies_titulacao = df_ies_titulacao[['SG_IES_TITULACAO', 'NM_IES_TITULACAO']].copy()
        df_ies_titulacao.rename(columns={
            'SG_IES_TITULACAO': 'sg_ies',
            'NM_IES_TITULACAO': 'des_ies'
        }, inplace=True)
        df_ies_titulacao['cod_entidade_capes'] = None  # IES de titulação pode não ter código CAPES
    else:
        df_ies_titulacao = pd.DataFrame()

    # 3. Combinar ambos os datasets
    print(f"IES de vínculo: {len(df_ies_vinculo):,}")
    print(f"IES de titulação: {len(df_ies_titulacao):,}")
    
    if not df_ies_titulacao.empty:
        # Alinhar colunas para concatenação
        all_columns = set(df_ies_vinculo.columns) | set(df_ies_titulacao.columns)
        for col in all_columns:
            if col not in df_ies_vinculo.columns:
                df_ies_vinculo[col] = None
            if col not in df_ies_titulacao.columns:
                df_ies_titulacao[col] = None
        
        df_ies_combined = pd.concat([df_ies_vinculo, df_ies_titulacao], ignore_index=True)
    else:
        df_ies_combined = df_ies_vinculo.copy()

    print(f"Registros combinados antes da desduplicação: {len(df_ies_combined):,}")

    # 4. Remover registros com dados vazios/nulos nas chaves principais
    df_ies_combined = df_ies_combined.dropna(subset=['sg_ies', 'des_ies'])
    df_ies_combined = df_ies_combined[
        (df_ies_combined['sg_ies'].str.strip() != '') & 
        (df_ies_combined['des_ies'].str.strip() != '')
    ]
    print(f"Após remoção de registros vazios: {len(df_ies_combined):,}")

    # 5. GARANTIR TUPLA ÚNICA: Desduplicar por NOME da IES priorizando registros mais completos
    # Estratégia: agrupar por des_ies e manter o registro com MENOS valores nulos
    print("Preparando desduplicação inteligente por NOME da IES (des_ies)...")
    
    # Calcular score de completude para cada registro (menos nulos = melhor score)
    def calculate_completeness_score(row):
        """Calcula score de completude: quanto menor, melhor (menos nulos)."""
        null_count = 0
        for value in row:
            if pd.isna(value) or value == '' or value is None:
                null_count += 1
        return null_count
    
    # Adicionar score de completude
    df_ies_combined['completeness_score'] = df_ies_combined.apply(calculate_completeness_score, axis=1)
    
    # Consolidar dados: manter o registro mais completo por NOME da IES
    def consolidate_ies_by_nome(group):
        """
        Consolida múltiplos registros da mesma IES (por des_ies), 
        priorizando o registro com MENOS valores nulos.
        """
        # Ordenar por score de completude (menor = melhor) e pegar o melhor
        group_sorted = group.sort_values('completeness_score')
        best_record = group_sorted.iloc[0].copy()
        
        # Complementar com dados não-nulos de outros registros
        for col in group.columns:
            if col == 'completeness_score':
                continue  # Pular coluna auxiliar
                
            if pd.isna(best_record[col]) or best_record[col] == '' or best_record[col] is None:
                # Buscar valores não-nulos em outros registros do grupo
                non_null_values = group[col].dropna()
                non_null_values = non_null_values[
                    (non_null_values != '') & 
                    (non_null_values.notna())
                ]
                if not non_null_values.empty:
                    best_record[col] = non_null_values.iloc[0]
        
        return best_record

    print("Consolidando registros duplicados por NOME da IES (priorizando menos nulos)...")
    df_ies_final = df_ies_combined.groupby('des_ies').apply(consolidate_ies_by_nome).reset_index(drop=True)
    
    # Remover coluna auxiliar de score
    if 'completeness_score' in df_ies_final.columns:
        df_ies_final = df_ies_final.drop(columns=['completeness_score'])
    
    print(f"Após consolidação e desduplicação por NOME da IES: {len(df_ies_final):,} IES únicas")

    # 6. Limpeza final e padronização
    # Padronizar campos texto
    text_cols = [
        'sg_ies', 'des_ies', 'des_regiao', 'des_municipio_programa', 'des_status_juridico', 
        'des_dependencia_adm'
    ]
    for col in text_cols:
        if col in df_ies_final.columns:
            df_ies_final[col] = df_ies_final[col].astype(str).str.strip()

    # 7. Adicionar registro SK=0 para valores não informados
    sk0_record = pd.DataFrame([{
        'ies_sk': 0,
        'cod_entidade_capes': 0,
        'sg_ies': 'XX',
        'des_ies': 'Não informado',
        'des_regiao': 'Não informado',
        'sg_uf': 'XX',
        'des_municipio_programa': 'Não informado',
        'des_status_juridico': 'Não informado',
        'des_dependencia_adm': 'Não informado'
    }])
    
    # 8. Gerar a chave substituta (Surrogate Key)
    df_ies_final['ies_sk'] = df_ies_final.index + 1
    
    # 9. Concatenar o registro SK=0
    final_dim = pd.concat([sk0_record, df_ies_final], ignore_index=True)
    
    # 10. Reordenar e selecionar colunas finais - APENAS características INSTITUCIONAIS PURAS
    final_cols = [
        'ies_sk',                           # Chave substituta
        'cod_entidade_capes',               # Código CAPES da entidade
        'sg_ies',                           # Sigla da IES
        'des_ies',                          # Nome da IES
        
        # Características institucionais PURAS
        'des_status_juridico',              # Status jurídico
        'des_dependencia_adm',              # Dependência administrativa
        'nr_cnpj_ies',                      # CNPJ da IES
        
        # Localização geográfica
        'des_regiao',                       # Região
        'sg_uf',                           # UF
        'des_municipio_programa',           # Município
        'cod_ibge_municipio'               # Código IBGE do município
    ]
    final_dim = final_dim[[col for col in final_cols if col in final_dim.columns]]
    
    print(f"Dimensão final de IES criada com {len(final_dim):,} registros.")
    print(f"Total de atributos institucionais puros: {len(final_dim.columns)}")
    return final_dim

def save_to_postgres(df: pd.DataFrame, engine, table_name: str):
    """Salva o DataFrame final no PostgreSQL."""
    print(f"Salvando dimensão na tabela 'public.{table_name}'...")
    try:
        with engine.begin() as conn:
            # Primeiro criar a tabela com estrutura explícita
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS dim_ies (
                ies_sk INTEGER PRIMARY KEY,
                cod_entidade_capes INTEGER,
                sg_ies VARCHAR(50),
                des_ies VARCHAR(255) NOT NULL,
                des_status_juridico VARCHAR(100),
                des_dependencia_adm VARCHAR(100),
                nr_cnpj_ies VARCHAR(20),
                des_regiao VARCHAR(50),
                sg_uf VARCHAR(2),
                des_municipio_programa VARCHAR(100),
                cod_ibge_municipio INTEGER
            );
            """
            
            # Executar a criação da tabela
            conn.exec_driver_sql(create_table_sql)
            
            # Limpar tabela se já existir dados
            conn.exec_driver_sql(f"DELETE FROM {table_name};")
            
        # Inserir dados usando to_sql com engine
        df.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
        print("Dimensão salva com sucesso!")
    except Exception as e:
        print(f"ERRO: Falha ao salvar a dimensão: {e}")
        raise

def main():
    """Função principal para orquestrar a criação da dimensão."""
    TABLE_NAME = 'dim_ies'
    
    print(f"INICIANDO GERAÇÃO DA TABELA {TABLE_NAME.upper()}")
    print("=" * 60)

    try:
        project_root = get_project_root()
        
        # 1. Ler dados do MinIO
        df_raw = load_parquet_from_minio(project_root / '.env')
        
        # 2. Criar a dimensão de IES
        dim_ies = create_ies_dimension(df_raw)
        
        # 3. Conectar ao banco de dados
        engine = get_db_engine(project_root / '.env')
        
        # 4. Salvar a dimensão no PostgreSQL
        save_to_postgres(dim_ies, engine, table_name=TABLE_NAME)
        
        # 5. Exibir preview e estatísticas
        print("\nPreview da dimensão gerada:")
        print(dim_ies.head())
        
        print("\nEstatísticas da dimensão:")
        print(f"  - Total de registros (IES únicas + SK=0): {len(dim_ies):,}")
        print(f"  - Total de atributos institucionais puros: {len(dim_ies.columns)}")
        
        # Contagem por dependência administrativa
        if 'des_dependencia_adm' in dim_ies.columns:
            print(f"\n📊 Contagem por Dependência Administrativa:")
            print(dim_ies['des_dependencia_adm'].value_counts().head(10).to_string())

        # Contagem por região
        if 'des_regiao' in dim_ies.columns:
            print(f"\n📊 Contagem por Região:")
            print(dim_ies['des_regiao'].value_counts().to_string())

        # Confirmação: apenas IES brasileiras
        print(f"\n🇧🇷 Confirmação: Dimensão contém apenas IES brasileiras")
        print(f"   Total de IES únicas: {len(dim_ies)-1:,} (+ 1 SK=0)")

    except Exception as e:
        print(f"\nO processo falhou. Motivo: {e}")
    
    print("\nProcesso concluído.")

if __name__ == "__main__":
    main()
