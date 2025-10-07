import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
import sys
from dotenv import load_dotenv

# Adicionar o diretório raiz ao path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from src.validation.data_validator import validate_dimension_data, get_validation_summary
from src.core.exceptions import DimensionCreationError, DataValidationError

# Carregar variáveis de ambiente
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

def criar_dimensao_tempo(data_inicio='2013-01-01', data_fim='2027-12-31'):
    """
    Cria a dimensão tempo com granularidade diária (abordagem vetorizada).
    """
    print("Criando dimensão tempo...")

    # Geração de todas as datas no intervalo
    datas = pd.date_range(start=data_inicio, end=data_fim, freq='D')

    # Mapeamento determinístico para nomes dos dias em PT-BR
    ptbr_dias = {
        0: 'SEGUNDA-FEIRA',
        1: 'TERÇA-FEIRA',
        2: 'QUARTA-FEIRA',
        3: 'QUINTA-FEIRA',
        4: 'SEXTA-FEIRA',
        5: 'SÁBADO',
        6: 'DOMINGO'
    }

    df = pd.DataFrame({'data_ts': datas})
    df['ano'] = df['data_ts'].dt.year
    df['mes'] = df['data_ts'].dt.month
    df['dia'] = df['data_ts'].dt.day

    dow = df['data_ts'].dt.dayofweek  # 0=segunda .. 6=domingo
    df['semestre'] = (df['mes'] > 6).map({True: 2, False: 1})
    df['trimestre'] = ((df['mes'] - 1) // 3) + 1
    # Mapeamento resiliente para nome do dia e flag fim de semana
    try:
        df['dia_semana'] = dow.map(ptbr_dias)
    except Exception:
        df['dia_semana'] = dow.apply(lambda d: ptbr_dias.get(int(d)))
    # Evita uso de .map em Series booleanas em ambientes onde .isin possa retornar ndarray
    is_weekend = None
    try:
        is_weekend = dow.isin([5, 6])
    except Exception:
        is_weekend = pd.Series(np.isin(dow, [5, 6]))
    df['fim_de_semana'] = np.where(is_weekend, 'S', 'N')

    # Converter timestamp para date
    df['data'] = df['data_ts'].dt.date
    df = df.drop(columns=['data_ts'])

    # Chave substituta sequencial (0 reservado para desconhecido)
    df['tempo_sk'] = range(1, len(df) + 1)

    # Linha 0 (desconhecido)
    desconhecido = {
        'tempo_sk': 0,
        'data': None,
        'ano': None,
        'semestre': None,
        'trimestre': None,
        'mes': None,
        'dia': None,
        'dia_semana': 'DESCONHECIDO',
        'fim_de_semana': 'N'
    }
    df_tempo = pd.concat([pd.DataFrame([desconhecido]), df], ignore_index=True)
    
    # Validar dados usando o sistema de validação
    try:
        validation_results = validate_dimension_data(df_tempo, 'tempo')
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
        # Não silenciar erros desconhecidos
        raise
    
    print(f"Dimensão tempo criada com {len(df_tempo)} registros")
    print(f"Período: {data_inicio} a {data_fim}")
    
    return df_tempo

def salvar_dimensao_tempo(df_tempo):
    """
    Salva a dimensão tempo no banco de dados PostgreSQL.
    """
    try:
        # Validar dados antes de salvar
        if df_tempo.empty:
            raise DimensionCreationError("DataFrame está vazio")
        
        # Verificar se tem a coluna sk
        if 'tempo_sk' not in df_tempo.columns:
            raise DimensionCreationError("DataFrame não possui coluna 'tempo_sk' obrigatória")
        
        # Criar conexão com o banco
        url = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(url)
        
        with engine.begin() as conn:
            # Primeiro criar a tabela com estrutura explícita (usando nomes padronizados)
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS dim_tempo (
                tempo_sk INTEGER PRIMARY KEY,
                data DATE,
                ano INTEGER,
                semestre INTEGER,
                trimestre INTEGER,
                mes INTEGER,
                dia INTEGER,
                dia_semana VARCHAR(20),
                fim_de_semana VARCHAR(1),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            # Executar a criação da tabela
            conn.exec_driver_sql(create_table_sql)
            
            # Limpar tabela se já existir dados
            conn.exec_driver_sql("DELETE FROM dim_tempo;")
            
            # Inserir dados
            df_tempo.to_sql('dim_tempo', conn, if_exists='append', index=False)
            print(f"Dimensão tempo salva no PostgreSQL com {len(df_tempo)} registros")
            
    except DimensionCreationError as e:
        print(f"Erro de criação da dimensão: {e}")
        raise
    except Exception as e:
        print(f"Erro ao salvar dimensão tempo: {e}")
        raise DimensionCreationError(f"Falha ao salvar dimensão tempo: {str(e)}")

if __name__ == "__main__":
    # Criar dimensão tempo
    df_tempo = criar_dimensao_tempo()
    
    # Salvar no banco
    salvar_dimensao_tempo(df_tempo)
    
    # Mostrar algumas estatísticas
    print("\nEstatísticas da dimensão tempo:")
    print(f"Anos cobertos: {df_tempo['ano'].min()} - {df_tempo['ano'].max()}")
    print(f"Total de dias: {len(df_tempo)}")
    print(f"Dias de fim de semana: {len(df_tempo[df_tempo['fim_de_semana'] == 'S'])}")
    print(f"Dias úteis: {len(df_tempo[df_tempo['fim_de_semana'] == 'N'])}")
