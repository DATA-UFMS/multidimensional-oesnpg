import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

def criar_dimensao_localidade():
    """
    Cria a dimensão localidade com dados dos estados brasileiros.
    """
    print("🗺️ Criando dimensão localidade...")
    
    # Primeiro, criar registro 0 (desconhecido/não aplicável)
    registro_desconhecido = {
        'uf': 'XX', 
        'nome_uf': 'DESCONHECIDO', 
        'sigla_uf': 'XX', 
        'regiao': 'DESCONHECIDO', 
        'latitude': None, 
        'longitude': None
    }
    
    # Dados dos estados brasileiros
    estados_brasil = [
        registro_desconhecido,  # Registro 0
        {'uf': 'AC', 'nome_uf': 'ACRE', 'sigla_uf': 'AC', 'regiao': 'NORTE', 'latitude': -8.77, 'longitude': -70.55},
        {'uf': 'AL', 'nome_uf': 'ALAGOAS', 'sigla_uf': 'AL', 'regiao': 'NORDESTE', 'latitude': -9.71, 'longitude': -35.73},
        {'uf': 'AP', 'nome_uf': 'AMAPÁ', 'sigla_uf': 'AP', 'regiao': 'NORTE', 'latitude': 1.41, 'longitude': -51.77},
        {'uf': 'AM', 'nome_uf': 'AMAZONAS', 'sigla_uf': 'AM', 'regiao': 'NORTE', 'latitude': -3.07, 'longitude': -61.66},
        {'uf': 'BA', 'nome_uf': 'BAHIA', 'sigla_uf': 'BA', 'regiao': 'NORDESTE', 'latitude': -12.96, 'longitude': -38.51},
        {'uf': 'CE', 'nome_uf': 'CEARÁ', 'sigla_uf': 'CE', 'regiao': 'NORDESTE', 'latitude': -3.71, 'longitude': -38.54},
        {'uf': 'DF', 'nome_uf': 'DISTRITO FEDERAL', 'sigla_uf': 'DF', 'regiao': 'CENTRO-OESTE', 'latitude': -15.83, 'longitude': -47.86},
        {'uf': 'ES', 'nome_uf': 'ESPÍRITO SANTO', 'sigla_uf': 'ES', 'regiao': 'SUDESTE', 'latitude': -19.19, 'longitude': -40.34},
        {'uf': 'GO', 'nome_uf': 'GOIÁS', 'sigla_uf': 'GO', 'regiao': 'CENTRO-OESTE', 'latitude': -16.64, 'longitude': -49.31},
        {'uf': 'MA', 'nome_uf': 'MARANHÃO', 'sigla_uf': 'MA', 'regiao': 'NORDESTE', 'latitude': -2.55, 'longitude': -44.30},
        {'uf': 'MT', 'nome_uf': 'MATO GROSSO', 'sigla_uf': 'MT', 'regiao': 'CENTRO-OESTE', 'latitude': -12.64, 'longitude': -55.42},
        {'uf': 'MS', 'nome_uf': 'MATO GROSSO DO SUL', 'sigla_uf': 'MS', 'regiao': 'CENTRO-OESTE', 'latitude': -20.51, 'longitude': -54.54},
        {'uf': 'MG', 'nome_uf': 'MINAS GERAIS', 'sigla_uf': 'MG', 'regiao': 'SUDESTE', 'latitude': -18.10, 'longitude': -44.38},
        {'uf': 'PA', 'nome_uf': 'PARÁ', 'sigla_uf': 'PA', 'regiao': 'NORTE', 'latitude': -5.53, 'longitude': -52.29},
        {'uf': 'PB', 'nome_uf': 'PARAÍBA', 'sigla_uf': 'PB', 'regiao': 'NORDESTE', 'latitude': -7.06, 'longitude': -35.55},
        {'uf': 'PR', 'nome_uf': 'PARANÁ', 'sigla_uf': 'PR', 'regiao': 'SUL', 'latitude': -24.89, 'longitude': -51.55},
        {'uf': 'PE', 'nome_uf': 'PERNAMBUCO', 'sigla_uf': 'PE', 'regiao': 'NORDESTE', 'latitude': -8.28, 'longitude': -35.07},
        {'uf': 'PI', 'nome_uf': 'PIAUÍ', 'sigla_uf': 'PI', 'regiao': 'NORDESTE', 'latitude': -8.28, 'longitude': -43.68},
        {'uf': 'RJ', 'nome_uf': 'RIO DE JANEIRO', 'sigla_uf': 'RJ', 'regiao': 'SUDESTE', 'latitude': -22.84, 'longitude': -43.15},
        {'uf': 'RN', 'nome_uf': 'RIO GRANDE DO NORTE', 'sigla_uf': 'RN', 'regiao': 'NORDESTE', 'latitude': -5.22, 'longitude': -36.52},
        {'uf': 'RS', 'nome_uf': 'RIO GRANDE DO SUL', 'sigla_uf': 'RS', 'regiao': 'SUL', 'latitude': -30.01, 'longitude': -51.22},
        {'uf': 'RO', 'nome_uf': 'RONDÔNIA', 'sigla_uf': 'RO', 'regiao': 'NORTE', 'latitude': -11.22, 'longitude': -62.80},
        {'uf': 'RR', 'nome_uf': 'RORAIMA', 'sigla_uf': 'RR', 'regiao': 'NORTE', 'latitude': 1.89, 'longitude': -61.22},
        {'uf': 'SC', 'nome_uf': 'SANTA CATARINA', 'sigla_uf': 'SC', 'regiao': 'SUL', 'latitude': -27.33, 'longitude': -49.44},
        {'uf': 'SP', 'nome_uf': 'SÃO PAULO', 'sigla_uf': 'SP', 'regiao': 'SUDESTE', 'latitude': -23.55, 'longitude': -46.64},
        {'uf': 'SE', 'nome_uf': 'SERGIPE', 'sigla_uf': 'SE', 'regiao': 'NORDESTE', 'latitude': -10.90, 'longitude': -37.07},
        {'uf': 'TO', 'nome_uf': 'TOCANTINS', 'sigla_uf': 'TO', 'regiao': 'NORTE', 'latitude': -10.25, 'longitude': -48.25}
    ]
    
    # Criar DataFrame
    df_localidade = pd.DataFrame(estados_brasil)
    
    # Adicionar surrogate key (começando do 0)
    df_localidade.insert(0, 'localidade_sk', range(0, len(df_localidade)))
    
    print(f"✅ Dimensão localidade criada com {len(df_localidade)} registros")
    
    return df_localidade

def salvar_dimensao_localidade(df_localidade):
    """
    Salva a dimensão localidade no banco de dados PostgreSQL.
    """
    try:
        # Criar conexão com o banco
        url = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(url)
        
        # Salvar no banco
        print("📝 Salvando no banco...")
        with engine.begin() as conn:
            df_localidade.to_sql('dim_localidade', conn, if_exists='replace', index=False, schema='public')
        print(f"✅ Dimensão localidade salva no PostgreSQL com {len(df_localidade)} registros")
            
    except Exception as e:
        print(f"❌ Erro ao salvar dimensão localidade: {e}")

if __name__ == "__main__":
    # Criar dimensão localidade
    df_localidade = criar_dimensao_localidade()
    
    # Salvar no banco
    salvar_dimensao_localidade(df_localidade)
    
    # Mostrar algumas estatísticas
    print("\n📊 Estatísticas da dimensão localidade:")
    print(f"Total de UFs: {len(df_localidade)}")
    print(f"Regiões: {df_localidade['regiao'].unique()}")
    print(f"Estados por região:")
    for regiao in df_localidade['regiao'].unique():
        count = len(df_localidade[df_localidade['regiao'] == regiao])
        print(f"  {regiao}: {count} estados")

