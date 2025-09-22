import pandas as pd
from datetime import datetime, timedelta
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

def criar_dimensao_tempo(data_inicio='2000-01-01', data_fim='2030-12-31'):
    """
    Cria a dimensão tempo com granularidade diária.
    """
    print("🕐 Criando dimensão tempo...")
    
    # Converter strings para datetime
    inicio = datetime.strptime(data_inicio, '%Y-%m-%d')
    fim = datetime.strptime(data_fim, '%Y-%m-%d')
    
    # Lista para armazenar os dados
    dados_tempo = []
    
    # Primeiro registro: linha 0 (desconhecido/não aplicável)
    dados_tempo.append({
        'tempo_sk': 0,
        'data_completa': None,
        'ano': None,
        'semestre': None,
        'trimestre': None,
        'mes': None,
        'dia': None,
        'dia_semana': 'DESCONHECIDO',
        'fim_de_semana': 'N'
    })
    
    # Gerar dados para cada dia no período
    data_atual = inicio
    tempo_sk = 1  # Começar do 1, pois 0 é reservado
    
    while data_atual <= fim:
        # Calcular atributos da data
        ano = data_atual.year
        mes = data_atual.month
        dia = data_atual.day
        dia_semana = data_atual.weekday() + 1  # 1=Segunda, 7=Domingo
        
        # Calcular semestre
        semestre = 1 if mes <= 6 else 2
        
        # Calcular trimestre
        if mes <= 3:
            trimestre = 1
        elif mes <= 6:
            trimestre = 2
        elif mes <= 9:
            trimestre = 3
        else:
            trimestre = 4
        
        # Verificar se é fim de semana
        fim_de_semana = 'S' if dia_semana in [6, 7] else 'N'
        
        # Adicionar registro
        dados_tempo.append({
            'tempo_sk': tempo_sk,
            'data_completa': data_atual.date(),
            'ano': ano,
            'semestre': semestre,
            'trimestre': trimestre,
            'mes': mes,
            'dia': dia,
            'dia_semana': data_atual.strftime("%A").upper().replace("Ç","C").replace("Á","A").replace("É","E").replace("Í","I").replace("Ó","O").replace("Ú","U").replace("Ã","A").replace("Õ","O").replace("Ê","E").replace("Ô","O").replace("À","A").replace("QUARTA FEIRA", "QUARTA-FEIRA"),
            'fim_de_semana': ('S' if dia_semana in [6, 7] else 'N').upper()
        })
        
        # Próximo dia
        data_atual += timedelta(days=1)
        tempo_sk += 1
    
    # Criar DataFrame
    df_tempo = pd.DataFrame(dados_tempo)
    
    print(f"✅ Dimensão tempo criada com {len(df_tempo)} registros")
    print(f"📅 Período: {data_inicio} a {data_fim}")
    
    return df_tempo

def salvar_dimensao_tempo(df_tempo):
    """
    Salva a dimensão tempo no banco de dados PostgreSQL.
    """
    try:
        # Criar conexão com o banco
        url = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(url)
        
        with engine.begin() as conn:
            # Primeiro criar a tabela com estrutura explícita
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS dim_tempo (
                tempo_sk INTEGER PRIMARY KEY,
                data_completa DATE NOT NULL,
                ano INTEGER NOT NULL,
                semestre INTEGER NOT NULL,
                trimestre INTEGER NOT NULL,
                mes INTEGER NOT NULL,
                nome_mes VARCHAR(20) NOT NULL,
                dia INTEGER NOT NULL,
                dia_semana INTEGER NOT NULL,
                nome_dia_semana VARCHAR(20) NOT NULL,
                numero_semana INTEGER NOT NULL,
                dia_ano INTEGER NOT NULL,
                eh_feriado BOOLEAN DEFAULT FALSE,
                eh_fim_semana BOOLEAN DEFAULT FALSE
            );
            """
            
            # Executar a criação da tabela
            conn.exec_driver_sql(create_table_sql)
            
            # Limpar tabela se já existir dados
            conn.exec_driver_sql("DELETE FROM dim_tempo;")
            
            # Inserir dados
            df_tempo.to_sql('dim_tempo', conn, if_exists='append', index=False)
            print(f"✅ Dimensão tempo salva no PostgreSQL com {len(df_tempo)} registros")
            
    except Exception as e:
        print(f"❌ Erro ao salvar dimensão tempo: {e}")

if __name__ == "__main__":
    # Criar dimensão tempo
    df_tempo = criar_dimensao_tempo()
    
    # Salvar no banco
    salvar_dimensao_tempo(df_tempo)
    
    # Mostrar algumas estatísticas
    print("\n📊 Estatísticas da dimensão tempo:")
    print(f"Anos cobertos: {df_tempo['ano'].min()} - {df_tempo['ano'].max()}")
    print(f"Total de dias: {len(df_tempo)}")
    print(f"Dias de fim de semana: {len(df_tempo[df_tempo['fim_de_semana'] == 'S'])}")
    print(f"Dias úteis: {len(df_tempo[df_tempo['fim_de_semana'] == 'N'])}")
