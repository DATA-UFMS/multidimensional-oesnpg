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
CSV_PATH = os.getenv("CSV_PATH")

# Caminho para o Excel
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
EXCEL_PATH = os.path.join(project_root, CSV_PATH, "curadoria_temas.xlsx")

def carregar_temas():
    df = pd.read_excel(EXCEL_PATH, sheet_name=0)
    df.columns = df.columns.str.strip().str.lower().str.replace('-', '_').str.replace(' ', '_')
    df = df.rename(columns={'tema': 'nome_tema', 'uf': 'nome_uf'})
    print(f"📥 Carregando temas do arquivo: {df.columns.tolist()}")
    df = df.drop_duplicates()
    
    # Criar uma tabela de temas únicos para gerar IDs
    temas_unicos = df[['nome_tema', 'nome_uf']].drop_duplicates().reset_index(drop=True)
    
    # Criar ID único para cada combinação tema + UF
    temas_unicos['tema_id'] = range(1, len(temas_unicos) + 1)
    
    print(f"📊 Mapeamento de temas:")
    print(f"   Registros originais: {len(df)}")
    print(f"   Temas únicos: {len(temas_unicos)}")
    
    # Fazer merge para trazer o tema_id de volta ao dataframe original
    df = df.merge(temas_unicos, on=['nome_tema', 'nome_uf'], how='left')
    
    # Adicionar registro 0 (desconhecido/não aplicável)
    registro_desconhecido = pd.DataFrame({
        'tema_id': [0],
        'nome_tema': ['Desconhecido'],
        'nome_uf': ['Desconhecido'],
        'id': [0],
        'palavra_chave': ['Desconhecido']
    })
    
    # Concatenar registro desconhecido com dados reais
    df_final = pd.concat([registro_desconhecido, df], ignore_index=True)
    
    # Adicionar surrogate key (começando do 0)
    df_final.insert(0, 'tema_sk', range(0, len(df_final)))
    
    # Reordenar colunas para melhor legibilidade
    df_final = df_final[['tema_sk', 'tema_id', 'id', 'nome_tema', 'nome_uf', 'palavra_chave']]
    
    print(f"📊 Resultado final:")
    print(f"   Total de registros: {len(df_final)}")
    print(f"   Tema IDs únicos: {df_final['tema_id'].nunique()}")
    print(f"   Temas únicos: {df_final['nome_tema'].nunique()}")
    
    return df_final


def salvar_dimensao_tema(df_tema):
    """
    Salva a dimensão tema no banco de dados PostgreSQL.
    """
    try:
        # Criar conexão com o banco
        url = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(url)
        
        # Salvar no banco - usando DROP CASCADE para remover dependências
        with engine.begin() as conn:
            from sqlalchemy import text
            
            # Verificar se a tabela existe
            result = conn.execute(text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dim_tema')"))
            table_exists = result.fetchone()[0]
            
            if table_exists:
                # Se existe, fazer DROP CASCADE para remover dependências
                conn.execute(text("DROP TABLE IF EXISTS dim_tema CASCADE"))
                print("⚠️ Tabela dim_tema removida com CASCADE (dependências também foram removidas)")
            
            # Criar nova tabela
            df_tema.to_sql('dim_tema', conn, if_exists='replace', index=False)
            print(f"✅ Dimensão tema criada no PostgreSQL com {len(df_tema)} registros")
            
    except Exception as e:
        print(f"❌ Erro ao salvar dimensão tema: {e}")

if __name__ == "__main__":
    # Criar dimensão tema
    df_tema = carregar_temas()
    
    # Salvar no banco
    salvar_dimensao_tema(df_tema)
    
    # Mostrar estatísticas
    print("\n📊 Estatísticas da dimensão tema:")
    print(f"Total de temas: {len(df_tema)}")
    print(f"UFs únicas: {df_tema['nome_uf'].nunique()}")
    print(f"Primeiro registro (SK=0): {df_tema.loc[0, 'nome_tema']}")

