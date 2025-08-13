import pandas as pd
from sqlalchemy import create_engine
import os
import sys
from dotenv import load_dotenv

# Adicionar diretório raiz ao path para imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from src.core.core import salvar_df_bd

# Carregar variáveis de ambiente
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")
CSV_PATH = os.getenv("CSV_PATH")

# Caminho para o Excel
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
EXCEL_PATH = os.path.join(project_root, CSV_PATH, "curadoria_temas.xlsx")

# Mapeamento de nome completo da UF para sigla
MAPEAMENTO_UF = {
    'ACRE': 'AC',
    'ALAGOAS': 'AL',
    'AMAPÁ': 'AP',
    'AMAZONAS': 'AM',
    'BAHIA': 'BA',
    'CEARÁ': 'CE',
    'DISTRITO FEDERAL': 'DF',
    'ESPÍRITO SANTO': 'ES',
    'GOIÁS': 'GO',
    'MARANHÃO': 'MA',
    'MATO GROSSO': 'MT',
    'MATO GROSSO DO SUL': 'MS',
    'MINAS GERAIS': 'MG',
    'PARÁ': 'PA',
    'PARAÍBA': 'PB',
    'PARANÁ': 'PR',
    'PERNAMBUCO': 'PE',
    'PIAUÍ': 'PI',
    'RIO DE JANEIRO': 'RJ',
    'RIO GRANDE DO NORTE': 'RN',
    'RIO GRANDE DO SUL': 'RS',
    'RONDÔNIA': 'RO',
    'RORAIMA': 'RR',
    'SANTA CATARINA': 'SC',
    'SÃO PAULO': 'SP',
    'SERGIPE': 'SE',
    'TOCANTINS': 'TO'
}

def carregar_temas():
    """
    Carrega temas do arquivo Excel e inclui sigla da UF.
    """
    print("📚 Carregando temas do arquivo Excel...")
    
    try:
        # Verificar se o arquivo existe
        if not os.path.exists(EXCEL_PATH):
            print(f"❌ Arquivo não encontrado: {EXCEL_PATH}")
            return pd.DataFrame()
        
        # Carregar dados do Excel
        df = pd.read_excel(EXCEL_PATH, sheet_name=0)
        df.columns = df.columns.str.strip().str.lower().str.replace('-', '_').str.replace(' ', '_')
        df = df.rename(columns={'tema': 'nome_tema', 'uf': 'nome_uf'})
        
        print(f"📥 Colunas no arquivo: {df.columns.tolist()}")
        print(f"📊 Registros carregados: {len(df)}")
        
        # Normalizar nome da UF para uppercase
        if 'nome_uf' in df.columns:
            df['nome_uf'] = df['nome_uf'].astype(str).str.strip().str.upper()
        
        # Adicionar sigla da UF baseada no mapeamento
        df['sigla_uf'] = df['nome_uf'].map(MAPEAMENTO_UF)
        
        # Verificar UFs que não foram mapeadas
        ufs_nao_mapeadas = df[df['sigla_uf'].isna()]['nome_uf'].unique()
        if len(ufs_nao_mapeadas) > 0:
            print(f"⚠️ UFs não mapeadas encontradas: {list(ufs_nao_mapeadas)}")
            # Preencher com 'XX' para UFs não mapeadas
            df['sigla_uf'] = df['sigla_uf'].fillna('XX')
        
        # Remover duplicatas
        df = df.drop_duplicates()
        
        # Criar uma tabela de temas únicos para gerar IDs
        # Agora incluindo sigla_uf na unicidade
        colunas_unicidade = ['nome_tema', 'nome_uf', 'sigla_uf']
        temas_unicos = df[colunas_unicidade].drop_duplicates().reset_index(drop=True)
        
        # Criar ID único para cada combinação tema + UF
        temas_unicos['tema_id'] = range(1, len(temas_unicos) + 1)
        
        print(f"📊 Mapeamento de temas:")
        print(f"   Registros originais: {len(df)}")
        print(f"   Temas únicos: {len(temas_unicos)}")
        print(f"   UFs únicas: {df['nome_uf'].nunique()}")
        print(f"   Siglas UF únicas: {df['sigla_uf'].nunique()}")
        
        # Fazer merge para trazer o tema_id de volta ao dataframe original
        df = df.merge(temas_unicos, on=colunas_unicidade, how='left')
        
        # Adicionar registro SK=0 (desconhecido/não aplicável)
        registro_sk0 = pd.DataFrame({
            'tema_id': [0],
            'nome_tema': ['DESCONHECIDO'],
            'nome_uf': ['DESCONHECIDO'],
            'sigla_uf': ['XX'],
            'id': [0],
            'palavra_chave': ['DESCONHECIDO']
        })
        
        # Concatenar registro desconhecido com dados reais
        df_final = pd.concat([registro_sk0, df], ignore_index=True)
        
        # Adicionar surrogate key (começando do 0)
        df_final.insert(0, 'tema_sk', range(0, len(df_final)))
        
        # Reordenar colunas para melhor legibilidade (incluindo sigla_uf)
        colunas_ordenadas = ['tema_sk', 'tema_id', 'id', 'nome_tema', 'nome_uf', 'sigla_uf', 'palavra_chave']
        # Verificar quais colunas existem antes de reordenar
        colunas_existentes = [col for col in colunas_ordenadas if col in df_final.columns]
        df_final = df_final[colunas_existentes]
        
        print(f"📊 Resultado final:")
        print(f"   Total de registros: {len(df_final)}")
        print(f"   Tema IDs únicos: {df_final['tema_id'].nunique()}")
        print(f"   Temas únicos: {df_final['nome_tema'].nunique()}")
        print(f"   UFs com sigla: {df_final[df_final['sigla_uf'] != 'XX']['sigla_uf'].nunique()}")
        
        return df_final
        
    except Exception as e:
        print(f"❌ Erro ao carregar temas: {e}")
        return pd.DataFrame()


def salvar_dimensao_tema(df_tema):
    """
    Salva a dimensão tema no banco de dados PostgreSQL.
    """
    try:
        # Usar a função salvar_df_bd que é mais robusta
        salvar_df_bd(df_tema, 'dim_tema')
        print(f"✅ Dimensão tema salva no PostgreSQL com {len(df_tema)} registros")
            
    except Exception as e:
        print(f"❌ Erro ao salvar dimensão tema: {e}")

if __name__ == "__main__":
    print("🚀 Iniciando processo de criação da dimensão Tema")
    print("📚 Fonte de dados: Excel curadoria_temas.xlsx")
    
    # Criar dimensão tema
    df_tema = carregar_temas()
    
    if df_tema.empty:
        print("❌ Nenhum dado foi retornado. Encerrando o script.")
        exit(1)
    
    # Salvar no banco
    salvar_dimensao_tema(df_tema)
    
    # Mostrar estatísticas detalhadas
    print("\n📊 Estatísticas da dimensão Tema:")
    print(f"Total de registros: {len(df_tema):,}")
    print(f"Temas únicos: {df_tema['nome_tema'].nunique():,}")
    print(f"UFs únicas: {df_tema['nome_uf'].nunique():,}")
    print(f"Primeiro registro (SK=0): {df_tema.loc[0, 'nome_tema']}")
    
    # Mostrar distribuição por UF
    if 'sigla_uf' in df_tema.columns:
        print(f"\n📍 Distribuição de temas por UF:")
        uf_counts = df_tema[df_tema['tema_sk'] != 0]['sigla_uf'].value_counts()
        for uf, count in uf_counts.items():
            nome_uf = df_tema[df_tema['sigla_uf'] == uf]['nome_uf'].iloc[0]
            print(f"  {uf} ({nome_uf}): {count:,} temas")
    
    # Mostrar alguns temas de exemplo
    if len(df_tema) > 1:
        print(f"\n� Amostra de temas (excluindo SK=0):")
        sample_df = df_tema[df_tema['tema_sk'] != 0].head(5)
        for _, row in sample_df.iterrows():
            print(f"  {row['tema_sk']} - {row['nome_tema']} ({row['sigla_uf']})")
    
    print(f"\n✅ Processo concluído! Dimensão Tema criada com sigla da UF.")

