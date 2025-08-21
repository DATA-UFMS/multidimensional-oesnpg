#!/usr/bin/env python3
"""
Script simplificado para criar tabela raw_tema.
Lê dados brutos da planilha macro_temas.oesnpg_v2.xlsx e adiciona mapeamento de UF.
"""

import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Configuração do banco via .env
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME") 
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

# Caminho para o arquivo Excel (absoluto)
script_dir = os.path.dirname(os.path.abspath(__file__))
EXCEL_PATH = os.path.join(script_dir, '..', 'data', 'macro_temas_oesnpg_v2.xlsx')

# Mapeamento UF
UF_MAP = {
    'ACRE': 'AC', 'ALAGOAS': 'AL', 'AMAPÁ': 'AP', 'AMAZONAS': 'AM', 'BAHIA': 'BA', 
    'CEARÁ': 'CE', 'DISTRITO FEDERAL': 'DF', 'ESPÍRITO SANTO': 'ES', 'GOIÁS': 'GO',
    'MARANHÃO': 'MA', 'MATO GROSSO': 'MT', 'MATO GROSSO DO SUL': 'MS', 'MINAS GERAIS': 'MG',
    'PARÁ': 'PA', 'PARAÍBA': 'PB', 'PARANÁ': 'PR', 'PERNAMBUCO': 'PE', 'PIAUÍ': 'PI',
    'RIO DE JANEIRO': 'RJ', 'RIO GRANDE DO NORTE': 'RN', 'RIO GRANDE DO SUL': 'RS',
    'RONDÔNIA': 'RO', 'RORAIMA': 'RR', 'SANTA CATARINA': 'SC', 'SÃO PAULO': 'SP',
    'SERGIPE': 'SE', 'TOCANTINS': 'TO'
}

def main():
    print("CRIANDO TABELA RAW_TEMA")
    print("=" * 50)
    
    # Carregar dados
    print("📥 Carregando dados...")
    df = pd.read_excel(EXCEL_PATH)
    
    # Remover colunas com valores todos nulos
    df = df.dropna(axis=1, how='all')
    
    # Remover coluna macro_tema_2_id se existir
    if 'macro_tema_2_id' in df.columns:
        df = df.drop('macro_tema_2_id', axis=1)
    
    # Ajustar nomes das colunas para padrão snake_case
    df.columns = df.columns.str.strip().str.lower().str.replace('-', '_').str.replace(' ', '_')
    
    # Renomear colunas conforme especificação (id para códigos originais, des_ para nomes/descrições)
    df = df.rename(columns={
        'macro_tema_1_id': 'id_macrotema_original',
        'macro_tema_1_label': 'des_macrotema',
        'tema': 'des_tema',
        'macro_tema_nome': 'des_macrotema',
        'palavra_chave': 'des_palavra_chave'
    })
    
    # Desnormalizar palavras-chave (expandir uma linha por palavra-chave)
    if 'des_palavra_chave' in df.columns:
        print("Desnormalizando palavras-chave...")
        # Separar palavras-chave por vírgula e criar uma linha para cada
        df_expandido = []
        for _, row in df.iterrows():
            palavras = str(row['des_palavra_chave']).split(',') if pd.notna(row['des_palavra_chave']) else ['']
            for palavra in palavras:
                nova_linha = row.copy()
                nova_linha['des_palavra_chave'] = palavra.strip()
                df_expandido.append(nova_linha)
        df = pd.DataFrame(df_expandido)
        print(f"📈 Expandido para {len(df)} registros após desnormalização")

    # Criar IDs únicos apenas para campos que não têm ID original
    print("Criando IDs únicos...")
    
    # ID para palavra-chave (baseado em valores únicos de des_palavra_chave)
    if 'des_palavra_chave' in df.columns:
        palavras_unicas = df['des_palavra_chave'].dropna().unique()
        palavra_to_id = {palavra: idx + 1 for idx, palavra in enumerate(sorted(palavras_unicas))}
        df['id_palavra_chave'] = df['des_palavra_chave'].map(palavra_to_id).fillna(0).astype(int)
    
    # Reorganizar colunas para deixar IDs próximos às suas descrições
    print("Reorganizando colunas...")
    colunas_ordenadas = []
    
    # Adicionar colunas existentes na ordem desejada
    for col in df.columns:
        if col == 'id_macrotema_original':
            colunas_ordenadas.append(col)
        elif col == 'des_macrotema':
            colunas_ordenadas.append(col)
        elif col == 'des_tema':
            colunas_ordenadas.append(col)
            if 'id_tema' in df.columns:
                colunas_ordenadas.append('id_tema')
        elif col == 'des_palavra_chave':
            colunas_ordenadas.append(col)
            if 'id_palavra_chave' in df.columns:
                colunas_ordenadas.append('id_palavra_chave')
        elif col not in ['id_tema', 'id_palavra_chave']:
            colunas_ordenadas.append(col)
    
    # Reorganizar DataFrame
    df = df[colunas_ordenadas]

    # Limpar strings
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.strip()
    
    # Se existe coluna UF, normalizar e adicionar sigla ao lado
    if 'uf' in df.columns:
        df['uf'] = df['uf'].str.upper()
        df['uf_sigla'] = df['uf'].map(UF_MAP).fillna('XX')
        # Reorganizar para ter sigla ao lado da UF
        cols = list(df.columns)
        uf_idx = cols.index('uf')
        cols.insert(uf_idx + 1, cols.pop(cols.index('uf_sigla')))
        df = df[cols]
    
    # Reorganizar colunas para que códigos fiquem próximos às descrições
    colunas_ordenadas = []
    for col in df.columns:
        colunas_ordenadas.append(col)
    
    # Reordenar especificamente para tema, macrotema e palavra-chave
    colunas_finais = []
    for col in colunas_ordenadas:
        if col not in ['cod_tema', 'cod_macrotema_gerado', 'cod_palavra_chave']:
            colunas_finais.append(col)
            # Adicionar código correspondente após a descrição
            if col == 'des_tema' and 'cod_tema' in df.columns:
                colunas_finais.append('cod_tema')
            elif col == 'des_macrotema' and 'cod_macrotema_gerado' in df.columns:
                colunas_finais.append('cod_macrotema_gerado')
            elif col == 'des_palavra_chave' and 'cod_palavra_chave' in df.columns:
                colunas_finais.append('cod_palavra_chave')
    
    df = df[colunas_finais]
    
    # Remover duplicatas
    df = df.drop_duplicates()
    
    print(f"Dados processados: {len(df)} registros")
    
    # Salvar no banco usando pandas to_sql
    print("Salvando no PostgreSQL...")
    
    # Criar conexão SQLAlchemy para usar com to_sql
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    
    # Usar to_sql do pandas
    df.to_sql('raw_tema', engine, if_exists='replace', index=False, method='multi')
    
    print(f"✅ Tabela raw_tema criada com {len(df)} registros")
    print(f"🎉 Processo concluído!")

if __name__ == "__main__":
    main()
