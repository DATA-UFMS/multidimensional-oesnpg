#!/usr/bin/env python3
"""
Script para criação da tabela raw_pq.
Processa dados de bolsas de Produtividade em Pesquisa (PQ) do arquivo CSV.
"""

import pandas as pd
import os
import unicodedata
import re
from base_raw import DatabaseManager, DataQualityAnalyzer, DataCleaner, print_header, print_status, print_summary

def load_pq_data():
    """Carrega dados do arquivo CSV de PQ"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, '..', 'data')
    csv_path = os.path.join(data_dir, 'Planilha_Mapa_Fomento_PQ.xlsx - Sheet 1.csv')
    
    print_status(f"Carregando arquivo: {os.path.basename(csv_path)}")
    
    if not os.path.exists(csv_path):
        print_status(f"Arquivo não encontrado: {csv_path}", "error")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
        print_status(f"Dados carregados: {len(df):,} registros")
        return df
    except Exception as e:
        print_status(f"Erro ao carregar arquivo: {e}", "error")
        return pd.DataFrame()

def normalize_string(s):
    """Remove acentos e caracteres especiais, converte para snake_case"""
    if pd.isna(s) or s == '':
        return s
    
    s = unicodedata.normalize('NFKD', str(s))
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'[^\w\s]', '', s)
    s = re.sub(r'\s+', '_', s.strip())
    return s.lower()

def extract_institution_info(institution_text):
    """Extrai nome e sigla da instituição"""
    if pd.isna(institution_text) or str(institution_text).strip() == '':
        return None, None
    
    text = str(institution_text).strip()
    
    # Padrões comuns de separação
    patterns = [
        r'^(.+?)\s+([A-Z]{2,}(?:-[A-Z]{2,})?)$',  # Nome + Sigla
        r'^(.+?)\s+\(([^)]+)\)$',  # Nome (Sigla)
        r'^(.+?)\s+-\s+([A-Z]{2,}(?:-[A-Z]{2,})?)$'  # Nome - Sigla
    ]
    
    for pattern in patterns:
        match = re.match(pattern, text)
        if match:
            nome = match.group(1).strip()
            sigla = match.group(2).strip()
            return nome, sigla
    
    return text, None

def standardize_pq_columns(df):
    """Padroniza nomes das colunas do dataset PQ"""
    column_mapping = {
        '# id lattes': 'id_lattes_original',
        '# nome beneficiário': 'des_beneficiario',
        '# nome país': 'des_pais',
        '# nome região': 'des_regiao',
        '# nome uf': 'des_uf',
        '# nome cidade': 'des_municipio',
        '# nome grande área': 'des_grande_area',
        '# nome área': 'des_area',
        '# nome sub-área': 'des_sub_area',
        '# cod modalidade': 'cod_modalidade',
        '# cod categoria nível': 'cod_categoria_nivel',
        '# nome instituto': 'des_instituto',
        '# data iníc io processo': 'dt_inicio_processo',
        '# data término processo': 'dt_termino_processo'
    }
    
    # Normalizar nomes das colunas
    df.columns = df.columns.str.strip().str.lower()
    df.columns = [normalize_string(col) for col in df.columns]
    
    # Aplicar mapeamento
    df = df.rename(columns=column_mapping)
    
    return df

def transform_pq_data(df):
    """Transforma dados de PQ"""
    print_status("Transformando dados de PQ...")
    
    # Padronizar colunas
    df = standardize_pq_columns(df)
    
    # Aplicar limpeza básica
    df = DataCleaner.clean_dataframe(df)
    
    # Criar ID Lattes limpo
    if 'id_lattes_original' in df.columns:
        df['id_lattes'] = df['id_lattes_original'].astype(str).str.replace(r'[^\d]', '', regex=True)
        df.loc[df['id_lattes'].str.len() == 0, 'id_lattes'] = None
    
    # Extrair informações da instituição
    if 'des_instituto' in df.columns:
        df[['des_instituicao_nome', 'des_instituicao_sigla']] = df['des_instituto'].apply(
            lambda x: pd.Series(extract_institution_info(x))
        )
    
    # Converter datas
    date_columns = ['dt_inicio_processo', 'dt_termino_processo']
    for col in date_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            except Exception:
                print_status(f"Não foi possível converter {col} para data", "warning")
    
    return df

def main():
    print_header("Criando Tabela RAW_PQ")
    
    # Carregar dados
    df = load_pq_data()
    if df.empty:
        print_status("Nenhum dado carregado. Processo encerrado.", "error")
        return
    
    # Transformar dados
    df_transformed = transform_pq_data(df)
    
    # Analisar qualidade
    DataQualityAnalyzer.analyze_dataframe(df_transformed, "RAW_PQ")
    
    # Salvar no banco
    db = DatabaseManager()
    success = db.save_dataframe(df_transformed, 'raw_pq')
    
    if success:
        print_summary(len(df_transformed), 'raw_pq')
    else:
        print_status("Falha ao salvar dados no banco", "error")

if __name__ == "__main__":
    main()
