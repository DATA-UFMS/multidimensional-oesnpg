#!/usr/bin/env python3
"""
Script para criação da tabela raw_docente.
Extrai dados de docentes da API CAPES e arquivos CSV.
"""

import pandas as pd
import os
from base_raw import CAPESApiExtractor, DatabaseManager, DataQualityAnalyzer, DataCleaner, print_header, print_status, print_summary

# Resource ID da API CAPES para docentes
DOCENTE_RESOURCE_ID = '7d9547c8-9a0d-433a-b2c8-ee9fbbdc5b3a'

def load_docente_api_data():
    """Carrega dados de docentes da API CAPES"""
    print_status("Extraindo dados de docentes da API CAPES...")
    
    extractor = CAPESApiExtractor()
    df = extractor.fetch_all_data(DOCENTE_RESOURCE_ID)
    
    if not df.empty:
        df['fonte_dados'] = 'API_CAPES'
        print_status(f"API CAPES: {len(df):,} registros extraídos", "success")
    
    return df

def load_docente_csv_data():
    """Carrega dados de docentes de arquivo CSV local"""
    print_status("Carregando dados de docentes de arquivo CSV...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, '..', 'data')
    csv_path = os.path.join(data_dir, 'br-capes-colsucup-docente-2021-2025-03-31.csv')
    
    if not os.path.exists(csv_path):
        print_status(f"Arquivo CSV não encontrado: {os.path.basename(csv_path)}", "warning")
        return pd.DataFrame()
    
    try:
        # Tentar diferentes encodings
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                df = pd.read_csv(csv_path, encoding=encoding)
                df['fonte_dados'] = 'CSV_LOCAL'
                print_status(f"CSV local: {len(df):,} registros carregados", "success")
                return df
            except UnicodeDecodeError:
                continue
        
        print_status("Erro: não foi possível decodificar o arquivo CSV", "error")
        return pd.DataFrame()
        
    except Exception as e:
        print_status(f"Erro ao carregar CSV: {e}", "error")
        return pd.DataFrame()

def transform_docente_data(df):
    """Transforma dados de docentes"""
    print_status("Transformando dados de docentes...")
    
    # Aplicar limpeza básica
    df = DataCleaner.clean_dataframe(df)
    
    # Padronizar campos específicos de docentes
    docente_field_mapping = {
        'nm_docente': 'des_docente_nome',
        'nm_pessoa': 'des_docente_nome',
        'id_lattes': 'id_lattes',
        'id_pessoa': 'id_pessoa_capes',
        'nm_programa': 'des_programa',
        'nm_ies': 'des_ies',
        'sg_uf_ies': 'sg_uf',
        'nm_regiao': 'des_regiao',
        'nm_area_avaliacao': 'des_area_avaliacao',
        'cd_programa': 'cod_programa',
        'cd_ies': 'cod_ies'
    }
    
    for old_col, new_col in docente_field_mapping.items():
        matching_cols = [col for col in df.columns if old_col in col.lower()]
        if matching_cols:
            df[new_col] = df[matching_cols[0]]
    
    # Limpar e padronizar ID Lattes
    if 'id_lattes' in df.columns:
        df['id_lattes'] = df['id_lattes'].astype(str).str.replace(r'[^\d]', '', regex=True)
        df.loc[df['id_lattes'].str.len() == 0, 'id_lattes'] = None
    
    # Criar ID único se não existir
    if 'id_docente' not in df.columns and 'id_pessoa_capes' in df.columns:
        df['id_docente'] = df['id_pessoa_capes']
    elif 'id_docente' not in df.columns:
        df['id_docente'] = range(1, len(df) + 1)
    
    return df

def main():
    print_header("Criando Tabela RAW_DOCENTE")
    
    all_data = []
    
    # Carregar dados da API
    df_api = load_docente_api_data()
    if not df_api.empty:
        all_data.append(df_api)
    
    # Carregar dados de CSV
    df_csv = load_docente_csv_data()
    if not df_csv.empty:
        all_data.append(df_csv)
    
    if not all_data:
        print_status("Nenhum dado carregado. Processo encerrado.", "error")
        return
    
    # Consolidar todos os dados
    df_consolidated = pd.concat(all_data, ignore_index=True)
    print_status(f"Dados consolidados: {len(df_consolidated):,} registros")
    
    # Transformar dados
    df_transformed = transform_docente_data(df_consolidated)
    
    # Analisar qualidade
    DataQualityAnalyzer.analyze_dataframe(df_transformed, "RAW_DOCENTE")
    
    # Salvar no banco
    db = DatabaseManager()
    success = db.save_dataframe(df_transformed, 'raw_docente')
    
    if success:
        print_summary(len(df_transformed), 'raw_docente')
    else:
        print_status("Falha ao salvar dados no banco", "error")

if __name__ == "__main__":
    main()
