#!/usr/bin/env python3
"""
Script unificado para criação da tabela raw_ies.
Combina dados de IES da API CAPES e arquivos CSV locais.
"""

import pandas as pd
import os
from base_raw import CAPESApiExtractor, DatabaseManager, DataQualityAnalyzer, DataCleaner, print_header, print_status, print_summary

# Resource ID da API CAPES para IES
IES_RESOURCE_ID = '62f82787-3f45-4b9e-8457-3366f60c264b'

def load_ies_api_data():
    """Carrega dados de IES da API CAPES"""
    print_status("Extraindo dados de IES da API CAPES...")
    
    extractor = CAPESApiExtractor()
    df = extractor.fetch_all_data(IES_RESOURCE_ID)
    
    if not df.empty:
        df['fonte_dados'] = 'API_CAPES'
        print_status(f"API CAPES: {len(df):,} registros extraídos", "success")
    
    return df

def load_ies_csv_data():
    """Carrega dados de IES de arquivos CSV locais"""
    print_status("Carregando dados de IES de arquivos CSV...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, '..', '..', 'seeds')
    
    csv_files = ['municipios.csv', 'tabela de codigos UF e Regiao IBGE.xlsx']
    all_csv_data = []
    
    for file in csv_files:
        file_path = os.path.join(data_dir, file)
        if os.path.exists(file_path):
            try:
                if file.endswith('.xlsx'):
                    df = pd.read_excel(file_path)
                else:
                    df = pd.read_csv(file_path, encoding='utf-8')
                
                df['fonte_dados'] = f'CSV_{file.split(".")[0].upper()}'
                all_csv_data.append(df)
                print_status(f"CSV {file}: {len(df):,} registros", "success")
                
            except Exception as e:
                print_status(f"Erro ao carregar {file}: {e}", "warning")
    
    if all_csv_data:
        return pd.concat(all_csv_data, ignore_index=True)
    else:
        return pd.DataFrame()

def transform_ies_data(df):
    """Transforma dados de IES"""
    print_status("Transformando dados de IES...")
    
    # Aplicar limpeza básica
    df = DataCleaner.clean_dataframe(df)
    
    # Padronizar campos específicos de IES
    ies_field_mapping = {
        'nm_entidade_ensino': 'des_ies_nome',
        'nm_ies': 'des_ies_nome',
        'sg_uf_programa': 'sg_uf',
        'sg_uf_ies': 'sg_uf',
        'nm_regiao': 'des_regiao',
        'nm_municipio_programa_ies': 'des_municipio',
        'nm_municipio': 'des_municipio',
        'ds_dependencia_administrativa': 'des_dependencia_administrativa',
        'cd_ies': 'cod_ies'
    }
    
    for old_col, new_col in ies_field_mapping.items():
        matching_cols = [col for col in df.columns if old_col in col.lower()]
        if matching_cols:
            df[new_col] = df[matching_cols[0]]
    
    # Criar ID único se não existir
    if 'id_ies' not in df.columns and 'cod_ies' in df.columns:
        df['id_ies'] = df['cod_ies']
    elif 'id_ies' not in df.columns:
        df['id_ies'] = range(1, len(df) + 1)
    
    return df

def main():
    print_header("Criando Tabela RAW_IES")
    
    all_data = []
    
    # Carregar dados da API
    df_api = load_ies_api_data()
    if not df_api.empty:
        all_data.append(df_api)
    
    # Carregar dados de CSV
    df_csv = load_ies_csv_data()
    if not df_csv.empty:
        all_data.append(df_csv)
    
    if not all_data:
        print_status("Nenhum dado carregado. Processo encerrado.", "error")
        return
    
    # Consolidar todos os dados
    df_consolidated = pd.concat(all_data, ignore_index=True)
    print_status(f"Dados consolidados: {len(df_consolidated):,} registros")
    
    # Transformar dados
    df_transformed = transform_ies_data(df_consolidated)
    
    # Analisar qualidade
    DataQualityAnalyzer.analyze_dataframe(df_transformed, "RAW_IES")
    
    # Salvar no banco
    db = DatabaseManager()
    success = db.save_dataframe(df_transformed, 'raw_ies')
    
    if success:
        print_summary(len(df_transformed), 'raw_ies')
    else:
        print_status("Falha ao salvar dados no banco", "error")

if __name__ == "__main__":
    main()
