#!/usr/bin/env python3
"""
Script para criação da tabela raw_ppg.
Processa dados de Programas de Pós-Graduação do arquivo CSV.
"""

import pandas as pd
import os
from base_raw import DatabaseManager, DataQualityAnalyzer, DataCleaner, print_header, print_status, print_summary

def load_ppg_data():
    """Carrega dados do arquivo CSV de PPG"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, '..', 'data')
    csv_path = os.path.join(data_dir, 'ppg_2024.csv')
    
    print_status(f"Carregando arquivo: {os.path.basename(csv_path)}")
    
    if not os.path.exists(csv_path):
        print_status(f"Arquivo não encontrado: {csv_path}", "error")
        return pd.DataFrame()
    
    try:
        # Tentar diferentes encodings e separadores
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                # Primeiro tentar com separador padrão
                df = pd.read_csv(csv_path, encoding=encoding)
                print_status(f"Dados carregados com encoding {encoding}: {len(df):,} registros")
                return df
            except Exception:
                # Tentar com separador ; 
                try:
                    df = pd.read_csv(csv_path, encoding=encoding, sep=';')
                    print_status(f"Dados carregados com encoding {encoding} e sep=';': {len(df):,} registros")
                    return df
                except Exception:
                    continue
        
        print_status("Erro: não foi possível decodificar o arquivo com os encodings testados", "error")
        return pd.DataFrame()
        
    except Exception as e:
        print_status(f"Erro ao carregar arquivo: {e}", "error")
        return pd.DataFrame()

def transform_ppg_data(df):
    """Transforma dados de PPG"""
    print_status("Transformando dados de PPG...")
    
    # Aplicar limpeza básica
    df = DataCleaner.clean_dataframe(df)
    
    # Padronizar campos específicos de PPG
    ppg_field_mapping = {
        'nm_programa': 'des_programa',
        'nm_ies': 'des_ies', 
        'sg_uf': 'sg_uf_programa',
        'nm_regiao': 'des_regiao',
        'nm_area_avaliacao': 'des_area_avaliacao',
        'nm_area_basica': 'des_area_basica',
        'cd_programa': 'cod_programa',
        'cd_ies': 'cod_ies',
        'nota_avaliacao': 'nota_capes'
    }
    
    for old_col, new_col in ppg_field_mapping.items():
        matching_cols = [col for col in df.columns if old_col in col.lower()]
        if matching_cols:
            df[new_col] = df[matching_cols[0]]
    
    # Converter nota CAPES para numérico
    if 'nota_capes' in df.columns:
        df['nota_capes'] = pd.to_numeric(df['nota_capes'], errors='coerce')
    
    # Criar ID único se não existir
    if 'id_ppg' not in df.columns and 'cod_programa' in df.columns:
        df['id_ppg'] = df['cod_programa']
    elif 'id_ppg' not in df.columns:
        df['id_ppg'] = range(1, len(df) + 1)
    
    return df

def main():
    print_header("Criando Tabela RAW_PPG")
    
    # Carregar dados
    df = load_ppg_data()
    if df.empty:
        print_status("Nenhum dado carregado. Processo encerrado.", "error")
        return
    
    # Transformar dados
    df_transformed = transform_ppg_data(df)
    
    # Analisar qualidade
    DataQualityAnalyzer.analyze_dataframe(df_transformed, "RAW_PPG")
    
    # Salvar no banco
    db = DatabaseManager()
    success = db.save_dataframe(df_transformed, 'raw_ppg')
    
    if success:
        print_summary(len(df_transformed), 'raw_ppg')
    else:
        print_status("Falha ao salvar dados no banco", "error")

if __name__ == "__main__":
    main()
