#!/usr/bin/env python3
"""
Script unificado para criação da tabela raw_tema.
Processa dados de temas/ODS do arquivo curadoria_temas.xlsx.
"""

import pandas as pd
import os
from base_raw import DatabaseManager, DataQualityAnalyzer, DataCleaner, print_header, print_status, print_summary

def load_tema_data():
    """Carrega dados do arquivo de temas"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, '..', 'data')
    excel_path = os.path.join(data_dir, 'curadoria_temas.xlsx')
    
    print_status(f"Carregando arquivo: {os.path.basename(excel_path)}")
    
    if not os.path.exists(excel_path):
        print_status(f"Arquivo não encontrado: {excel_path}", "error")
        return pd.DataFrame()
    
    try:
        df = pd.read_excel(excel_path)
        print_status(f"Dados carregados: {len(df):,} registros")
        return df
    except Exception as e:
        print_status(f"Erro ao carregar arquivo: {e}", "error")
        return pd.DataFrame()

def transform_tema_data(df):
    """Transforma dados de temas"""
    print_status("Transformando dados de temas...")
    
    # Aplicar limpeza básica
    df = DataCleaner.clean_dataframe(df)
    
    # Desnormalizar dados se necessário (múltiplos temas por linha)
    tema_cols = [col for col in df.columns if 'tema' in col.lower()]
    if tema_cols:
        tema_col = tema_cols[0]
        
        # Explodir temas separados por ; ou ,
        if df[tema_col].astype(str).str.contains('[;,]').any():
            df[tema_col] = df[tema_col].astype(str).str.split('[;,]')
            df = df.explode(tema_col)
            df[tema_col] = df[tema_col].str.strip()
            print_status(f"Dados expandidos após desnormalização: {len(df):,} registros")
    
    # Padronizar campos específicos
    tema_fields = ['tema', 'ods', 'categoria', 'descricao']
    for field in tema_fields:
        matching_cols = [col for col in df.columns if field in col.lower()]
        if matching_cols:
            col = matching_cols[0]
            df[f'des_{field}'] = df[col]
    
    # Criar IDs se não existirem
    if 'id' not in df.columns:
        df['id_tema'] = range(1, len(df) + 1)
    
    return df

def main():
    print_header("Criando Tabela RAW_TEMA")
    
    # Carregar dados
    df = load_tema_data()
    if df.empty:
        print_status("Nenhum dado carregado. Processo encerrado.", "error")
        return
    
    # Transformar dados
    df_transformed = transform_tema_data(df)
    
    # Analisar qualidade
    DataQualityAnalyzer.analyze_dataframe(df_transformed, "RAW_TEMA")
    
    # Salvar no banco
    db = DatabaseManager()
    success = db.save_dataframe(df_transformed, 'raw_tema')
    
    if success:
        print_summary(len(df_transformed), 'raw_tema')
    else:
        print_status("Falha ao salvar dados no banco", "error")

if __name__ == "__main__":
    main()
