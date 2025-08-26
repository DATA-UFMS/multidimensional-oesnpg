#!/usr/bin/env python3
"""
Script para criação da tabela raw_producao.
Extrai dados de produção acadêmica da API CAPES (ARTPE, LIVRO, etc.).
"""

import pandas as pd
from base_raw import CAPESApiExtractor, DatabaseManager, DataQualityAnalyzer, DataCleaner, print_header, print_status, print_summary

# Configuração dos datasets da CAPES (Resource IDs reais 2021-2024)
DATASETS_CAPES = {
    'ARTPE': {
        'resource_id': '85e0faae-9db0-4d0d-9bfe-38281c666b13',
        'descricao': 'Artigos em Periódicos (2021-2024)',
        'prioridade': 1
    },
    'LIVRO': {
        'resource_id': 'b953a29f-e9a8-41af-af25-ffb25be51cf6',
        'descricao': 'Livros Publicados (2021-2024)',
        'prioridade': 2
    },
    'APTRA': {
        'resource_id': '284f0f5b-319f-4c2d-bba6-ddff45b69c28',
        'descricao': 'Apresentação de Trabalhos (2021-2024)',
        'prioridade': 3
    },
    'ANAIS': {
        'resource_id': '31e59def-5a18-459d-b8c0-f7befeb62400',
        'descricao': 'Trabalhos em Anais de Eventos (2021-2024)',
        'prioridade': 4
    }
}

def extract_producao_data(limit_datasets=2):
    """Extrai dados de produção da API CAPES"""
    extractor = CAPESApiExtractor()
    all_data = []
    
    # Ordenar por prioridade
    sorted_datasets = sorted(DATASETS_CAPES.items(), key=lambda x: x[1]['prioridade'])
    
    for i, (tipo, config) in enumerate(sorted_datasets):
        if i >= limit_datasets:
            print_status(f"Limitando a {limit_datasets} datasets para teste", "warning")
            break
            
        print_status(f"Extraindo {config['descricao']}...")
        
        df = extractor.fetch_all_data(config['resource_id'])
        
        if not df.empty:
            df['tipo_producao'] = tipo
            df['des_tipo_producao'] = config['descricao']
            all_data.append(df)
            print_status(f"{tipo}: {len(df):,} registros extraídos", "success")
        else:
            print_status(f"{tipo}: Nenhum dado extraído", "warning")
    
    if all_data:
        df_final = pd.concat(all_data, ignore_index=True)
        print_status(f"Total consolidado: {len(df_final):,} registros", "success")
        return df_final
    else:
        print_status("Nenhum dado extraído de qualquer dataset", "error")
        return pd.DataFrame()

def transform_producao_data(df):
    """Transforma dados de produção"""
    print_status("Transformando dados de produção...")
    
    # Aplicar limpeza básica
    df = DataCleaner.clean_dataframe(df)
    
    # Padronizar campos comuns
    campo_mapping = {
        'nm_docente': 'des_autor',
        'nm_producao': 'des_titulo',
        'nm_veiculo': 'des_veiculo',
        'dt_ano_base': 'ano_producao',
        'id_pessoa': 'id_pessoa_capes',
        'cd_programa_ies': 'cod_programa'
    }
    
    for old_col, new_col in campo_mapping.items():
        matching_cols = [col for col in df.columns if old_col in col.lower()]
        if matching_cols:
            df[new_col] = df[matching_cols[0]]
    
    # Converter ano para inteiro
    if 'ano_producao' in df.columns:
        df['ano_producao'] = pd.to_numeric(df['ano_producao'], errors='coerce')
    
    # Criar ID único se não existir
    if 'id_producao' not in df.columns:
        df['id_producao'] = range(1, len(df) + 1)
    
    return df

def main():
    print_header("Criando Tabela RAW_PRODUCAO")
    
    # Extrair dados da API
    df = extract_producao_data(limit_datasets=2)  # Limitando para teste
    
    if df.empty:
        print_status("Nenhum dado extraído. Processo encerrado.", "error")
        return
    
    # Transformar dados
    df_transformed = transform_producao_data(df)
    
    # Analisar qualidade
    DataQualityAnalyzer.analyze_dataframe(df_transformed, "RAW_PRODUCAO")
    
    # Salvar no banco
    db = DatabaseManager()
    success = db.save_dataframe(df_transformed, 'raw_producao')
    
    if success:
        print_summary(len(df_transformed), 'raw_producao')
    else:
        print_status("Falha ao salvar dados no banco", "error")

if __name__ == "__main__":
    main()
