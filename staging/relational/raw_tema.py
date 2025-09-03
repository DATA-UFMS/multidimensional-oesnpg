#!/usr/bin/env python3
"""
Script RAW para dados de Temas
Lê da planilha macro_temas_oesnpg_v2.xlsx (aba macro-temas-v2)
Extrai: id_tema, uf, tema_nome, macrotema_id, palavrachave_nome (desnormalizado), sigla_uf
Salva na tabela raw_tema no PostgreSQL
"""

import pandas as pd
from datetime import datetime
from pathlib import Path
import sys
import os

# Adiciona o diretório base ao path para importar módulos
sys.path.append(str(Path(__file__).parent))

try:
    from base_raw import (
        DatabaseManager, DataQualityAnalyzer, DataCleaner, FileManager,
        print_header, print_status, apply_naming_convention, validate_required_fields
    )
except ImportError:
    print("❌ Erro: Não foi possível importar módulos base. Verifique se base_raw_updated.py está no mesmo diretório.")
    sys.exit(1)

def load_macro_temas_planilha():
    """Carrega dados da planilha macro_temas_oesnpg_v2.xlsx, aba macro-temas-v2"""
    print_status("Carregando planilha macro_temas_oesnpg_v2.xlsx...", "info")
    
    possible_paths = [
        "../data/macro_temas_oesnpg_v2.xlsx",
        "staging/data/macro_temas_oesnpg_v2.xlsx",
        "/home/ubuntu/upload/macro_temas_oesnpg_v2.xlsx",
        "macro_temas_oesnpg_v2.xlsx"
    ]
    
    filepath = None
    for path in possible_paths:
        if os.path.exists(path):
            filepath = path
            break
    
    if not filepath:
        print_status("❌ Arquivo macro_temas_oesnpg_v2.xlsx não encontrado", "error")
        return pd.DataFrame()
    
    try:
        # Tenta carregar a aba específica "macro-temas-v2"
        try:
            df = pd.read_excel(filepath, sheet_name='macro-temas-v2')
            print_status(f"✅ Carregados {len(df)} registros da aba 'macro-temas-v2'", "success")
        except:
            # Se não encontrar a aba, carrega a primeira aba
            df = pd.read_excel(filepath)
            print_status(f"✅ Carregados {len(df)} registros da primeira aba", "success")
        
        print_status(f"   Colunas encontradas: {list(df.columns)}", "info")
        return df
        
    except Exception as e:
        print_status(f"❌ Erro ao carregar planilha: {str(e)}", "error")
        return pd.DataFrame()

def process_macro_temas_data(df):
    """Processa dados da planilha e desnormaliza palavras-chave"""
    print_status("Processando dados da planilha...", "info")
    
    if df.empty:
        return pd.DataFrame()
    
    registros = []
    palavrachave_id_counter = 1  # Contador para IDs únicos de palavras-chave
    
    for idx, row in df.iterrows():
        # Extrai campos principais
        id_tema = row['ID'] if 'ID' in row else None
        uf = row['UF'] if 'UF' in row else None
        tema_nome = row['TEMA'] if 'TEMA' in row else None
        macrotema_id = row['macro_tema_1_id'] if 'macro_tema_1_id' in row else None
        palavras_chave = row['PALAVRA-CHAVE'] if 'PALAVRA-CHAVE' in row else None
        
        # Cria sigla_uf (mapeamento de UF para sigla)
        uf_to_sigla = {
            'ACRE': 'AC', 'ALAGOAS': 'AL', 'AMAPÁ': 'AP', 'AMAZONAS': 'AM',
            'BAHIA': 'BA', 'CEARÁ': 'CE', 'DISTRITO FEDERAL': 'DF', 'ESPÍRITO SANTO': 'ES',
            'GOIÁS': 'GO', 'MARANHÃO': 'MA', 'MATO GROSSO': 'MT', 'MATO GROSSO DO SUL': 'MS',
            'MINAS GERAIS': 'MG', 'PARÁ': 'PA', 'PARAÍBA': 'PB', 'PARANÁ': 'PR',
            'PERNAMBUCO': 'PE', 'PIAUÍ': 'PI', 'RIO DE JANEIRO': 'RJ', 'RIO GRANDE DO NORTE': 'RN',
            'RIO GRANDE DO SUL': 'RS', 'RONDÔNIA': 'RO', 'RORAIMA': 'RR', 'SANTA CATARINA': 'SC',
            'SÃO PAULO': 'SP', 'SERGIPE': 'SE', 'TOCANTINS': 'TO'
        }
        sigla_uf = uf_to_sigla.get(str(uf).upper(), str(uf)) if pd.notna(uf) else None
        
        # Valida campos obrigatórios
        if pd.isna(id_tema) or pd.isna(tema_nome):
            continue
        
        # Desnormaliza palavras-chave
        if pd.notna(palavras_chave) and str(palavras_chave).strip():
            # Separa por vírgula e limpa
            palavras_lista = [p.strip() for p in str(palavras_chave).split(',')]
            
            for palavra in palavras_lista:
                if palavra:  # Se não estiver vazia
                    registros.append({
                        'id_tema': int(id_tema),
                        'uf': str(uf).strip() if pd.notna(uf) else None,
                        'tema_nome': str(tema_nome).strip(),
                        'macrotema_id': int(macrotema_id) if pd.notna(macrotema_id) else None,
                        'palavrachave_id': palavrachave_id_counter,
                        'palavrachave_nome': palavra,
                        'sigla_uf': sigla_uf,
                        'data_carga': datetime.now(),
                        'fonte_arquivo': 'macro_temas_oesnpg_v2.xlsx'
                    })
                    palavrachave_id_counter += 1
        else:
            # Se não tem palavras-chave, cria registro sem palavra-chave
            registros.append({
                'id_tema': int(id_tema),
                'uf': str(uf).strip() if pd.notna(uf) else None,
                'tema_nome': str(tema_nome).strip(),
                'macrotema_id': int(macrotema_id) if pd.notna(macrotema_id) else None,
                'palavrachave_id': None,
                'palavrachave_nome': None,
                'sigla_uf': sigla_uf,
                'data_carga': datetime.now(),
                'fonte_arquivo': 'macro_temas_oesnpg_v2.xlsx'
            })
    
    df_result = pd.DataFrame(registros)
    
    print_status(f"✅ Processados {len(df_result)} registros desnormalizados", "success")
    print_status(f"   • Temas únicos: {df_result['id_tema'].nunique()}", "info")
    print_status(f"   • Palavras-chave únicas: {df_result['palavrachave_nome'].nunique()}", "info")
    print_status(f"   • UFs únicas: {df_result['uf'].nunique()}", "info")
    print_status(f"   • IDs de palavras-chave: 1 a {palavrachave_id_counter-1}", "info")
    
    return df_result

def main():
    """Função principal"""
    print_header("RAW - Dados de Temas (macro_temas_oesnpg_v2.xlsx)")
    
    # 1. Carregar dados da planilha
    df_planilha = load_macro_temas_planilha()
    
    if df_planilha.empty:
        print_status("❌ Nenhum dado foi carregado da planilha", "error")
        return
    
    # 2. Processar e desnormalizar dados
    df_processed = process_macro_temas_data(df_planilha)
    
    if df_processed.empty:
        print_status("❌ Nenhum dado foi processado", "error")
        return
    
    # 3. Validar campos obrigatórios
    required_fields = ['id_tema', 'tema_nome']
    if not validate_required_fields(df_processed, required_fields, "Temas"):
        print_status("⚠️ Continuando com campos obrigatórios ausentes", "warning")
    
    # 4. Analisar qualidade dos dados
    analyzer = DataQualityAnalyzer()
    print_status("Analisando qualidade dos dados...", "info")
    analyzer.analyze_dataframe(df_processed, "Temas Processados")
    
    # 5. Salvar no banco PostgreSQL
    db_manager = DatabaseManager()
    
    try:
        print_status("Salvando dados na tabela raw_tema...", "info")
        success = db_manager.save_dataframe(
            df_processed,
            'raw_tema',
            if_exists='replace'
        )
        
        if success:
            print_status("✅ Dados salvos com sucesso na tabela raw_tema", "success")
        else:
            print_status("⚠️ Problemas ao salvar dados no banco", "warning")
            
    except Exception as e:
        print_status(f"❌ Erro ao salvar no banco: {str(e)}", "error")
        
        # Salva em CSV como backup
        print_status("Salvando em CSV como backup...", "info")
        df_processed.to_csv('raw_tema_backup.csv', index=False, encoding='utf-8')
        print_status("✅ Backup salvo em raw_tema_backup.csv", "success")
    
    # 6. Relatório final
    print_status("Processamento concluído!", "success")
    print(f"   • Registros processados: {len(df_processed)}")
    print(f"   • Temas únicos: {df_processed['id_tema'].nunique()}")
    print(f"   • Palavras-chave únicas: {df_processed['palavrachave_nome'].nunique()}")
    print(f"   • UFs únicas: {df_processed['uf'].nunique()}")
    print(f"   • Tabela criada: raw_tema")
    
    # Estatísticas por UF
    print(f"\n   • Distribuição por UF (top 10):")
    uf_counts = df_processed['uf'].value_counts().head(10)
    for uf, count in uf_counts.items():
        print(f"     - {uf}: {count} registros")
    
    # Estatísticas por macro-tema
    if 'macrotema_id' in df_processed.columns:
        macro_counts = df_processed['macrotema_id'].value_counts().head(5)
        if len(macro_counts) > 0:
            print(f"\n   • Distribuição por macro-tema (top 5):")
            for macro_id, count in macro_counts.items():
                print(f"     - Macro-tema {macro_id}: {count} registros")

if __name__ == "__main__":
    main()