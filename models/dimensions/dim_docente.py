import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from models.utils import salvar_df_bd

# Carregar variáveis de ambiente
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")
CSV_PATH = os.getenv("CSV_PATH")

def extrair_dados_docente_csv():
    """
    Extrai dados dos Docentes do arquivo CSV.
    """
    print("👨‍🏫 Extraindo dados dos Docentes do arquivo CSV...")
    
    try:
        # Caminho completo do arquivo CSV usando caminho absoluto
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        csv_file = os.path.join(project_root, CSV_PATH, "br-capes-colsucup-docente-2021-2025-03-31.csv")
        
        if not os.path.exists(csv_file):
            print(f"❌ Arquivo CSV não encontrado: {csv_file}")
            return pd.DataFrame()
        
        # Ler o CSV com encoding adequado e separador de ponto e vírgula
        # Tentar diferentes encodings
        encodings = ['latin1', 'iso-8859-1', 'cp1252', 'utf-8']
        df_raw = None
        
        for encoding in encodings:
            try:
                df_raw = pd.read_csv(csv_file, sep=';', encoding=encoding)
                print(f"✅ CSV carregado com encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
        
        if df_raw is None:
            print("❌ Não foi possível carregar o CSV com nenhum encoding testado")
            return pd.DataFrame()
        
        print(f"📊 Dados brutos carregados: {len(df_raw)} registros")
        
        # Mapeamento de colunas do CSV para nossa dimensão
        colunas_mapeamento = {
            'ID_PESSOA': 'id_pessoa',
            'NM_DOCENTE': 'nome_docente',
            'TP_DOCUMENTO_DOCENTE': 'tipo_documento',
            'NR_DOCUMENTO_DOCENTE': 'numero_documento',
            'AN_NASCIMENTO_DOCENTE': 'ano_nascimento',
            'DS_FAIXA_ETARIA': 'faixa_etaria',
            'DS_TIPO_NACIONALIDADE_DOCENTE': 'nacionalidade',
            'NM_PAIS_NACIONALIDADE_DOCENTE': 'pais_nacionalidade',
            'DS_CATEGORIA_DOCENTE': 'categoria_docente',
            'DS_TIPO_VINCULO_DOCENTE_IES': 'tipo_vinculo',
            'DS_REGIME_TRABALHO': 'regime_trabalho',
            'CD_CAT_BOLSA_PRODUTIVIDADE': 'categoria_bolsa_produtividade',
            'IN_DOUTOR': 'eh_doutor',
            'AN_TITULACAO': 'ano_titulacao',
            'NM_GRAU_TITULACAO': 'grau_titulacao',
            'CD_AREA_BASICA_TITULACAO': 'codigo_area_titulacao',
            'NM_AREA_BASICA_TITULACAO': 'area_titulacao',
            'SG_IES_TITULACAO': 'sigla_ies_titulacao',
            'NM_IES_TITULACAO': 'nome_ies_titulacao',
            'NM_PAIS_IES_TITULACAO': 'pais_ies_titulacao',
            'AN_BASE': 'ano_base'
        }
        
        # Verificar quais colunas existem no CSV
        colunas_existentes = {}
        for col_original, col_nova in colunas_mapeamento.items():
            if col_original in df_raw.columns:
                colunas_existentes[col_original] = col_nova
            else:
                print(f"⚠️ Coluna não encontrada no CSV: {col_original}")
        
        # Selecionar apenas colunas existentes
        df_docente = df_raw[list(colunas_existentes.keys())].copy()
        df_docente = df_docente.rename(columns=colunas_existentes)
        
        # Limpar e tratar dados
        df_docente = tratar_dados_docente(df_docente)
        
        print(f"✅ Dados dos Docentes extraídos do CSV: {len(df_docente)} registros")
        
        return df_docente
        
    except Exception as e:
        print(f"❌ Erro ao extrair dados do CSV de docentes: {e}")
        return pd.DataFrame()

def tratar_dados_docente(df_docente):
    """
    Trata e limpa os dados dos docentes.
    """
    try:
        # Remover linhas duplicadas baseado no ID da pessoa
        if 'id_pessoa' in df_docente.columns:
            print(f"📊 Registros antes de remover duplicatas: {len(df_docente)}")
            df_docente = df_docente.drop_duplicates(subset=['id_pessoa'])
            print(f"📊 Registros após remover duplicatas: {len(df_docente)}")
        
        # Tratar valores nulos
        df_docente = df_docente.fillna('Não informado')
        
        # Tratar colunas numéricas
        colunas_numericas = ['ano_nascimento', 'ano_titulacao', 'codigo_area_titulacao', 'ano_base']
        for col in colunas_numericas:
            if col in df_docente.columns:
                df_docente[col] = pd.to_numeric(df_docente[col], errors='coerce').fillna(0).astype(int)
        
        # Normalizar textos
        colunas_texto = ['nome_docente', 'tipo_documento', 'faixa_etaria', 'nacionalidade', 
                        'pais_nacionalidade', 'categoria_docente', 'tipo_vinculo', 'regime_trabalho',
                        'grau_titulacao', 'area_titulacao', 'sigla_ies_titulacao', 'nome_ies_titulacao',
                        'pais_ies_titulacao']
        for col in colunas_texto:
            if col in df_docente.columns:
                df_docente[col] = df_docente[col].astype(str).str.strip().str.upper()
        
        # Tratar campo eh_doutor (converter S/N para Sim/Não)
        if 'eh_doutor' in df_docente.columns:
            df_docente['eh_doutor'] = df_docente['eh_doutor'].map({
                'S': 'Sim', 'N': 'Não'
            }).fillna('Não informado')
        
        # Tratar categoria de bolsa produtividade (NA para Não se aplica)
        if 'categoria_bolsa_produtividade' in df_docente.columns:
            df_docente['categoria_bolsa_produtividade'] = df_docente['categoria_bolsa_produtividade'].replace(
                'NA', 'Não se aplica'
            )
        
        # Mascarar documento (manter apenas os primeiros 3 e últimos 2 dígitos)
        if 'numero_documento' in df_docente.columns:
            def mascarar_documento(doc):
                doc_str = str(doc)
                if len(doc_str) >= 5:
                    return doc_str[:3] + '*' * (len(doc_str) - 5) + doc_str[-2:]
                return '***'
            
            df_docente['numero_documento'] = df_docente['numero_documento'].apply(mascarar_documento)
        
        # Calcular idade aproximada baseada no ano de nascimento
        if 'ano_nascimento' in df_docente.columns:
            ano_atual = 2024  # Ano de referência
            df_docente['idade_aproximada'] = ano_atual - df_docente['ano_nascimento']
            df_docente['idade_aproximada'] = df_docente['idade_aproximada'].clip(lower=0)
        
        # Calcular tempo desde titulação
        if 'ano_titulacao' in df_docente.columns:
            ano_atual = 2024
            df_docente['tempo_titulacao'] = ano_atual - df_docente['ano_titulacao']
            df_docente['tempo_titulacao'] = df_docente['tempo_titulacao'].clip(lower=0)
        
        # Adicionar registro SK=0 (desconhecido/não aplicável)
        registro_sk0 = create_sk0_record(df_docente.columns)
        df_docente = pd.concat([registro_sk0, df_docente], ignore_index=True)
        
        # Adicionar surrogate key (começando do 0)
        df_docente.insert(0, 'docente_sk', range(0, len(df_docente)))
        
        return df_docente
        
    except Exception as e:
        print(f"❌ Erro ao tratar dados de docentes: {e}")
        return df_docente

def create_sk0_record(columns):
    """
    Cria o registro SK=0 para valores desconhecidos/não aplicáveis.
    """
    registro_sk0 = {}
    
    for col in columns:
        if col in ['ano_nascimento', 'ano_titulacao', 'codigo_area_titulacao', 'ano_base', 'idade_aproximada', 'tempo_titulacao']:
            registro_sk0[col] = 0
        elif col == 'id_pessoa':
            registro_sk0[col] = 0  # ID especial para registro desconhecido
        else:
            registro_sk0[col] = 'DESCONHECIDO'
    
    return pd.DataFrame([registro_sk0])

def salvar_dimensao_docente(df_docente):
    """
    Salva a dimensão Docente no banco de dados PostgreSQL.
    """
    try:
        # Usar a função salvar_df_bd que é mais robusta
        salvar_df_bd(df_docente, 'dim_docente')
        print(f"✅ Dimensão Docente salva no PostgreSQL com {len(df_docente)} registros")
            
    except Exception as e:
        print(f"❌ Erro ao salvar dimensão Docente: {e}")

if __name__ == "__main__":
    print("🚀 Iniciando processo de criação da dimensão Docente")
    print("📚 Fonte de dados: CSV CAPES")
    
    # Extrair dados dos docentes
    df_docente = extrair_dados_docente_csv()
    
    if df_docente.empty:
        print("❌ Nenhum dado foi retornado. Encerrando o script.")
        exit(1)
    
    # Salvar no banco
    salvar_dimensao_docente(df_docente)
    
    # Mostrar algumas estatísticas
    print("\n📊 Estatísticas da dimensão Docente:")
    print(f"Total de docentes únicos: {len(df_docente)}")
    
    if 'categoria_docente' in df_docente.columns:
        print(f"\nDocentes por categoria:")
        for categoria in sorted(df_docente['categoria_docente'].unique()):
            count = len(df_docente[df_docente['categoria_docente'] == categoria])
            print(f"  {categoria}: {count} docentes")
    
    if 'regime_trabalho' in df_docente.columns:
        print(f"\nDocentes por regime de trabalho:")
        for regime in sorted(df_docente['regime_trabalho'].unique()):
            count = len(df_docente[df_docente['regime_trabalho'] == regime])
            print(f"  {regime}: {count} docentes")
    
    if 'eh_doutor' in df_docente.columns:
        print(f"\nDocentes doutores:")
        for status in sorted(df_docente['eh_doutor'].unique()):
            count = len(df_docente[df_docente['eh_doutor'] == status])
            print(f"  {status}: {count} docentes")
    
    if 'grau_titulacao' in df_docente.columns:
        print(f"\nDocentes por grau de titulação:")
        for grau in sorted(df_docente['grau_titulacao'].unique()):
            count = len(df_docente[df_docente['grau_titulacao'] == grau])
            print(f"  {grau}: {count} docentes")
    
    if 'faixa_etaria' in df_docente.columns:
        print(f"\nDocentes por faixa etária:")
        for faixa in sorted(df_docente['faixa_etaria'].unique()):
            count = len(df_docente[df_docente['faixa_etaria'] == faixa])
            print(f"  {faixa}: {count} docentes")
    
    # Estatísticas numéricas (excluindo registro SK=0)
    if len(df_docente) > 1:  # Tem mais que só o registro SK=0
        df_stats = df_docente[df_docente['docente_sk'] != 0]
        
        if 'idade_aproximada' in df_docente.columns and len(df_stats) > 0:
            idade_media = df_stats['idade_aproximada'].mean()
            idade_min = df_stats[df_stats['idade_aproximada'] > 0]['idade_aproximada'].min()
            idade_max = df_stats['idade_aproximada'].max()
            print(f"\nIdades (excluindo registro SK=0):")
            print(f"  Idade média: {idade_media:.1f} anos")
            print(f"  Idade mínima: {idade_min} anos")
            print(f"  Idade máxima: {idade_max} anos")
        
        if 'tempo_titulacao' in df_docente.columns and len(df_stats) > 0:
            tempo_medio = df_stats['tempo_titulacao'].mean()
            tempo_min = df_stats[df_stats['tempo_titulacao'] > 0]['tempo_titulacao'].min()
            tempo_max = df_stats['tempo_titulacao'].max()
            print(f"\nTempo desde titulação (excluindo registro SK=0):")
            print(f"  Tempo médio: {tempo_medio:.1f} anos")
            print(f"  Tempo mínimo: {tempo_min} anos")
            print(f"  Tempo máximo: {tempo_max} anos")
    
    # Mostrar informação sobre ano base se disponível
    if 'ano_base' in df_docente.columns:
        anos_unicos = sorted(df_docente['ano_base'].unique())
        anos_sem_zero = [ano for ano in anos_unicos if ano != 0]  # Excluir SK=0
        if anos_sem_zero:
            print(f"\n📅 Anos base presentes nos dados: {', '.join(map(str, anos_sem_zero))}")
    
    print(f"\n✅ Processo concluído! Dimensão Docente criada com sucesso.")
    print("💡 A dimensão inclui informações detalhadas sobre formação, vinculação e características dos docentes.")
