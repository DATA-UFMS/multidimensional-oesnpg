


import pandas as pd
from sqlalchemy import create_engine
import os
import sys
from dotenv import load_dotenv
# Adicionar o diretório raiz ao path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from src.utils.naming_conventions import NamingConventions
from src.validation.data_validator import validate_dimension_data, get_validation_summary
from src.core.exceptions import DimensionCreationError, DataValidationError


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

def extrair_dados_ods():
    """
    Extrai dados dos 17 Objetivos de Desenvolvimento Sustentável (ODS) da ONU.
    """
    print("🎯 Extraindo dados dos ODS (Objetivos de Desenvolvimento Sustentável)...")
    
    try:
        # Dados dos 17 ODS da ONU com temas relacionados
        ods_data = [
            (1, "Erradicação da pobreza", "Acabar com a pobreza em todas as suas formas, em todos os lugares.", "pobreza extrema, renda, vulnerabilidade social, proteção social, acesso a serviços básicos"),
            (2, "Fome zero e agricultura sustentável", "Acabar com a fome, alcançar a segurança alimentar e melhorar a nutrição e promover a agricultura sustentável.", "segurança alimentar, nutrição, agricultura familiar, produtividade agrícola, sistemas alimentares sustentáveis"),
            (3, "Saúde e bem-estar", "Assegurar uma vida saudável e promover o bem-estar para todos, em todas as idades.", "mortalidade materno-infantil, doenças transmissíveis, saúde mental, cobertura universal, acesso a medicamentos"),
            (4, "Educação de qualidade", "Assegurar a educação inclusiva e equitativa e de qualidade, e promover oportunidades de aprendizagem ao longo da vida para todos.", "educação básica, alfabetização, ensino superior, formação técnica, igualdade de acesso educacional"),
            (5, "Igualdade de gênero", "Alcançar a igualdade de gênero e empoderar todas as mulheres e meninas.", "empoderamento feminino, violência de gênero, participação política, direitos reprodutivos, liderança feminina"),
            (6, "Água potável e saneamento", "Assegurar a disponibilidade e gestão sustentável da água e saneamento para todos.", "acesso à água potável, saneamento básico, gestão de recursos hídricos, qualidade da água, higiene"),
            (7, "Energia limpa e acessível", "Assegurar o acesso confiável, sustentável, moderno e a preço acessível à energia para todos.", "energias renováveis, eficiência energética, acesso universal à energia, matriz energética limpa, tecnologias sustentáveis"),
            (8, "Trabalho decente e crescimento econômico", "Promover o crescimento econômico sustentado, inclusivo e sustentável, emprego pleno e produtivo e trabalho decente para todos.", "emprego pleno, trabalho decente, crescimento econômico, produtividade, empreendedorismo, direitos trabalhistas"),
            (9, "Indústria, inovação e infraestrutura", "Construir infraestruturas resilientes, promover a industrialização inclusiva e sustentável e fomentar a inovação.", "infraestrutura resiliente, industrialização sustentável, inovação tecnológica, pesquisa e desenvolvimento, conectividade"),
            (10, "Redução das desigualdades", "Reduzir a desigualdade dentro dos países e entre eles.", "desigualdade de renda, inclusão social, migração segura, políticas redistributivas, discriminação"),
            (11, "Cidades e comunidades sustentáveis", "Tornar as cidades e os assentamentos humanos inclusivos, seguros, resilientes e sustentáveis.", "urbanização sustentável, habitação adequada, transporte público, gestão de resíduos, planejamento urbano, patrimônio cultural"),
            (12, "Consumo e produção responsáveis", "Assegurar padrões de produção e de consumo sustentáveis.", "eficiência de recursos, economia circular, gestão de resíduos, sustentabilidade corporativa, consumo consciente"),
            (13, "Ação contra a mudança global do clima", "Tomar medidas urgentes para combater a mudança climática e seus impactos.", "mitigação climática, adaptação às mudanças climáticas, emissões de gases do efeito estufa, resiliência climática, financiamento climático"),
            (14, "Vida na água", "Conservar e usar sustentavelmente os oceanos, os mares e os recursos marinhos para o desenvolvimento sustentável.", "conservação marinha, pesca sustentável, poluição oceânica, ecossistemas aquáticos, acidificação dos oceanos"),
            (15, "Vida terrestre", "Proteger, recuperar e promover o uso sustentável dos ecossistemas terrestres.", "biodiversidade, desertificação, gestão florestal sustentável, conservação de habitats, espécies ameaçadas"),
            (16, "Paz, justiça e instituições eficazes", "Promover sociedades pacíficas e inclusivas para o desenvolvimento sustentável.", "estado de direito, justiça, transparência, corrupção, instituições eficazes, acesso à justiça"),
            (17, "Parcerias e meios de implementação", "Fortalecer os meios de implementação e revitalizar a parceria global para o desenvolvimento sustentável.", "cooperação internacional, financiamento, transferência de tecnologia, capacitação, parcerias multissetoriais")
        ]
        
        # Converter para DataFrame
        df_ods = pd.DataFrame(ods_data, columns=["ods_numero", "ods_nome", "ods_descricao", "ods_temas_relacionados"])
        
        # Tratar e limpar dados
        df_ods = tratar_dados_ods(df_ods)
        
        print(f"✅ Dados dos ODS extraídos: {len(df_ods)} registros")
        
        return df_ods
        
    except Exception as e:
        print(f"❌ Erro ao extrair dados dos ODS: {e}")
        return pd.DataFrame()

def tratar_dados_ods(df_ods):
    """
    Trata e limpa os dados dos ODS.
    """
    try:
        # Normalizar textos
        df_ods['ods_nome'] = df_ods['ods_nome'].str.strip().str.title()
        df_ods['ods_descricao'] = df_ods['ods_descricao'].str.strip()
        
        # Adicionar campos auxiliares
        df_ods['ods_codigo'] = 'ODS-' + df_ods['ods_numero'].astype(str).str.zfill(2)
        df_ods['ods_status'] = 'Ativo'
        df_ods['ods_categoria'] = df_ods['ods_numero'].apply(categorizar_ods)
        
        # Adicionar registro SK=0 (desconhecido/não aplicável)
        registro_sk0 = create_sk0_record()
        df_ods = pd.concat([registro_sk0, df_ods], ignore_index=True)
        
        # Adicionar surrogate key (começando do 0)
        df_ods.insert(0, 'ods_sk', range(0, len(df_ods)))
        
        return df_ods
        
    except Exception as e:
        print(f"❌ Erro ao tratar dados dos ODS: {e}")
        return df_ods

def categorizar_ods(numero):
    """
    Categoriza os ODS em grupos temáticos.
    """
    categorias = {
        1: 'Social',      # Erradicação da pobreza
        2: 'Ambiental',   # Fome zero
        3: 'Social',      # Saúde e bem-estar
        4: 'Social',      # Educação de qualidade
        5: 'Social',      # Igualdade de gênero
        6: 'Ambiental',   # Água potável
        7: 'Ambiental',   # Energia limpa
        8: 'Econômico',   # Trabalho decente
        9: 'Econômico',   # Indústria e inovação
        10: 'Social',     # Redução das desigualdades
        11: 'Ambiental',  # Cidades sustentáveis
        12: 'Ambiental',  # Consumo responsável
        13: 'Ambiental',  # Ação climática
        14: 'Ambiental',  # Vida na água
        15: 'Ambiental',  # Vida terrestre
        16: 'Governança', # Paz e justiça
        17: 'Governança'  # Parcerias
    }
    return categorias.get(numero, 'Geral')

def create_sk0_record():
    """
    Cria o registro SK=0 para valores desconhecidos/não aplicáveis.
    """
    registro_sk0 = {
        'ods_numero': 0,
        'ods_nome': 'DESCONHECIDO',
        'ods_descricao': 'Registro para valores desconhecidos ou não aplicáveis',
        'ods_temas_relacionados': 'DESCONHECIDO',
        'ods_codigo': 'ODS-00',
        'ods_status': 'DESCONHECIDO',
        'ods_categoria': 'DESCONHECIDO'
    }
    
    return pd.DataFrame([registro_sk0])

def salvar_dimensao_ods(df_ods):
    """
    Salva a dimensão ODS no banco de dados PostgreSQL.
    """
    try:
        # Criar conexão com o banco
        from sqlalchemy import create_engine
        import os
        from dotenv import load_dotenv
        
        load_dotenv()
        DB_HOST = os.getenv("DB_HOST")
        DB_NAME = os.getenv("DB_NAME")
        DB_USER = os.getenv("DB_USER")
        DB_PASS = os.getenv("DB_PASS")
        DB_PORT = os.getenv("DB_PORT")
        
        url = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(url)
        
        with engine.begin() as conn:
            # Primeiro criar a tabela com estrutura explícita
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS dim_ods (
                ods_sk INTEGER PRIMARY KEY,
                ods_numero INTEGER NOT NULL,
                ods_nome VARCHAR(255) NOT NULL,
                ods_descricao TEXT,
                ods_temas_relacionados TEXT,
                ods_codigo VARCHAR(20) NOT NULL,
                ods_status VARCHAR(50),
                ods_categoria VARCHAR(50)
            );
            """
            
            # Executar a criação da tabela
            conn.exec_driver_sql(create_table_sql)
            
            # Limpar tabela se já existir dados
            conn.exec_driver_sql("DELETE FROM dim_ods;")
            
            # Inserir dados
            df_ods.to_sql('dim_ods', conn, if_exists='append', index=False)
        print(f"✅ Dimensão ODS salva no PostgreSQL com {len(df_ods)} registros")
            
    except Exception as e:
        print(f"❌ Erro ao salvar dimensão ODS: {e}")

if __name__ == "__main__":
    print("🚀 Iniciando processo de criação da dimensão ODS")
    print("🎯 Fonte de dados: Objetivos de Desenvolvimento Sustentável da ONU")
    
    # Extrair dados dos ODS
    df_ods = extrair_dados_ods()
    
    if df_ods.empty:
        print("❌ Nenhum dado foi retornado. Encerrando o script.")
        exit(1)
    
    # Salvar no banco
    salvar_dimensao_ods(df_ods)
    
    # Mostrar algumas estatísticas
    print("\n📊 Estatísticas da dimensão ODS:")
    print(f"Total de ODS: {len(df_ods)}")
    
    if 'ods_categoria' in df_ods.columns:
        print(f"\nODS por categoria:")
        for categoria in sorted(df_ods['ods_categoria'].unique()):
            count = len(df_ods[df_ods['ods_categoria'] == categoria])
            print(f"  {categoria}: {count} ODS")
    
    if 'ods_status' in df_ods.columns:
        print(f"\nODS por status:")
        for status in sorted(df_ods['ods_status'].unique()):
            count = len(df_ods[df_ods['ods_status'] == status])
            print(f"  {status}: {count} ODS")
    
    # Mostrar lista dos ODS (excluindo registro SK=0)
    df_stats = df_ods[df_ods['ods_sk'] != 0]
    if len(df_stats) > 0:
        print(f"\n🎯 Lista dos ODS:")
        for _, row in df_stats.iterrows():
            print(f"  {row['ods_codigo']}: {row['ods_nome']}")
    
    print(f"\n✅ Processo concluído! Dimensão ODS criada com sucesso.")
    print("💡 A dimensão inclui os 17 Objetivos de Desenvolvimento Sustentável da ONU organizados por categorias.")
    print("🔗 Esta dimensão pode ser usada para análises de alinhamento da pesquisa com os ODS.")