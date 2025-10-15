


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
    Extrai dados dos 20 Objetivos de Desenvolvimento Sustentável (17 ODS oficiais da ONU + 3 ODS expandidos).
    Baseado no documento: "Descritores 17 ODS (+18+19+20) CACS 23062024_Versão Consolidada"
    """
    print("🎯 Extraindo dados dos ODS (Objetivos de Desenvolvimento Sustentável)...")
    
    try:
        # Dados dos 17 ODS oficiais da ONU + 3 ODS expandidos (18, 19, 20)
        ods_data = [
            (1, "Erradicação da pobreza", "Acabar com a pobreza em todas as suas formas, em todos os lugares.", "pobreza extrema, renda per capita, vulnerabilidade social, proteção social, acesso a serviços básicos, desigualdade de renda, linha de pobreza, segurança econômica"),
            (2, "Fome zero e agricultura sustentável", "Acabar com a fome, alcançar a segurança alimentar e melhorar a nutrição e promover a agricultura sustentável.", "segurança alimentar, nutrição infantil, agricultura familiar, produtividade agrícola, sistemas alimentares sustentáveis, agroecologia, soberania alimentar, desnutrição"),
            (3, "Saúde e bem-estar", "Assegurar uma vida saudável e promover o bem-estar para todos, em todas as idades.", "mortalidade materno-infantil, doenças transmissíveis, saúde mental, cobertura universal de saúde, acesso a medicamentos, epidemias, saúde reprodutiva, vacinação"),
            (4, "Educação de qualidade", "Assegurar a educação inclusiva e equitativa e de qualidade, e promover oportunidades de aprendizagem ao longo da vida para todos.", "educação básica universal, alfabetização, ensino superior, formação técnica profissional, igualdade de acesso educacional, educação para desenvolvimento sustentável, infraestrutura escolar"),
            (5, "Igualdade de gênero", "Alcançar a igualdade de gênero e empoderar todas as mulheres e meninas.", "empoderamento feminino, violência de gênero, participação política das mulheres, direitos reprodutivos, liderança feminina, discriminação de gênero, igualdade salarial, trabalho doméstico não remunerado"),
            (6, "Água potável e saneamento", "Assegurar a disponibilidade e gestão sustentável da água e saneamento para todos.", "acesso universal à água potável, saneamento básico, gestão integrada de recursos hídricos, qualidade da água, eficiência hídrica, proteção de ecossistemas aquáticos, higiene"),
            (7, "Energia limpa e acessível", "Assegurar o acesso confiável, sustentável, moderno e a preço acessível à energia para todos.", "energias renováveis, eficiência energética, acesso universal à energia, matriz energética limpa, tecnologias sustentáveis de energia, energia solar, energia eólica, biomassa"),
            (8, "Trabalho decente e crescimento econômico", "Promover o crescimento econômico sustentado, inclusivo e sustentável, emprego pleno e produtivo e trabalho decente para todos.", "emprego pleno e produtivo, trabalho decente, crescimento econômico inclusivo, produtividade econômica, empreendedorismo, direitos trabalhistas, trabalho infantil, trabalho forçado"),
            (9, "Indústria, inovação e infraestrutura", "Construir infraestruturas resilientes, promover a industrialização inclusiva e sustentável e fomentar a inovação.", "infraestrutura resiliente, industrialização sustentável, inovação tecnológica, pesquisa e desenvolvimento, conectividade, acesso à internet, pequenas indústrias, transferência de tecnologia"),
            (10, "Redução das desigualdades", "Reduzir a desigualdade dentro dos países e entre eles.", "desigualdade de renda, inclusão social e econômica, migração segura e ordenada, políticas redistributivas, discriminação, igualdade de oportunidades, representação nos processos decisórios"),
            (11, "Cidades e comunidades sustentáveis", "Tornar as cidades e os assentamentos humanos inclusivos, seguros, resilientes e sustentáveis.", "urbanização sustentável, habitação adequada e acessível, transporte público sustentável, gestão de resíduos urbanos, planejamento urbano participativo, patrimônio cultural e natural, espaços públicos seguros"),
            (12, "Consumo e produção responsáveis", "Assegurar padrões de produção e de consumo sustentáveis.", "eficiência no uso de recursos naturais, economia circular, gestão sustentável de resíduos, práticas sustentáveis corporativas, consumo consciente, desperdício de alimentos, produtos químicos e resíduos perigosos"),
            (13, "Ação contra a mudança global do clima", "Tomar medidas urgentes para combater a mudança climática e seus impactos.", "mitigação climática, adaptação às mudanças climáticas, redução de emissões de gases do efeito estufa, resiliência climática, financiamento climático, educação climática, desastres relacionados ao clima"),
            (14, "Vida na água", "Conservar e usar sustentavelmente os oceanos, os mares e os recursos marinhos para o desenvolvimento sustentável.", "conservação marinha e costeira, pesca sustentável, poluição oceânica, ecossistemas aquáticos, acidificação dos oceanos, biodiversidade marinha, áreas marinhas protegidas, recursos genéticos marinhos"),
            (15, "Vida terrestre", "Proteger, recuperar e promover o uso sustentável dos ecossistemas terrestres, gerir de forma sustentável as florestas, combater a desertificação.", "biodiversidade terrestre, desertificação, gestão florestal sustentável, conservação de habitats, espécies ameaçadas de extinção, degradação do solo, tráfico de fauna e flora, ecossistemas de montanha"),
            (16, "Paz, justiça e instituições eficazes", "Promover sociedades pacíficas e inclusivas para o desenvolvimento sustentável, proporcionar o acesso à justiça para todos.", "redução da violência, estado de direito, acesso à justiça, transparência institucional, combate à corrupção, instituições eficazes e responsáveis, participação cidadã, identidade legal universal"),
            (17, "Parcerias e meios de implementação", "Fortalecer os meios de implementação e revitalizar a parceria global para o desenvolvimento sustentável.", "cooperação internacional, assistência oficial ao desenvolvimento, financiamento para desenvolvimento, transferência de tecnologia, capacitação institucional, parcerias multissetoriais, comércio internacional justo"),
            (18, "Ciência, tecnologia e inovação", "Promover o desenvolvimento científico, tecnológico e a inovação como motores do desenvolvimento sustentável.", "pesquisa científica, desenvolvimento tecnológico, inovação para sustentabilidade, acesso ao conhecimento científico, formação de recursos humanos em CT&I, infraestrutura de pesquisa, propriedade intelectual, divulgação científica"),
            (19, "Cultura e desenvolvimento sustentável", "Reconhecer e promover o papel da cultura como dimensão essencial do desenvolvimento sustentável.", "diversidade cultural, patrimônio cultural material e imaterial, indústrias criativas, direitos culturais, diálogo intercultural, cultura e educação, cultura e meio ambiente, expressões culturais tradicionais"),
            (20, "Governança global e cooperação internacional", "Fortalecer a governança global e a cooperação internacional para alcançar os objetivos de desenvolvimento sustentável.", "multilateralismo, instituições internacionais, cooperação sul-sul, diplomacia para desenvolvimento, acordos internacionais, reforma da governança global, representatividade internacional, solidariedade global")
        ]
        
        # Converter para DataFrame
        df_ods = pd.DataFrame(ods_data, columns=["ods_numero", "ods_nome", "ods_descricao", "ods_temas_relacionados"])
        
        # Adicionar campo de tipo (Oficial da ONU ou Expandido)
        df_ods['ods_tipo'] = df_ods['ods_numero'].apply(lambda x: 'Oficial ONU' if x <= 17 else 'Expandido')
        
        # Tratar e limpar dados
        df_ods = tratar_dados_ods(df_ods)
        
        print(f"✅ Dados dos ODS extraídos: {len(df_ods)} registros (17 ODS oficiais + 3 expandidos)")
        
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
    Categoriza os ODS em grupos temáticos baseado nas dimensões do desenvolvimento sustentável.
    Inclui os 17 ODS oficiais + 3 ODS expandidos (18, 19, 20).
    """
    categorias = {
        1: 'Social',          # Erradicação da pobreza
        2: 'Social-Ambiental', # Fome zero e agricultura sustentável
        3: 'Social',          # Saúde e bem-estar
        4: 'Social',          # Educação de qualidade
        5: 'Social',          # Igualdade de gênero
        6: 'Ambiental',       # Água potável e saneamento
        7: 'Ambiental-Econômico', # Energia limpa e acessível
        8: 'Econômico-Social', # Trabalho decente e crescimento econômico
        9: 'Econômico',       # Indústria, inovação e infraestrutura
        10: 'Social',         # Redução das desigualdades
        11: 'Social-Ambiental', # Cidades e comunidades sustentáveis
        12: 'Ambiental-Econômico', # Consumo e produção responsáveis
        13: 'Ambiental',      # Ação contra a mudança climática
        14: 'Ambiental',      # Vida na água
        15: 'Ambiental',      # Vida terrestre
        16: 'Governança',     # Paz, justiça e instituições eficazes
        17: 'Governança',     # Parcerias e meios de implementação
        18: 'Econômico-Social', # Ciência, tecnologia e inovação
        19: 'Social',         # Cultura e desenvolvimento sustentável
        20: 'Governança'      # Governança global e cooperação internacional
    }
    return categorias.get(numero, 'Geral')

def create_sk0_record():
    """
    Cria o registro SK=0 para valores desconhecidos/não aplicáveis.
    """
    registro_sk0 = {
        'ods_numero': 0,
        'ods_nome': 'Não informado',
        'ods_descricao': 'Registro para valores desconhecidos ou não aplicáveis',
        'ods_temas_relacionados': 'Não informado',
        'ods_codigo': 'ODS-00',
        'ods_status': 'Não informado',
        'ods_categoria': 'Não informado',
        'ods_tipo': 'Não informado'
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
                ods_categoria VARCHAR(50),
                ods_tipo VARCHAR(50) NOT NULL DEFAULT 'Oficial ONU',
                CONSTRAINT check_ods_numero CHECK (ods_numero >= 0 AND ods_numero <= 20)
            );
            
            COMMENT ON TABLE dim_ods IS 'Dimensão dos Objetivos de Desenvolvimento Sustentável - 17 ODS oficiais da ONU + 3 ODS expandidos (Ciência/Tecnologia, Cultura, Governança Global)';
            COMMENT ON COLUMN dim_ods.ods_sk IS 'Surrogate key da dimensão ODS';
            COMMENT ON COLUMN dim_ods.ods_numero IS 'Número do ODS (1-20, onde 18-20 são ODS expandidos)';
            COMMENT ON COLUMN dim_ods.ods_tipo IS 'Tipo do ODS: Oficial ONU (1-17) ou Expandido (18-20)';
            COMMENT ON COLUMN dim_ods.ods_categoria IS 'Categoria temática: Social, Ambiental, Econômico, Governança, ou combinações';
            COMMENT ON COLUMN dim_ods.ods_temas_relacionados IS 'Descritores e palavras-chave relacionadas ao ODS para classificação de pesquisas';
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
    print("🎯 Fonte: Descritores 17 ODS (+18+19+20) CACS - Versão Consolidada")
    print("📋 Inclui: 17 ODS oficiais da ONU + 3 ODS expandidos")
    
    # Extrair dados dos ODS
    df_ods = extrair_dados_ods()
    
    if df_ods.empty:
        print("❌ Nenhum dado foi retornado. Encerrando o script.")
        exit(1)
    
    # Salvar no banco
    salvar_dimensao_ods(df_ods)
    
    # Mostrar algumas estatísticas
    print("\n📊 Estatísticas da dimensão ODS:")
    print(f"Total de registros: {len(df_ods)} (incluindo SK=0)")
    
    # Estatísticas por tipo
    if 'ods_tipo' in df_ods.columns:
        print(f"\n📌 ODS por tipo:")
        df_stats_tipo = df_ods[df_ods['ods_sk'] != 0]
        for tipo in sorted(df_stats_tipo['ods_tipo'].unique()):
            count = len(df_stats_tipo[df_stats_tipo['ods_tipo'] == tipo])
            print(f"  {tipo}: {count} ODS")
    
    # Estatísticas por categoria
    if 'ods_categoria' in df_ods.columns:
        print(f"\n🏷️  ODS por categoria:")
        df_stats_cat = df_ods[df_ods['ods_sk'] != 0]
        for categoria in sorted(df_stats_cat['ods_categoria'].unique()):
            count = len(df_stats_cat[df_stats_cat['ods_categoria'] == categoria])
            print(f"  {categoria}: {count} ODS")
    
    # Mostrar lista completa dos ODS (excluindo registro SK=0)
    df_lista = df_ods[df_ods['ods_sk'] != 0].sort_values('ods_numero')
    if len(df_lista) > 0:
        print(f"\n🎯 Lista completa dos ODS:")
        print("\n📍 ODS Oficiais da ONU (1-17):")
        for _, row in df_lista[df_lista['ods_numero'] <= 17].iterrows():
            print(f"  {row['ods_codigo']}: {row['ods_nome']} [{row['ods_categoria']}]")
        
        print("\n📍 ODS Expandidos (18-20):")
        for _, row in df_lista[df_lista['ods_numero'] > 17].iterrows():
            print(f"  {row['ods_codigo']}: {row['ods_nome']} [{row['ods_categoria']}]")
    
    print(f"\n✅ Processo concluído! Dimensão ODS criada com sucesso.")
    print("💡 A dimensão inclui 20 ODS organizados por categorias e tipos.")
    print("🔬 ODS 18-20 são expansões para contemplar Ciência/Tecnologia, Cultura e Governança Global.")
    print("🔗 Esta dimensão pode ser usada para análises de alinhamento da pesquisa de pós-graduação com os ODS.")
    print("📝 Os descritores em 'ods_temas_relacionados' facilitam a classificação automática de produções.")