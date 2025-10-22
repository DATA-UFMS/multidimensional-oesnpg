


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

MACROCATEGORIAS = {
    "Social": {
        "numeros": {1, 2, 3, 4, 5, 10, 18, 19},
        "ods_associados": (
            "ODS 1 - Erradicação da Pobreza; "
            "ODS 2 - Fome Zero e Agricultura Sustentável; "
            "ODS 3 - Saúde e Bem-Estar; "
            "ODS 4 - Educação de Qualidade; "
            "ODS 5 - Igualdade de Gênero; "
            "ODS 10 - Redução das Desigualdades; "
            "ODS 18 - Cultura da Paz e Direitos Humanos; "
            "ODS 19 - Educação Superior de Qualidade, Inclusiva e Sustentável"
        ),
        "foco_principal": "Foco em bem-estar humano, inclusão social, igualdade de gênero e raça, formação educacional e valorização da diversidade cultural.",
    },
    "Econômica": {
        "numeros": {7, 8, 9, 11, 20},
        "ods_associados": (
            "ODS 7 - Energia Acessível e Limpa; "
            "ODS 8 - Trabalho Decente e Crescimento Econômico; "
            "ODS 9 - Indústria, Inovação e Infraestrutura; "
            "ODS 11 - Cidades e Comunidades Sustentáveis; "
            "ODS 20 - Ciência, Tecnologia e Inovação para o Desenvolvimento Sustentável"
        ),
        "foco_principal": "Foco em crescimento sustentável, inovação, geração de emprego, infraestrutura inteligente e transformação tecnológica.",
    },
    "Ambiental": {
        "numeros": {6, 12, 13, 14, 15},
        "ods_associados": (
            "ODS 6 - Água Potável e Saneamento; "
            "ODS 12 - Consumo e Produção Responsáveis; "
            "ODS 13 - Ação Contra a Mudança Global do Clima; "
            "ODS 14 - Vida na Água; "
            "ODS 15 - Vida Terrestre"
        ),
        "foco_principal": "Foco em conservação ambiental, uso sustentável dos recursos naturais, saneamento básico e biodiversidade.",
    },
    "Institucional / Governança": {
        "numeros": {16, 17},
        "transversal": {18, 20},
        "ods_associados": (
            "ODS 16 - Paz, Justiça e Instituições Eficazes; "
            "ODS 17 - Parcerias e Meios de Implementação; "
            "ODS 18 - Cultura da Paz e Direitos Humanos (Transversal); "
            "ODS 20 - Ciência, Tecnologia e Inovação para o Desenvolvimento Sustentável (Transversal)"
        ),
        "foco_principal": "Foco em governança democrática, cooperação interinstitucional, fortalecimento de políticas públicas e implementação dos ODS.",
    },
}

def extrair_dados_ods():
    """
    Extrai dados dos 20 Objetivos de Desenvolvimento Sustentável (17 ODS oficiais da ONU + 3 ODS expandidos).
    Baseado no documento: "Descritores 17 ODS (+18+19+20) CACS 23062024_Versão Consolidada"
    """
    print("Extraindo dados dos ODS (Objetivos de Desenvolvimento Sustentável)...")

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
            (18, "Cultura da paz e direitos humanos", "Promover a cultura de paz, o respeito aos direitos humanos e a resolução pacífica de conflitos em todos os níveis.", "direitos humanos, cultura de paz, resolução de conflitos, mediação, justiça restaurativa, educação em direitos, diversidade cultural, inclusão social"),
            (19, "Educação superior de qualidade, inclusiva e sustentável", "Garantir educação superior de qualidade, inclusiva e sustentável, fortalecendo ensino, pesquisa e extensão com equidade.", "educação superior, universidades, inclusão acadêmica, permanência estudantil, qualidade acadêmica, pesquisa e extensão, inovação pedagógica, sustentabilidade institucional"),
            (20, "Ciência, tecnologia e inovação para o desenvolvimento sustentável", "Fomentar a ciência, a tecnologia e a inovação como bases para o desenvolvimento sustentável e a difusão do conhecimento.", "pesquisa científica, desenvolvimento tecnológico, inovação para sustentabilidade, acesso ao conhecimento científico, formação de recursos humanos em CT&I, infraestrutura de pesquisa, propriedade intelectual, divulgação científica")
        ]
        
        # Converter para DataFrame
        df_ods = pd.DataFrame(
            ods_data,
            columns=["ods_numero", "ods_nome", "ods_descricao", "ods_temas_relacionados"],
        )

        # Adicionar campo de tipo (Oficial da ONU ou Expandido)
        df_ods['ods_tipo'] = df_ods['ods_numero'].apply(
            lambda x: 'Oficial ONU' if x <= 17 else 'Expandido'
        )

        # Tratar e limpar dados
        df_ods = tratar_dados_ods(df_ods)

        print(f"Dados dos ODS extraídos: {len(df_ods)} registros (17 ODS oficiais + 3 expandidos)")

        return df_ods

    except Exception as e:
        print(f"Erro ao extrair dados dos ODS: {e}")
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

        macrocategoria_info = df_ods['ods_numero'].apply(_mapear_macrocategoria)
        df_ods['ods_macrocategoria'] = macrocategoria_info.apply(lambda item: item[0])
        df_ods['ods_associados'] = macrocategoria_info.apply(lambda item: item[1])
        df_ods['ods_foco_principal'] = macrocategoria_info.apply(lambda item: item[2])

        # A associação entre ODS e temas será tratada nas tabelas fato, portanto
        # removemos os descritores textuais neste ponto para manter a dimensão enxuta.
        df_ods = df_ods.drop(columns=['ods_temas_relacionados'], errors='ignore')

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
    18: 'Social',         # Cultura da paz e direitos humanos
    19: 'Social',         # Educação superior de qualidade, inclusiva e sustentável
    20: 'Econômico-Social' # Ciência, tecnologia e inovação para o desenvolvimento sustentável
    }
    return categorias.get(numero, 'Geral')


def _mapear_macrocategoria(numero: int):
    for nome, info in MACROCATEGORIAS.items():
        if numero in info.get("numeros", set()):
            return (
                nome,
                info["ods_associados"],
                info["foco_principal"],
            )

    for nome, info in MACROCATEGORIAS.items():
        if numero in info.get("transversal", set()):
            return (
                f"{nome} (Transversal)",
                info["ods_associados"],
                info["foco_principal"],
            )

    return (
        'Expansão Complementar',
        f'ODS {numero}',
        'Eixos complementares definidos nas expansões dos ODS.',
    )


def create_sk0_record():
    """
    Cria o registro SK=0 para valores desconhecidos/não aplicáveis.
    """
    registro_sk0 = {
        'ods_numero': 0,
        'ods_nome': 'Não informado',
        'ods_descricao': 'Registro para valores desconhecidos ou não aplicáveis',
        'ods_codigo': 'ODS-00',
        'ods_status': 'Não informado',
        'ods_categoria': 'Não informado',
        'ods_tipo': 'Não informado',
        'ods_macrocategoria': 'Não informado',
        'ods_associados': 'Não informado',
        'ods_foco_principal': 'Não informado'
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
            conn.exec_driver_sql("DROP TABLE IF EXISTS dim_ods CASCADE;")

            # Primeiro criar a tabela com estrutura explícita
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS dim_ods (
                ods_sk INTEGER PRIMARY KEY,
                ods_numero INTEGER NOT NULL,
                ods_nome VARCHAR(255) NOT NULL,
                ods_descricao TEXT,
                ods_codigo VARCHAR(20) NOT NULL,
                ods_status VARCHAR(50),
                ods_categoria VARCHAR(50),
                ods_macrocategoria VARCHAR(60),
                ods_associados TEXT,
                ods_foco_principal TEXT,
                ods_tipo VARCHAR(50) NOT NULL DEFAULT 'Oficial ONU',
                CONSTRAINT check_ods_numero CHECK (ods_numero >= 0 AND ods_numero <= 20)
            );
            
            COMMENT ON TABLE dim_ods IS 'Dimensão dos Objetivos de Desenvolvimento Sustentável - 17 ODS oficiais da ONU + 3 ODS expandidos (Ciência/Tecnologia, Cultura, Governança Global)';
            COMMENT ON COLUMN dim_ods.ods_sk IS 'Surrogate key da dimensão ODS';
            COMMENT ON COLUMN dim_ods.ods_numero IS 'Número do ODS (1-20, onde 18-20 são ODS expandidos)';
            COMMENT ON COLUMN dim_ods.ods_tipo IS 'Tipo do ODS: Oficial ONU (1-17) ou Expandido (18-20)';
            COMMENT ON COLUMN dim_ods.ods_categoria IS 'Categoria temática: Social, Ambiental, Econômico, Governança, ou combinações';
            COMMENT ON COLUMN dim_ods.ods_macrocategoria IS 'Agrupamento estratégico dos ODS em macrocategorias definidas pela CAPES';
            COMMENT ON COLUMN dim_ods.ods_associados IS 'Lista de ODS associados à macrocategoria';
            COMMENT ON COLUMN dim_ods.ods_foco_principal IS 'Foco principal da macrocategoria dos ODS';
            """
            
            # Executar a criação da tabela
            conn.exec_driver_sql(create_table_sql)
            
            # Limpar tabela se já existir dados
            conn.exec_driver_sql("DELETE FROM dim_ods;")
            
            # Inserir dados
            df_ods.to_sql('dim_ods', conn, if_exists='append', index=False)
        print(f"Dimensão ODS salva no PostgreSQL com {len(df_ods)} registros")

    except Exception as e:
        print(f"Erro ao salvar dimensão ODS: {e}")

if __name__ == "__main__":
    print("Iniciando processo de criação da dimensão ODS")
    print("Fonte: Descritores 17 ODS (+18+19+20) CACS - Versão Consolidada")
    print("Inclui: 17 ODS oficiais da ONU + 3 ODS expandidos")
    
    # Extrair dados dos ODS
    df_ods = extrair_dados_ods()
    
    if df_ods.empty:
        print("Nenhum dado foi retornado. Encerrando o script.")
        exit(1)
    
    # Salvar no banco
    salvar_dimensao_ods(df_ods)
    
    # Mostrar algumas estatísticas
    print("\nEstatísticas da dimensão ODS:")
    print(f"Total de registros: {len(df_ods)} (incluindo SK=0)")
    
    # Estatísticas por tipo
    if 'ods_tipo' in df_ods.columns:
        print("\nODS por tipo:")
        df_stats_tipo = df_ods[df_ods['ods_sk'] != 0]
        for tipo in sorted(df_stats_tipo['ods_tipo'].unique()):
            count = len(df_stats_tipo[df_stats_tipo['ods_tipo'] == tipo])
            print(f"  {tipo}: {count} ODS")
    
    # Estatísticas por categoria
    if 'ods_categoria' in df_ods.columns:
        print("\nODS por categoria:")
        df_stats_cat = df_ods[df_ods['ods_sk'] != 0]
        for categoria in sorted(df_stats_cat['ods_categoria'].unique()):
            count = len(df_stats_cat[df_stats_cat['ods_categoria'] == categoria])
            print(f"  {categoria}: {count} ODS")
    
    # Mostrar lista completa dos ODS (excluindo registro SK=0)
    df_lista = df_ods[df_ods['ods_sk'] != 0].sort_values('ods_numero')
    if len(df_lista) > 0:
        print("\nLista completa dos ODS:")
        print("\nODS Oficiais da ONU (1-17):")
        for _, row in df_lista[df_lista['ods_numero'] <= 17].iterrows():
            print(f"  {row['ods_codigo']}: {row['ods_nome']} [{row['ods_categoria']}]")
        
        print("\nODS Expandidos (18-20):")
        for _, row in df_lista[df_lista['ods_numero'] > 17].iterrows():
            print(f"  {row['ods_codigo']}: {row['ods_nome']} [{row['ods_categoria']}]")
    print("\nProcesso concluído. Dimensão ODS criada com sucesso.")
    print("A dimensão inclui 20 ODS organizados por categorias e tipos.")
    print("ODS 18-20 são expansões para contemplar Ciência/Tecnologia, Cultura e Governança Global.")
    print("Esta dimensão pode ser usada para análises de alinhamento da pesquisa de pós-graduação com os ODS.")
    print("A associação entre ODS e temas deve ser tratada nas tabelas fato para garantir flexibilidade analítica.")