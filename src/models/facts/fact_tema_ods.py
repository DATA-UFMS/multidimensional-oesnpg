#!/usr/bin/env python3
"""
Tabela Fato: fact_tema_ods
Associa temas/palavras-chave com Objetivos de Desenvolvimento Sustentável (ODS)

Esta é uma tabela fato sem medidas (factless fact table) que registra
o relacionamento entre temas de pesquisa e ODS para análises de alinhamento.
"""

import sys
import os
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Adicionar diretório raiz ao path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent.parent
sys.path.insert(0, str(project_root))

# Importar metadados compartilhados de ODS
try:
    from src.models.dimensions.dim_ods import MACROCATEGORIAS  # type: ignore
except ModuleNotFoundError:
    MACROCATEGORIAS = {}

# Carregar variáveis de ambiente
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")


ODS_DESCRITORES = {
    1: "pobreza extrema, renda per capita, vulnerabilidade social, proteção social, acesso a serviços básicos, desigualdade de renda, linha de pobreza, segurança econômica",
    2: "segurança alimentar, nutrição infantil, agricultura familiar, produtividade agrícola, sistemas alimentares sustentáveis, agroecologia, soberania alimentar, desnutrição",
    3: "mortalidade materno-infantil, doenças transmissíveis, saúde mental, cobertura universal de saúde, acesso a medicamentos, epidemias, saúde reprodutiva, vacinação",
    4: "educação básica universal, alfabetização, ensino superior, formação técnica profissional, igualdade de acesso educacional, educação para desenvolvimento sustentável, infraestrutura escolar",
    5: "empoderamento feminino, violência de gênero, participação política das mulheres, direitos reprodutivos, liderança feminina, discriminação de gênero, igualdade salarial, trabalho doméstico não remunerado",
    6: "acesso universal à água potável, saneamento básico, gestão integrada de recursos hídricos, qualidade da água, eficiência hídrica, proteção de ecossistemas aquáticos, higiene",
    7: "energias renováveis, eficiência energética, acesso universal à energia, matriz energética limpa, tecnologias sustentáveis de energia, energia solar, energia eólica, biomassa",
    8: "emprego pleno e produtivo, trabalho decente, crescimento econômico inclusivo, produtividade econômica, empreendedorismo, direitos trabalhistas, trabalho infantil, trabalho forçado",
    9: "infraestrutura resiliente, industrialização sustentável, inovação tecnológica, pesquisa e desenvolvimento, conectividade, acesso à internet, pequenas indústrias, transferência de tecnologia",
    10: "desigualdade de renda, inclusão social e econômica, migração segura e ordenada, políticas redistributivas, discriminação, igualdade de oportunidades, representação nos processos decisórios",
    11: "urbanização sustentável, habitação adequada e acessível, transporte público sustentável, gestão de resíduos urbanos, planejamento urbano participativo, patrimônio cultural e natural, espaços públicos seguros",
    12: "eficiência no uso de recursos naturais, economia circular, gestão sustentável de resíduos, práticas sustentáveis corporativas, consumo consciente, desperdício de alimentos, produtos químicos e resíduos perigosos",
    13: "mitigação climática, adaptação às mudanças climáticas, redução de emissões de gases do efeito estufa, resiliência climática, financiamento climático, educação climática, desastres relacionados ao clima",
    14: "conservação marinha e costeira, pesca sustentável, poluição oceânica, ecossistemas aquáticos, acidificação dos oceanos, biodiversidade marinha, áreas marinhas protegidas, recursos genéticos marinhos",
    15: "biodiversidade terrestre, desertificação, gestão florestal sustentável, conservação de habitats, espécies ameaçadas de extinção, degradação do solo, tráfico de fauna e flora, ecossistemas de montanha",
    16: "redução da violência, estado de direito, acesso à justiça, transparência institucional, combate à corrupção, instituições eficazes e responsáveis, participação cidadã, identidade legal universal",
    17: "cooperação internacional, assistência oficial ao desenvolvimento, financiamento para desenvolvimento, transferência de tecnologia, capacitação institucional, parcerias multissetoriais, comércio internacional justo",
    18: "direitos humanos, cultura de paz, resolução de conflitos, mediação, justiça restaurativa, educação em direitos, diversidade cultural, inclusão social",
    19: "educação superior, universidades, inclusão acadêmica, permanência estudantil, qualidade acadêmica, pesquisa e extensão, inovação pedagógica, sustentabilidade institucional",
    20: "pesquisa científica, desenvolvimento tecnológico, inovação para sustentabilidade, acesso ao conhecimento científico, formação de recursos humanos em CT&I, infraestrutura de pesquisa, propriedade intelectual, divulgação científica",
}


def _construir_mapa_macrocategorias():
    mapa = defaultdict(list)

    for nome, info in MACROCATEGORIAS.items():
        foco = info.get("foco_principal", "")

        for numero in info.get("numeros", set()):
            try:
                numero_int = int(numero)
            except (TypeError, ValueError):
                continue

            mapa[numero_int].append({
                "nome": nome,
                "tipo": "principal",
                "foco": foco,
            })

        for numero in info.get("transversal", set()):
            try:
                numero_int = int(numero)
            except (TypeError, ValueError):
                continue

            mapa[numero_int].append({
                "nome": nome,
                "tipo": "transversal",
                "foco": foco,
            })

    return {chave: valor for chave, valor in mapa.items()}


ODS_MACROCATEGORIAS = _construir_mapa_macrocategorias()


def _normalizar_numero_ods(valor):
    if pd.isna(valor):
        return None

    if isinstance(valor, (int, float)) and not isinstance(valor, bool):
        try:
            return int(valor)
        except (TypeError, ValueError):
            return None

    texto = str(valor)
    digitos = ''.join(ch for ch in texto if ch.isdigit())
    return int(digitos) if digitos else None


def _formatar_rotulo_macrocategoria(info):
    nome = info.get("nome", "NÃO INFORMADO")
    if info.get("tipo") == "transversal":
        return f"{nome} (Transversal)"
    return nome


def _montar_observacao(palavra, macro_rotulos):
    base = f"Match automático: \"{palavra}\" encontrada em descritores ODS"
    if macro_rotulos:
        return f"{base}; macrocategoria(s): {', '.join(macro_rotulos)}"
    return base


def criar_tabela_fact_tema_ods():
    """
    Cria a estrutura da tabela fato fact_tema_ods
    """
    print("Criando estrutura da tabela fact_tema_ods...")
    
    url = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(url)
    
    # Verificar se as dimensões existem
    check_dims = """
    SELECT 
        EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dim_tema') as tem_tema,
        EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dim_ods') as tem_ods
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(check_dims)).fetchone()
        tem_tema = result[0] if result else False
        tem_ods = result[1] if result else False
    
    print(f"   dim_tema existe: {'Sim' if tem_tema else 'Não'}")
    print(f"   dim_ods existe: {'Sim' if tem_ods else 'Não'}")
    
    if not tem_tema or not tem_ods:
        print("\nCriando tabela sem foreign keys (dimensões faltando)")
        fk_clausula = ""
    else:
        print("\nCriando tabela com foreign keys")
        fk_clausula = """
        -- Chaves estrangeiras
        ,CONSTRAINT fk_fact_tema_ods_tema 
            FOREIGN KEY (tema_sk) REFERENCES dim_tema(tema_sk)
        ,CONSTRAINT fk_fact_tema_ods_ods 
            FOREIGN KEY (ods_sk) REFERENCES dim_ods(ods_sk)
        """
    
    ddl = f"""
    -- Remover tabela se existir
    DROP TABLE IF EXISTS fact_tema_ods CASCADE;
    
    -- Criar tabela fato
    CREATE TABLE fact_tema_ods (
        tema_ods_id SERIAL PRIMARY KEY,
        tema_sk INTEGER NOT NULL,
        ods_sk INTEGER NOT NULL,
        tipo_associacao VARCHAR(50) DEFAULT 'Manual',
        nivel_confianca DECIMAL(5,2),
        data_associacao DATE DEFAULT CURRENT_DATE,
        usuario_associacao VARCHAR(100),
        observacao TEXT,
        ativo BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        {fk_clausula}
        -- Garantir unicidade da associação
        ,CONSTRAINT uk_tema_ods UNIQUE (tema_sk, ods_sk)
    );
    
    -- Índices para otimizar consultas
    CREATE INDEX idx_fact_tema_ods_tema ON fact_tema_ods(tema_sk);
    CREATE INDEX idx_fact_tema_ods_ods ON fact_tema_ods(ods_sk);
    CREATE INDEX idx_fact_tema_ods_tipo ON fact_tema_ods(tipo_associacao);
    CREATE INDEX idx_fact_tema_ods_ativo ON fact_tema_ods(ativo);
    
    -- Comentários
    COMMENT ON TABLE fact_tema_ods IS 'Tabela fato que associa temas de pesquisa com ODS';
    COMMENT ON COLUMN fact_tema_ods.tema_ods_id IS 'Chave primária da associação';
    COMMENT ON COLUMN fact_tema_ods.tema_sk IS 'FK para dim_tema';
    COMMENT ON COLUMN fact_tema_ods.ods_sk IS 'FK para dim_ods';
    COMMENT ON COLUMN fact_tema_ods.tipo_associacao IS 'Manual, Automática, Validada, etc.';
    COMMENT ON COLUMN fact_tema_ods.nivel_confianca IS 'Nível de confiança da associação (0-100%)';
    COMMENT ON COLUMN fact_tema_ods.ativo IS 'Se a associação está ativa';
    """
    
    with engine.begin() as conn:
        conn.execute(text(ddl))
    
    print("Tabela fact_tema_ods criada com sucesso!")
    return True


def mapear_temas_ods_automatico():
    """
    Cria mapeamentos automáticos entre temas e ODS baseado em palavras-chave
    """
    print("\nCriando mapeamentos automáticos tema-ODS...")
    
    url = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(url)
    
    # Carregar temas e ODS
    df_temas = pd.read_sql("SELECT tema_sk, palavra_chave FROM dim_tema WHERE tema_sk > 0", engine)
    df_ods = pd.read_sql("SELECT ods_sk, ods_numero, ods_nome FROM dim_ods WHERE ods_sk > 0", engine)
    df_ods['descritores'] = df_ods['ods_numero'].map(ODS_DESCRITORES).fillna('')

    print(f"Temas carregados: {len(df_temas)}")
    print(f"ODS carregados: {len(df_ods)}")
    
    # Criar mapeamentos baseados em palavras-chave
    mapeamentos = []
    
    for _, tema in df_temas.iterrows():
        palavra_chave = str(tema['palavra_chave']).lower()
        
        for _, ods in df_ods.iterrows():
            descritores = str(ods['descritores']).lower()
            nome_ods = str(ods['ods_nome']).lower()
            
            # Verificar match
            if palavra_chave in descritores or palavra_chave in nome_ods:
                nivel_confianca = 80.0 if palavra_chave in descritores else 60.0
                
                mapeamentos.append({
                    'tema_sk': tema['tema_sk'],
                    'ods_sk': ods['ods_sk'],
                    'tipo_associacao': 'Automática',
                    'nivel_confianca': nivel_confianca,
                    'data_associacao': datetime.now().date(),
                    'usuario_associacao': 'Sistema',
                    'observacao': f'Match automático: "{palavra_chave}" encontrada em descritores ODS',
                    'ativo': True
                })
    
    if mapeamentos:
        df_mapeamentos = pd.DataFrame(mapeamentos)

        # Remover duplicatas
        df_mapeamentos = df_mapeamentos.drop_duplicates(subset=['tema_sk', 'ods_sk'])

        # Inserir no banco
        df_mapeamentos.to_sql('fact_tema_ods', engine, if_exists='append', index=False)

        print(f"{len(df_mapeamentos)} mapeamentos automáticos criados")

        # Estatísticas
        print("\nEstatísticas dos mapeamentos:")
        print(f"   Temas com pelo menos 1 ODS: {df_mapeamentos['tema_sk'].nunique()}")
        print(f"   ODS com pelo menos 1 tema: {df_mapeamentos['ods_sk'].nunique()}")
        print(f"   Média de ODS por tema: {len(df_mapeamentos) / df_mapeamentos['tema_sk'].nunique():.2f}")

        return True
    else:
        print("Nenhum mapeamento automático encontrado")
        return False


def criar_mapeamentos_manuais_exemplo():
    """
    Cria alguns mapeamentos manuais de exemplo para demonstração
    """
    print("\nCriando mapeamentos manuais de exemplo...")
    
    url = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(url)
    
    # Exemplos de mapeamentos manuais importantes
    mapeamentos_manuais = [
        # Exemplo: temas de sustentabilidade mapeados para vários ODS
        # Ajustar os tema_sk conforme sua base de dados real
    ]
    
    if mapeamentos_manuais:
        df_manuais = pd.DataFrame(mapeamentos_manuais)
        df_manuais.to_sql('fact_tema_ods', engine, if_exists='append', index=False)
        print(f"{len(mapeamentos_manuais)} mapeamentos manuais criados")
    else:
        print("Nenhum mapeamento manual de exemplo configurado")
    
    return True


def exibir_estatisticas():
    """
    Exibe estatísticas da tabela fact_tema_ods
    """
    print("\nESTATÍSTICAS DA TABELA FACT_TEMA_ODS")
    print("="*80)
    
    url = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(url)
    
    queries = {
        'Total de associações': "SELECT COUNT(*) as total FROM fact_tema_ods WHERE ativo = TRUE",
        'Associações por tipo': """
            SELECT tipo_associacao, COUNT(*) as total 
            FROM fact_tema_ods 
            WHERE ativo = TRUE
            GROUP BY tipo_associacao 
            ORDER BY total DESC
        """,
        'Top 10 ODS mais associados': """
            SELECT 
                o.numero_ods,
                o.nome_ods,
                COUNT(*) as total_temas
            FROM fact_tema_ods f
            JOIN dim_ods o ON f.ods_sk = o.ods_sk
            WHERE f.ativo = TRUE
            GROUP BY o.numero_ods, o.nome_ods
            ORDER BY total_temas DESC
            LIMIT 10
        """,
        'Top 10 Temas mais associados': """
            SELECT 
                t.palavra_chave,
                COUNT(*) as total_ods
            FROM fact_tema_ods f
            JOIN dim_tema t ON f.tema_sk = t.tema_sk
            WHERE f.ativo = TRUE
            GROUP BY t.palavra_chave
            ORDER BY total_ods DESC
            LIMIT 10
        """
    }
    
    for titulo, query in queries.items():
        print(f"\n{titulo}:")
        print("-"*80)
        try:
            df = pd.read_sql(query, engine)
            if len(df) > 0:
                print(df.to_string(index=False))
            else:
                print("Sem dados")
        except Exception as e:
            print(f"Erro ao executar query: {e}")
    
    print("\n" + "="*80)


def main():
    """
    Função principal
    """
    print("="*80)
    print("CRIAÇÃO DA TABELA FATO: FACT_TEMA_ODS")
    print("="*80)
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        # 1. Criar estrutura da tabela
        criar_tabela_fact_tema_ods()
        
        # 2. Verificar se dimensões existem antes de popular
        url = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(url)
        
        check_dims = """
        SELECT 
            EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dim_tema') as tem_tema,
            EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dim_ods') as tem_ods
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(check_dims)).fetchone()
            tem_tema = result[0] if result else False
            tem_ods = result[1] if result else False
        
        if tem_tema and tem_ods:
            # 3. Criar mapeamentos automáticos
            mapear_temas_ods_automatico()
            
            # 4. Criar mapeamentos manuais de exemplo
            criar_mapeamentos_manuais_exemplo()
            
            # 5. Exibir estatísticas
            exibir_estatisticas()
        else:
            print("\nDimensões não encontradas. Tabela criada mas não populada.")
            print(f"   Execute dim_tema.py e dim_ods.py antes de popular esta tabela.")
            print(f"   Depois execute este script novamente para criar os mapeamentos.")
        
        print("\n" + "="*80)
        print("Tabela fact_tema_ods criada e populada com sucesso!")
        print("="*80)
        print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("\nPróximos passos:")
        print("   1. Revisar e validar os mapeamentos automáticos")
        print("   2. Adicionar mapeamentos manuais específicos do domínio")
        print("   3. Usar esta tabela para análises de alinhamento ODS")
        print("   4. Criar views ou reports baseados nas associações")
        
        return True
        
    except Exception as e:
        print(f"\nErro ao criar tabela fact_tema_ods: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    sucesso = main()
    sys.exit(0 if sucesso else 1)
