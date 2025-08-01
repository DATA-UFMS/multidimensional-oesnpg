#!/usr/bin/env python3
"""
🏭 SCRIPT PRINCIPAL PARA CRIAÇÃO DA TABELA FATO
===============================================
Cria a tabela fato_pos_graduacao com dados gerados a partir das dimensões.
Usa psycopg2 diretamente para máxima compatibilidade.

Uso: python models/facts/create_fact_table.py
"""

import pandas as pd
import numpy as np
import random
import psycopg2
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

def conectar_banco():
    """Conecta ao banco PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")
        return None

def obter_dimensoes():
    """Obtém dados das dimensões disponíveis"""
    print("📊 Obtendo dados das dimensões...")
    
    conn = conectar_banco()
    if conn is None:
        return None
    
    try:
        # Verificar dimensões disponíveis
        df_tabelas = pd.read_sql(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name LIKE %s",
            conn, params=['public', 'dim_%']
        )
        
        tabelas_disponiveis = df_tabelas['table_name'].tolist()
        print(f"📋 Dimensões disponíveis: {tabelas_disponiveis}")
        
        # Carregar dimensões essenciais
        df_tempo = pd.read_sql("SELECT tempo_sk, ano, mes FROM dim_tempo WHERE ano BETWEEN 2021 AND 2024", conn)
        df_localidade = pd.read_sql("SELECT localidade_sk, uf, regiao FROM dim_localidade WHERE localidade_sk > 0", conn)
        
        # Dimensões opcionais
        df_ies = None
        df_tema = None
        df_ods = None
        df_docente = None
        
        if 'dim_ies' in tabelas_disponiveis:
            df_ies = pd.read_sql("SELECT ies_sk, sigla_uf FROM dim_ies WHERE ies_sk > 0 LIMIT 100", conn)
        
        if 'dim_tema' in tabelas_disponiveis:
            # Carregar uma amostra representativa de temas de diferentes UFs
            df_tema = pd.read_sql("""
                SELECT DISTINCT tema_sk, nome_tema, nome_uf 
                FROM dim_tema 
                WHERE tema_sk > 0 
                ORDER BY tema_sk
            """, conn)
        
        if 'dim_ods' in tabelas_disponiveis:
            df_ods = pd.read_sql("SELECT ods_sk FROM dim_ods WHERE ods_sk > 0", conn)
        
        if 'dim_docente' in tabelas_disponiveis:
            df_docente = pd.read_sql("SELECT docente_sk FROM dim_docente WHERE docente_sk > 0 LIMIT 100", conn)
        
        print(f"✅ Tempo: {len(df_tempo)}, Localidade: {len(df_localidade)}")
        if df_ies is not None:
            print(f"✅ IES: {len(df_ies)}")
        if df_tema is not None:
            print(f"✅ Tema: {len(df_tema)}")
        if df_ods is not None:
            print(f"✅ ODS: {len(df_ods)}")
        if df_docente is not None:
            print(f"✅ Docente: {len(df_docente)}")
        
        conn.close()
        return df_tempo, df_localidade, df_ies, df_tema, df_ods, df_docente
        
    except Exception as e:
        print(f"❌ Erro ao carregar dimensões: {e}")
        conn.close()
        return None

def gerar_tabela_fato(dimensoes):
    """Gera dados para a tabela fato"""
    if dimensoes is None:
        return None
    
    df_tempo, df_localidade, df_ies, df_tema, df_ods, df_docente = dimensoes
    
    print("🏭 Gerando tabela fato...")
    
    dados_fato = []
    
    # Gerar registros para cada ano
    for ano in [2021, 2022, 2023, 2024]:
        print(f"  Processando ano {ano}...")
        
        # Filtrar tempo para o ano
        tempo_ano = df_tempo[df_tempo['ano'] == ano]
        if tempo_ano.empty:
            continue
        
        # Para cada localidade, gerar registros com temas apropriados para essa UF
        for _, localidade in df_localidade.iterrows():
            uf_localidade = localidade['uf']
            
            # Mapeamento de siglas UF para nomes completos (para match com dim_tema)
            mapa_uf = {
                'AC': 'ACRE', 'AL': 'ALAGOAS', 'AP': 'AMAPÁ', 'AM': 'AMAZONAS',
                'BA': 'BAHIA', 'CE': 'CEARÁ', 'DF': 'DISTRITO FEDERAL', 'ES': 'ESPÍRITO SANTO',
                'GO': 'GOIÁS', 'MA': 'MARANHÃO', 'MT': 'MATO GROSSO', 'MS': 'MATO GROSSO DO SUL',
                'MG': 'MINAS GERAIS', 'PA': 'PARÁ', 'PB': 'PARAÍBA', 'PR': 'PARANÁ',
                'PE': 'PERNAMBUCO', 'PI': 'PIAUÍ', 'RJ': 'RIO DE JANEIRO', 'RN': 'RIO GRANDE DO NORTE',
                'RS': 'RIO GRANDE DO SUL', 'RO': 'RONDÔNIA', 'RR': 'RORAIMA', 'SC': 'SANTA CATARINA',
                'SP': 'SÃO PAULO', 'SE': 'SERGIPE', 'TO': 'TOCANTINS'
            }
            
            uf_nome_completo = mapa_uf.get(uf_localidade, uf_localidade)
            
            # Buscar todos os temas relacionados a essa UF
            if df_tema is not None and not df_tema.empty:
                temas_uf = df_tema[df_tema['nome_uf'] == uf_nome_completo]
                
                # Se não houver temas específicos para essa UF, usar uma amostra geral
                if temas_uf.empty:
                    print(f"⚠️ Nenhum tema encontrado para {uf_nome_completo}, usando amostra geral")
                    temas_uf = df_tema.sample(n=min(10, len(df_tema)), random_state=42)
                
                # Usar TODOS os temas dessa UF para gerar registros na fato
                for _, tema in temas_uf.iterrows():
                    # Usar primeiro tempo do ano
                    tempo_sk = tempo_ano.iloc[0]['tempo_sk']
                    
                    # Chaves das outras dimensões
                    ies_sk = 1
                    if df_ies is not None and not df_ies.empty:
                        ies_sk = df_ies.iloc[random.randint(0, len(df_ies)-1)]['ies_sk']
                    
                    ods_sk = 1
                    if df_ods is not None and not df_ods.empty:
                        ods_sk = df_ods.iloc[random.randint(0, len(df_ods)-1)]['ods_sk']
                    
                    docente_sk = 1
                    if df_docente is not None and not df_docente.empty:
                        docente_sk = df_docente.iloc[random.randint(0, len(df_docente)-1)]['docente_sk']
                    
                    # Gerar métricas aleatórias
                    base_multiplicador = 1.0 + (ano - 2021) * 0.1
                    
                    registro = {
                        'tempo_sk': tempo_sk,
                        'ppg_sk': 1,  # Usar padrão
                        'ies_sk': ies_sk,
                        'localidade_sk': localidade['localidade_sk'],
                        'tema_sk': tema['tema_sk'],
                        'producao_sk': 1,  # Usar padrão
                        'ods_sk': ods_sk,
                        'docente_sk': docente_sk,
                        
                        # Métricas simplificadas
                        'num_cursos': int(random.uniform(1, 10) * base_multiplicador),
                        'num_trabalhos_conclusao': int(random.uniform(5, 30) * base_multiplicador),
                        'num_tc_mestrado': int(random.uniform(10, 50) * base_multiplicador),
                        'num_tc_doutorado': int(random.uniform(2, 20) * base_multiplicador),
                        'num_trabalhos_pesquisa': int(random.uniform(5, 25) * base_multiplicador),
                        'num_artigos_publicados': int(random.uniform(2, 15) * base_multiplicador),
                        'num_livros_publicados': int(random.uniform(0, 3) * base_multiplicador),
                        'num_capitulos_livro': int(random.uniform(0, 8) * base_multiplicador),
                        'num_producao_tecnica': int(random.uniform(0, 10) * base_multiplicador),
                        'num_orientacoes': int(random.uniform(1, 15) * base_multiplicador),
                        'num_docentes_total': int(random.uniform(5, 50) * base_multiplicador),
                        'num_doutores': int(random.uniform(2, 40) * base_multiplicador),
                        'num_regime_dedicacao': int(random.uniform(2, 30) * base_multiplicador),
                        'num_discentes_matriculados': int(random.uniform(20, 200) * base_multiplicador),
                        'num_bolsas_concedidas': int(random.uniform(5, 50) * base_multiplicador),
                        'investimento_pesquisa': round(random.uniform(10000, 300000) * base_multiplicador, 2),
                        'nota_avaliacao_capes': round(random.uniform(3.0, 6.0), 1),
                        'impacto_ods': round(random.uniform(0.1, 0.8), 2)
                    }
                    
                    dados_fato.append(registro)
    
    df_fato = pd.DataFrame(dados_fato)
    print(f"✅ Tabela fato gerada com {len(df_fato)} registros")
    
    return df_fato

def salvar_tabela_fato(df_fato):
    """Salva a tabela fato no banco"""
    if df_fato is None or df_fato.empty:
        print("❌ Nenhum dado para salvar")
        return False
    
    conn = conectar_banco()
    if conn is None:
        return False
    
    try:
        # Deletar tabela existente se houver
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS fato_pos_graduacao")
        
        # Criar tabela
        create_table_sql = '''
        CREATE TABLE fato_pos_graduacao (
            tempo_sk INTEGER,
            ppg_sk INTEGER,
            ies_sk INTEGER,
            localidade_sk INTEGER,
            tema_sk INTEGER,
            producao_sk INTEGER,
            ods_sk INTEGER,
            docente_sk INTEGER,
            num_cursos INTEGER,
            num_trabalhos_conclusao INTEGER,
            num_tc_mestrado INTEGER,
            num_tc_doutorado INTEGER,
            num_trabalhos_pesquisa INTEGER,
            num_artigos_publicados INTEGER,
            num_livros_publicados INTEGER,
            num_capitulos_livro INTEGER,
            num_producao_tecnica INTEGER,
            num_orientacoes INTEGER,
            num_docentes_total INTEGER,
            num_doutores INTEGER,
            num_regime_dedicacao INTEGER,
            num_discentes_matriculados INTEGER,
            num_bolsas_concedidas INTEGER,
            investimento_pesquisa NUMERIC(15,2),
            nota_avaliacao_capes NUMERIC(3,1),
            impacto_ods NUMERIC(3,2)
        )
        '''
        
        cursor.execute(create_table_sql)
        conn.commit()
        
        # Inserir dados linha por linha
        insert_sql = '''
            INSERT INTO fato_pos_graduacao (
                tempo_sk, ppg_sk, ies_sk, localidade_sk, tema_sk, producao_sk, ods_sk, docente_sk,
                num_cursos, num_trabalhos_conclusao, num_tc_mestrado, num_tc_doutorado,
                num_trabalhos_pesquisa, num_artigos_publicados, num_livros_publicados,
                num_capitulos_livro, num_producao_tecnica, num_orientacoes, num_docentes_total,
                num_doutores, num_regime_dedicacao, num_discentes_matriculados, num_bolsas_concedidas,
                investimento_pesquisa, nota_avaliacao_capes, impacto_ods
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        
        # Converter DataFrame para lista de tuplas
        dados_para_inserir = []
        for _, row in df_fato.iterrows():
            dados_para_inserir.append(tuple(row))
        
        # Inserir em lotes
        cursor.executemany(insert_sql, dados_para_inserir)
        conn.commit()
        
        print(f"✅ Tabela fato salva com {len(df_fato)} registros")
        
        # Verificar resultado
        cursor.execute("SELECT COUNT(*) FROM fato_pos_graduacao")
        count = cursor.fetchone()[0]
        print(f"✅ Verificação: {count} registros na tabela")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Erro ao salvar: {e}")
        conn.close()
        return False

def mostrar_estatisticas():
    """Mostra estatísticas da tabela fato"""
    conn = conectar_banco()
    if conn is None:
        return
    
    try:
        # Estatísticas gerais
        stats = pd.read_sql('''
            SELECT 
                COUNT(*) as total_registros,
                COUNT(DISTINCT tempo_sk) as periodos_tempo,
                COUNT(DISTINCT localidade_sk) as localidades,
                COUNT(DISTINCT ies_sk) as instituicoes,
                SUM(num_cursos) as total_cursos,
                SUM(num_trabalhos_conclusao) as total_trabalhos
            FROM fato_pos_graduacao
        ''', conn)
        
        print(f"\n📊 Estatísticas da Tabela Fato:")
        print(f"Total de registros: {stats.iloc[0]['total_registros']:,}")
        print(f"Períodos de tempo: {stats.iloc[0]['periodos_tempo']}")
        print(f"Localidades: {stats.iloc[0]['localidades']}")
        print(f"Instituições: {stats.iloc[0]['instituicoes']}")
        print(f"Total de cursos: {stats.iloc[0]['total_cursos']:,}")
        print(f"Total de trabalhos: {stats.iloc[0]['total_trabalhos']:,}")
        
        # Estatísticas por ano
        stats_ano = pd.read_sql('''
            SELECT 
                t.ano,
                COUNT(*) as registros,
                SUM(f.num_cursos) as cursos,
                SUM(f.num_tc_mestrado) as mestres,
                SUM(f.num_tc_doutorado) as doutores
            FROM fato_pos_graduacao f
            JOIN dim_tempo t ON f.tempo_sk = t.tempo_sk
            GROUP BY t.ano
            ORDER BY t.ano
        ''', conn)
        
        print(f"\n📊 Estatísticas por Ano:")
        for _, row in stats_ano.iterrows():
            print(f"  {row['ano']}: {row['registros']} registros, {row['cursos']} cursos, {row['mestres']} mestres, {row['doutores']} doutores")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro ao obter estatísticas: {e}")
        conn.close()

def main():
    """Função principal"""
    print("🚀 Iniciando geração da tabela fato...")
    
    # Obter dimensões
    dimensoes = obter_dimensoes()
    if dimensoes is None:
        print("❌ Falha ao obter dimensões")
        return
    
    # Gerar tabela fato
    df_fato = gerar_tabela_fato(dimensoes)
    if df_fato is None:
        print("❌ Falha ao gerar tabela fato")
        return
    
    # Salvar no banco
    if salvar_tabela_fato(df_fato):
        print("🎉 Tabela fato criada com sucesso!")
        mostrar_estatisticas()
    else:
        print("❌ Falha ao salvar tabela fato")

if __name__ == "__main__":
    main()
