#!/usr/bin/env python3
"""
Script para executar o SQL de criação de chaves primárias e estrangeiras
Executa primeiro as PKs nas dimensões e depois as FKs na tabela fato
Versão corrigida para lidar com transações abortadas
"""

import psycopg2
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

def executar_sql_fks():
    """Executa o script SQL de criação de PKs e FKs com tratamento robusto de erros"""
    conn = None
    cursor = None
    
    try:
        # Conectar ao banco
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        
        conn.autocommit = True  # Cada comando será commitado automaticamente
        cursor = conn.cursor()

        # Primeiro: Executar o script SQL de criação de PKs nas dimensões
        print("Executando script de criação de chaves primárias...")
        
        # Lista das PKs para criar individualmente
        pk_commands = [
            ("dim_tempo", "tempo_sk"),
            ("dim_localidade", "localidade_sk"), 
            ("dim_ppg", "ppg_sk"),
            ("dim_ies", "ies_sk"),
            ("dim_tema", "tema_sk"),
            ("dim_producao", "producao_sk"),
            ("dim_ods", "ods_sk"),
            ("dim_docente", "docente_sk")
        ]
        
        for table_name, pk_column in pk_commands:
            try:
                # Verificar se a tabela existe
                cursor.execute(f"""
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = '{table_name}' AND table_schema = 'public'
                """)
                
                if cursor.fetchone():
                    # Verificar se a PK já existe
                    cursor.execute(f"""
                        SELECT constraint_name 
                        FROM information_schema.table_constraints 
                        WHERE table_name = '{table_name}' 
                        AND constraint_type = 'PRIMARY KEY'
                        AND table_schema = 'public'
                    """)
                    
                    existing_pk = cursor.fetchone()
                    
                    if existing_pk:
                        print(f"  ℹ️ PK já existe em {table_name}: {existing_pk[0]}")
                    else:
                        # Criar PK
                        pk_sql = f"""
                        ALTER TABLE {table_name} 
                        ADD CONSTRAINT pk_{table_name} 
                        PRIMARY KEY ({pk_column})
                        """
                        cursor.execute(pk_sql)
                        print(f"  ✅ PK criada em {table_name}")
                else:
                    print(f"  ⚠️ Tabela {table_name} não existe")
                    
            except Exception as e:
                print(f"  ❌ Erro ao criar PK em {table_name}: {e}")
        
        # Segundo: Verificar se fato_pos_graduacao existe
        cursor.execute("""
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = 'fato_pos_graduacao' AND table_schema = 'public'
        """)
        
        if not cursor.fetchone():
            print("❌ Tabela fato_pos_graduacao não existe! Execute primeiro o script de criação da FATO.")
            return
        
        print("Executando script de criação de chaves estrangeiras...")
        
        # Lista das FKs para criar individualmente (apenas as que fazem sentido para nossa FATO)
        fk_commands = [
            ("tempo_sk", "dim_tempo", "tempo_sk"),
            ("localidade_sk", "dim_localidade", "localidade_sk"),
            ("ies_sk", "dim_ies", "ies_sk"),
            ("tema_sk", "dim_tema", "tema_sk")
        ]
        
        for fk_column, ref_table, ref_column in fk_commands:
            try:
                # Verificar se a coluna existe na tabela fato
                cursor.execute(f"""
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'fato_pos_graduacao' 
                    AND column_name = '{fk_column}'
                    AND table_schema = 'public'
                """)
                
                if cursor.fetchone():
                    # Verificar se a FK já existe
                    cursor.execute(f"""
                        SELECT constraint_name 
                        FROM information_schema.table_constraints 
                        WHERE table_name = 'fato_pos_graduacao' 
                        AND constraint_type = 'FOREIGN KEY'
                        AND constraint_name = 'fk_fato_{ref_table.replace("dim_", "")}'
                        AND table_schema = 'public'
                    """)
                    
                    existing_fk = cursor.fetchone()
                    
                    if existing_fk:
                        print(f"  ℹ️ FK já existe: {existing_fk[0]}")
                    else:
                        # Criar FK
                        fk_name = f"fk_fato_{ref_table.replace('dim_', '')}"
                        fk_sql = f"""
                        ALTER TABLE fato_pos_graduacao 
                        ADD CONSTRAINT {fk_name}
                        FOREIGN KEY ({fk_column}) REFERENCES {ref_table}({ref_column})
                        """
                        cursor.execute(fk_sql)
                        print(f"  ✅ FK criada: {fk_name}")
                else:
                    print(f"  ⚠️ Coluna {fk_column} não existe na tabela fato_pos_graduacao")
                    
            except Exception as e:
                print(f"  ❌ Erro ao criar FK {fk_column}: {e}")
        
        
        print("✅ Script executado com sucesso!")
        
        # Verificar FKs criadas
        cursor.execute("""
            SELECT 
                tc.constraint_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY' 
                AND tc.table_name = 'fato_pos_graduacao'
                AND tc.table_schema = 'public'
            ORDER BY tc.constraint_name
        """)
        
        fks = cursor.fetchall()
        
        print(f"\n📋 Chaves estrangeiras criadas ({len(fks)}):")
        for fk in fks:
            constraint_name, column_name, foreign_table = fk
            print(f"  ✅ {constraint_name}: {column_name} -> {foreign_table}")
        
        # Verificar PKs criadas nas dimensões
        cursor.execute("""
            SELECT 
                tc.table_name,
                tc.constraint_name,
                string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as pk_columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY' 
                AND tc.table_name LIKE 'dim_%'
                AND tc.table_schema = 'public'
            GROUP BY tc.table_name, tc.constraint_name
            ORDER BY tc.table_name
        """)
        
        pks = cursor.fetchall()
        
        print(f"\n🔑 Chaves primárias nas dimensões ({len(pks)}):")
        for pk in pks:
            table_name, constraint_name, pk_columns = pk
            print(f"  ✅ {table_name}: {pk_columns}")
        
    except Exception as e:
        print(f"❌ Erro geral no script: {e}")
        
    finally:
        # Fechar conexões de forma segura
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    executar_sql_fks()
