#!/usr/bin/env python3
"""
Script para executar o SQL de criação de chaves primárias e estrangeiras
Executa primeiro as PKs nas dimensões e depois as FKs na tabela fato
"""

import psycopg2
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

def executar_sql_fks():
    """Executa o script SQL de criação de PKs e FKs"""
    try:
        # Conectar ao banco
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        
        cursor = conn.cursor()

        # Primeiro: Executar o script SQL de criação de PKs nas dimensões
        print("🔑 Executando script de criação de chaves primárias...")
        try:
            with open('sql/ddl/add_primary_keys_dimensoes.sql', 'r', encoding='utf-8') as file:
                pk_sql_content = file.read()
            
            cursor.execute(pk_sql_content)
            conn.commit()
            print("✅ Chaves primárias criadas com sucesso!")
            
        except Exception as e:
            print(f"⚠️  Aviso ao criar PKs (podem já existir): {e}")

        # Segundo: Ler o arquivo SQL de FKs
        with open('sql/ddl/add_fks_simples_fato.sql', 'r', encoding='utf-8') as file:
            sql_content = file.read()
        
        print("🚀 Executando script de criação de chaves estrangeiras...")
        
        # Executar o script
        cursor.execute(sql_content)
        conn.commit()
        
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
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro ao executar script: {e}")

if __name__ == "__main__":
    executar_sql_fks()
