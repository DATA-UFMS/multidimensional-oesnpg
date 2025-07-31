#!/usr/bin/env python3
"""
Script para executar o SQL de criação de chaves estrangeiras
"""

import psycopg2
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

def executar_sql_fks():
    """Executa o script SQL de criação de FKs"""
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
        
        # Ler o arquivo SQL
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
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro ao executar script: {e}")

if __name__ == "__main__":
    executar_sql_fks()
