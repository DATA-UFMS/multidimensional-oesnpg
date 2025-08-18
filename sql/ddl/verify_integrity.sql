-- =====================================================
-- VERIFICAR INTEGRIDADE
-- Script simples para verificar PKs e FKs criadas
-- =====================================================

-- Verificar Primary Keys
SELECT 
    'PRIMARY KEYS' as tipo,
    table_name,
    constraint_name,
    column_name
FROM information_schema.key_column_usage
WHERE table_schema = 'public'
  AND table_name LIKE 'dim_%'
  AND constraint_name LIKE 'pk_%'
ORDER BY table_name;

-- Verificar Foreign Keys
SELECT 
    'FOREIGN KEYS' as tipo,
    tc.table_name, 
    tc.constraint_name, 
    kcu.column_name, 
    ccu.table_name AS referenced_table,
    ccu.column_name AS referenced_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY' 
  AND tc.table_name = 'fato_pos_graduacao'
ORDER BY tc.constraint_name;
