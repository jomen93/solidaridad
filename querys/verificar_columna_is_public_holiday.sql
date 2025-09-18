-- Verifica que la columna is_public_holiday existe en la tabla
SELECT name AS column_name
FROM pragma_table_info('accounts')
WHERE name = 'is_public_holiday';
