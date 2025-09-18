-- Consulta de ejemplo: Total de transacciones por categoría
SELECT transaction_category, COUNT(*) as total
FROM accounts
GROUP BY transaction_category
ORDER BY total DESC;
