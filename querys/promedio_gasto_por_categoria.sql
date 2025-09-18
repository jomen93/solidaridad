-- Promedio de gasto por categoría (solo egresos)
SELECT 
  transaction_category AS categoria,
  AVG(CASE WHEN net_transaction_amount < 0 THEN -net_transaction_amount END) AS promedio_gasto
FROM accounts
GROUP BY categoria
ORDER BY promedio_gasto DESC;
