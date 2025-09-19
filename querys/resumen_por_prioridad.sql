-- Resumen por prioridad de categoría
SELECT 
  category_priority AS prioridad,
  COUNT(*) AS n_transacciones,
  SUM(net_transaction_amount) AS monto_neto,
  AVG(ABS(net_transaction_amount)) AS ticket_medio_abs
FROM accounts
GROUP BY prioridad
ORDER BY n_transacciones DESC;
