-- Flujo mensual: ingresos, gastos (positivo) y neto
SELECT 
  transaction_year AS anio,
  transaction_month AS mes,
  SUM(CASE WHEN net_transaction_amount > 0 THEN net_transaction_amount ELSE 0 END) AS ingresos,
  SUM(CASE WHEN net_transaction_amount < 0 THEN -net_transaction_amount ELSE 0 END) AS gastos,
  SUM(net_transaction_amount) AS flujo_neto
FROM accounts
GROUP BY anio, mes
ORDER BY anio, mes;
