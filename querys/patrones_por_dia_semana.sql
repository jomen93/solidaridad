-- Patrones de transacciones por día de la semana
SELECT 
  transaction_day_of_week AS dia_semana,
  COUNT(*) AS n_transacciones,
  SUM(net_transaction_amount) AS monto_neto,
  AVG(ABS(net_transaction_amount)) AS ticket_medio_abs
FROM accounts
GROUP BY dia_semana
ORDER BY n_transacciones DESC;
