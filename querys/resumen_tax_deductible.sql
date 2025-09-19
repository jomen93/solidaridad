-- Resumen por deducibilidad de impuestos
SELECT 
  category_tax_deductible AS es_deducible,
  COUNT(*) AS n_transacciones,
  SUM(net_transaction_amount) AS monto_neto
FROM accounts
GROUP BY es_deducible
ORDER BY n_transacciones DESC;
