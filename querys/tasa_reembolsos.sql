-- Tasa y volumen de reembolsos por mes
SELECT
  year_month,
  COUNT(*) AS transacciones,
  SUM(CASE WHEN is_refund=1 THEN 1 ELSE 0 END) AS reembolsos,
  ROUND(100.0 * SUM(CASE WHEN is_refund=1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS tasa_reembolso_pct,
  SUM(CASE WHEN is_refund=1 THEN amount_abs ELSE 0 END) AS monto_reembolsos
FROM accounts
GROUP BY year_month
ORDER BY year_month;
