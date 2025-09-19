-- Resumen de transacciones en festivos por mes
SELECT
  year_month,
  SUM(CASE WHEN is_public_holiday = 1 THEN 1 ELSE 0 END) AS transacciones_en_festivo,
  SUM(CASE WHEN is_public_holiday = 1 AND is_expense = 1 THEN amount_abs ELSE 0 END) AS gasto_en_festivo,
  COUNT(*) AS total_transacciones,
  ROUND(100.0 * SUM(CASE WHEN is_public_holiday = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_transacciones_festivo
FROM accounts
GROUP BY year_month
ORDER BY year_month;
