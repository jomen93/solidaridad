-- Gasto mensual en suscripciones (por keywords)
SELECT
  year_month,
  SUM(CASE WHEN is_expense THEN amount_abs ELSE 0 END) AS gasto_suscripciones,
  COUNT(*) AS transacciones_suscripcion
FROM accounts
WHERE has_keyword_subscription = 1
GROUP BY year_month
ORDER BY year_month;
