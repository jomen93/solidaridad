-- Comparativa de gasto entre fin de semana y días hábiles por mes
SELECT
  year_month,
  CASE WHEN is_weekend=1 THEN 'weekend' ELSE 'weekday' END AS tipo_dia,
  SUM(CASE WHEN is_expense THEN amount_abs ELSE 0 END) AS gasto
FROM accounts
GROUP BY year_month, tipo_dia
ORDER BY year_month, tipo_dia;
