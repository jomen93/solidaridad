-- Gasto discrecional por mes
SELECT
  year_month,
  SUM(CASE WHEN is_expense THEN amount_abs ELSE 0 END) AS gasto_discrecional
FROM accounts
WHERE is_discretionary = 1
GROUP BY year_month
ORDER BY year_month;
