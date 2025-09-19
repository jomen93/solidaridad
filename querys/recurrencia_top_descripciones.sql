-- Descripciones más recurrentes y su gasto total
WITH base AS (
  SELECT
    LOWER(COALESCE(transaction_description, '')) AS desc_low,
    amount_abs,
    is_expense
  FROM accounts
)
SELECT
  desc_low AS descripcion,
  COUNT(*) AS frecuencia,
  SUM(CASE WHEN is_expense THEN amount_abs ELSE 0 END) AS gasto_total
FROM base
GROUP BY desc_low
HAVING COUNT(*) >= 3
ORDER BY gasto_total DESC, frecuencia DESC
LIMIT 50;
