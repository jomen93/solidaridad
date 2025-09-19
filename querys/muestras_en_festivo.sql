-- Muestra de transacciones marcadas como festivo
SELECT
  transaction_date,
  transaction_category,
  transaction_description,
  is_public_holiday,
  amount_abs
FROM accounts
WHERE is_public_holiday = 1
ORDER BY transaction_date DESC
LIMIT 50;
