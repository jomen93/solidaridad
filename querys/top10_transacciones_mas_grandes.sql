-- Top 10 transacciones más grandes (valor absoluto)
SELECT 
  transaction_date,
  transaction_description,
  transaction_category,
  net_transaction_amount,
  ABS(net_transaction_amount) AS abs_amount
FROM accounts
ORDER BY abs_amount DESC
LIMIT 10;
