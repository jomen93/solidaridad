-- Transacciones outlier por z-score dentro de su categoría
SELECT
  transaction_date,
  transaction_category,
  transaction_description,
  net_transaction_amount,
  cat_net_mean,
  cat_net_std,
  ROUND(cat_net_zscore, 3) AS zscore
FROM accounts
WHERE ABS(cat_net_zscore) >= 3
ORDER BY ABS(cat_net_zscore) DESC;
