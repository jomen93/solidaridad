-- Transacciones por categoría (conteo y orden descendente)
SELECT transaction_category AS categoria,
       COUNT(*) AS total_transacciones
FROM accounts
GROUP BY transaction_category
ORDER BY total_transacciones DESC;
