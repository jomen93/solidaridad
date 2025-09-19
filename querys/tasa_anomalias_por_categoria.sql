-- Tasa de anomalías por categoría
SELECT 
  transaction_category AS categoria,
  AVG(CASE WHEN is_anomaly = 1 THEN 1.0 ELSE 0.0 END) AS tasa_anomalia
FROM accounts
GROUP BY categoria
ORDER BY tasa_anomalia DESC;
