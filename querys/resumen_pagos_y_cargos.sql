-- Resumen de pagos y cargos (banderas)
SELECT 
  SUM(CASE WHEN is_payment_transaction = 1 THEN 1 ELSE 0 END) AS n_pagos,
  SUM(CASE WHEN is_fee_transaction = 1 THEN 1 ELSE 0 END) AS n_cargos,
  SUM(CASE WHEN is_large_transaction = 1 THEN 1 ELSE 0 END) AS n_grandes,
  AVG(CASE WHEN is_payment_transaction = 1 THEN net_transaction_amount END) AS avg_monto_pagos,
  AVG(CASE WHEN is_fee_transaction = 1 THEN net_transaction_amount END) AS avg_monto_cargos
FROM accounts;
