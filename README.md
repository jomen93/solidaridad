# Prueba Técnica – Ingeniero de Datos

## Breve explicación de la solución

Este proyecto implementa un pipeline ETL en Python para extraer, transformar y analizar datos de la Fake Bank API. El flujo automatiza la descarga de datos, su limpieza y enriquecimiento, y finalmente los pone a disposición para análisis SQL en SQLite. La estructura modular permite mantener y escalar fácilmente el proceso.

### Estructura del pipeline
- **Ingesta:** Descarga los datos de la API y los almacena en `data/raw/`.
- **Transformación:** Limpia, normaliza y enriquece los datos, guardando el resultado en formato Parquet en `data/processed/`.
- **Carga y análisis:** Carga el dataset procesado en SQLite y ejecuta consultas SQL para responder preguntas de negocio.

## Consultas SQL y resultados

### 1. Transacciones por categoría
```sql
SELECT transaction_category, COUNT(*) as total 
FROM accounts 
GROUP BY transaction_category
```
**Resultado:**
| transaction_category     | total |
|-------------------------|-------|
| Dining                  | 44    |
| Fee/Interest Charge     | 14    |
| Gas/Automotive          | 11    |
| Health Care             | 23    |
| Merchandise             | 52    |
| Other                   | 3     |
| Other Services          | 16    |
| Other Travel            | 3     |
| Payment/Credit          | 16    |
| Phone/Cable             | 5     |
| air                     | 1     |
| beauty                  | 524   |
| food                    | 494   |
| fuel                    | 498   |
| gaz                     | 1     |
| restaurants             | 481   |
| taxi                    | 495   |

### 2. Monto neto promedio por categoría
```sql
SELECT transaction_category, AVG(net_transaction_amount) as avg_net_amount 
FROM accounts 
GROUP BY transaction_category
```
**Resultado:**
| transaction_category     | avg_net_amount |
|-------------------------|----------------|
| Dining                  | -17.48         |
| Fee/Interest Charge     | -5.14          |
| Gas/Automotive          | -24.34         |
| Health Care             | -52.82         |
| Merchandise             | -37.63         |
| Other                   | -1.98          |
| Other Services          | -21.13         |
| Other Travel            | -79.38         |
| Payment/Credit          | 282.49         |
| Phone/Cable             | -66.55         |
| air                     | 62.20          |
| beauty                  | -47.51         |
| food                    | -51.51         |
| fuel                    | -50.41         |
| gaz                     | 62.20          |
| restaurants             | -51.77         |
| taxi                    | -52.67         |

### 3. Transacciones marcadas como anomalía
```sql
SELECT * FROM accounts WHERE is_anomaly = 1
```
**Resultado:**
Se detectaron 14 transacciones anómalas. Ejemplo de filas:

| transaction_date | transaction_description | ... | is_anomaly | ... |
|------------------|------------------------|-----|------------|-----|
| 2016-01-08       | ...                    | ... | 1          | ... |
| 2016-01-20       | ...                    | ... | 1          | ... |
| ...              | ...                    | ... | ...        | ... |

## Procesamiento opcional realizado
- **Normalización de columnas:** Se estandarizaron los nombres de columnas a snake_case para consistencia.
- **Conversión de fechas:** Las fechas se transformaron al formato estándar `YYYY-MM-DD`.
- **Enriquecimiento:** Se integraron datos de categorías y se generaron variables adicionales como `net_transaction_amount`, `transaction_size`, y banderas como `is_anomaly`, `is_fee_transaction`, etc.
- **Detección de anomalías:** Se marcó como anómalas aquellas transacciones que cumplen ciertos criterios (por ejemplo, montos atípicos o categorías inusuales) para facilitar el análisis de calidad y riesgos.

---

**Nota:** El pipeline es reproducible y modular. Solo necesitas instalar dependencias, ejecutar el flujo principal y luego correr las consultas SQL para obtener los resultados.
