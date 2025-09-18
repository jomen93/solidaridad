# Prueba Técnica – Ingeniero de Datos - ITRIO SAS

Solución ETL completa para la Fake Bank API: extracción, limpieza, enriquecimiento y carga a SQLite, con queries SQL listas para ejecutar y compartir.

## Estructura de carpetas y roles

```
solidaridad/
├─ src/
│  ├─ extract/
│  │  └─ data_extractor.py      # Llama a la API y guarda raw (data/raw)
│  ├─ transform/
│  │  └─ data_transformer.py    # Limpia, normaliza y agrega features de negocio
│  ├─ load/
│  │  └─ data_loader.py         # Carga a SQLite y genera volcado .sql
│  ├─ enrich/
│  │  └─ external_enrichment.py # Festivos (Nager.Date) y FX (Frankfurter)
│  └─ config.py                 # Parámetros (API, rutas, ENRICHMENT_CONFIG)
├─ data/
│  ├─ raw/                      # Datos crudos (parquet)
│  └─ processed/                # Datos procesados y etl_results.sqlite
├─ querys/
│  ├─ *.sql                     # Consultas de análisis
│  ├─ etl_results_dump.sql      # Volcado SQL actualizado por el pipeline
│  ├─ results/*.csv             # Resultados de las queries
│  └─ run_queries.py            # Ejecuta todas las queries y exporta a CSV
├─ main.py                      # Orquestador del ETL (end-to-end)
├─ requirements.txt             # Dependencias
└─ README.md
```

## Orquestador (main.py)
Flujo end-to-end:
- Extrae cuentas desde la API y guarda en `data/raw/`.
- Transforma datos (normaliza columnas, fechas y montos; agrega variables como net_transaction_amount, z-score por categoría, is_refund, flags temporales, etc.).
- Enriquecimiento externo opcional: marca festivos por país (`is_public_holiday`) y, si se habilita FX y hay `currency`, crea montos normalizados (p. ej. `net_transaction_amount_USD`).
- Carga en SQLite (`data/processed/etl_results.sqlite`) y genera `querys/etl_results_dump.sql`.
- Restaura el volcado y ejecuta automáticamente todas las queries `.sql` en `querys/`, exportando a `querys/results/*.csv`.

Entradas/Salidas (I/O) por etapa:
- Extract → Input: API `https://api.sampleapis.com/fakebank/accounts` | Output: `data/raw/accounts_YYYYMMDD_HHMMSS.parquet`.
- Transform → Input: parquet más reciente de `data/raw/` | Output: dataframe en memoria con columnas limpias y features.
- Enrich → Input: dataframe transformado | Output: mismas filas + columnas extra (`is_public_holiday`, montos en `*_USD` si FX activo).
- Load → Input: dataframe final | Output: `data/processed/etl_results.sqlite` (tabla `accounts`) y `querys/etl_results_dump.sql`.

Configuración clave en `src/config.py` → `ENRICHMENT_CONFIG`:
- `enable_holidays` (True/False), `holiday_country_code` (ej. 'US', 'ES', 'MX').
- `enable_fx` (True/False), `fx_target_currency` (ej. 'USD').

Ejemplo de configuración:
```python
ENRICHMENT_CONFIG = {
	'enable_holidays': True,
	'holiday_country_code': 'US',  # Cambiar a 'ES' o 'MX' según tu caso
	'enable_fx': False,
	'fx_target_currency': 'USD'
}
```

## Cómo ejecutar el pipeline (macOS, zsh)

Requisitos:
- Python 3.9+ recomendado
- Conexión a internet (para extraer datos y enriquecer con festivos/FX)

```sh
# 1) Crear y activar venv (opcional, recomendado)
python3 -m venv .venv
source .venv/bin/activate

# 2) Instalar dependencias
pip install -r requirements.txt

# 3) Ejecutar el pipeline end-to-end
python main.py
```

Salidas esperadas:
- `data/processed/etl_results.sqlite` y `querys/etl_results_dump.sql`.
- CSVs en `querys/results/` con los resultados de todas las `.sql`.

Cómo cambiar la configuración antes de ejecutar:
- Edita `src/config.py` → `ENRICHMENT_CONFIG`:
	- `enable_holidays`: True/False
	- `holiday_country_code`: 'US' (cámbialo a 'ES', 'MX', 'CO', etc.)
	- `enable_fx`: True/False (requiere columna `currency` en los datos)
	- `fx_target_currency`: 'USD'

## Cómo correr queries

Opción A (automático): ya se ejecutan al final de `python main.py`.

Opción B (manual): ejecutar el runner de queries.
```sh
python querys/run_queries.py
```
El runner detecta `etl_results_dump.sql`, lo restaura a una base temporal y ejecuta todas las `.sql` en `querys/`, guardando CSV en `querys/results/`.

Para agregar una nueva query: crea un archivo `.sql` en `querys/` (ej. `mi_analisis.sql`). La próxima ejecución la incluirá y generará `querys/results/mi_analisis.csv`.

Listado de queries incluidas y objetivo:
- `flujo_mensual_ingresos_gastos.sql`: ingresos, gastos y flujo por mes.
- `transacciones_por_categoria.sql`: conteo por categoría.
- `promedio_gasto_por_categoria.sql`: ticket medio de gasto por categoría.
- `top10_transacciones_mas_grandes.sql`: top por monto absoluto.
- `resumen_por_prioridad.sql`: actividad por prioridad de categoría.
- `resumen_pagos_y_cargos.sql`: pagos/créditos vs cargos/fees y tamaños.
- `tasa_anomalias_por_categoria.sql`: porcentaje de outliers por categoría.
- `gasto_discrecional_mensual.sql`: gasto mensual en categorías discrecionales.
- `suscripciones_mensuales.sql`: gasto y transacciones con keywords de suscripción.
- `outliers_por_categoria.sql`: outliers por z-score dentro de su categoría.
- `fin_de_semana_vs_semana.sql`: comparación de gasto weekend vs weekday.
- `recurrencia_top_descripciones.sql`: descripciones recurrentes y gasto total.
- `verificar_columna_is_public_holiday.sql`: valida existencia de la columna.
- `resumen_festivos_por_mes.sql`: % y monto en días festivos por mes.
- `muestras_en_festivo.sql`: muestra de filas marcadas como festivo.

Ejecutar una sola query manualmente (alternativa):
```sh
# 1) Crear una base temporal a partir del dump
sqlite3 /tmp/etl_tmp.sqlite < querys/etl_results_dump.sql

# 2) Ejecutar una SQL específica
sqlite3 /tmp/etl_tmp.sqlite ".read querys/mi_analisis.sql"

# 3) (Opcional) Exportar a CSV desde sqlite3
sqlite3 -header -csv /tmp/etl_tmp.sqlite "SELECT * FROM accounts LIMIT 5;" > sample.csv
```

## Variables analíticas destacadas
- `net_transaction_amount`, `amount_abs`, `is_income`, `is_expense`, `transaction_size`.
- Temporales: `year_month`, `transaction_day_of_week`, `is_weekend`, `is_month_end`, `is_public_holiday` (si habilitado).
- Categoría/negocio: `category_type`, `category_priority`, `category_priority_level`, `is_tax_deductible`, `tax_deductible_amount`.
- Texto/recurrencia: `has_keyword_subscription`, `description_txn_count`, `is_recurring_description`, `is_duplicate_candidate`, `days_since_prev_same_desc`.
- Estadística por categoría: `cat_net_mean`, `cat_net_std`, `cat_net_zscore`, `spend_vs_category_mean`.
- `is_refund`.

Data dictionary (compacto):
- Identificadores y base: `transaction_id`, `transaction_description`, `transaction_category`, `transaction_date` (YYYY-MM-DD).
- Montos: `credit_amount`, `debit_amount`, `net_transaction_amount` (crédito - débito), `amount_abs`.
- Temporal: `transaction_year`, `transaction_month`, `transaction_day_of_week`, `transaction_quarter`, `year_month`, `is_weekend`, `is_month_end`, `is_month_start`, `week_of_year`, `is_public_holiday` (si festivos activo).
- Negocio/categorías: `category_type`, `category_priority`, `category_priority_level` (1/2/3), `category_tax_deductible`, `is_tax_deductible`, `tax_deductible_amount`, `is_fee_transaction`, `is_payment_transaction`, `is_large_transaction`.
- Texto/recurrencia: `description_length`, `has_keyword_subscription`, `has_atm`, `has_transfer`, `has_refund_keyword`, `description_txn_count`, `is_recurring_description`, `is_duplicate_candidate`, `days_since_prev_same_desc`.
- Estadísticos por categoría: `cat_net_mean`, `cat_net_std`, `cat_net_zscore`, `spend_vs_category_mean`.
- Calidad de datos: `data_quality_score`.

## Troubleshooting rápido
- ¿No aparece `is_public_holiday` en los CSV?
	- Revisa `ENRICHMENT_CONFIG.enable_holidays=True` y el país.
	- Vuelve a correr `python main.py` para regenerar el volcado.
	- Opcional: elimina `querys/etl_results_dump.sql` y `data/processed/etl_results.sqlite` antes de ejecutar.
	- Ejecuta `python querys/run_queries.py` y revisa `querys/results/verificar_columna_is_public_holiday.csv`.

- ¿No hay resultados en una query?
	- Verifica que la tabla `accounts` tenga filas en el dump/DB actual.
	- Asegúrate de que las columnas usadas por la query existan (pueden depender del enriquecimiento).

Validación de festivos paso a paso:
1) Asegura `enable_holidays=True` y `holiday_country_code` correcto en `src/config.py`.
2) Ejecuta `python main.py` y verifica el mensaje “2.1 Enriqueciendo…”.
3) Ejecuta `python querys/run_queries.py`.
4) Revisa `querys/results/verificar_columna_is_public_holiday.csv` (debe listar la columna).
5) Revisa `querys/results/muestras_en_festivo.csv` (deberías ver fechas marcadas 1; ej. US: 2025-07-04).

Reset rápido del entorno (para regenerar todo limpio):
```sh
rm -f querys/etl_results_dump.sql data/processed/etl_results.sqlite
python main.py
python querys/run_queries.py
```

## Notas
- El proyecto es modular y reproducible; las salidas estándar permiten compartir datos vía `etl_results_dump.sql`.
- Cambios de país de festivos y FX se hacen en `src/config.py` sin tocar el código del pipeline.

Detalle de archivos clave:
- `src/extract/data_extractor.py`: obtiene `accounts` desde `https://api.sampleapis.com/fakebank/accounts` y puede guardar raw en parquet.
- `src/transform/data_transformer.py`: normaliza nombres/fechas/montos, enriquece por categoría y crea features (anomalías, flags temporales, recurrencia, etc.).
- `src/enrich/external_enrichment.py`: consulta festivos (Nager.Date `PublicHolidays/{year}/{country}`) y FX (Frankfurter) y añade columnas.
- `src/load/data_loader.py`: guarda en SQLite y exporta el volcado SQL (usado por el runner y para compartir).
- `querys/run_queries.py`: detecta el dump/DB, restaura a una DB temporal y ejecuta todas las SQL, exportando a CSV.
