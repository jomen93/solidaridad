import sqlite3
import pandas as pd
import glob
import os

# Buscar el archivo Parquet más reciente en data/processed/
parquet_files = glob.glob('data/processed/*.parquet')
if not parquet_files:
    raise FileNotFoundError('No se encontró ningún archivo Parquet en data/processed/.')

latest_file = max(parquet_files, key=os.path.getctime)
print(f"Usando archivo: {latest_file}")

# Leer el archivo Parquet
df = pd.read_parquet(latest_file)

# Crear/conectar a la base de datos SQLite
DB_PATH = 'data/processed/etl_results.sqlite'
conn = sqlite3.connect(DB_PATH)


# Mostrar nombres de columnas para depuración
print("Columnas disponibles en el DataFrame:")
print(df.columns.tolist())

# Cargar el DataFrame a una tabla llamada 'accounts'
df.to_sql('accounts', conn, if_exists='replace', index=False)

# Consultas de negocio

# Consultas de negocio actualizadas según las columnas disponibles
queries = {
    'Transacciones por categoría': """
        SELECT transaction_category, COUNT(*) as total 
        FROM accounts 
        GROUP BY transaction_category
    """,
    'Monto neto promedio por categoría': """
        SELECT transaction_category, AVG(net_transaction_amount) as avg_net_amount 
        FROM accounts 
        GROUP BY transaction_category
    """,
    'Transacciones marcadas como anomalía': """
        SELECT * FROM accounts WHERE is_anomaly = 1
    """
}

for desc, sql in queries.items():
    print(f"\n--- {desc} ---")
    print(pd.read_sql_query(sql, conn))

conn.close()
