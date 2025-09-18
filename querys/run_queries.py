"""
Ejecutor de consultas SQL escalable.

Coloca tus archivos .sql en esta carpeta y ejecuta este script. Él hará:
- Detectar si existe un volcado SQL (etl_results_dump.sql) en querys/ o una base SQLite en data/processed/.
- Si hay volcado, lo restaurará a una base temporal y ejecutará las queries allí.
- Guardará los resultados de cada query en archivos CSV dentro de querys/results/ para compartir.
"""
import sqlite3
import glob
import os
import tempfile
import subprocess
import pandas as pd

SQL_DIR = os.path.dirname(__file__)
DB_SQLITE = os.path.join(SQL_DIR, '..', 'data', 'processed', 'etl_results.sqlite')
DUMP_SQL = os.path.join(SQL_DIR, 'etl_results_dump.sql')
RESULTS_DIR = os.path.join(SQL_DIR, 'results')
os.makedirs(RESULTS_DIR, exist_ok=True)

# Buscar todos los archivos .sql de consultas (excluyendo el dump)
sql_files = [f for f in sorted(glob.glob(os.path.join(SQL_DIR, '*.sql'))) if not f.endswith('etl_results_dump.sql')]

if not sql_files:
    print("No se encontraron archivos .sql de consulta en la carpeta querys.")
    exit(0)

def get_connection():
    """Obtener una conexión SQLite desde DB existente o restaurando el dump a una temporal."""
    if os.path.exists(DUMP_SQL):
        # Restaurar a DB temporal
        tmp = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
        tmp_db_path = tmp.name
        tmp.close()
        cmd = f"sqlite3 {tmp_db_path} < {DUMP_SQL}"
        res = subprocess.run(cmd, shell=True)
        if res.returncode != 0:
            print(f"Error restaurando volcado: {DUMP_SQL}")
            os.unlink(tmp_db_path)
            raise SystemExit(1)
        conn = sqlite3.connect(tmp_db_path)
        return conn, tmp_db_path
    elif os.path.exists(DB_SQLITE):
        return sqlite3.connect(DB_SQLITE), None
    else:
        print("No se encontró ni el volcado .sql ni la base SQLite.")
        raise SystemExit(1)

conn, tmp_path = get_connection()

for sql_file in sql_files:
    name = os.path.splitext(os.path.basename(sql_file))[0]
    print(f"\n--- Ejecutando: {os.path.basename(sql_file)} ---")
    with open(sql_file, 'r', encoding='utf-8') as f:
        query = f.read()
        try:
            df = pd.read_sql_query(query, conn)
            print(df.head(20))
            out_csv = os.path.join(RESULTS_DIR, f"{name}.csv")
            df.to_csv(out_csv, index=False)
            print(f"Resultados guardados en: {out_csv}")
        except Exception as e:
            print(f"Error ejecutando {sql_file}: {e}")

conn.close()
if tmp_path and os.path.exists(tmp_path):
    os.unlink(tmp_path)
print("\nTodas las queries han sido ejecutadas.")
