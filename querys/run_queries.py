"""
Script para ejecutar consultas SQL automáticas sobre la base de datos generada por el pipeline ETL.
Coloca tus archivos .sql en esta carpeta y este script los ejecutará sobre la base SQLite generada.
"""
import sqlite3
import glob
import os

DB_PATH = os.path.join('..', 'data', 'processed', 'etl_results.sqlite')
SQL_DIR = os.path.dirname(__file__)

# Buscar todos los archivos .sql en la carpeta querys
sql_files = sorted(glob.glob(os.path.join(SQL_DIR, '*.sql')))

if not os.path.exists(DB_PATH):
    print(f"No se encontró la base de datos: {DB_PATH}")
    exit(1)

if not sql_files:
    print("No se encontraron archivos .sql en la carpeta de queries.")
    exit(1)

conn = sqlite3.connect(DB_PATH)

for sql_file in sql_files:
    print(f"\n--- Ejecutando: {os.path.basename(sql_file)} ---")
    with open(sql_file, 'r', encoding='utf-8') as f:
        query = f.read()
        try:
            result = conn.execute(query)
            rows = result.fetchall()
            columns = [desc[0] for desc in result.description] if result.description else []
            print(f"Columnas: {columns}")
            for row in rows:
                print(row)
            print(f"Total filas: {len(rows)}")
        except Exception as e:
            print(f"Error ejecutando {sql_file}: {e}")

conn.close()
print("\nTodas las queries han sido ejecutadas.")
