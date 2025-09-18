
"""
Pipeline principal ETL
Orquesta el proceso completo de Extract, Transform, Load
"""

import os
import pandas as pd
from src.extract.data_extractor import DataExtractor
from src.transform.data_transformer import DataTransformer
from src.load.data_loader import DataLoader
from src.enrich.external_enrichment import ExternalEnrichment
from src.config import ENRICHMENT_CONFIG

class ETLPipeline:
    """
    Pipeline principal que orquesta todo el proceso ETL
    """

    def __init__(self):
        self.extractor   = DataExtractor()
        self.transformer = DataTransformer()
        self.loader      = DataLoader()


    def run_pipeline(self, endpoint: str = 'fakebank/accounts'):
        """
        Ejecuta el pipeline completo ETL

        Args:
            endpoint: Endpoint de la API a extraer
        """

        print("="*50)
        print("1. Extrayendo datos...")
        accounts_result = self.extractor.extract_fakebank_data('accounts', 'parquet', save=True)
        print(f"✓ Extracción completada: {accounts_result['extraction_result']['metadata']['total_records']} registros extraídos")

        print("="*50)
        print("2. Transformando datos...")
        latest_file = self.transformer.find_latest_raw_file('accounts')
        transform_result = self.transformer.transform_from_raw_file(
            latest_file,
            save_processed=True,
            processed_format='parquet'
        )
        print(f"✓ Transformación completada: {transform_result['transformed_records_count']} registros procesados")

        # 2.1 Enriquecimiento externo (opcional)
        print("2.1 Enriqueciendo con datos externos (festivos/FX) si está habilitado...")
        processed_data = transform_result['transformed_data']
        enricher = ExternalEnrichment(
            holiday_country_code=ENRICHMENT_CONFIG.get('holiday_country_code','US'),
            fx_target_currency=ENRICHMENT_CONFIG.get('fx_target_currency','USD')
        )
        processed_df = pd.DataFrame(processed_data)
        processed_df = enricher.enrich(
            processed_df,
            enable_holidays=ENRICHMENT_CONFIG.get('enable_holidays', True),
            enable_fx=ENRICHMENT_CONFIG.get('enable_fx', False),
            fx_target_currency=ENRICHMENT_CONFIG.get('fx_target_currency', 'USD')
        )
        processed_data = processed_df.to_dict('records')

        print("="*50)
        print("3. Cargando datos en SQLite y generando volcado .sql...")
        db_path = 'data/processed/etl_results.sqlite'
        queries_dir = 'querys'
        os.makedirs(queries_dir, exist_ok=True)
        sql_dump_path = os.path.join(queries_dir, 'etl_results_dump.sql')
        table_name = 'accounts'
        load_result = self.loader.save_to_database(
            processed_data,
            db_path=db_path,
            sql_dump_path=sql_dump_path,
            table_name=table_name
        )
        if load_result['success']:
            print(f"✓ Carga completada: datos guardados en {db_path} y volcado SQL en {sql_dump_path}")
        else:
            print(f"✗ Error al guardar en base de datos: {load_result['error']}")

        print("="*50)
        print("4. Restaurando volcado .sql y ejecutando queries automáticas de la carpeta 'querys'...")
        import sqlite3, glob, tempfile, subprocess
        queries_dir = 'querys'
        sql_files = sorted(glob.glob(f'{queries_dir}/*.sql'))
        if not sql_files:
            print("No se encontraron archivos .sql en la carpeta 'querys'.")
        elif not os.path.exists(sql_dump_path):
            print(f"No se encontró el volcado SQL: {sql_dump_path}")
        else:
            # Crear base temporal a partir del volcado .sql
            with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp_db:
                tmp_db_path = tmp_db.name
            # Restaurar el volcado
            restore_cmd = f"sqlite3 {tmp_db_path} < {sql_dump_path}"
            result = subprocess.run(restore_cmd, shell=True)
            if result.returncode != 0:
                print(f"Error restaurando la base desde {sql_dump_path}")
            else:
                conn = sqlite3.connect(tmp_db_path)
                for sql_file in sql_files:
                    print(f"\n--- Ejecutando: {sql_file} ---")
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
                print("✓ Todas las queries automáticas han sido ejecutadas sobre la base restaurada del volcado .sql")
            # Eliminar base temporal
            os.remove(tmp_db_path)

        print("="*50)
        print("=== Pipeline ETL COMPLETADO EXITOSAMENTE ===")

        return {
            'status': 'success' if load_result['success'] else 'error',
            'extracted_records': accounts_result["extraction_result"]['metadata']['total_records'],
            'transformed_records': transform_result['transformed_records_count'],
            'db_path': db_path,
            'sql_dump_path': sql_dump_path,
            'error': load_result['error'] if not load_result['success'] else None
        }


if __name__ == "__main__":
    # Ejecutar pipeline
    pipeline = ETLPipeline()
    result = pipeline.run_pipeline()
