"""
Pipeline principal ETL
Orquesta el proceso completo de Extract, Transform, Load
"""
from src.extract.data_extractor import DataExtractor
from src.transform.data_transformer import DataTransformer
from src.load.data_loader import DataLoader

from IPython import embed

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
        print("=== Iniciando Pipeline ETL ===")

        # EXTRACT
        print("1. Extrayendo datos...")
        accounts_result = self.extractor.extract_fakebank_data('accounts', 'parquet', save=True)
        print(f"✓ Datos extraídos: {accounts_result['extraction_result']['metadata']['total_records']} registros")

        # TRANSFORM
        print("2. Transformando datos...")
        latest_file = self.transformer.find_latest_raw_file('accounts')
        transform_result = self.transformer.transform_from_raw_file(
            latest_file,
            save_processed=True,
            processed_format='parquet'
        )
        print(f"✓ Datos transformados: {transform_result['transformed_records_count']} registros")

        # LOAD: Guardar en SQLite y exportar .sql
        print("3. Cargando datos en SQLite y generando volcado .sql...")
        processed_data = transform_result['transformed_data']
        db_path = 'data/processed/etl_results.sqlite'
        sql_dump_path = 'etl_results_dump.sql'
        table_name = 'accounts'
        load_result = self.loader.save_to_database(
            processed_data,
            db_path=db_path,
            sql_dump_path=sql_dump_path,
            table_name=table_name
        )
        if load_result['success']:
            print(f"✓ Datos guardados en {db_path} y volcado SQL en {sql_dump_path}")
        else:
            print(f"✗ Error al guardar en base de datos: {load_result['error']}")

        # Ejecutar queries automáticas si existen
        print("4. Ejecutando queries automáticas de la carpeta 'querys'...")
        import sqlite3, glob
        queries_dir = 'querys'
        sql_files = sorted(glob.glob(f'{queries_dir}/*.sql'))
        if not sql_files:
            print("No se encontraron archivos .sql en la carpeta 'querys'.")
        else:
            conn = sqlite3.connect(db_path)
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

        print("=== Pipeline ETL Completado ===")

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
