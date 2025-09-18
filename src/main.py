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

        print("3. Cargando datos...")


        print("=== Pipeline ETL Completado ===")

        return {
            'status': 'success',
            'extracted_records': accounts_result["extraction_result"]['metadata']['total_records'],
            'transformed_records': transform_result['transformed_records_count'],
            'transformed_data': transform_result['transformed_data']
        }


if __name__ == "__main__":
    # Ejecutar pipeline
    pipeline = ETLPipeline()
    result = pipeline.run_pipeline()
