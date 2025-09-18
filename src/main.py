"""
Pipeline principal ETL
Orquesta el proceso completo de Extract, Transform, Load
"""

import sys
import os

# Agregar el directorio src al path para importaciones
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from extract.data_extractor import DataExtractor
from transform.data_transformer import DataTransformer
from load.data_loader import DataLoader


class ETLPipeline:
    """
    Pipeline principal que orquesta todo el proceso ETL
    """

    def __init__(self):
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = DataLoader()

    def run_pipeline(self, endpoint: str = 'fakebank/accounts'):
        """
        Ejecuta el pipeline completo ETL

        Args:
            endpoint: Endpoint de la API a extraer
        """
        print("=== Iniciando Pipeline ETL ===")

        # EXTRACT
        print("1. Extrayendo datos...")
        extract_result = self.extractor.get_data(endpoint)

        if not extract_result['success']:
            print(f"Error en extracción: {extract_result['error']}")
            return

        print(f"✓ Datos extraídos: {extract_result['metadata']['total_records']} registros")

        # TRANSFORM
        print("2. Transformando datos...")
        transformed_data = self.transformer.transform_data(extract_result['data'])
        print("✓ Datos transformados")

        # LOAD
        print("3. Cargando datos...")
        # Aquí se implementaría la carga según necesidades
        print("✓ Datos listos para cargar")

        print("=== Pipeline ETL Completado ===")

        return {
            'status': 'success',
            'extracted_records': extract_result['metadata']['total_records'],
            'transformed_data': transformed_data
        }


if __name__ == "__main__":
    # Ejecutar pipeline
    pipeline = ETLPipeline()
    result = pipeline.run_pipeline()
