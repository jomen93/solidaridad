"""
Extractor de datos para APIs REST
Basado en el ejemplo de JavaScript proporcionado
"""

import requests
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from IPython import embed

class DataExtractor:
    """
    Clase para extraer datos de APIs REST
    Equivalente al componente React con fetch
    """

    def __init__(self, base_url: str = "https://api.sampleapis.com", timeout: int = 30):
        """
        Inicializa el extractor de datos

        Args:
            base_url: URL base de la API
            timeout: Timeout en segundos para las requests
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()

        # Configurar headers por defecto
        self.session.headers.update({
            'User-Agent': 'DataExtractor/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })

        # Configurar logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def get_data(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Extrae datos de una API REST
        Equivalente a la función getData del ejemplo de JavaScript

        Args:
            endpoint: Endpoint de la API (ej: 'fakebank/accounts')
            params: Parámetros opcionales para la request

        Returns:
            Dict con los datos extraídos y metadata

        Raises:
            Exception: Si hay error en la extracción
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            self.logger.info(f"Extrayendo datos de: {url}")

            # Realizar la request (equivalente al fetch)
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()

            # Convertir a JSON (equivalente al .json())
            data = response.json()

            # Log de éxito
            records_count = len(data) if isinstance(data, list) else 1
            self.logger.info(f"Datos extraídos exitosamente. Total registros: {records_count}")

            # Retornar datos con metadata
            return {
                'success': True,
                'data': data,
                'metadata': {
                    'source_url': url,
                    'extracted_at': datetime.now().isoformat(),
                    'total_records': records_count,
                    'status_code': response.status_code
                },
                'error': None
            }

        except requests.exceptions.RequestException as e:
            # Manejo de errores (equivalente al catch del JavaScript)
            error_message = f"Error de conexión: {str(e)}"
            self.logger.error(error_message)

            return {
                'success': False,
                'data': None,
                'metadata': {
                    'source_url': url,
                    'extracted_at': datetime.now().isoformat(),
                    'error_type': 'RequestException'
                },
                'error': error_message
            }

        except json.JSONDecodeError as e:
            # Error de decodificación JSON
            error_message = f"Error decodificando JSON: {str(e)}"
            self.logger.error(error_message)

            return {
                'success': False,
                'data': None,
                'metadata': {
                    'source_url': url,
                    'extracted_at': datetime.now().isoformat(),
                    'error_type': 'JSONDecodeError'
                },
                'error': error_message
            }

        except Exception as e:
            # Error general
            error_message = f"Error inesperado: {str(e)}"
            self.logger.error(error_message)

            return {
                'success': False,
                'data': None,
                'metadata': {
                    'source_url': url,
                    'extracted_at': datetime.now().isoformat(),
                    'error_type': 'UnexpectedError'
                },
                'error': error_message
            }

    def get_fakebank_accounts(self) -> Dict[str, Any]:
        """
        Método específico para extraer datos del ejemplo proporcionado
        Equivalente directo al fetch de 'https://api.sampleapis.com/fakebank/accounts'
        """
        return self.get_data('fakebank/accounts')

    def get_data_with_retry(self, endpoint: str, max_retries: int = 3, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Extrae datos con reintentos automáticos en caso de fallo

        Args:
            endpoint: Endpoint de la API
            max_retries: Número máximo de reintentos
            params: Parámetros opcionales para la request

        Returns:
            Dict con los datos extraídos y metadata
        """
        for attempt in range(max_retries + 1):
            result = self.get_data(endpoint, params)

            if result['success']:
                if attempt > 0:
                    self.logger.info(f"Extracción exitosa en el intento {attempt + 1}")
                return result

            if attempt < max_retries:
                self.logger.warning(f"Intento {attempt + 1} falló. Reintentando...")
            else:
                self.logger.error(f"Todos los intentos fallaron después de {max_retries + 1} intentos")

        return result



if __name__ == "__main__":

    extractor = DataExtractor()
    result = extractor.get_fakebank_accounts()
    embed()

    if result['success']:
        print(f"Datos extraídos: {len(result['data'])} registros")
        print(f"Primer registro: {result['data'][0] if result['data'] else 'No data'}")
    else:
        print(f"Error: {result['error']}")
