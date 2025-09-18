"""
Archivo de configuración para el proyecto ETL
"""

# Configuración de la API
API_CONFIG = {
    'base_url': 'https://api.sampleapis.com',
    'timeout': 30,
    'max_retries': 3,
    'retry_delay': 1
}

# Configuración de archivos
FILE_CONFIG = {
    'data_raw_path': 'data/raw',
    'data_processed_path': 'data/processed',
    'logs_path': 'logs'
}

# Configuración de logging
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file_handler': True,
    'console_handler': True
}
