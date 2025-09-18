"""
REST APIs Data Extractor
"""

import requests
import json
import logging
import os
import sys
from typing import Dict, Any, Optional
from datetime import datetime

# Add path for DataLoader import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from load.data_loader import DataLoader


class DataExtractor:
    """
    Class to extract data from REST APIs
    """

    def __init__(self, base_url: str = "https://api.sampleapis.com", timeout: int = 30):
        """
        Initialize the data extractor

        Args:
            base_url: Base URL of the API
            timeout: Timeout in seconds for requests
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()
        self.loader = DataLoader()

        # Configure default headers
        self.session.headers.update({
            'User-Agent': 'DataExtractor/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })

        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def get_data(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Extract data from a REST API
        Equivalent to the getData function from the JavaScript example

        Args:
            endpoint: API endpoint (e.g., 'fakebank/accounts')
            params: Optional parameters for the request

        Returns:
            Dict with extracted data and metadata

        Raises:
            Exception: If there's an error in extraction
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            self.logger.info(f"Extracting data from: {url}")

            # Make the request
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()

            # Convert to JSON
            data = response.json()

            # Success log
            records_count = len(data) if isinstance(data, list) else 1
            self.logger.info(f"Data extracted successfully. Total records: {records_count}")

            # Return data with metadata
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
            error_message = f"Connection error: {str(e)}"
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
            error_message = f"JSON decoding error: {str(e)}"
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
            error_message = f"Unexpected error: {str(e)}"
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

    def extract_fakebank_data(self, data_type: str, save_format: str = 'parquet', save: bool = False) -> Dict[str, Any]:
        """
        Extract specific fakebank data type and optionally save to raw

        Args:
            data_type: Type of fakebank data ('accounts' or 'account_types')
            save_format: Format to save data ('parquet', 'json', 'csv')
            save: If True, saves data to data/raw directory

        Returns:
            Dict with extraction results and optionally save results
        """
        # Map data types to endpoints
        endpoint_mapping = {
            'accounts': 'fakebank/accounts',
            'account_types': 'fakebank/accountTypes'
        }

        if data_type not in endpoint_mapping:
            error_message = f"Unsupported fakebank data type: {data_type}. Available: {list(endpoint_mapping.keys())}"
            self.logger.error(error_message)
            return {
                'success': False,
                'data_type': data_type,
                'endpoint': None,
                'extraction_result': None,
                'save_result': None,
                'error': error_message
            }

        endpoint = endpoint_mapping[data_type]

        # Extract data
        self.logger.info(f"Starting extraction for {data_type} from {endpoint}")
        extraction_result = self.get_data(endpoint)

        if not extraction_result['success']:
            self.logger.error(f"Extraction failed for {data_type}: {extraction_result['error']}")
            return {
                'success': False,
                'data_type': data_type,
                'endpoint': endpoint,
                'extraction_result': extraction_result,
                'save_result': None,
                'error': f"Extraction failed: {extraction_result['error']}"
            }

        # Save if requested
        save_result = None
        if save:
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{data_type}_{timestamp}.{save_format}"
            filepath = f"data/raw/{filename}"

            # Save using DataLoader
            self.logger.info(f"Saving {data_type} data to {filepath}")
            save_result = self.loader.save_data(extraction_result['data'], filepath, save_format)

            if save_result['success']:
                self.logger.info(f"Successfully saved {data_type} data: {save_result['records_count']} records")
            else:
                self.logger.error(f"Failed to save {data_type} data: {save_result['error']}")

        return {
            'success': extraction_result['success'] and (save_result['success'] if save else True),
            'data_type': data_type,
            'endpoint': endpoint,
            'extraction_result': extraction_result,
            'save_result': save_result,
            'error': save_result.get('error') if save and save_result and not save_result['success'] else None
        }



if __name__ == "__main__":
    # Example usage
    extractor = DataExtractor()

    print("=== Extract Data Examples ===")

    # Example 1: Extract accounts data only (no save)
    print("\n1. Extracting accounts data (no save)...")
    accounts_result = extractor.extract_fakebank_data('accounts')

    if accounts_result['success']:
        print(f"✅ Success! Extracted {accounts_result['extraction_result']['metadata']['total_records']} records")
    else:
        print(f"❌ Failed: {accounts_result['error']}")

    # Example 2: Extract and save accounts as parquet
    print("\n2. Extracting and saving accounts data as parquet...")
    accounts_save_result = extractor.extract_fakebank_data('accounts', 'parquet', save=True)

    if accounts_save_result['success']:
        print(f"✅ Success! Saved to: {accounts_save_result['save_result']['filepath']}")
        print(f"Records: {accounts_save_result['save_result']['records_count']}")
    else:
        print(f"❌ Failed: {accounts_save_result['error']}")

    # Example 3: Extract and save account types as CSV
    print("\n3. Extracting and saving account types as CSV...")
    account_types_result = extractor.extract_fakebank_data('accounts', 'csv', save=True)

    if account_types_result['success']:
        print(f"✅ Success! Saved to: {account_types_result['save_result']['filepath']}")
        print(f"Records: {account_types_result['save_result']['records_count']}")
    else:
        print(f"❌ Failed: {account_types_result['error']}")
