"""
Data loading module
Implementation for saving data to different formats and destinations
"""

import json
import csv
import os
import logging
from typing import Dict, Any, List, Union
from datetime import datetime
import pandas as pd


class DataLoader:
    """
    Class for loading/saving data to final destinations
    """

    def __init__(self):
        """
        Initialize the data loader
        """
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def save_to_csv(self, data: Union[List[Dict], Dict], filepath: str, encoding: str = 'utf-8') -> Dict[str, Any]:
        """
        Save data to CSV format

        Args:
            data: Data to save (list of dicts or single dict)
            filepath: Path where to save the CSV file
            encoding: File encoding (default: utf-8)

        Returns:
            Dict with operation result and metadata
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            # Convert single dict to list
            if isinstance(data, dict):
                data = [data]

            # Validate data
            if not data:
                raise ValueError("No data to save")

            # Get all possible fieldnames from the data
            fieldnames = set()
            for record in data:
                if isinstance(record, dict):
                    fieldnames.update(record.keys())

            fieldnames = sorted(list(fieldnames))

            # Save to CSV file
            with open(filepath, 'w', newline='', encoding=encoding) as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)

            # Get file size
            file_size = os.path.getsize(filepath)

            self.logger.info(f"Data saved successfully to CSV: {filepath}")
            self.logger.info(f"File size: {file_size} bytes, Records: {len(data)}")

            return {
                'success': True,
                'filepath': filepath,
                'file_size': file_size,
                'records_count': len(data),
                'columns': fieldnames,
                'format': 'csv',
                'error': None
            }

        except Exception as e:
            error_message = f"Error saving to CSV: {str(e)}"
            self.logger.error(error_message)

            return {
                'success': False,
                'filepath': filepath,
                'file_size': 0,
                'records_count': 0,
                'columns': [],
                'format': 'csv',
                'error': error_message
            }

    def save_to_parquet(self, data: Union[List[Dict], Dict], filepath: str) -> Dict[str, Any]:
        """
        Save data to parquet format with data type cleaning

        Args:
            data: Data to save (list of dicts or single dict)
            filepath: Path where to save the parquet file

        Returns:
            Dict with operation result and metadata
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            # Convert single dict to list
            if isinstance(data, dict):
                data = [data]

            # Validate data
            if not data:
                raise ValueError("No data to save")

            # Convert to DataFrame
            df = pd.DataFrame(data)

            # Clean data for parquet compatibility
            df_cleaned = df.copy()

            # Handle each column
            for col in df_cleaned.columns:
                # Skip if already numeric
                if pd.api.types.is_numeric_dtype(df_cleaned[col]):
                    continue

                # Handle string columns that might contain numbers
                if df_cleaned[col].dtype == 'object':
                    # Try to convert to numeric with explicit exception handling
                    try:
                        df_cleaned[col] = pd.to_numeric(df_cleaned[col])
                    except (ValueError, TypeError):
                        # If conversion fails, keep as object and handle special cases
                        pass

                    # If still object type, convert to string and handle special cases
                    if df_cleaned[col].dtype == 'object':
                        # Convert to string first
                        df_cleaned[col] = df_cleaned[col].astype(str)

                        # Replace empty dicts/objects that cause struct type errors
                        df_cleaned[col] = df_cleaned[col].replace(['{}', '[]', 'nan', 'NaN'], None)

            # Add extraction metadata as a column
            df_cleaned['extraction_timestamp'] = datetime.now().isoformat()

            # Save to parquet with error handling
            try:
                df_cleaned.to_parquet(filepath, index=False, engine='pyarrow')
            except Exception as parquet_error:
                # If parquet fails, convert all remaining object columns to string
                self.logger.warning(f"First parquet attempt failed: {str(parquet_error)}")
                self.logger.info("Converting all object columns to string and retrying...")

                for col in df_cleaned.select_dtypes(include=['object']).columns:
                    df_cleaned[col] = df_cleaned[col].astype(str)

                # Try saving again
                df_cleaned.to_parquet(filepath, index=False, engine='pyarrow')

            # Get file size
            file_size = os.path.getsize(filepath)

            self.logger.info(f"Data saved successfully to parquet: {filepath}")
            self.logger.info(f"Shape: {df_cleaned.shape}, File size: {file_size} bytes")

            return {
                'success': True,
                'filepath': filepath,
                'file_size': file_size,
                'records_count': len(df_cleaned),
                'columns': df_cleaned.columns.tolist(),
                'shape': df_cleaned.shape,
                'format': 'parquet',
                'error': None
            }

        except Exception as e:
            error_message = f"Error saving to parquet: {str(e)}"
            self.logger.error(error_message)

            return {
                'success': False,
                'filepath': filepath,
                'file_size': 0,
                'records_count': 0,
                'columns': [],
                'format': 'parquet',
                'error': error_message
            }

    def save_data(self, data: Union[List[Dict], Dict], filepath: str, format_type: str = 'json', **kwargs) -> Dict[str, Any]:
        """
        Generic method to save data in different formats

        Args:
            data: Data to save
            filepath: Path where to save the file
            format_type: Format type ('csv', 'parquet')
            **kwargs: Additional arguments for specific save methods

        Returns:
            Dict with operation result and metadata
        """
        format_type = format_type.lower()

        if format_type == 'csv':
            return self.save_to_csv(data, filepath, **kwargs)
        elif format_type == 'parquet':
            return self.save_to_parquet(data, filepath, **kwargs)
        else:
            error_message = f"Unsupported format: {format_type}"
            self.logger.error(error_message)

            return {
                'success': False,
                'filepath': filepath,
                'format': format_type,
                'error': error_message
            }

    def save_to_database(self, data, db_path='data/processed/etl_results.sqlite', sql_dump_path='etl_results_dump.sql', table_name='accounts'):
        """
        Save data to SQLite database and export SQL dump
        Args:
            data: Data to save (list of dicts, dict, or DataFrame)
            db_path: Path to SQLite database file
            sql_dump_path: Path to export SQL dump file
            table_name: Name of the table to save data
        Returns:
            Dict with operation result and metadata
        """
        import sqlite3
        import pandas as pd
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(db_path), exist_ok=True)

            # Convert to DataFrame if needed
            if isinstance(data, dict):
                data = [data]
            if not isinstance(data, pd.DataFrame):
                df = pd.DataFrame(data)
            else:
                df = data

            # Save DataFrame to SQLite
            conn = sqlite3.connect(db_path)
            df.to_sql(table_name, conn, if_exists='replace', index=False)

            # Export SQL dump
            with open(sql_dump_path, 'w', encoding='utf-8') as f:
                for line in conn.iterdump():
                    f.write(f'{line}\n')

            conn.close()

            self.logger.info(f"Data saved to SQLite: {db_path} and SQL dump: {sql_dump_path}")
            return {
                'success': True,
                'db_path': db_path,
                'sql_dump_path': sql_dump_path,
                'records_count': len(df),
                'columns': df.columns.tolist(),
                'format': 'sqlite',
                'error': None
            }
        except Exception as e:
            error_message = f"Error saving to SQLite or exporting SQL: {str(e)}"
            self.logger.error(error_message)
            return {
                'success': False,
                'db_path': db_path,
                'sql_dump_path': sql_dump_path,
                'records_count': 0,
                'columns': [],
                'format': 'sqlite',
                'error': error_message
            }
