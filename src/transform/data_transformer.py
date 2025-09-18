"""
Data transformation module
Implementation of data cleaning, normalization and enrichment
"""

import pandas as pd
import re
import logging
from datetime import datetime, date
from typing import Dict, Any, List, Union, Optional
import json
import numpy as np


class DataTransformer:
    """
    Class for transforming and cleaning extracted data
    """

    def __init__(self):
        """
        Initialize the data transformer
        """
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        # Transaction categories mapping for data enrichment
        # This mapping serves as a simulated lookup table to enrich transaction data
        # with additional business context and metadata for analytical purposes.
        #
        # Design decisions and rationale:
        # 1. BASED ON REAL DATA: Uses actual category values found in the dataset
        #    ['Other Services', 'Health Care', 'Payment/Credit', etc.]
        #
        # 2. TYPE CLASSIFICATION: Groups categories by business domain/industry
        #    - 'healthcare': Medical and health-related expenses
        #    - 'transportation': Travel, fuel, automotive expenses
        #    - 'food_beverage': Dining, restaurants, food purchases
        #    - 'utilities': Phone, cable, essential services
        #    - 'retail': Merchandise and general purchases
        #    - 'service': Professional and other services
        #    - 'payment': Credit payments and financial transfers
        #    - 'fee': Bank fees and interest charges
        #    - 'travel': Travel-related expenses
        #    - 'personal_care': Beauty and personal care
        #    - 'miscellaneous': Uncategorized expenses
        #
        # 3. TAX DEDUCTIBILITY: Indicates if expenses are typically tax-deductible
        #    True for: Healthcare, Transportation (business use), Travel
        #    False for: Personal expenses like dining, beauty, entertainment
        #    Based on common tax regulations (US/Europe standards)
        #
        # 4. PRIORITY LEVELS: Risk and compliance priority for financial analysis
        #    'high': Critical for compliance, health monitoring (Health Care, Fees, Payments)
        #    'medium': Regular operational expenses (Utilities, Transportation, Services)
        #    'low': Discretionary/lifestyle expenses (Dining, Beauty, Entertainment)
        #
        # This enrichment enables:
        # - Automated expense categorization and reporting
        # - Tax preparation and deduction identification
        # - Spending pattern analysis by category type
        # - Risk assessment and anomaly detection
        # - Business intelligence and financial planning
        self.transaction_categories_mapping = {
            'Other Services': {'type': 'service', 'tax_deductible': False, 'priority': 'medium'},
            'Health Care': {'type': 'healthcare', 'tax_deductible': True, 'priority': 'high'},
            'Payment/Credit': {'type': 'payment', 'tax_deductible': False, 'priority': 'high'},
            'Merchandise': {'type': 'retail', 'tax_deductible': False, 'priority': 'low'},
            'Phone/Cable': {'type': 'utilities', 'tax_deductible': False, 'priority': 'medium'},
            'Fee/Interest Charge': {'type': 'fee', 'tax_deductible': False, 'priority': 'high'},
            'Other': {'type': 'miscellaneous', 'tax_deductible': False, 'priority': 'low'},
            'Dining': {'type': 'food_beverage', 'tax_deductible': False, 'priority': 'low'},
            'Gas/Automotive': {'type': 'transportation', 'tax_deductible': True, 'priority': 'medium'},
            'Other Travel': {'type': 'travel', 'tax_deductible': True, 'priority': 'medium'},
            'restaurants': {'type': 'food_beverage', 'tax_deductible': False, 'priority': 'low'},
            'beauty': {'type': 'personal_care', 'tax_deductible': False, 'priority': 'low'},
            'fuel': {'type': 'transportation', 'tax_deductible': True, 'priority': 'medium'},
            'air': {'type': 'transportation', 'tax_deductible': True, 'priority': 'medium'},
            'gaz': {'type': 'transportation', 'tax_deductible': True, 'priority': 'medium'},
            'food': {'type': 'food_beverage', 'tax_deductible': False, 'priority': 'low'},
            'taxi': {'type': 'transportation', 'tax_deductible': True, 'priority': 'medium'}
        }

    def normalize_column_names(self, data: Union[List[Dict], pd.DataFrame]) -> pd.DataFrame:
        """
        Normalize column names to snake_case and standardize naming conventions

        Args:
            data: Input data (list of dicts or DataFrame)

        Returns:
            DataFrame with normalized column names
        """
        # Convert to DataFrame if needed
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data.copy()

        # Create mapping for column name normalization
        column_mapping = {}

        for col in df.columns:
            # Convert to snake_case
            normalized = re.sub(r'([A-Z])', r'_\1', col).lower()
            normalized = re.sub(r'^_', '', normalized)  # Remove leading underscore
            normalized = re.sub(r'_+', '_', normalized)  # Remove multiple underscores

            # Standardize specific column names based on your data structure
            standardization_map = {
                'category': 'transaction_category',
                'credit': 'credit_amount',
                'debit': 'debit_amount',
                'description': 'transaction_description',
                'id': 'transaction_id',
                'transactiondate': 'transaction_date',
                'transaction_date': 'transaction_date'
            }

            final_name = standardization_map.get(normalized, normalized)
            column_mapping[col] = final_name

        # Apply column renaming
        df = df.rename(columns=column_mapping)

        self.logger.info(f"Column normalization completed. Renamed: {column_mapping}")
        return df

    def convert_date_formats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert date columns to standard YYYY-MM-DD format

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with standardized date formats
        """
        df_processed = df.copy()
        date_columns = ['transaction_date', 'created_date', 'date', 'updated_date']

        for col in date_columns:
            if col in df_processed.columns:
                try:
                    # Handle different date formats
                    df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce')

                    # Convert to YYYY-MM-DD format (string)
                    df_processed[col] = df_processed[col].dt.strftime('%Y-%m-%d')

                    # Handle any conversion errors (NaT values)
                    df_processed[col] = df_processed[col].replace('NaT', None)

                    self.logger.info(f"Date conversion completed for column: {col}")

                except Exception as e:
                    self.logger.warning(f"Could not convert dates in column {col}: {str(e)}")

        return df_processed

    def clean_financial_amounts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize financial amounts (remove currency symbols, convert to float)

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with cleaned financial amounts
        """
        df_processed = df.copy()
        financial_columns = ['credit_amount', 'debit_amount', 'amount', 'balance']

        for col in financial_columns:
            if col in df_processed.columns:
                try:
                    # Convert to string first to handle mixed types
                    df_processed[col] = df_processed[col].astype(str)

                    # Replace empty strings and NaN with 0
                    df_processed[col] = df_processed[col].replace(['', 'nan', 'NaN', 'None'], '0')

                    # Remove currency symbols and clean the data
                    df_processed[col] = df_processed[col].str.replace(r'[$,€£¥]', '', regex=True)
                    df_processed[col] = df_processed[col].str.replace(r'[^\d.-]', '', regex=True)

                    # Convert to numeric
                    df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').fillna(0)

                    # Round to 2 decimal places for financial precision
                    df_processed[col] = df_processed[col].round(2)

                    self.logger.info(f"Financial amount cleaning completed for column: {col}")

                except Exception as e:
                    self.logger.warning(f"Could not clean financial amounts in column {col}: {str(e)}")

        return df_processed

    def enrich_with_transaction_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich transactions with additional category information

        Args:
            df: DataFrame with transaction data

        Returns:
            DataFrame enriched with transaction category details
        """
        df_enriched = df.copy()

        # Enriquecer con información de categorías de transacciones
        for category, info in self.transaction_categories_mapping.items():
            mask = df_enriched['transaction_category'] == category
            for key, value in info.items():
                df_enriched.loc[mask, f'category_{key}'] = value

        # Manejar categorías no mapeadas
        unmapped_mask = ~df_enriched['transaction_category'].isin(self.transaction_categories_mapping.keys())
        df_enriched.loc[unmapped_mask, 'category_type'] = 'unknown'
        df_enriched.loc[unmapped_mask, 'category_tax_deductible'] = False
        df_enriched.loc[unmapped_mask, 'category_priority'] = 'low'

        self.logger.info("Transaction category enrichment completed.")
        return df_enriched

    def add_custom_validations_and_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add custom validations and computed features based on transaction data

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with additional validations and computed fields
        """
        df_validated = df.copy()

        # 1. Calcular balance neto de la transacción
        if 'credit_amount' in df_validated.columns and 'debit_amount' in df_validated.columns:
            df_validated['net_transaction_amount'] = (
                df_validated['credit_amount'] - abs(df_validated['debit_amount'])
            )

        # 2. Clasificar transacciones por monto
        if 'net_transaction_amount' in df_validated.columns:
            df_validated['transaction_size'] = pd.cut(
                abs(df_validated['net_transaction_amount']),
                bins=[0, 10, 50, 200, 1000, float('inf')],
                labels=['micro', 'small', 'medium', 'large', 'very_large'],
                right=False
            )

        # 3. Detectar transacciones anómalas (valores extremos)
        if 'net_transaction_amount' in df_validated.columns:
            Q1 = df_validated['net_transaction_amount'].quantile(0.25)
            Q3 = df_validated['net_transaction_amount'].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            df_validated['is_anomaly'] = (
                (df_validated['net_transaction_amount'] < lower_bound) |
                (df_validated['net_transaction_amount'] > upper_bound)
            )

        # 4. Agregar información temporal
        if 'transaction_date' in df_validated.columns:
            df_processed_dates = pd.to_datetime(df_validated['transaction_date'], errors='coerce')
            df_validated['transaction_year'] = df_processed_dates.dt.year
            df_validated['transaction_month'] = df_processed_dates.dt.month
            df_validated['transaction_day_of_week'] = df_processed_dates.dt.day_name()
            df_validated['transaction_quarter'] = df_processed_dates.dt.quarter

        # 5. Validar integridad de datos
        df_validated['data_quality_score'] = self._calculate_data_quality_score(df_validated)

        # 6. Agregar banderas de negocio
        df_validated['is_fee_transaction'] = df_validated.get('transaction_category', '').str.contains('Fee', case=False, na=False)
        df_validated['is_payment_transaction'] = df_validated.get('transaction_category', '').str.contains('Payment', case=False, na=False)
        df_validated['is_large_transaction'] = abs(df_validated.get('net_transaction_amount', 0)) > 500

        # 7. Agregar timestamp de procesamiento
        df_validated['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        self.logger.info("Custom validations and features completed")
        return df_validated

    def _calculate_data_quality_score(self, df: pd.DataFrame) -> pd.Series:
        """
        Calculate a data quality score for each record (0-100)

        Args:
            df: Input DataFrame

        Returns:
            Series with quality scores
        """
        scores = pd.Series(100.0, index=df.index)  # Start with perfect score

        # Deduct points for missing critical fields
        critical_fields = ['transaction_id', 'transaction_category', 'transaction_date']
        for field in critical_fields:
            if field in df.columns:
                scores -= df[field].isna() * 20  # -20 points for missing critical field

        # Deduct points for missing amounts (both credit and debit empty)
        if 'credit_amount' in df.columns and 'debit_amount' in df.columns:
            both_empty = (df['credit_amount'] == 0) & (df['debit_amount'] == 0)
            scores -= both_empty * 15  # -15 points for no transaction amount

        # Deduct points for missing transaction description
        if 'transaction_description' in df.columns:
            scores -= df['transaction_description'].isna() * 5  # -5 points for missing description

        # Deduct points for anomalous transactions (might indicate data errors)
        if 'is_anomaly' in df.columns:
            scores -= df['is_anomaly'] * 10  # -10 points for anomalous values

        return scores.clip(0, 100)  # Ensure scores are between 0-100

    def transform_data(self, data: Union[List[Dict], Dict]) -> List[Dict]:
        """
        Main transformation method that applies all transformations

        Args:
            data: Input data (transaction data)

        Returns:
            Transformed data as list of dictionaries
        """
        try:
            self.logger.info("Starting data transformation process...")

            # Convert to DataFrame
            if isinstance(data, dict):
                data = [data]

            df = pd.DataFrame(data)
            self.logger.info(f"Initial data shape: {df.shape}")

            # 1. Normalize column names
            df = self.normalize_column_names(df)

            # 2. Convert date formats
            df = self.convert_date_formats(df)

            # 3. Clean financial amounts
            df = self.clean_financial_amounts(df)

            # 4. Enrich with transaction categories (SOLO ESTO QUEDA)
            df = self.enrich_with_transaction_categories(df)

            # 5. Add custom validations and features
            df = self.add_custom_validations_and_features(df)

            # Convert back to list of dictionaries
            result = df.to_dict('records')

            self.logger.info(f"Transformation completed. Final data shape: {df.shape}")
            self.logger.info(f"Columns after transformation: {list(df.columns)}")

            return result

        except Exception as e:
            self.logger.error(f"Error in data transformation: {str(e)}")
            # Return original data if transformation fails
            return data if isinstance(data, list) else [data]

    def clean_data(self, data: Union[List[Dict], Dict]) -> List[Dict]:
        """
        Clean and validate data (basic cleaning without enrichment)

        Args:
            data: Input data

        Returns:
            Cleaned data as list of dictionaries
        """
        try:
            self.logger.info("Starting data cleaning process...")

            # Convert to DataFrame
            if isinstance(data, dict):
                data = [data]

            df = pd.DataFrame(data)

            # Basic cleaning operations
            df = self.normalize_column_names(df)
            df = self.convert_date_formats(df)
            df = self.clean_financial_amounts(df)

            # Remove duplicate records (if any)
            initial_count = len(df)
            df = df.drop_duplicates(subset=['transaction_id'], keep='first')
            final_count = len(df)

            if initial_count != final_count:
                self.logger.info(f"Removed {initial_count - final_count} duplicate records")

            result = df.to_dict('records')

            self.logger.info("Data cleaning completed")
            return result

        except Exception as e:
            self.logger.error(f"Error in data cleaning: {str(e)}")
            return data if isinstance(data, list) else [data]

    def read_from_raw_parquet(self, filepath: str) -> List[Dict]:
        """
        Read data from parquet files in data/raw directory

        Args:
            filepath: Path to the parquet file (e.g., 'data/raw/accounts_20250917_232249.parquet')

        Returns:
            Data as list of dictionaries
        """
        try:
            import os

            # Check if file exists
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"File not found: {filepath}")

            # Read parquet file
            df = pd.read_parquet(filepath)

            # Convert to list of dictionaries
            data = df.to_dict('records')

            self.logger.info(f"Successfully read {len(data)} records from {filepath}")
            self.logger.info(f"Columns found: {list(df.columns)}")

            return data

        except Exception as e:
            self.logger.error(f"Error reading parquet file {filepath}: {str(e)}")
            return []

    def transform_from_raw_file(self, raw_filepath: str, save_processed: bool = False,
                               processed_format: str = 'parquet') -> Dict[str, Any]:
        """
        Read data from raw parquet file, transform it, and optionally save to processed directory

        Args:
            raw_filepath: Path to raw parquet file
            save_processed: Whether to save transformed data to processed directory
            processed_format: Format for processed file ('parquet', 'csv', 'json')

        Returns:
            Dict with transformation results and metadata
        """
        try:
            self.logger.info(f"Starting transformation from raw file: {raw_filepath}")

            # Read data from raw file
            raw_data = self.read_from_raw_parquet(raw_filepath)

            if not raw_data:
                return {
                    'success': False,
                    'error': 'No data found in raw file',
                    'raw_filepath': raw_filepath,
                    'processed_filepath': None,
                    'transformed_data': None
                }

            # Transform the data
            transformed_data = self.transform_data(raw_data)

            result = {
                'success': True,
                'raw_filepath': raw_filepath,
                'raw_records_count': len(raw_data),
                'transformed_records_count': len(transformed_data),
                'transformed_data': transformed_data,
                'processed_filepath': None,
                'error': None
            }

            # Save to processed if requested
            if save_processed:
                import os
                from datetime import datetime

                # Generate processed filename
                raw_filename = os.path.basename(raw_filepath)
                base_name = raw_filename.replace('.parquet', '')
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                processed_filename = f"{base_name}_processed_{timestamp}.{processed_format}"
                processed_filepath = f"data/processed/{processed_filename}"

                # Create processed directory if it doesn't exist
                os.makedirs('data/processed', exist_ok=True)

                # Save processed data
                if processed_format.lower() == 'parquet':
                    df_processed = pd.DataFrame(transformed_data)
                    df_processed.to_parquet(processed_filepath, index=False)
                elif processed_format.lower() == 'csv':
                    df_processed = pd.DataFrame(transformed_data)
                    df_processed.to_csv(processed_filepath, index=False)
                elif processed_format.lower() == 'json':
                    import json
                    with open(processed_filepath, 'w') as f:
                        json.dump(transformed_data, f, indent=2, default=str)

                result['processed_filepath'] = processed_filepath
                self.logger.info(f"Processed data saved to: {processed_filepath}")

            self.logger.info("Transformation from raw file completed successfully")
            return result

        except Exception as e:
            error_message = f"Error in transform_from_raw_file: {str(e)}"
            self.logger.error(error_message)

            return {
                'success': False,
                'error': error_message,
                'raw_filepath': raw_filepath,
                'processed_filepath': None,
                'transformed_data': None
            }

    def find_latest_raw_file(self, data_type: str = 'accounts') -> str:
        """
        Find the most recent raw parquet file for a given data type

        Args:
            data_type: Type of data ('accounts', 'account_types')

        Returns:
            Path to the most recent parquet file
        """
        import os
        import glob

        try:
            # Pattern to match parquet files only
            pattern = f"data/raw/{data_type}_*.parquet"
            files = glob.glob(pattern)

            if not files:
                raise FileNotFoundError(f"No parquet files found for {data_type} in data/raw/")

            # Sort by modification time (most recent first)
            files.sort(key=os.path.getmtime, reverse=True)
            latest_file = files[0]

            self.logger.info(f"Found latest {data_type} parquet file: {latest_file}")
            return latest_file

        except Exception as e:
            self.logger.error(f"Error finding latest raw file: {str(e)}")
            return ""


# Example usage and testing
if __name__ == "__main__":

    transformer = DataTransformer()

    print("🚀 ETL Data Transformer - Testing with Real Raw Data")
    print("=" * 60)

    # Test 1: Process latest raw parquet file
    print("\n=== Processing Latest Raw Parquet File ===")

    try:
        # Find latest parquet file
        print("🔍 Searching for latest accounts parquet file...")
        latest_parquet = transformer.find_latest_raw_file('accounts')

        if latest_parquet:
            print(f"📁 Found latest file: {latest_parquet}")

            # Show raw data sample
            raw_data = transformer.read_from_raw_parquet(latest_parquet)
            print(f"📊 Raw data loaded: {len(raw_data)} records")

            if raw_data:
                print("\n📝 Sample raw record:")
                for key, value in list(raw_data[0].items())[:6]:
                    print(f"  {key}: {value}")

            # Transform the data
            print(f"\n🔄 Applying transformations...")
            result = transformer.transform_from_raw_file(
                latest_parquet,
                save_processed=True,
                processed_format='parquet'
            )

            if result['success']:
                print(f"✅ Transformation successful!")
                print(f"📈 Raw records: {result['raw_records_count']}")
                print(f"📈 Transformed records: {result['transformed_records_count']}")
                print(f"💾 Processed file: {result['processed_filepath']}")

                # Show sample transformed record
                if result['transformed_data']:
                    print(f"\n🔍 Sample transformed record (first 10 fields):")
                    sample_record = result['transformed_data'][0]
                    for key, value in list(sample_record.items())[:10]:
                        print(f"  {key}: {value}")

                    print(f"\n📊 Total fields after transformation: {len(sample_record)}")

                    # Show new fields added
                    original_fields = ['category', 'credit', 'debit', 'description', 'id', 'transactionDate']
                    new_fields = [field for field in sample_record.keys()
                                if field not in [f.lower().replace('transactiondate', 'transaction_date')
                                              for f in original_fields]]
                    print(f"🆕 New fields added: {len(new_fields)}")
                    print(f"   {', '.join(new_fields[:10])}{'...' if len(new_fields) > 10 else ''}")

                    # Quick data quality analysis
                    df_analysis = pd.DataFrame(result['transformed_data'])

                    print(f"\n📊 Quick Analysis:")
                    if 'data_quality_score' in df_analysis.columns:
                        avg_quality = df_analysis['data_quality_score'].mean()
                        print(f"   Average quality score: {avg_quality:.2f}/100")

                    if 'transaction_category' in df_analysis.columns:
                        top_category = df_analysis['transaction_category'].value_counts().index[0]
                        top_count = df_analysis['transaction_category'].value_counts().iloc[0]
                        print(f"   Most common category: {top_category} ({top_count} transactions)")

                    if 'net_transaction_amount' in df_analysis.columns:
                        total_net = df_analysis['net_transaction_amount'].sum()
                        print(f"   Total net amount: ${total_net:,.2f}")

            else:
                print(f"❌ Transformation failed: {result['error']}")
        else:
            print("❌ No parquet files found for accounts")

    except Exception as e:
        print(f"❌ Error: {str(e)}")

