"""
Pipeline principal ETL
Orquesta el proceso completo de Extract, Transform, Load
"""

import os
import logging
import sqlite3
import glob
import tempfile
import subprocess
import pandas as pd

from src.extract.data_extractor import DataExtractor
from src.transform.data_transformer import DataTransformer
from src.load.data_loader import DataLoader
from src.enrich.external_enrichment import ExternalEnrichment
from src.config import ENRICHMENT_CONFIG
from src.logging_config import setup_rich_logging, get_rich_logger

class ETLPipeline:
    """
    Pipeline principal que orquesta todo el proceso ETL
    """

    def __init__(self):
        # Setup Rich logging first (initialize once for entire application)
        self.console = setup_rich_logging(level=logging.INFO)
        self.logger = get_rich_logger(__name__)

        # Initialize ETL components
        self.extractor   = DataExtractor()
        self.transformer = DataTransformer()
        self.loader      = DataLoader()

    def run_pipeline(self, endpoint: str = 'fakebank/accounts'):
        """
        Ejecuta el pipeline completo ETL

        Args:
            endpoint: Endpoint de la API a extraer
        """

        self.logger.info("[bold blue]" + "="*60 + "[/bold blue]")
        self.logger.info("[bold white]🚀 INICIANDO PIPELINE ETL[/bold white]")
        self.logger.info("[bold blue]" + "="*60 + "[/bold blue]")

        # EXTRACT
        self.logger.info("[bold green]📥 1. EXTRAYENDO DATOS...[/bold green]")
        accounts_result = self.extractor.extract_fakebank_data('accounts', 'parquet', save=True)

        if accounts_result['success']:
            records_count = accounts_result['extraction_result']['metadata']['total_records']
            self.logger.info(f"[green]✓ Extracción completada: {records_count} registros extraídos[/green]")
        else:
            self.logger.error(f"[red]✗ Error en extracción: {accounts_result['error']}[/red]")
            return {'status': 'error', 'error': accounts_result['error']}

        # TRANSFORM
        self.logger.info("\n[bold yellow]🔄 2. TRANSFORMANDO DATOS...[/bold yellow]")
        latest_file = self.transformer.find_latest_raw_file('accounts')

        if not latest_file:
            self.logger.error("[red]✗ No se encontró archivo raw para procesar[/red]")
            return {'status': 'error', 'error': 'No raw file found'}

        transform_result = self.transformer.transform_from_raw_file(
            latest_file,
            save_processed=True,
            processed_format='parquet'
        )

        if transform_result['success']:
            self.logger.info(f"[yellow]✓ Transformación completada: {transform_result['transformed_records_count']} registros procesados[/yellow]")
        else:
            self.logger.error(f"[red]✗ Error en transformación: {transform_result['error']}[/red]")
            return {'status': 'error', 'error': transform_result['error']}

        # ENRICH
        self.logger.info("\n[bold cyan]🌟 2.1 ENRIQUECIENDO CON DATOS EXTERNOS...[/bold cyan]")
        processed_data = transform_result['transformed_data']

        try:
            enricher = ExternalEnrichment(
                holiday_country_code=ENRICHMENT_CONFIG.get('holiday_country_code','US'),
                fx_target_currency=ENRICHMENT_CONFIG.get('fx_target_currency','USD')
            )
            processed_df = pd.DataFrame(processed_data)

            # Apply enrichments based on config
            enable_holidays = ENRICHMENT_CONFIG.get('enable_holidays', True)
            enable_fx = ENRICHMENT_CONFIG.get('enable_fx', False)

            if enable_holidays:
                self.logger.info("[cyan]📅 Enriqueciendo con festivos públicos...[/cyan]")
            if enable_fx:
                self.logger.info("[cyan]💱 Enriqueciendo con tipos de cambio...[/cyan]")

            processed_df = enricher.enrich(
                processed_df,
                enable_holidays=enable_holidays,
                enable_fx=enable_fx,
                fx_target_currency=ENRICHMENT_CONFIG.get('fx_target_currency', 'USD')
            )
            processed_data = processed_df.to_dict('records')
            self.logger.info("[cyan]✓ Enriquecimiento completado[/cyan]")

        except Exception as e:
            self.logger.warning(f"[yellow]⚠️ Error en enriquecimiento (continuando): {str(e)}[/yellow]")
            # Continue with original data if enrichment fails

        # LOAD
        self.logger.info("\n[bold magenta]💾 3. CARGANDO DATOS EN SQLITE...[/bold magenta]")
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
            self.logger.info(f"[magenta]✓ Carga completada:[/magenta]")
            self.logger.info(f"   [dim]• Base de datos: {db_path}[/dim]")
            self.logger.info(f"   [dim]• Volcado SQL: {sql_dump_path}[/dim]")
        else:
            self.logger.error(f"[red]✗ Error al guardar en base de datos: {load_result['error']}[/red]")
            return {'status': 'error', 'error': load_result['error']}

        # EXECUTE QUERIES
        self.logger.info("\n[bold blue]📊 4. EJECUTANDO QUERIES AUTOMÁTICAS...[/bold blue]")
        self._execute_sql_queries(queries_dir, sql_dump_path)

        # SUCCESS SUMMARY
        self.logger.info("\n[bold green]" + "="*60 + "[/bold green]")
        self.logger.info("[bold white]🎉 PIPELINE ETL COMPLETADO EXITOSAMENTE[/bold white]")
        self.logger.info("[bold green]" + "="*60 + "[/bold green]")

        # Final summary table
        self.console.print("\n[bold]📋 Resumen Final:[/bold]")
        summary_data = [
            ["Registros extraídos", f"{accounts_result['extraction_result']['metadata']['total_records']:,}"],
            ["Registros transformados", f"{transform_result['transformed_records_count']:,}"],
            ["Base de datos", db_path],
            ["Volcado SQL", sql_dump_path],
            ["Estado", "[green]SUCCESS[/green]"]
        ]

        from rich.table import Table
        table = Table(show_header=True, header_style="bold blue")
        table.add_column("Métrica", style="cyan")
        table.add_column("Valor", style="magenta")

        for row in summary_data:
            table.add_row(*row)

        self.console.print(table)

        return {
            'status': 'success',
            'extracted_records': accounts_result["extraction_result"]['metadata']['total_records'],
            'transformed_records': transform_result['transformed_records_count'],
            'db_path': db_path,
            'sql_dump_path': sql_dump_path,
            'error': None
        }

    def _execute_sql_queries(self, queries_dir: str, sql_dump_path: str):
        """
        Execute all SQL queries in the queries directory

        Args:
            queries_dir: Directory containing SQL files
            sql_dump_path: Path to SQL dump file
        """
        sql_files = sorted([f for f in glob.glob(f'{queries_dir}/*.sql')
                           if not f.endswith('etl_results_dump.sql')])

        if not sql_files:
            self.logger.info("[yellow]ℹ️ No se encontraron archivos .sql en la carpeta 'querys'[/yellow]")
            return

        if not os.path.exists(sql_dump_path):
            self.logger.error(f"[red]✗ No se encontró el volcado SQL: {sql_dump_path}[/red]")
            return

        self.logger.info(f"[blue]📁 Encontrados {len(sql_files)} archivos SQL para ejecutar[/blue]")

        try:
            # Create temporary database from SQL dump
            with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp_db:
                tmp_db_path = tmp_db.name

            # Restore dump to temporary database
            restore_cmd = f"sqlite3 {tmp_db_path} < {sql_dump_path}"
            result = subprocess.run(restore_cmd, shell=True, capture_output=True, text=True)

            if result.returncode != 0:
                self.logger.error(f"[red]✗ Error restaurando la base desde {sql_dump_path}[/red]")
                self.logger.error(f"[red]{result.stderr}[/red]")
                return

            self.logger.info("[green]✓ Base temporal restaurada desde volcado SQL[/green]")

            # Execute each SQL file
            conn = sqlite3.connect(tmp_db_path)
            executed_count = 0

            for sql_file in sql_files:
                query_name = os.path.basename(sql_file)
                self.logger.info(f"\n[cyan]📝 Ejecutando: {query_name}[/cyan]")

                try:
                    with open(sql_file, 'r', encoding='utf-8') as f:
                        query = f.read()

                    result = conn.execute(query)
                    rows = result.fetchall()
                    columns = [desc[0] for desc in result.description] if result.description else []

                    self.logger.info(f"[green]   ✓ Columnas: {columns}[/green]")

                    # Show first few rows
                    for i, row in enumerate(rows[:3]):
                        self.logger.info(f"[dim]   Fila {i+1}: {row}[/dim]")

                    if len(rows) > 3:
                        self.logger.info(f"[dim]   ... y {len(rows)-3} filas más[/dim]")

                    self.logger.info(f"[green]   📊 Total filas: {len(rows)}[/green]")
                    executed_count += 1

                except Exception as e:
                    self.logger.error(f"[red]   ✗ Error ejecutando {query_name}: {str(e)}[/red]")

            conn.close()

            self.logger.info(f"\n[green]✓ Queries ejecutadas exitosamente: {executed_count}/{len(sql_files)}[/green]")

        except Exception as e:
            self.logger.error(f"[red]✗ Error general ejecutando queries: {str(e)}[/red]")

        finally:
            # Clean up temporary database
            if 'tmp_db_path' in locals() and os.path.exists(tmp_db_path):
                os.remove(tmp_db_path)
                self.logger.info("[dim]🗑️ Base temporal eliminada[/dim]")


if __name__ == "__main__":
    # Ejecutar pipeline
    pipeline = ETLPipeline()
    result = pipeline.run_pipeline()

    # Final status
    if result['status'] == 'success':
        print("\n🎯 Pipeline ejecutado exitosamente!")
    else:
        print(f"\n❌ Pipeline falló: {result.get('error', 'Unknown error')}")
