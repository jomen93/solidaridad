from rich.logging import RichHandler
from rich.console import Console
import logging

def setup_rich_logging(level=logging.INFO):
    """Configura Rich logging para todo el proyecto"""

    # Crear console personalizada
    console = Console(
        width=120,
        force_terminal=True,
        color_system="auto",
        stderr=False
    )

    # Configurar handler con Rich
    rich_handler = RichHandler(
        console=console,
        rich_tracebacks=True,
        show_path=False,
        show_time=True,
        omit_repeated_times=False,
        markup=True,
        show_level=True
    )

    # Formato más simple
    formatter = logging.Formatter(
        fmt="%(message)s"
    )
    rich_handler.setFormatter(formatter)

    # Limpiar handlers existentes
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Configurar logging root
    root_logger.setLevel(level)
    root_logger.addHandler(rich_handler)

    # Asegurar que los loggers específicos también usen Rich
    loggers_to_configure = [
        '__main__',
        'src.extract.data_extractor',
        'src.transform.data_transformer',
        'src.load.data_loader',
        'src.enrich.external_enrichment'
    ]

    for logger_name in loggers_to_configure:
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        logger.handlers = []
        logger.addHandler(rich_handler)
        logger.propagate = False

    return console

def get_rich_logger(name: str):
    """Obtiene un logger configurado para usar con Rich"""
    logger = logging.getLogger(name)

    if not logger.handlers:
        console = Console(
            width=120,
            force_terminal=True,
            color_system="auto"
        )

        rich_handler = RichHandler(
            console=console,
            rich_tracebacks=True,
            show_path=False,
            show_time=True,
            markup=True,
            show_level=True
        )

        logger.addHandler(rich_handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False

    return logger
