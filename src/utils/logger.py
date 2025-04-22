from loguru import logger
import sys
from pathlib import Path

def setup_logging():
    """Configura el sistema de logging con salida a consola y archivo"""
    # Crear directorio de logs si no existe
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logger.remove()
    
    # Configuración para consola
    logger.add(
        sys.stdout,
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )
    
    # Configuración para archivo
    logger.add(
        log_dir / "app.log",
        rotation="1 week",
        retention="1 month",
        compression="zip",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{function}:{line} - {message}"
    )
    
    logger.info("Logging configurado correctamente")