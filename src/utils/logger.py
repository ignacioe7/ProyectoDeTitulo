from loguru import logger
import sys
from pathlib import Path

def setup_logging():
    """Prepara todo para que podamos registrar lo que pasa, en la consola y en un archivo"""
    # Nos aseguramos de que la carpeta 'logs' esté creada
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Quitamos cualquier configuración previa de loguru para empezar de cero
    logger.remove()

    # Configuramos cómo se ven los mensajes en la pantalla (consola)
    logger.add(
        sys.stdout,
        colorize=True, # Le ponemos colores para que sea más fácil de leer
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO" # Solo mostraremos mensajes de nivel INFO o superior en la consola
    )

    # Configuramos cómo se guardan los mensajes en el archivo 'app.log'
    logger.add(
        log_dir / "app.log", # El archivo donde guardaremos todo
        rotation="1 week", # Cada semana creará un archivo nuevo
        retention="1 month", # Guardará los archivos de log por un mes
        compression="zip", # Comprimirá los archivos viejos para que ocupen menos espacio
        level="DEBUG", # En el archivo guardaremos todo, desde DEBUG para arriba
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{function}:{line} - {message}" # El formato para el archivo, sin colores
    )

    logger.info("El sistema de logging ya está listo")