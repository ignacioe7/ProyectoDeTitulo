from loguru import logger
import sys
from pathlib import Path

def setup_logging():
  """configura logging para consola y archivo"""
  # crear directorio logs si no existe
  log_dir = Path("logs")
  log_dir.mkdir(exist_ok=True)

  # limpiar configs previas
  logger.remove()

  # config para consola - solo INFO y superior
  logger.add(
    sys.stdout,
    colorize=True, # colores para leer facil
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan> - <level>{message}</level>",
    level="INFO" 
  )

  # config para archivo - todo incluyendo DEBUG
  logger.add(
    log_dir / "app.log", 
    rotation="1 week", # rota cada semana
    retention="1 month", # borra despues de un mes
    compression="zip", # comprime logs viejos
    level="DEBUG", 
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{function}:{line} - {message}" # sin colores para archivo
  )