# MÓDULO DE CONFIGURACIÓN DEL SISTEMA DE LOGGING
# Implementa configuración dual para consola y archivo usando loguru
# Proporciona rotación automática y compresión de logs históricos

from loguru import logger
import sys
from pathlib import Path

# ====================================================================================================================
#                                        CONFIGURAR SISTEMA DE LOGGING GLOBAL
# ====================================================================================================================

def setup_logging():
  # CONFIGURA LOGGING DUAL PARA CONSOLA Y ARCHIVO CON DIFERENTES NIVELES
  # Establece formato colorizado para consola y detallado para archivo
  # Implementa rotación automática y limpieza de logs antiguos
  
  # crear directorio logs si no existe para almacenamiento de archivos
  log_dir = Path("logs")
  log_dir.mkdir(exist_ok=True)

  # limpiar configuraciones previas para evitar duplicación
  logger.remove()

  # configuración para salida de consola con nivel INFO mínimo
  logger.add(
    sys.stdout,
    colorize=True, # colores para facilitar lectura en consola
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan> - <level>{message}</level>",
    level="INFO" 
  )

  # configuración para archivo con nivel DEBUG completo
  logger.add(
    log_dir / "app.log", 
    rotation="1 week", # crear nuevo archivo cada semana
    retention="1 month", # eliminar archivos después de un mes
    compression="zip", # comprimir logs antiguos para ahorrar espacio
    level="DEBUG", 
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{function}:{line} - {message}" # formato detallado sin colores para archivo
  )