# MÓDULO DE CONSTANTES Y CONFIGURACIÓN GLOBAL DEL SISTEMA
# Define URLs base, headers de navegador y rutas principales del proyecto
# Centraliza configuración para scraping de TripAdvisor y manejo de archivos

from pathlib import Path

# ====================================================================================================================
#                                             CONFIGURACIÓN DE SCRAPING
# ====================================================================================================================

# URL base principal para todas las peticiones a TripAdvisor
BASE_URL = "https://www.tripadvisor.com"

# headers HTTP que simulan navegador real para evitar detección anti-bot
HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9", 
}

# ====================================================================================================================
#                                            CONFIGURACIÓN DE RUTAS
# ====================================================================================================================

# calcular directorio raíz del proyecto subiendo 3 niveles desde este archivo
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# directorio principal donde se almacenan todos los datos del proyecto
DATA_DIR = PROJECT_ROOT / "data"

# crear directorio de datos automáticamente si no existe
if not DATA_DIR.exists():
  DATA_DIR.mkdir(parents=True, exist_ok=True)

# ruta como string para compatibilidad con código legacy
CONSOLIDATED_DATA_PATH = str(DATA_DIR / "consolidated_data.json") 

# ====================================================================================================================
#                                        CLASE DE CONFIGURACIÓN DE RUTAS
# ====================================================================================================================

class PathConfig:
  # CENTRALIZA TODAS LAS RUTAS PRINCIPALES DEL PROYECTO
  # Proporciona acceso consistente a archivos y directorios críticos
  # Utiliza pathlib para compatibilidad multiplataforma
  
  # archivo JSON principal con todos los datos consolidados
  CONSOLIDATED_JSON = DATA_DIR / "consolidated_data.json"
  
  # directorio que contiene configuraciones específicas por región
  REGIONS_DIR = DATA_DIR / "regions"
  
  # archivo de configuración con URLs y parámetros por región
  REGIONS_FILE = REGIONS_DIR / "regions.json"
  
  # directorio para almacenar archivos de log del sistema
  LOGS_DIR = PROJECT_ROOT / "logs"

# ====================================================================================================================
#                                              FUNCIONES AUXILIARES
# ====================================================================================================================

def get_headers(referer: str = BASE_URL) -> dict:
  # GENERA HEADERS HTTP CON REFERER PERSONALIZADO PARA SCRAPING
  # Combina headers base con referer específico para simular navegación real
  # Retorna diccionario completo de headers listo para usar en requests
  return {**HEADERS, "Referer": referer}