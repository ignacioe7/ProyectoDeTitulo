from pathlib import Path

# === CONFIG DE SCRAPING ===

BASE_URL = "https://www.tripadvisor.com" # url base tripadvisor

# headers para parecer navegador real
HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9", 
}

# === RUTAS DEL PROYECTO ===

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent # raiz del proyecto

# directorio principal de datos
DATA_DIR = PROJECT_ROOT / "data"

# crear directorio si no existe
if not DATA_DIR.exists():
  DATA_DIR.mkdir(parents=True, exist_ok=True)

# ruta como string para compatibilidad
CONSOLIDATED_DATA_PATH = str(DATA_DIR / "consolidated_data.json") 

class PathConfig:
  # RUTAS PRINCIPALES DEL PROYECTO
  # Define ubicaciones de archivos y directorios importantes
  # Usa pathlib para compatibilidad multiplataforma
  
  # archivos principales
  CONSOLIDATED_JSON = DATA_DIR / "consolidated_data.json" # datos consolidados
  REGIONS_DIR = DATA_DIR / "regions" # carpeta regiones
  REGIONS_FILE = REGIONS_DIR / "regions.json" # config regiones
  LOGS_DIR = PROJECT_ROOT / "logs" # carpeta logs

# ===============================================================
# GENERAR HEADERS CON REFERER
# ===============================================================

def get_headers(referer: str = BASE_URL) -> dict:
  # GENERA HEADERS CON REFERER PERSONALIZADO
  # Combina headers base con referer especifico
  # Util para simular navegacion real entre paginas
  return {**HEADERS, "Referer": referer}