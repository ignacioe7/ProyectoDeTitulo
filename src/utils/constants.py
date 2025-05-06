# URL base del sitio web.
BASE_URL = "https://www.tripadvisor.com"

# Encabezados HTTP para simular un navegador.
HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
}

# Configuración de las rutas de las carpetas.
class PathConfig:
  DATA_DIR = "data"
  ATTRACTIONS_DIR = f"{DATA_DIR}/attractions"
  REGIONS_DIR = f"{DATA_DIR}/regions"
  OUTPUT_DIR = "output"
  LOGS_DIR = "logs"

def get_headers(referer: str = BASE_URL) -> dict:
  """
  Genera un diccionario de encabezados HTTP, opcionalmente
  añadiendo un 'Referer' específico.
  """
  return {**HEADERS, "Referer": referer}