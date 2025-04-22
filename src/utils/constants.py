# URL base y configuraciones compartidas
BASE_URL = "https://www.tripadvisor.com"

# Encabezados HTTP para scraping
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Configuración de directorios
class PathConfig:
    DATA_DIR = "data"
    ATTRACTIONS_DIR = f"{DATA_DIR}/attractions"
    REGIONS_DIR = f"{DATA_DIR}/regions"
    OUTPUT_DIR = "output"
    LOGS_DIR = "logs"

def get_headers(referer: str = BASE_URL) -> dict:
    """Genera headers HTTP con referer personalizable"""
    return {**HEADERS, "Referer": referer}

