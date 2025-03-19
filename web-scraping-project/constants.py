
# URL base de TripAdvisor
BASE_URL = "https://www.tripadvisor.com"

# Encabezados HTTP para simular un navegador real
HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
}

def get_headers(referer: str = BASE_URL) -> dict:
  """Obtiene encabezados HTTP con referrer opcional"""
  return {**HEADERS, "Referer": referer}