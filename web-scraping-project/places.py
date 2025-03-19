from typing import List, Optional
import asyncio
import json
import httpx
from loguru import logger as log
from parsel import Selector
from constants import BASE_URL, HEADERS

class AttractionScraper:
  """Clase para extraer URLs de atracciones desde TripAdvisor"""
  
  def __init__(self):
    self.client = None
  
  async def __aenter__(self):
    self.client = httpx.AsyncClient(headers=HEADERS, follow_redirects=True)
    return self
  
  async def __aexit__(self, exc_type, exc_val, exc_tb):
    if self.client:
      await self.client.aclose()

  async def scrape_page(self, url: str) -> List[str]:
    """Extrae URLs de atracciones de una sola página"""
    response = await self.client.get(url)
    selector = Selector(text=response.text)
    
    urls = []
    attraction_divs = selector.css('div.hZuqH.y')
    
    for div in attraction_divs:
      href = div.css('header.VLKGO div.NxKBB div div.alPVI a::attr(href)').get()
      if href:
        urls.append(f"{BASE_URL}{href}")
        log.debug(f"URL de atracción encontrada: {href}")
    
    return urls

  async def get_next_page_url(self, response_text: str) -> Optional[str]:
    """Obtiene URL para la siguiente página de atracciones"""
    selector = Selector(text=response_text)
    next_link = selector.css('a.BrOJk[data-smoke-attr="pagination-next-arrow"]::attr(href)').get()
    
    if next_link:
      log.debug(f"Enlace a siguiente página: {next_link}")
      return f"{BASE_URL}{next_link}"
    return None

  async def get_all_attractions(self) -> List[str]:
    """Obtiene URLs para todas las atracciones en Valparaíso"""
    try:
      urls = []
      page_count = 1
      current_url = f"{BASE_URL}/Attractions-g294306-Activities-a_allAttractions.true-Valparaiso_Valparaiso_Region.html"
      
      while current_url:
        log.info(f"Extrayendo página {page_count}")
        response = await self.client.get(current_url)
        
        # Obtener URLs de la página actual
        page_urls = await self.scrape_page(current_url)
        urls.extend(page_urls)
        log.info(f"Encontradas {len(page_urls)} atracciones en página {page_count}")
        
        # Guardar progreso en archivo
        with open('attractions_urls.json', 'w', encoding='utf-8') as f:
          json.dump(urls, f, indent=2)
        
        # Obtener URL de siguiente página
        next_url = await self.get_next_page_url(response.text)
        if next_url:
          current_url = next_url
          page_count += 1
          await asyncio.sleep(2)  # Limitación de tasa
        else:
          break
      
      return urls
      
    except Exception as e:
      log.error(f"Error extrayendo atracciones: {e}")
      raise


async def load_urls_from_json(filename: str = 'attractions_urls.json') -> List[str]:
  """Carga URLs de atracciones desde archivo JSON"""
  try:
    with open(filename, 'r', encoding='utf-8') as f:
      return json.load(f)
  except FileNotFoundError:
    log.error(f"Archivo {filename} no encontrado")
    return []
  
async def main():
  """Función principal de ejecución"""
  log.info("Iniciando extractor de URLs de atracciones")
  try:
    async with AttractionScraper() as scraper:
      urls = await scraper.get_all_attractions()
      log.info(f"Extracción exitosa de {len(urls)} URLs de atracciones")
      
      # Guardar resultados finales
      with open('attractions_urls.json', 'w', encoding='utf-8') as f:
        json.dump(urls, f, indent=2)
          
  except Exception as e:
    log.error(f"Script falló: {e}")
    return 1
  return 0

if __name__ == "__main__":
  exit_code = asyncio.run(main())
  exit(exit_code)