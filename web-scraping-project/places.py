from datetime import datetime
import re
from typing import Dict, List, Optional
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

  async def scrape_page(self, url: str) -> List[Dict]:
      """Extrae URLs y metadatos de atracciones de una sola página"""
      response = await self.client.get(url)
      selector = Selector(text=response.text)
      
      attractions_data = []
      attraction_divs = selector.css('div.hZuqH.y')
      
      for div in attraction_divs:
          try:
              # Obtener URL
              href = div.css('header.VLKGO div.NxKBB div div.alPVI a::attr(href)').get()
              if not href:
                  continue
                  
              full_url = f"{BASE_URL}{href}"
              
              
              # Extraer nombre del lugar
              raw_name = None
              raw_name = div.xpath('.//div[contains(@class, "XfVdV")]/text()').getall()
              if raw_name and len(raw_name) > 0:
                  place_name = ' '.join([t.strip() for t in raw_name if t.strip()])
              else:
                  place_name = (
                      div.xpath('string(.//div[contains(@class, "XfVdV") and contains(@class, "AIbhI")])').get() or
                      div.xpath('string(.//h3[contains(@class, "biGQs")]//div[contains(@class, "XfVdV")])').get() or
                      div.xpath('string(.//div[contains(@class, "ATCbm")]//div[contains(@class, "XfVdV")])').get() or
                      div.xpath('string(.//h3[contains(@class, "biGQs")]//span//div[contains(@class, "XfVdV")])').get() or
                      "Lugar Sin Nombre"
                  )
              
              
              # Limpiar nombre del lugar
              place_name = place_name.strip()
              place_name = re.sub(r'^\d+\.\s*', '', place_name)
              if not place_name:
                  place_name = "Lugar Sin Nombre"
              
              
              # Extraer puntuación
              rating_text = div.xpath('.//svg[contains(@class, "UctUV")]//title/text()').get()
              rating = 0.0
              if rating_text:
                  match = re.search(r'(\d+(\.\d+)?)', rating_text)
                  if match:
                      rating = float(match.group(1))
              
              # Extraer número de reseñas
              reviews_count = 0
              reviews_text = div.css('span.biGQs._P.pZUbB.osNWb::text').get()
              if reviews_text:
                  reviews_count = int(reviews_text.replace(',', ''))
              
              # Extraer tipo de lugar
              place_type_raw = div.css('div.biGQs._P.pZUbB.hmDzD::text').get() or "Sin categoría"
              place_type = place_type_raw.replace(' • ', ', ').strip()
              
              # Almacenar datos completos
              attraction_data = {
                  "place_name": place_name.strip(),
                  "place_type": place_type.strip(),
                  "rating": rating,
                  "reviews_count": reviews_count,
                  "url": full_url
              }
              
              attractions_data.append(attraction_data)
              
          except Exception as e:
              log.warning(f"Error extrayendo datos de una atracción: {e}")
      
      return attractions_data

  async def get_next_page_url(self, response_text: str) -> Optional[str]:
    """Obtiene URL para la siguiente página de atracciones"""
    selector = Selector(text=response_text)
    next_link = selector.css('a.BrOJk[data-smoke-attr="pagination-next-arrow"]::attr(href)').get()
    
    if next_link:
      log.debug(f"Enlace a siguiente página: {next_link}")
      return f"{BASE_URL}{next_link}"
    return None

  async def get_all_attractions(self) -> List[Dict]:
      """Obtiene datos de todas las atracciones en Valparaíso"""
      try:
          all_attractions = []
          page_count = 1
          current_url = f"{BASE_URL}/Attractions-g294306-Activities-a_allAttractions.true-Valparaiso_Valparaiso_Region.html"
          
          while current_url:
              response = await self.client.get(current_url)
              
              # Obtener datos de la página actual
              page_attractions = await self.scrape_page(current_url)
              all_attractions.extend(page_attractions)
              log.info(f"Encontradas {len(page_attractions)} atracciones en página {page_count}")
              
              # Guardar progreso en archivo
              with open('attractions_data.json', 'w', encoding='utf-8') as f:
                  json.dump({
                      "attractions": all_attractions
                  }, f, indent=2, ensure_ascii=False)
              
              # Obtener URL de siguiente página
              next_url = await self.get_next_page_url(response.text)
              if next_url:
                  current_url = next_url
                  page_count += 1
                  await asyncio.sleep(2)
              else:
                  break
          
          return all_attractions
          
      except Exception as e:
          log.error(f"Error extrayendo atracciones: {e}")
          raise

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