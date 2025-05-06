import asyncio
import random
import httpx
from typing import Dict, List, Optional
from loguru import logger as log 
from parsel import Selector # Para parsear HTML

from src.core.metrics import ReviewMetricsCalculator
from src.core.parsers.review_parser import ReviewParser 
from src.utils.networking import smart_sleep 
from ..models.attraction import Attraction
from ..utils.constants import BASE_URL, HEADERS, get_headers 


class AttractionScraper:
  """Clase para sacar URLs de atracciones de TripAdvisor"""

  def __init__(self):
    self.client = None # El cliente http

  async def __aenter__(self):
    # Iniciamos el cliente al entrar al contexto
    self.client = httpx.AsyncClient(headers=HEADERS, follow_redirects=True)
    return self

  async def __aexit__(self, exc_type, exc_val, exc_tb):
    # Cerramos el cliente al salir
    if self.client:
      await self.client.aclose()

  async def scrape_page(self, url: str) -> List[Dict]:
    """Scrapea una página de resultados de atracciones"""
    try:
      response = await self.client.get(url) # Hacemos la petición
      selector = Selector(response.text) # Creamos el selector
      attractions = [] # Lista para guardar las atracciones

      # Iteramos sobre cada 'tarjeta' de atracción en la página
      for card in selector.xpath('//article[contains(@class, "GTuVU")]'):
        try:
          # Datos por defecto por si algo falla
          attraction_data = {
            "position": None,
            "place_name": "Lugar Sin Nombre",
            "place_type": "Sin categoría",
            "rating": 0.0,
            "reviews_count": 0,
            "url": ""
          }

          # Extraer URL
          href = card.xpath('.//a[contains(@href, "/Attraction_Review-")]/@href').get()
          if href:
            # Limpiamos la URL y añadimos la base
            attraction_data["url"] = f"{BASE_URL}{href.split('#')[0]}"

          # Extraer posición y nombre
          name_div = card.xpath('.//div[contains(@class, "XfVdV") and contains(@class, "AIbhI")]')
          if name_div:
            name_text = name_div.xpath('string(.)').get().strip()
            if '.' in name_text: # A veces viene con el número de posición
              parts = name_text.split('.', 1)
              try:
                attraction_data["position"] = int(parts[0].strip())
              except (ValueError, IndexError):
                pass # Si falla la conversión, no pasa nada
              attraction_data["place_name"] = parts[1].strip()
            else: # Si no, es solo el nombre
              attraction_data["place_name"] = name_text

          # Extraer puntuación (rating)
          rating_div = card.xpath('.//div[contains(@class, "MyMKp")]//div[contains(@class, "biGQs") and contains(@class, "_P") and contains(@class, "hmDzD")]')
          if rating_div:
            rating_text = rating_div.xpath('text()').get()
            if rating_text and '.' in rating_text: # Aseguramos que sea un número decimal
              try:
                attraction_data["rating"] = float(rating_text.strip())
              except ValueError:
                pass # Ignoramos si no se puede convertir

          # Extraer número de reseñas
          # Intentamos dos selectores diferentes porque a veces cambia
          reviews_div = card.xpath('.//a[contains(@class, "BMQDV")]//div[@class="f Q2"]/div[contains(@class, "biGQs") and contains(@class, "_P") and contains(@class, "hmDzD")][last()]')
          if not reviews_div:
            reviews_div = card.xpath('.//div[contains(@class, "Q2")]//div[contains(@class, "biGQs") and contains(@class, "_P") and contains(@class, "hmDzD")][last()]')

          if reviews_div:
            reviews_text = reviews_div.xpath('text()').get()
            if reviews_text:
              # Limpiamos el texto (quitamos puntos y comas)
              cleaned_text = reviews_text.strip().replace('.', '').replace(',', '')
              if cleaned_text.isdigit(): # Comprobamos si es un número
                try:
                  attraction_data["reviews_count"] = int(cleaned_text)
                except ValueError:
                  pass # Ignoramos si falla

          # Extraer tipo de lugar (ej: "Monumentos y puntos de interés")
          type_section = card.xpath('.//div[contains(@class, "dxkoL")]')
          if type_section:
            type_div = type_section.xpath('.//div[contains(@class, "biGQs") and contains(@class, "_P") and contains(@class, "hmDzD")][1]')
            if type_div:
              type_text = type_div.xpath('text()').get()
              # Comprobamos que no sea un número o tenga puntos
              if type_text and not any(c.isdigit() for c in type_text) and '.' not in type_text:
                attraction_data["place_type"] = type_text.strip()

          attractions.append(attraction_data) # Añadimos la atracción a la lista

        except Exception as e:
          # Si falla algo extrayendo una atracción, logueamos y seguimos
          log.warning(f"Error extrayendo datos de atracción: {e}")
          continue # Pasamos a la siguiente tarjeta

      return attractions # Devolvemos la lista de atracciones de esta página

    except Exception as e:
      # Si falla toda la página, logueamos el error y lanzamos la excepción
      log.error(f"Error en scrape_page: {e}")
      raise

  async def get_next_page_url(self, response_text: str) -> Optional[str]:
    """Busca el enlace a la siguiente página de resultados"""
    selector = Selector(text=response_text)
    # Usamos un selector CSS para encontrar el botón de 'siguiente'
    next_link = selector.css('a.BrOJk[data-smoke-attr="pagination-next-arrow"]::attr(href)').get()
    # Si existe, construimos la URL completa
    return f"{BASE_URL}{next_link}" if next_link else None

  async def get_all_attractions(self, region_url: str) -> List[Dict]:
    """Obtiene todas las atracciones de una región, página por página"""
    all_attractions = [] # Lista para todas las atracciones
    page_count = 1
    current_url = region_url # Empezamos por la URL inicial

    while current_url: # Mientras haya una URL a la que ir
      log.info(f"Scrapeando página {page_count}: {current_url}")
      response = await self.client.get(current_url) # Pedimos la página
      # Scrapeamos las atracciones de la página actual
      page_attractions = await self.scrape_page(current_url)
      all_attractions.extend(page_attractions) # Añadimos las encontradas
      log.info(f"Encontradas {len(page_attractions)} atracciones en página {page_count}")

      # Buscamos la URL de la siguiente página
      next_url = await self.get_next_page_url(response.text)
      if not next_url:
        log.info("No hay más páginas de atracciones")
        break # Si no hay, salimos del bucle

      current_url = next_url # Actualizamos la URL
      page_count += 1
      await smart_sleep(page_count) # Esperamos un poco

    log.success(f"Scraping de atracciones completado Total: {len(all_attractions)}")
    return all_attractions


class ReviewScraper:
  """Scraper para sacar reseñas
  - Paginación
  - Idioma inglés
  - Reintentos
  - Usa el ReviewParser
  """

  def __init__(self, max_retries: int = 3, max_concurrency: int = 3):
    self.client = None # Cliente http
    self.max_retries = max_retries # Máximos reintentos
    self.max_concurrency = max_concurrency # Peticiones simultáneas
    self.parser = ReviewParser() # El parser de reseñas
    self.metrics = ReviewMetricsCalculator() # Para calcular métricas

  async def __aenter__(self):
    # Configuramos el cliente http al entrar
    self.client = httpx.AsyncClient(
      headers=get_headers(), # Headers actualizados
      follow_redirects=True, # Sigue redirecciones
      timeout=httpx.Timeout(30.0), # Timeout
      limits=httpx.Limits(max_connections=self.max_concurrency) # Límite de conexiones
    )
    return self

  async def __aexit__(self, exc_type, exc_val, exc_tb):
    # Cerramos el cliente al salir
    if self.client:
      await self.client.aclose()

  async def scrape_reviews(self, attraction: Attraction, max_pages: Optional[int] = None) -> Dict:
    """Scrapea las reseñas de una atracción específica"""
    log.info(f"Iniciando scraping de reseñas para: {attraction.place_name}")
    # Aseguramos URL en inglés
    english_url = self._ensure_english_view(attraction.url)
    # Obtenemos métricas (total, inglés)
    metrics = await self._get_review_metrics(english_url, attraction.reviews_count)
    log.debug(f"{attraction.place_name} - Métricas: {metrics}")

    # Si no hay reseñas en inglés, paramos
    if metrics['english_reviews'] == 0:
      log.warning(f"{attraction.place_name} - No hay reseñas en inglés")
      return self._build_empty_response(attraction, metrics)

    # Calculamos cuántas páginas scrapear
    total_pages_to_scrape = self._calculate_pages(metrics, max_pages)
    log.info(f"{attraction.place_name} - Scrapeando {total_pages_to_scrape} páginas")

    # Scrapeamos las páginas
    reviews = await self._scrape_paginated_reviews(
      base_url=english_url,
      attraction_name=attraction.place_name,
      total_pages=total_pages_to_scrape
    )

    log.success(f"{attraction.place_name} - Scraping finalizado {len(reviews)} reseñas")
    # Construimos la respuesta final
    return self._build_success_response(attraction, metrics, reviews)

  async def scrape_multiple_attractions(self, attractions: List[Attraction]) -> List[Dict]:
    """Scrapea reseñas para una lista de atracciones en paralelo"""
    # Semáforo para limitar concurrencia
    semaphore = asyncio.Semaphore(self.max_concurrency)

    # Función helper para procesar una atracción
    async def process_attraction(attraction_dict):
      # Convertimos a objeto Attraction si hace falta
      if isinstance(attraction_dict, dict):
          attraction_obj = Attraction(**attraction_dict)
      else:
          attraction_obj = attraction_dict

      async with semaphore: # Adquirimos semáforo
        log.info(f"Procesando atracción: {attraction_obj.place_name}")
        try:
          # Llamamos al método principal
          result = await self.scrape_reviews(attraction_obj)
          return result
        except Exception as e:
          log.error(f"Error procesando {attraction_obj.place_name}: {e}")
          # Devolvemos un fallo si algo va mal
          return {
              **attraction_obj.__dict__,
              "reviews": [],
              "scrape_status": "failed",
              "error": str(e)
          }

    # Lanzamos tareas en paralelo
    tasks = [process_attraction(a) for a in attractions]
    results = await asyncio.gather(*tasks) # Esperamos a que terminen
    log.info(f"Procesadas {len(results)} atracciones")
    return results # Devolvemos resultados

  # -------------------- Métodos internos (privados) --------------------

  async def _scrape_paginated_reviews(self, base_url: str, attraction_name: str, total_pages: int) -> List[Dict]:
    """Maneja la paginación, reintentos y delays"""
    all_reviews = [] # Lista para todas las reseñas
    seen_reviews = set() # Para evitar duplicados

    for page in range(total_pages): # Iteramos por las páginas
      current_page_num = page + 1 # Número de página real
      # Construimos la URL de la página
      page_url = self._build_page_url(base_url, current_page_num)
      log.debug(f"{attraction_name} - Scrapeando página {current_page_num}/{total_pages}")

      # Intentamos obtener reseñas con reintentos
      for attempt in range(1, self.max_retries + 1):
        try:
          # Scrapeamos la página
          reviews_on_page = await self._scrape_single_page(page_url)

          # Procesamos las reseñas
          new_reviews_count = 0
          if reviews_on_page:
            for review in reviews_on_page:
              # Hash para detectar duplicados
              review_hash = self._generate_review_hash(review)
              if review_hash not in seen_reviews: # Si no la hemos visto
                seen_reviews.add(review_hash) # La marcamos
                all_reviews.append(review) # La añadimos
                new_reviews_count += 1

            log.success(f"{attraction_name} - Página {current_page_num}: {len(reviews_on_page)} encontradas ({new_reviews_count} nuevas)")
            # Esperamos antes de la siguiente
            await smart_sleep(current_page_num)
            break # Salimos del bucle de reintentos

          else: # Si no se encontraron reseñas
              log.warning(f"{attraction_name} - Página {current_page_num}: No se encontraron reseñas (intento {attempt})")
              if attempt < self.max_retries:
                  await self._exponential_backoff(attempt) # Esperamos
              else:
                  log.error(f"{attraction_name} - Página {current_page_num}: Fallo tras {self.max_retries} intentos")
                  break # Rompemos

        except httpx.ReadTimeout:
            log.warning(f"{attraction_name} - Página {current_page_num}: Timeout (intento {attempt})")
            if attempt == self.max_retries:
                log.error(f"{attraction_name} - Página {current_page_num}: Timeout final")
                break # Salimos
            await self._exponential_backoff(attempt) # Esperamos

        except Exception as e:
          # Otro error
          log.error(f"{attraction_name} - Error pág {current_page_num} (intento {attempt}): {str(e)}")
          if attempt == self.max_retries:
            log.error(f"{attraction_name} - Página {current_page_num}: Fallo final")
            break # Salimos
          # Esperamos (backoff)
          await self._exponential_backoff(attempt)

    return all_reviews # Devolvemos todas las reseñas únicas

  def _generate_review_hash(self, review: Dict) -> int:
    """Genera un hash para identificar una reseña"""
    # Usamos campos clave
    key_fields = (
      review.get('username', ''),
      review.get('title', ''),
      review.get('written_date', ''),
      str(review.get('rating', 0)) # Rating a string
    )
    return hash(key_fields) # Devolvemos el hash

  async def _scrape_single_page(self, url: str) -> List[Dict]:
    """Obtiene y parsea una única página de reseñas"""
    log.debug(f"Haciendo GET a: {url}")
    response = await self.client.get(url, headers=get_headers(referer=url)) # Petición GET
    response.raise_for_status() # Error si 4xx o 5xx

    log.debug(f"Parseando respuesta de: {url}")
    # Usamos el parser
    reviews = self.parser.parse_reviews_page(response.text, url)
    # Validamos
    valid_reviews = [r for r in reviews if self.parser.validate_review(r)]
    log.debug(f"Encontradas {len(reviews)} reseñas, {len(valid_reviews)} válidas en {url}")
    return valid_reviews

  async def _get_review_metrics(self, url: str, default_count: int) -> Dict:
    """Obtiene el número total y en inglés de reseñas"""
    try:
      log.debug(f"Obteniendo métricas de: {url}")
      response = await self.client.get(url, headers=get_headers(referer=url)) # GET
      response.raise_for_status()
      selector = Selector(response.text) # Selector

      # Extraemos total y en inglés
      total = self.parser.extract_total_reviews(selector) or default_count
      english = self.parser.extract_english_reviews(selector)

      # Calculamos páginas en inglés (10 por pág)
      english_pages = (english + 9) // 10 if english is not None and english > 0 else 0

      log.debug(f"Métricas: Total={total}, English={english}, Pages={english_pages}")
      return {
        "total_reviews": total,
        "english_reviews": english if english is not None else 0, # Aseguramos 0 si None
        "english_pages": english_pages
      }
    except Exception as e:
      log.error(f"Error obteniendo métricas de {url}: {e}")
      # Valores por defecto si falla
      return {
          "total_reviews": default_count,
          "english_reviews": 0,
          "english_pages": 0
      }


  # -------------------- Helpers (funciones de ayuda) --------------------

  def _ensure_english_view(self, url: str) -> str:
    """Asegura que la URL tenga el filtro de inglés"""
    if "filterLang=en" not in url: # Si no lo tiene
      # Lo añadimos
      separator = "&" if "?" in url else "?"
      new_url = f"{url}{separator}filterLang=en"
      log.debug(f"Añadiendo filtro inglés: {url} -> {new_url}")
      return new_url
    return url # Si ya lo tiene, la devolvemos

  def _build_page_url(self, base_url: str, page: int) -> str:
    """
    Construye la URL para una página específica de reseñas de TripAdvisor,
    insertando '-or<offset>-' para páginas > 1
    Versión simplificada basada en el patrón observado
    """
    # Quitamos lo que venga después del ?
    base_url_path = base_url.split('?')[0]

    offset = (page - 1) * 10
    # Partimos la URL justo después de "-Reviews-"
    parts = base_url_path.split("-Reviews-")
    
    # Armamos la URL nueva metiendo el '-or<offset>-' en medio
    final_url_path = f"{parts[0]}-Reviews-or{offset}-{parts[1]}"
    log.debug(f"Construyendo URL para página {page} (offset {offset}): {final_url_path}")
    return final_url_path
    
  def _calculate_pages(self, metrics: Dict, max_pages: Optional[int]) -> int:
    """Decide cuántas páginas scrapear"""
    # Páginas en inglés disponibles
    available_pages = metrics.get('english_pages', 0)
    if max_pages is not None and max_pages > 0: # Si hay límite
      # El mínimo entre disponibles y límite
      pages_to_scrape = min(available_pages, max_pages)
      log.debug(f"Límite {max_pages} aplicado Scrapeando {pages_to_scrape}/{available_pages}")
      return pages_to_scrape
    else: # Sin límite, todas las disponibles
      log.debug(f"Sin límite Scrapeando {available_pages} páginas")
      return available_pages

  def _build_empty_response(self, attraction: Attraction, metrics: Dict) -> Dict:
    """Crea respuesta cuando no hay reseñas en inglés"""
    log.info(f"Creando respuesta vacía para {attraction.place_name}")
    # Datos de la atracción + métricas + estado
    return {
      **attraction.__dict__, # Atributos de Attraction
      "total_reviews": metrics.get('total_reviews', attraction.reviews_count),
      "english_reviews": 0, # Es 0
      "reviews": [], # Lista vacía
      "scrape_status": "no_english_reviews" # Estado
    }

  def _build_success_response(self, attraction: Attraction, metrics: Dict, reviews: List) -> Dict:
    """Crea respuesta cuando el scraping fue (más o menos) bien"""
    log.info(f"Creando respuesta para {attraction.place_name} con {len(reviews)} reseñas")
    # Estado basado en si obtuvimos reseñas
    status = "completed" if reviews else "failed_no_reviews_found"

    # Combinamos todo
    return {
      **attraction.__dict__,
      "place_name": attraction.place_name,
      "place_type": attraction.place_type,
      "rating": attraction.rating,
      "url": attraction.url,
      # Métricas obtenidas
      "total_reviews": metrics.get('total_reviews', attraction.reviews_count),
      "english_reviews": metrics.get('english_reviews', 0),
      "reviews": reviews, # Las reseñas
      "scrape_status": status
    }

  async def _exponential_backoff(self, attempt: int):
    """Espera un tiempo creciente antes de reintentar"""
    # Delay: 2^intento, máx 60s
    delay = min(2 ** attempt, 60)
    # Añadimos aleatoriedad
    wait_time = delay + random.random()
    log.warning(f"Intento {attempt} fallido Esperando {wait_time:.2f} seg...")
    await asyncio.sleep(wait_time) # Pausa