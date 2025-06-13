import asyncio
import json
import os
import random
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import httpx
from loguru import logger as log
from parsel import Selector

from .metrics import ReviewMetricsCalculator
from .parsers import ReviewParser, ReviewParserConfig
from ..utils import get_headers, smart_sleep, HEADERS, BASE_URL


class AttractionScraper:
  """Extrae URLs y datos básicos de atracciones de TripAdvisor"""

  def __init__(self):
    self.client = None

  async def __aenter__(self):
    self.client = httpx.AsyncClient(headers=HEADERS, follow_redirects=True)
    return self

  async def __aexit__(self, exc_type, exc_val, exc_tb):
    if self.client:
      await self.client.aclose()

  async def get_page_html(self, url: str) -> Optional[str]:
    """Obtiene HTML de una página"""
    try:
      response = await self.client.get(url)
      response.raise_for_status()
      return response.text
    except httpx.HTTPStatusError as e:
      log.error(f"HTTP {e.response.status_code} en {url}")
    except httpx.RequestError as e:
      log.error(f"Error de red en {url}: {e}")
    except Exception:
      log.error(f"Error obteniendo HTML de {url}")
    return None

  async def scrape_page(self, url: str) -> List[Dict]:
    """Scrapea una página de atracciones con selectores corregidos"""
    html_content = await self.get_page_html(url)
    if not html_content:
      log.error(f"Sin HTML para {url}")
      return []
    
    try:
      selector = Selector(html_content)
      attractions = []
      
      # ✅ CORREGIDO: Selector específico para artículos
      cards = selector.xpath('//article[contains(@class, "GTuVU")]')
      
      if not cards:
        log.warning(f"No se encontraron tarjetas de atracciones en {url}")
        return []
      
      log.debug(f"Encontradas {len(cards)} tarjetas en {url}")
      
      for idx, card in enumerate(cards):
        try:
          attraction_data = {
            "position": None,
            "attraction_name": "Lugar Desconocido",
            "place_type": "Sin Categoría", 
            "rating": 0.0,
            "reviews_count": 0,
            "url": "",
          }
          
          # ✅ CORREGIDO: URL (líneas 11 y 27 del HTML)
          href = card.xpath('.//a[contains(@href, "/Attraction_Review-")]/@href').get()
          if not href:
            log.debug(f"Tarjeta {idx+1}: Sin URL válida")
            continue
            
          clean_href = href.split('#')[0].split('?')[0]
          attraction_data["url"] = f"https://www.tripadvisor.com{clean_href}" if not clean_href.startswith('http') else clean_href
          
          # ✅ CORREGIDO: Nombre y posición (línea 32)
          name_element = card.xpath('.//div[contains(@class, "XfVdV") and contains(@class, "AIbhI")]')
          if name_element:
            full_text = name_element.xpath('string(.)').get("").strip()
            log.debug(f"Texto completo extraído: '{full_text}'")
            
            if full_text:
              # Separar número de posición del nombre
              if '.' in full_text:
                parts = full_text.split('.', 1)
                try:
                  position_text = parts[0].strip()
                  if position_text.isdigit():
                    attraction_data["position"] = int(position_text)
                    attraction_data["attraction_name"] = parts[1].strip() if len(parts) > 1 else full_text
                  else:
                    attraction_data["attraction_name"] = full_text
                except (ValueError, IndexError):
                  attraction_data["attraction_name"] = full_text
              else:
                attraction_data["attraction_name"] = full_text
          
          # ✅ CORREGIDO: Rating (línea 41)
          rating_element = card.xpath('.//div[@data-automation="bubbleRatingValue"]')
          if rating_element:
            rating_text = rating_element.xpath('text()').get()
            if rating_text:
              try:
                rating_value = float(rating_text.strip())
                if 0 <= rating_value <= 5:
                  attraction_data["rating"] = rating_value
                  log.debug(f"Rating extraído: {rating_value}")
              except ValueError:
                log.debug(f"Error parsing rating: {rating_text}")
          
          # ✅ CORREGIDO: Número de reseñas (línea 48)
          reviews_element = card.xpath('.//div[@data-automation="bubbleLabel"]')
          if reviews_element:
            reviews_text = reviews_element.xpath('text()').get()
            if reviews_text:
              # Limpiar texto y extraer número
              cleaned_text = reviews_text.strip().replace(',', '').replace('.', '')
              log.debug(f"Texto de reseñas extraído: '{reviews_text}' -> '{cleaned_text}'")
              
              try:
                reviews_count = int(cleaned_text)
                if reviews_count >= 0:
                  attraction_data["reviews_count"] = reviews_count
                  log.debug(f"Reseñas extraídas: {reviews_count}")
              except ValueError:
                log.debug(f"Error parsing reviews count: {reviews_text}")
          
          # ✅ CORREGIDO: Tipo de lugar (línea 62)
          type_element = card.xpath('.//div[contains(@class, "dxkoL")]//div[contains(@class, "biGQs") and contains(@class, "hmDzD")]')
          if type_element:
            type_text = type_element.xpath('text()').get()
            if type_text and type_text.strip():
              # Limpiar HTML entities
              clean_type = type_text.strip().replace('&amp;', '&')
              # Validar que no sea rating o número
              if not any(c.isdigit() for c in clean_type) and '.' not in clean_type:
                attraction_data["place_type"] = clean_type
                log.debug(f"Tipo extraído: {clean_type}")
          
          # ✅ VALIDACIÓN: Solo agregar si tiene datos básicos válidos
          if (attraction_data["url"] and 
              attraction_data["attraction_name"] != "Lugar Desconocido" and
              len(attraction_data["attraction_name"]) > 2):
            
            attractions.append(attraction_data)
            log.info(f"✅ Atracción extraída: {attraction_data['attraction_name']} "
                     f"(pos: {attraction_data['position']}, rating: {attraction_data['rating']}, "
                     f"reseñas: {attraction_data['reviews_count']}, tipo: {attraction_data['place_type']})")
          else:
            log.warning(f"❌ Tarjeta {idx+1}: Datos insuficientes - {attraction_data}")
          
        except Exception as e:
          log.error(f"Error extrayendo tarjeta {idx+1}: {e}")
          continue
      
      log.info(f"Extraídas {len(attractions)} atracciones válidas de {len(cards)} tarjetas")
      return attractions
      
    except Exception as e:
      log.error(f"Error scrapeando {url}: {e}")
      return []

  async def get_next_page_url(self, response_text: str) -> Optional[str]:
    """Obtiene URL de siguiente página"""
    selector = Selector(text=response_text)
    next_link = selector.css('a.BrOJk[data-smoke-attr="pagination-next-arrow"]::attr(href)').get()
    return f"{BASE_URL}{next_link}" if next_link else None

  async def get_all_attractions(self, region_url: str) -> List[Dict]:
    """Obtiene todas las atracciones de una región"""
    all_attractions_list = []
    page_count = 1
    current_url = region_url
    
    while current_url:
      log.info(f"Scrapeando página {page_count}")
      html_content = await self.get_page_html(current_url)
      
      if not html_content:
        log.error(f"Sin HTML para página {page_count}")
        break
      
      page_attractions_list = await self.scrape_page(current_url)
      all_attractions_list.extend(page_attractions_list)
      log.info(f"{len(page_attractions_list)} atracciones en página {page_count}")
      
      next_url = await self.get_next_page_url(html_content)
      if not next_url:
        log.info("No hay más páginas")
        break
      
      current_url = next_url
      page_count += 1
      await smart_sleep(page_count)
    
    log.info(f"Total atracciones: {len(all_attractions_list)}")
    return all_attractions_list


# Lock global para guardado de JSON
JSON_SAVE_LOCK = threading.Lock()

class ReviewScraper:
  """Scrapea reseñas de atracciones con concurrencia"""
  
  REVIEWS_PER_PAGE = 10

  def __init__(self,
               max_retries: int = 3,
               max_concurrency: int = 3,
               json_output_filepath: Optional[str] = None,
               stop_event: Optional[asyncio.Event] = None,
               inter_attraction_base_delay: float = 10.0,
               target_language: str = "english"):
    self.client = None
    self.max_retries = max_retries
    self.max_concurrency = max(1, min(3, max_concurrency))
    self.parser = ReviewParser()
    self.config = ReviewParserConfig()
    self.problematic_urls: List[str] = []
    self.target_language = target_language
  
    # Semáforo para controlar concurrencia
    self.concurrency_semaphore = asyncio.Semaphore(self.max_concurrency)
  
    self.json_output_filepath = json_output_filepath
    self.stop_event = stop_event if stop_event is not None else asyncio.Event()
    self.inter_attraction_base_delay = inter_attraction_base_delay

  async def __aenter__(self):
    """Context manager entrada"""
    self.client = httpx.AsyncClient(
      headers=get_headers(),
      follow_redirects=True,
      timeout=httpx.Timeout(30.0),
      limits=httpx.Limits(max_connections=self.max_concurrency * 2, max_keepalive_connections=self.max_concurrency)
    )
    return self

  async def __aexit__(self, exc_type, exc_val, exc_tb):
    """Context manager salida"""
    if self.client:
      await self.client.aclose()

  # Método principal para múltiples atracciones
  async def scrape_multiple_attractions(self, 
                                       attractions_data_list: List[Dict], 
                                       region_name: str,
                                       target_language: str = "english",
                                       attraction_callback=None,
                                       stop_event=None) -> List[Dict]:
    """Scrapea múltiples atracciones con clasificación por prioridad y soporte multilenguaje"""
    
    if stop_event:
      self.stop_event = stop_event
    
    # Usar idioma especificado
    self.target_language = target_language
    
    DEFER_THRESHOLD = 10
    log.info(f"Clasificando {len(attractions_data_list)} atracciones en {region_name} para idioma: {self.target_language}")
    
    # Listas de prioridad
    p1_newly_scraped: List[Dict] = []
    p2_many_missing: List[Dict] = []
    p3_few_missing: List[Dict] = []
    p4_up_to_date: List[Dict] = []
    p5_zero_zero_scraped: List[Dict] = []
  
    # Clasificación por prioridad con estructura multilenguaje
    for att_data in attractions_data_list:
      attraction_name_for_log = att_data.get("attraction_name", "Atracción Desconocida")
  
      if self.stop_event.is_set():
        log.info(f"Clasificación detenida para {attraction_name_for_log}")
        continue
  
      if not att_data.get("url"):
        log.warning(f"Sin URL para {attraction_name_for_log}")
        continue
      
      # ✅ NUEVO: Ignorar atracciones sin reseñas y marcarlas como previamente scrapeadas
      if att_data.get("reviews_count", 0) == 0:
        log.debug(f"Sin reseñas: {attraction_name_for_log}")
        att_data["previously_scraped"] = True
        continue
      
      # ✅ NUEVO: Obtener datos específicos del idioma objetivo
      language_data = att_data.get("languages", {}).get(self.target_language, {})
      current_scraped_reviews = len(language_data.get("reviews", []))
      stored_language_count = language_data.get("reviews_count", 0)
      
      att_data_for_priority = att_data.copy()
      
      # Caso inconsistencia: más scrapeadas que las esperadas
      if stored_language_count < current_scraped_reviews:
        log.warning(f"Inconsistencia en {attraction_name_for_log}: corrigiendo conteo")
        if "languages" not in att_data_for_priority:
          att_data_for_priority["languages"] = {}
        if self.target_language not in att_data_for_priority["languages"]:
          att_data_for_priority["languages"][self.target_language] = {}
        
        att_data_for_priority["languages"][self.target_language].update({
          "reviews_count": current_scraped_reviews,
          "previously_scraped": True
        })
  
      effective_language_count = att_data_for_priority.get("languages", {}).get(self.target_language, {}).get("reviews_count", 0)
      is_previously_scraped = att_data_for_priority.get("languages", {}).get(self.target_language, {}).get("previously_scraped", False)
  
      # Asignar prioridad
      if not is_previously_scraped:
        log.debug(f"P1 nueva: {attraction_name_for_log}")
        p1_newly_scraped.append(att_data_for_priority)
      else:
        if effective_language_count == 0 and current_scraped_reviews == 0:
          log.debug(f"P5 cero/cero: {attraction_name_for_log}")
          p5_zero_zero_scraped.append(att_data_for_priority)
        else:
          missing_reviews = effective_language_count - current_scraped_reviews
          if missing_reviews > DEFER_THRESHOLD:
            log.debug(f"P2 muchas faltantes: {attraction_name_for_log}")
            p2_many_missing.append(att_data_for_priority)
          elif 0 < missing_reviews <= DEFER_THRESHOLD:
            log.debug(f"P3 pocas faltantes: {attraction_name_for_log}")
            p3_few_missing.append(att_data_for_priority)
          else:
            log.debug(f"P4 actualizada: {attraction_name_for_log}")
            p4_up_to_date.append(att_data_for_priority)
  
    # Combinar listas por prioridad
    ordered_attractions_to_scrape = (
      p1_newly_scraped + 
      p2_many_missing + 
      p3_few_missing + 
      p4_up_to_date + 
      p5_zero_zero_scraped
    )
  
    log.info(f"Clasificación completa para {self.target_language}: P1={len(p1_newly_scraped)} P2={len(p2_many_missing)} "
             f"P3={len(p3_few_missing)} P4={len(p4_up_to_date)} P5={len(p5_zero_zero_scraped)}")
  
    return await self._process_attractions_concurrently(
      ordered_attractions_to_scrape, 
      region_name, 
      attraction_callback
    )

  async def _process_attractions_concurrently(self, 
                                            attractions_list: List[Dict], 
                                            region_name: str, 
                                            attraction_callback=None) -> List[Dict]:
    """Procesa atracciones de forma concurrente"""
    
    all_results = []
    completed_count = 0
    total_attractions = len(attractions_list)
    
    async def process_single_attraction(idx: int, att_data_item: Dict) -> Optional[Dict]:
      """Procesa una atracción con control de concurrencia"""
      nonlocal completed_count
      
      if self.stop_event.is_set():
        return None
        
      attraction_name = att_data_item.get("attraction_name", "Atracción Desconocida")
      
      async with self.concurrency_semaphore:
        if self.stop_event.is_set():
          return None
          
        try:
          log.info(f"[{idx+1}/{total_attractions}] Iniciando: {attraction_name}")
          
          result = await self.scrape_reviews(att_data_item, region_name)
          
          if result and isinstance(result, dict):
            newly_scraped_count = len(result.get("newly_scraped_reviews", []))
            scrape_status = result.get("scrape_status", "unknown")
            
            if attraction_callback:
              attraction_callback(idx, attraction_name, newly_scraped_count, scrape_status)
            
            completed_count += 1
            log.info(f"[{completed_count}/{total_attractions}] Completado: {attraction_name} ({newly_scraped_count} nuevas)")
            return result
          
        except Exception as e:
          log.error(f"Error procesando {attraction_name}: {e}")
          error_result = self._build_error_response(att_data_item, "error_processing", str(e))
          
          if attraction_callback:
            attraction_callback(idx, attraction_name, 0, "error_processing")
          
          completed_count += 1
          return error_result
        
        # Delay entre atracciones
        if not self.stop_event.is_set():
          await asyncio.sleep(random.uniform(0.3, 0.7))
      
      return None

    # Crear tareas concurrentes
    tasks = []
    for idx, att_data_item in enumerate(attractions_list):
      if self.stop_event.is_set():
        break
      task = asyncio.create_task(process_single_attraction(idx, att_data_item))
      tasks.append(task)
    
    # Esperar tareas
    if tasks:
      log.info(f"Ejecutando {len(tasks)} tareas con concurrencia {self.max_concurrency}")
      results = await asyncio.gather(*tasks, return_exceptions=True)
      
      for result in results:
        if isinstance(result, dict):
          all_results.append(result)
        elif isinstance(result, Exception):
          log.error(f"Excepción en tarea: {result}")
    
    log.info(f"Procesamiento concurrente completado: {len(all_results)} resultados")
    return all_results

  # Métodos principales
  async def scrape_reviews(self, attraction_data: Dict, region_name: str) -> Dict:
    """Scrapea reseñas para una atracción específica en el idioma objetivo"""
    attraction_name_val = attraction_data.get("attraction_name", "Atracción Desconocida")
    attraction_url = attraction_data.get("url")
    
    # ✅ CORREGIDO: Obtener datos específicos del idioma
    language_data = attraction_data.get("languages", {}).get(self.target_language, {})
    stored_language_count = language_data.get("reviews_count", 0)
    stored_reviews_from_json = language_data.get("reviews", [])
  
    log.debug(f"Scraping reseñas en {self.target_language}: {attraction_name_val}")
  
    if not attraction_url:
      log.error(f"Sin URL: {attraction_name_val}")
      return self._build_error_response(attraction_data, "missing_url", "URL no disponible")
  
    # ✅ NUEVO: Generar URL para el idioma específico
    language_url = ReviewMetricsCalculator.generate_language_url(attraction_url, self.target_language)
    
    # ✅ NUEVO: Obtener métricas específicas del idioma
    site_metrics = await self._get_review_metrics_for_language(language_url)
    current_site_language_reviews = site_metrics.get("total_reviews", 0)
    is_correct_language_view = site_metrics.get("is_correct_language_view", False)
    
    log.debug(f"Métricas sitio {attraction_name_val} ({self.target_language}): Total={current_site_language_reviews}, Vista correcta={is_correct_language_view}")
  
    # ✅ NUEVO: Validar que estamos en la vista del idioma correcto
    if not is_correct_language_view:
      log.warning(f"Vista de idioma incorrecta para {attraction_name_val}: esperado {self.target_language}")
      return self._build_error_response(attraction_data, "incorrect_language_view", f"Vista de idioma incorrecta: esperado {self.target_language}")
  
    if self.stop_event.is_set():
      log.info(f"Detenido por usuario: {attraction_name_val}")
      return self._build_error_response(attraction_data, "stopped_by_user_before_processing", "detenido")
  
    # Sin reseñas en el idioma objetivo
    if current_site_language_reviews == 0:
      log.debug(f"Sin reseñas en {self.target_language}: {attraction_name_val}")
      if self.json_output_filepath:
        await self._save_reviews_to_json_incrementally_internal(
          region_name_to_update=region_name,
          attraction_url=attraction_url,
          new_reviews_data=[],
          site_language_count=0,
          language_code=self.target_language,
          attraction_name_if_new=attraction_name_val
        )
      return {
        "attraction_name": attraction_name_val,
        "url": attraction_url,
        "newly_scraped_reviews": [],
        "current_site_language_reviews_count": 0,
        "language": self.target_language,
        "scrape_status": f"no_{self.target_language}_reviews_on_site"
      }
  
    # ✅ NUEVO: Generar hashes únicos globales para evitar duplicados entre idiomas
    all_existing_review_ids = set()
    all_existing_reviews = []
    for lang, lang_data in attraction_data.get("languages", {}).items():
        for review in lang_data.get("reviews", []):
            if isinstance(review, dict):
                all_existing_reviews.append(review)
                review_id = review.get("review_id", "").strip()
                if review_id and review_id != "":
                    all_existing_review_ids.add(review_id)
    
    processed_review_hashes: Set[int] = {self._generate_review_hash(r) for r in all_existing_reviews if isinstance(r, dict)}

    # ✅ NUEVO: Sets para tracking de la sesión completa de scraping
    session_processed_review_ids = set(all_existing_review_ids)  # Empezar con los existentes
    session_processed_hashes = set(processed_review_hashes)
    
    # Ya está actualizada
    if current_site_language_reviews == stored_language_count and len(stored_reviews_from_json) >= current_site_language_reviews:
      log.debug(f"Ya actualizada: {attraction_name_val}")
      if self.json_output_filepath:
        await self._save_reviews_to_json_incrementally_internal(
          region_name_to_update=region_name,
          attraction_url=attraction_url,
          new_reviews_data=[],
          site_language_count=current_site_language_reviews,
          language_code=self.target_language,
          attraction_name_if_new=attraction_name_val
        )
      return {
        "attraction_name": attraction_name_val,
        "url": attraction_url,
        "newly_scraped_reviews": [],
        "current_site_language_reviews_count": current_site_language_reviews,
        "language": self.target_language,
        "scrape_status": "no_action_needed_up_to_date"
      }
  
    # Proceso de scraping en 3 fases
    all_reviews_scraped_this_run_accumulator: List[Dict] = []
    processed_pages_this_run: Set[int] = set()
    scrape_status_parts: List[str] = []
    
    initial_scraped_count = len(stored_reviews_from_json)
    # ✅ CORREGIDO: Usar datos del idioma específico
    initial_language_count = stored_language_count
    
    # FASE 1: Reseñas nuevas
    # ✅ CORREGIDO: Usar variables del idioma específico
    expected_new_reviews = current_site_language_reviews - initial_language_count
    if expected_new_reviews > 0:
      num_pages_for_new = (expected_new_reviews + self.REVIEWS_PER_PAGE - 1) // self.REVIEWS_PER_PAGE
      log.debug(f"Fase 1 {attraction_name_val}: {expected_new_reviews} nuevas esperadas")
      
      for page_num in range(1, num_pages_for_new + 1):
        if self.stop_event.is_set():
          scrape_status_parts.append("stopped_during_new_reviews")
          break
          
        # ✅ CORREGIDO: Usar URL del idioma específico
        page_url = self._build_page_url(language_url, page_num)
        reviews_on_page = await self._scrape_single_page_with_retries(page_url, attraction_name_val)
        processed_pages_this_run.add(page_num)
        
        if not reviews_on_page:
          break
          
        newly_found_on_this_page_list: List[Dict] = []
        for review in reviews_on_page:
            review_id = review.get("review_id", "").strip()
            is_duplicate = False
            
            if review_id and review_id != "":
                # ✅ Comparar contra todas las reseñas: JSON + sesión actual
                if review_id in session_processed_review_ids:
                    is_duplicate = True
                else:
                    session_processed_review_ids.add(review_id)
            else:
                # ✅ Fallback: comparar por hash
                review_hash = self._generate_review_hash(review)
                if review_hash in session_processed_hashes:
                    is_duplicate = True
                else:
                    session_processed_hashes.add(review_hash)
            
            if not is_duplicate:
                newly_found_on_this_page_list.append(review)
                all_reviews_scraped_this_run_accumulator.append(review)
        
        if newly_found_on_this_page_list and self.json_output_filepath:
          await self._save_reviews_to_json_incrementally_internal(
            region_name_to_update=region_name,
            attraction_url=attraction_url,
            new_reviews_data=newly_found_on_this_page_list,
            site_language_count=current_site_language_reviews,
            language_code=self.target_language,
            attraction_name_if_new=attraction_name_val
          )
        
        if len(reviews_on_page) < self.REVIEWS_PER_PAGE:
          break
          
        if page_num < num_pages_for_new:
          await smart_sleep(current_page=page_num, base_delay=random.uniform(0.3, 0.8))
      
      scrape_status_parts.append(f"phase1_completed_pages_{len([p for p in processed_pages_this_run if p <= num_pages_for_new])}")
    else:
      scrape_status_parts.append("no_new_reviews_expected")
    
    updated_scraped_count = initial_scraped_count + len(all_reviews_scraped_this_run_accumulator)
    # ✅ CORREGIDO: Usar variable del idioma específico
    updated_language_count = current_site_language_reviews
    
    # FASE 2: Reseñas históricas faltantes
    # ✅ CORREGIDO: Usar variables del idioma específico
    missing_historical = updated_language_count - updated_scraped_count
    if missing_historical > 0 and not self.stop_event.is_set():
      start_page_historical = (updated_scraped_count // self.REVIEWS_PER_PAGE) + 1
      total_pages_needed = (updated_language_count + self.REVIEWS_PER_PAGE - 1) // self.REVIEWS_PER_PAGE
      end_page_historical = total_pages_needed
      
      log.debug(f"Fase 2 {attraction_name_val}: {missing_historical} históricas faltantes")
      
      for page_num in range(start_page_historical, end_page_historical + 1):
        if self.stop_event.is_set():
          scrape_status_parts.append("stopped_during_historical_reviews")
          break
          
        # ✅ CORREGIDO: Usar URL del idioma específico
        page_url = self._build_page_url(language_url, page_num)
        reviews_on_page = await self._scrape_single_page_with_retries(page_url, attraction_name_val)
        processed_pages_this_run.add(page_num)
        
        if not reviews_on_page:
          break
        
        newly_found_on_this_page_list: List[Dict] = []
        for review in reviews_on_page:
          review_hash = self._generate_review_hash(review)
          if review_hash not in processed_review_hashes:
            newly_found_on_this_page_list.append(review)
            all_reviews_scraped_this_run_accumulator.append(review)
            processed_review_hashes.add(review_hash)
        
        if newly_found_on_this_page_list and self.json_output_filepath:
          await self._save_reviews_to_json_incrementally_internal(
            region_name_to_update=region_name,
            attraction_url=attraction_url,
            new_reviews_data=newly_found_on_this_page_list,
            site_language_count=current_site_language_reviews,
            language_code=self.target_language,
            attraction_name_if_new=attraction_name_val
          )
        
        if len(reviews_on_page) < self.REVIEWS_PER_PAGE:
          break
          
        if page_num < end_page_historical:
          await smart_sleep(current_page=page_num, base_delay=random.uniform(0.3, 0.8))
      
      historical_pages = len([p for p in processed_pages_this_run if p >= start_page_historical])
      scrape_status_parts.append(f"phase2_completed_pages_{historical_pages}")
    else:
      if missing_historical <= 0:
        scrape_status_parts.append("no_historical_reviews_missing")
      else:
        scrape_status_parts.append("phase2_skipped_due_to_stop")
    
    # FASE 3: Scraping de emergencia
    # ✅ CORREGIDO: Usar variables del idioma específico
    final_missing = current_site_language_reviews - len(processed_review_hashes)
    if final_missing > 0 and not self.stop_event.is_set():
      max_page_processed = max(processed_pages_this_run) if processed_pages_this_run else 0
      emergency_start = max_page_processed + 1
      estimated_total_pages = (current_site_language_reviews + self.REVIEWS_PER_PAGE - 1) // self.REVIEWS_PER_PAGE
      max_emergency_pages = min(15, estimated_total_pages - max_page_processed)
      
      log.warning(f"Fase 3 {attraction_name_val}: {final_missing} aún faltantes")
      
      emergency_found = 0
      consecutive_empty = 0
      
      for page_num in range(emergency_start, emergency_start + max_emergency_pages):
        if self.stop_event.is_set() or consecutive_empty >= 3:
          break
          
        # ✅ CORREGIDO: Usar URL del idioma específico
        page_url = self._build_page_url(language_url, page_num)
        reviews_on_page = await self._scrape_single_page_with_retries(page_url, attraction_name_val)
        processed_pages_this_run.add(page_num)
        
        if not reviews_on_page:
          consecutive_empty += 1
          continue
        else:
          consecutive_empty = 0
        
        newly_found_emergency: List[Dict] = []
        for review in reviews_on_page:
          review_hash = self._generate_review_hash(review)
          if review_hash not in processed_review_hashes:
            newly_found_emergency.append(review)
            all_reviews_scraped_this_run_accumulator.append(review)
            processed_review_hashes.add(review_hash)
            emergency_found += 1
        
        if newly_found_emergency and self.json_output_filepath:
          await self._save_reviews_to_json_incrementally_internal(
            region_name_to_update=region_name,
            attraction_url=attraction_url,
            new_reviews_data=newly_found_emergency,
            site_language_count=current_site_language_reviews,
            language_code=self.target_language,
            attraction_name_if_new=attraction_name_val
          )
        
        if len(processed_review_hashes) >= current_site_language_reviews:
          break
          
        await smart_sleep(current_page=page_num, base_delay=random.uniform(0.5, 1.2))
      
      if emergency_found > 0:
        scrape_status_parts.append(f"phase3_emergency_found_{emergency_found}")
      else:
        scrape_status_parts.append("phase3_emergency_no_results")
    else:
      if final_missing <= 0:
        scrape_status_parts.append("no_emergency_needed")
      else:
        scrape_status_parts.append("phase3_skipped_due_to_stop")
    
    # Determinar status final
    final_status_str = "_".join(s for s in scrape_status_parts if s) or "unknown_state"
    
    if self.stop_event.is_set():
      final_status_str = "stopped" + ("_with_data" if all_reviews_scraped_this_run_accumulator else "")
    elif not all_reviews_scraped_this_run_accumulator:
      if len(processed_review_hashes) >= current_site_language_reviews:
        final_status_str = "completed_up_to_date"
      else:
        final_status_str = "completed_no_new_found"
    else:
      is_fully_complete = len(processed_review_hashes) >= current_site_language_reviews
      final_status_str = "completed_found_reviews" + ("_fully_updated" if is_fully_complete else "_partially_incomplete")
    
    # Guardar metadatos finales
    if self.json_output_filepath and not self.stop_event.is_set():
      await self._save_reviews_to_json_incrementally_internal(
        region_name_to_update=region_name,
        attraction_url=attraction_url,
        new_reviews_data=[],
        site_language_count=current_site_language_reviews,
        language_code=self.target_language,
        attraction_name_if_new=attraction_name_val
      )
    
    log.debug(f"Finalizado {attraction_name_val}: {final_status_str} nuevas={len(all_reviews_scraped_this_run_accumulator)}")
    
    return {
      "attraction_name": attraction_name_val,
      "url": attraction_url,
      "newly_scraped_reviews": all_reviews_scraped_this_run_accumulator,
      "current_site_language_reviews_count": current_site_language_reviews,
      "language": self.target_language,  # ✅ AÑADIDO
      "final_scraped_count_in_json": len(processed_review_hashes),
      "scrape_status": final_status_str
    }
 
  # Métodos de apoyo
  async def _scrape_single_page_with_retries(self, url: str, attraction_name: str) -> List[Dict]:
    """Scrapea una página (sin reintentos según nuevos requerimientos)"""
    if self.stop_event.is_set():
      log.info(f"Scraping cancelado para {attraction_name}")
      return []
      
    try:
      log.debug(f"Scrapeando página {url}")
      response = await self.client.get(url, headers=get_headers(referer=url))
      response.raise_for_status()
      parsed_reviews = self.parser.parse_reviews_page(response.text, url)
      return parsed_reviews
      
    except httpx.HTTPStatusError as e_http:
      log.warning(f"HTTP {e_http.response.status_code} en {url}")
      if e_http.response.status_code == 403:
        log.error(f"Error 403 en {url} - BLOQUEO DETECTADO")
        self.stop_event.set()
      return []
      
    except Exception as e:
      log.error(f"Error scrapeando {url}: {e}")
      return []
  
  def _generate_review_hash(self, review: Dict) -> int:
    """Genera hash único para una reseña"""
    review_id_from_parser = review.get("review_id")
    if review_id_from_parser:
      return hash(str(review_id_from_parser))
    
    key_fields = (
      review.get('username', '').strip().lower(),
      review.get('title', '').strip().lower(),
      review.get('written_date', ''),
    )
    return hash(key_fields)

  async def _get_review_metrics_for_language(self, url: str) -> Dict:
    """Obtiene métricas de reseñas para un idioma específico con reintentos"""
    try:
      log.debug(f"Obteniendo métricas para {self.target_language}: {url}")
      response = await self.client.get(url, headers=get_headers(referer=url))
      response.raise_for_status()
      selector = Selector(response.text)
      
      # Usar ReviewMetricsCalculator mejorado
      metrics = ReviewMetricsCalculator.get_review_metrics_for_language(selector, self.target_language)
      
      # ✅ Validación adicional basada en A.JSON
      if metrics["source"] == "none" or metrics["total_reviews"] == 0:
        log.warning(f"No se encontraron métricas válidas para {self.target_language}")
        
        # Intentar generar URL correcta si no estamos en la vista adecuada
        if not metrics["is_correct_language_view"]:
          corrected_url = ReviewMetricsCalculator.generate_language_url(url.split('?')[0], self.target_language)
          if corrected_url != url:
            log.info(f"Reintentando con URL de dominio específico: {corrected_url}")
            try:
              response = await self.client.get(corrected_url, headers=get_headers(referer=corrected_url))
              response.raise_for_status()
              selector = Selector(response.text)
              metrics = ReviewMetricsCalculator.get_review_metrics_for_language(selector, self.target_language)
            except Exception as e:
              log.warning(f"Fallo al reintentar con URL corregida: {e}")
      
      # ⚠️ Log de advertencia si hay discrepancias
      if (metrics.get("pagination_count") and metrics.get("language_button_count") and 
          abs(metrics["pagination_count"] - metrics["language_button_count"]) > 50):
        log.warning(f"Gran discrepancia en {self.target_language}: "
                   f"paginación={metrics['pagination_count']}, "
                   f"botón={metrics['language_button_count']}")
      
      return metrics
      
    except httpx.HTTPStatusError as e_http_metrics:
      log.error(f"HTTP {e_http_metrics.response.status_code} obteniendo métricas")
      if e_http_metrics.response.status_code == 403:
        log.error(f"Error 403 obteniendo métricas - BLOQUEO DETECTADO")
        self.stop_event.set()
      return {"total_reviews": 0, "is_correct_language_view": False, "source": "error"}
      
    except Exception as e:
      log.error(f"Error obteniendo métricas: {e}")
      return {"total_reviews": 0, "is_correct_language_view": False, "source": "error"}
  
  async def _save_reviews_to_json_incrementally_internal(self,
                                                        region_name_to_update: str,
                                                        attraction_url: str,
                                                        new_reviews_data: List[Dict],
                                                        site_language_count: int,
                                                        language_code: str,
                                                        attraction_name_if_new: Optional[str] = None):
    """Guarda reseñas de forma incremental al JSON con soporte multilenguaje"""
    if not self.json_output_filepath:
      log.warning("Ruta JSON no configurada")
      return
  
    def _io_bound_save():
      with JSON_SAVE_LOCK:
        full_data = {"regions": []}
        
        # Cargar datos existentes
        try:
          if os.path.exists(self.json_output_filepath) and os.path.getsize(self.json_output_filepath) > 0:
            with open(self.json_output_filepath, 'r', encoding='utf-8') as f:
              content = f.read()
              if content.strip():
                loaded_json = json.loads(content)
                if isinstance(loaded_json, dict) and "regions" in loaded_json and isinstance(loaded_json["regions"], list):
                  full_data = loaded_json
        except json.JSONDecodeError:
          log.warning(f"Error decodificando JSON desde {self.json_output_filepath}")
        except Exception as e:
          log.error(f"Error leyendo JSON: {e}")
  
        # Encontrar región objetivo
        target_region_obj = None
        for region_obj in full_data.get("regions", []):
          if region_obj.get("region_name") == region_name_to_update:
            target_region_obj = region_obj
            break
  
        if not target_region_obj:
          log.error(f"Región '{region_name_to_update}' no encontrada")
          return
  
        # Encontrar o crear atracción
        attraction_to_update = None
        attraction_idx = -1
        if "attractions" not in target_region_obj or not isinstance(target_region_obj.get("attractions"), list):
          target_region_obj["attractions"] = []
  
        for i, attraction_json_obj in enumerate(target_region_obj.get("attractions", [])):
          if attraction_json_obj.get("url") == attraction_url:
            attraction_to_update = attraction_json_obj
            attraction_idx = i
            break
  
        if not attraction_to_update:
          log.error(f"Atracción con URL {attraction_url} no encontrada")
          return
  
        # ✅ CORREGIDO: Estructura multilenguaje
        if not attraction_to_update.get("languages"):
          attraction_to_update["languages"] = {}
      
        if language_code not in attraction_to_update["languages"]:
            attraction_to_update["languages"][language_code] = {
                "reviews": [],
                "reviews_count": 0,                    # Total esperado del sitio
                "stored_reviews": 0,                   # Reseñas únicas guardadas
                "skipped_duplicates": 0,               # Duplicadas saltadas (acumulativo)
                "previously_scraped": False,
                "last_scrape_date": None
            }
  
        language_data = attraction_to_update["languages"][language_code]
  
        # Actualizar metadatos del idioma
        language_data["reviews_count"] = site_language_count
        language_data["last_scrape_date"] = datetime.now(timezone.utc).isoformat()
        language_data["previously_scraped"] = True
  
        # Procesar reseñas evitando duplicados globales
        existing_reviews_list = language_data.get("reviews", [])
        all_existing_review_ids = set()
        all_existing_reviews = []
  
        # Obtener todas las reseñas existentes de todos los idiomas
        for lang, lang_data in attraction_to_update.get("languages", {}).items():
            for review in lang_data.get("reviews", []):
                if isinstance(review, dict):
                    all_existing_reviews.append(review)
                    review_id = review.get("review_id", "").strip()
                    if review_id and review_id != "":
                        all_existing_review_ids.add(review_id)
  
        existing_global_hashes = {self._generate_review_hash(r) for r in all_existing_reviews if isinstance(r, dict)}
  
        processed_review_ids_this_session = set()
        processed_hashes_this_session = set()

        existing_reviews_current_lang = language_data.get("reviews", [])
        current_lang_review_ids = set()
        current_lang_hashes = set()
        for review in existing_reviews_current_lang:
            if isinstance(review, dict):
                review_id = review.get("review_id", "").strip()
                if review_id and review_id != "":
                    current_lang_review_ids.add(review_id)
                current_lang_hashes.add(self._generate_review_hash(review))

        # Obtener reseñas existentes de OTROS idiomas (para detectar duplicados entre idiomas)
        other_languages_review_ids = set()
        other_languages_hashes = set()
        for lang, lang_data in attraction_to_update.get("languages", {}).items():
            if lang != language_code:  # Solo otros idiomas, no el actual
                for review in lang_data.get("reviews", []):
                    if isinstance(review, dict):
                        review_id = review.get("review_id", "").strip()
                        if review_id and review_id != "":
                            other_languages_review_ids.add(review_id)
                        other_languages_hashes.add(self._generate_review_hash(review))

        processed_review_ids_this_session = set()
        processed_hashes_this_session = set()
        added_this_save_call = 0
        skipped_duplicates = 0

        if new_reviews_data:
            for review_item_data in new_reviews_data:
                if not isinstance(review_item_data, dict):
                    continue
                
                review_id = review_item_data.get("review_id", "").strip()
                is_duplicate = False
                duplicate_reason = ""

                if review_id and review_id != "":
                    # Verificar si ya existe en el idioma actual
                    if review_id in current_lang_review_ids or review_id in processed_review_ids_this_session:
                        is_duplicate = True
                        duplicate_reason = "same_language"
                    # Verificar si ya existe en otros idiomas
                    elif review_id in other_languages_review_ids:
                        is_duplicate = True
                        duplicate_reason = "cross_language"
                    else:
                        processed_review_ids_this_session.add(review_id)
                else:
                    # Fallback: usar hash
                    review_hash = self._generate_review_hash(review_item_data)
                    if review_hash in current_lang_hashes or review_hash in processed_hashes_this_session:
                        is_duplicate = True
                        duplicate_reason = "same_language_hash"
                    elif review_hash in other_languages_hashes:
                        is_duplicate = True
                        duplicate_reason = "cross_language_hash"
                    else:
                        processed_hashes_this_session.add(review_hash)

                # Guardar o contar como duplicado
                if not is_duplicate:
                    existing_reviews_list.append(review_item_data)
                    added_this_save_call += 1
                else:
                    skipped_duplicates += 1
                    log.debug(f"Review duplicada ({duplicate_reason}): {review_id}")

        language_data["reviews"] = existing_reviews_list
        language_data["stored_reviews"] = len(existing_reviews_list)
        language_data["skipped_duplicates"] += skipped_duplicates
        
  
        # Asegurar nombre de atracción
        if not attraction_to_update.get("attraction_name") or "Nueva Atracción" in attraction_to_update.get("attraction_name", ""):  # ✅ CORREGIDO
          if attraction_name_if_new:
            attraction_to_update["attraction_name"] = attraction_name_if_new
  
        attraction_name_log = attraction_to_update.get('attraction_name', attraction_url) 
        if added_this_save_call > 0:
          log.info(f"JSON: {added_this_save_call} nuevas reseñas en {language_code} para '{attraction_name_log}' "
                  f"total_único={attraction_to_update['scraped_reviews_count']}")
        elif new_reviews_data:
          log.debug(f"JSON: metadatos actualizados para '{attraction_name_log}' en {language_code}")
        else:
          log.debug(f"JSON: metadatos para '{attraction_name_log}' {language_code}_count={site_language_count}")
        
        # Actualizar atracción en lista
        target_region_obj["attractions"][attraction_idx] = attraction_to_update
  
        # Guardar archivo
        try:
          with open(self.json_output_filepath, 'w', encoding='utf-8') as f:
            json.dump(full_data, f, ensure_ascii=False, indent=2)
        except IOError as e:
          log.error(f"Error E/O escribiendo JSON: {e}")
        except Exception as e:
          log.error(f"Error guardando JSON: {e}")
  
    await asyncio.to_thread(_io_bound_save)

  def _build_page_url(self, base_url_page1: str, page_number: int) -> str:
    """Construye URL para página específica"""
    offset = (page_number - 1) * self.REVIEWS_PER_PAGE
    parts = base_url_page1.split("-Reviews-")
    
    if len(parts) != 2:
      log.warning(f"Formato URL inesperado: {base_url_page1}")
      if page_number == 1:
        return base_url_page1
      return base_url_page1.replace("-Reviews-", f"-Reviews-or{offset}-") if offset > 0 else base_url_page1
    
    url_prefix = parts[0] + "-Reviews"
    url_suffix = parts[1]
    
    if page_number == 1 or offset == 0:
      return f"{url_prefix}-{url_suffix}"
    else:
      return f"{url_prefix}-or{offset}-{url_suffix}"

  def _build_error_response(self, attraction_data: Dict, status_code: str, error_message: str) -> Dict:
    """Construye respuesta de error estándar para multilenguaje"""
    attraction_name_val = attraction_data.get("attraction_name", "Desconocido")
    url = attraction_data.get("url", "")
    
    # ✅ CORREGIDO: Usar datos del idioma específico
    language_data = attraction_data.get("languages", {}).get(self.target_language, {})
    initial_language_count = language_data.get("reviews_count", 0)
    current_scraped_in_json = len(language_data.get("reviews", []))
    
    return {
      "attraction_name": attraction_name_val,
      "url": url,
      "newly_scraped_reviews": [],
      "current_site_language_reviews_count": initial_language_count,
      "language": self.target_language,  # ✅ AÑADIDO
      "final_scraped_count_in_json": current_scraped_in_json,
      "scrape_status": status_code,
      "error": error_message
    }