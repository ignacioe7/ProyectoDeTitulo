import re
import asyncio
from typing import Dict
from httpx import Response, AsyncClient
from parsel import Selector
from loguru import logger

# Diccionario para rastrear intentos por URL base
RETRY_COUNTERS = {}

async def parse_things_to_do_page(result: Response, client: AsyncClient, retry_count: int = 0) -> Dict:
  """Analiza la página de atracciones de TripAdvisor y extrae las reseñas."""
  selector = Selector(result.text)
  url = str(result.url)

  # Extraer la URL base (sin offset de páginas)
  base_url = re.sub(r'-or\d+-', '-Reviews-', url.split('?')[0])
  
  # Obtener información principal de la atracción
  place_name = selector.css("h1.biGQs._P.fiohW.eIegw::text").get() or "Atracción Desconocida"
  attraction = selector.css("span.eojVo::text").get() or "Atracción"
 
  # Obtener puntuación
  try:
    score = float(selector.css("div[data-automation='reviewBubbleScore']::text").get() or 0)
  except (ValueError, TypeError):
    score = 0.0
    logger.warning(f"{place_name}: No se pudo analizar la puntuación, usando 0.0 por defecto")

  # Obtener total de reseñas
  try:
    total_reviews_span = selector.css("span[data-automation='reviewCount']::text").get()
    total_reviews = int(total_reviews_span.split()[0].replace(',', '')) if total_reviews_span else 0
  except (ValueError, IndexError):
    total_reviews = 0
    logger.warning(f"{place_name}: No se pudo analizar el conteo total de reseñas")

  # Obtener número actual de reseñas disponibles
  actual_total_reviews = 0
  reviews_div = selector.css("div.Ci").get()
  if reviews_div:
    total_reviews_displayed = ''.join(selector.css("div.Ci").xpath(".//text()").getall())
    match = re.search(r'of\s+(\d+)', total_reviews_displayed)
    if match:
      actual_total_reviews = int(match.group(1))
      logger.info(f"{place_name}: {actual_total_reviews} reseñas disponibles para extraer")
    else:
      actual_total_reviews = total_reviews
  else:
    actual_total_reviews = total_reviews

  # Cálculo correcto de paginación
  if actual_total_reviews > 0:
    total_pages = actual_total_reviews // 10
    if actual_total_reviews % 10 > 0:
      total_pages += 1
  else:
    total_pages = 1  # Mínimo una página

  # Determinar página actual
  current_page = 1
  page_match = re.search(r'Reviews-or(\d+)-', url)
  if page_match:
    offset = int(page_match.group(1))
    if offset > 0:
      current_page = (offset // 10) + 1
  
  # Validar que la página actual no exceda las páginas totales
  if current_page > total_pages:
    logger.debug(f"URL original: {url}")
    logger.debug(f"Offset extraído: {offset if 'offset' in locals() else 'No hay offset'}")
    logger.debug(f"Total reviews: {actual_total_reviews}")
    
    # En lugar de corregir, terminamos el procesamiento si excede el total
    logger.warning(f"Página actual ({current_page}) excede el total de páginas ({total_pages}), terminando extracción")
    return {
      "place_name": place_name,
      "attraction": attraction,
      "score": score,
      "url": url,
      "total_reviews": total_reviews,
      "available_reviews": actual_total_reviews, 
      "current_page": current_page,
      "total_pages": total_pages,
      "review_counts": {},
      "reviews": [],
      "is_last_page": True
    }

  logger.info(f"Procesando {place_name}: Página {current_page}/{total_pages} - {actual_total_reviews} reseñas totales")
  
  # Inicializar variables
  reviews = []
  total_collected = 0
  
  # Buscar tarjetas de reseñas
  review_cards = selector.xpath("//div[@class='_c' and @data-automation='reviewCard']")
  
  # Verificar si hay filtros aplicados - con límite de intentos
  if not review_cards:
    clear_filter = selector.css("div.LbPSX div[data-automation='tab'] button.UikNM").get()
    
    # Si encontramos el botón o detectamos la estructura de filtros
    if clear_filter or selector.css("div.LbPSX").get():
      # Usar un contador por URL base
      if base_url not in RETRY_COUNTERS:
        RETRY_COUNTERS[base_url] = 0
      
      # Incrementar contador para esta atracción
      RETRY_COUNTERS[base_url] += 1
      current_retries = RETRY_COUNTERS[base_url]
      
      if current_retries >= 10:
        logger.warning(f"¡ATENCIÓN! Demasiados intentos ({current_retries}/10) con filtros en URL: {base_url}")
        logger.warning(f"Abandonando esta atracción y continuando con la siguiente")
        # Limpiar el contador para la próxima vez
        RETRY_COUNTERS[base_url] = 0
        return {
          "place_name": place_name,
          "attraction": attraction,
          "score": score,
          "url": url,
          "total_reviews": total_reviews,
          "available_reviews": 0,
          "current_page": current_page,
          "total_pages": 0,
          "review_counts": {},
          "reviews": [],
          "error": "Demasiados intentos con filtros",
          "is_last_page": True  # Marcar como última página para terminar
        }
      
      logger.info(f"Filtros detectados (intento {current_retries}/10) para {place_name}")
      logger.info(f"URL con filtros: {url}")
      logger.info(f"URL base: {base_url}")
      
      # Intentar con la página base sin offset ni filtros
      clean_url = re.sub(r'-or\d+-', '-Reviews-', url.split('?')[0])
      
      logger.info(f"Intentando URL limpia: {clean_url}")
      await asyncio.sleep(3)  # Pausa antes de reintentar
      
      new_response = await client.get(clean_url)
      return await parse_things_to_do_page(new_response, client, retry_count + 1)

  # Obtener conteo de reseñas por calificación
  try:
    review_counts = {}
    for i, rating in enumerate(['excellent', 'very_good', 'average', 'poor', 'terrible'], 1):
      count_text = selector.xpath(f"//div[@class='jxnKb'][{i}]//div[@class='biGQs _P fiohW biKBZ osNWb']/text()").get() or '0'
      review_counts[rating] = int(count_text.replace(',', ''))
  except Exception:
    review_counts = {"excellent": 0, "very_good": 0, "average": 0, "poor": 0, "terrible": 0}
    logger.warning(f"{place_name}: Error al obtener conteo de reseñas por calificación")

  # Procesar cada tarjeta de reseña
  for card in review_cards:
    # Detener si ya hemos recolectado todas las reseñas disponibles
    if total_collected >= min(10, actual_total_reviews - ((current_page - 1) * 10)):
      break

    try:
      # Extraer nombre de usuario, ubicación y total de contribuciones
      user_info_block = card.xpath(".//div[@class='QIHsu Zb']")
      name = user_info_block.xpath(".//span[@class='biGQs _P fiohW fOtGX']/a/text()").get() or "SIN NOMBRE"
      
      info_spans = user_info_block.xpath(".//div[@class='vYLts']//div[@class='biGQs _P pZUbB osNWb']/span/text()").getall()
      if len(info_spans) >= 2:
        location = info_spans[0]
        contributions = info_spans[1]
      elif len(info_spans) == 1:
        location = "SIN UBICACIÓN"
        contributions = info_spans[0]
      else:
        location = "SIN UBICACIÓN"
        contributions = "SIN INFORMACIÓN"
      
      # Extraer calificación de la reseña
      rating_text = card.xpath(".//svg[contains(@class, 'UctUV')]//title/text()").get() or "0.0"
      rating = float(rating_text.split()[0]) if rating_text else 0.0

      # Extraer título
      title = card.xpath(".//div[contains(@class, 'biGQs')]//span[@class='yCeTE']/text()").get() or "SIN TÍTULO"

      # Extraer información de visita
      visit_info = card.xpath(".//div[@class='RpeCd']/text()").get() or "SIN FECHA"
      if "•" in visit_info:
        visit_date, companion_type = visit_info.split("•", 1)  # Limitar a un solo split
        visit_date = visit_date.strip()
        companion_type = companion_type.strip()
      else:
        visit_date = visit_info.strip()
        companion_type = "SIN INFORMACIÓN"

      # Extraer texto de la reseña
      review_text = card.xpath(".//span[@class='JguWG']//span[@class='yCeTE']/text()").get() or "SIN TEXTO DE RESEÑA"

      # Extraer fecha de escritura
      written_date = card.xpath(".//div[contains(@class, 'ncFvv')]/text()").get() or "SIN FECHA DE ESCRITURA"
      if written_date.startswith("Written "):
        written_date = written_date.replace("Written ", "")

      # Agregar reseña a la lista
      reviews.append({
        "username": name,
        "location": location,
        "contributions": contributions,
        "rating": rating,
        "title": title,
        "visit_date": visit_date,
        "companion_type": companion_type,
        "review_text": review_text,
        "written_date": written_date
      })
      
      total_collected += 1
      
    except Exception as e:
      logger.warning(f"{place_name}: Error al extraer reseña: {e}")
      continue

  logger.info(f"{place_name}: Recolectadas {total_collected} reseñas de la página {current_page}")

  # Determinar si es la última página basado en:
  # 1. Si estamos en la última página según el cálculo
  # 2. Si recolectamos menos de 10 reseñas (probable última página parcial)
  is_last_page = (current_page >= total_pages) or (total_collected < 10)
  
  return {
    "place_name": place_name,
    "attraction": attraction,
    "score": score,
    "url": url,
    "total_reviews": total_reviews,
    "available_reviews": actual_total_reviews,
    "current_page": current_page,
    "total_pages": total_pages,
    "review_counts": review_counts,
    "reviews": reviews,
    "is_last_page": is_last_page  # Indica si es la última página
  }