from asyncio import log
from datetime import datetime
import re
import asyncio
from typing import Dict, List, Optional
from httpx import Response, AsyncClient
import httpx
from parsel import Selector
from loguru import logger

# Variables globales
RETRY_COUNTERS = {}
LAST_KNOWN_ATTRACTIONS = {}
PAGES_PROCESSED = 0
PROBLEMATIC_URLS = []


async def count_attraction_metrics(client: httpx.AsyncClient, url: str, reviews_count: int) -> Dict:
    """
    Cuenta las métricas de una atracción, como el número de reseñas y páginas.
    Retorna un diccionario con las métricas.
    """
    try:
        response = await client.get(url)
        selector = Selector(response.text)

        # Verificar el botón de idioma
        language_button = selector.css('button.Datwj[aria-haspopup="listbox"] .biGQs._P::text').get('')
        logger.debug(f"Botón de idioma detectado: {language_button}")  # Log para ver el botón de idioma
        if "English" in language_button:
            # Intentar extraer el número de reseñas disponibles
            available_reviews = extract_available_reviews(selector, response.text)
            logger.debug(f"Número de reseñas extraídas: {available_reviews}")  # Log adicional para depuración

            # Si no se pudo extraer el número de reseñas, usar el valor de reviews_count del JSON
            if available_reviews == 0:
                available_reviews = reviews_count
                logger.info(f"Usando reviews_count del JSON: {available_reviews} reseñas")

            # Calcular número de páginas (10 reseñas por página)
            total_pages = (available_reviews + 9) // 10 if available_reviews > 0 else 0

            return {
                "reviews_count": available_reviews,
                "total_pages": total_pages
            }
            
        logger.info(f"Atracción en 'All languages'. Ignorando: {url}")
        return {"reviews_count": 0, "total_pages": 0}

    except Exception as e:
        logger.error(f"Error al obtener métricas para {url}: {e}")
        return {"reviews_count": 0, "total_pages": 0}


def extract_available_reviews(selector: Selector, html_text: str) -> int:
    """
    Extrae el número de reseñas disponibles de una página.
    """
    # Intentar extraer de un texto específico
    results_text = selector.css('div.Ci::text').get('')
    if results_text and 'of' in results_text:
        match = re.search(r'of\s+([\d,]+)', results_text)
        if match:
            return int(match.group(1).replace(',', ''))

    # Buscar en el HTML completo como respaldo
    all_matches = re.findall(r'showing.*?results.*?of.*?([\d,]+)', html_text.lower())
    if all_matches:
        try:
            return int(all_matches[0].replace(',', ''))
        except (ValueError, IndexError):
            pass

    # Si no se encuentra, retornar 0
    return 0


async def scrape_attraction_with_metrics(client: httpx.AsyncClient, url: str, basic_metadata: Dict, max_pages: Optional[int] = None) -> Dict:
    """
    Extrae todas las reseñas de una atracción, utilizando métricas precalculadas.
    """
    place_name = basic_metadata.get("place_name", "Desconocido")


    try:
        # Obtener métricas actualizadas
        updated_metrics = await count_attraction_metrics(client, url)
        basic_metadata.update(updated_metrics)

        # Si no hay reseñas, retornar datos básicos
        if updated_metrics["reviews_count"] == 0:
            return {
                **basic_metadata,
                "reviews": [],
                "url": url
            }

        # Calcular páginas a procesar
        total_pages = basic_metadata.get("total_pages", 1)
        max_pages_limit = min(total_pages, max_pages) if max_pages is not None else total_pages

        logger.info(f"Procesando {place_name}: {updated_metrics['reviews_count']} reseñas en {max_pages_limit} páginas")

        # Extraer reseñas de todas las páginas
        reviews_data = await extract_reviews_from_all_pages(client, url, basic_metadata, max_pages_limit)

        return {
            **basic_metadata,
            "url": url,
            "reviews": reviews_data
        }

    except Exception as e:
        logger.error(f"Error al procesar {place_name}: {e}")
        return {
            **basic_metadata,
            "reviews": [],
            "url": url
        }


async def extract_reviews_from_all_pages(client: httpx.AsyncClient, base_url: str, metadata: Dict, max_pages: int) -> List[Dict]:
    """
    Extrae reseñas de todas las páginas de una atracción.
    """
    reviews_data = []
    place_name = metadata.get("place_name", "Desconocido")

    # Procesar cada página
    for page in range(1, max_pages + 1):
        page_url = get_page_url(base_url, page)
        logger.info(f"Procesando {place_name}: Página {page}/{max_pages}")

        try:
            page_response = await client.get(page_url)
            page_data = await parse_things_to_do_page(page_response, client, metadata)

            if page_data["reviews"]:
                reviews_data.extend(page_data["reviews"])
                logger.info(f"{place_name}: Añadidas {len(page_data['reviews'])} reseñas de la página {page}")
            else:
                logger.warning(f"{place_name}: No se encontraron reseñas en la página {page}")
                break  # Detener el proceso si no hay reseñas en la página

            # Pausas inteligentes para evitar bloqueos
            await smart_sleep(page)

        except Exception as e:
            logger.error(f"Error procesando página {page} de {place_name}: {e}")
            await asyncio.sleep(5)

    return reviews_data


def get_page_url(base_url: str, page: int) -> str:
    """
    Genera la URL de una página específica basada en el número de página.
    """

    if page == 1:
        return base_url
    offset = (page - 1) * 10
    if "-Reviews-" in base_url:
        return base_url.replace("-Reviews-", f"-Reviews-or{offset}-")
    return base_url.replace("Review-", f"Review-or{offset}-")


async def smart_sleep(page: int):
    """
    Realiza pausas inteligentes para evitar ser bloqueado.
    """
    if page % 50 == 0:
        pause_time = 45
    elif page % 10 == 0:
        pause_time = 15
    else:
        pause_time = 2 + (page // 100)
    logger.info(f"Pausa de {pause_time} segundos...")
    await asyncio.sleep(pause_time)


async def parse_things_to_do_page(response: Response, client: AsyncClient, metadata: Dict) -> Dict:
    """
    Parsea una página de reseñas y extrae la información relevante.
    """

    global PAGES_PROCESSED
    PAGES_PROCESSED += 1

    selector = Selector(response.text)
    url = str(response.url)

    # Extraer reseñas de la página
    reviews = extract_reviews_from_page(selector, current_url=url)

    return {
        **metadata,
        "url": url,
        "reviews": reviews,
        "is_last_page": len(reviews) < 10  # Asumimos que es la última página si hay menos de 10 reseñas
    }


def extract_reviews_from_page(selector: Selector, current_url: str) -> List[Dict]:
    """
    Extrae las reseñas de una página con manejo de errores mejorado.
    """
    reviews = []
    review_cards = selector.xpath("//div[@data-automation='reviewCard']")

    if not review_cards:
        logger.warning(f"No se encontraron reseñas en la página: {current_url}")
        return []

    logger.info(f"Se encontraron {len(review_cards)} tarjetas de reseñas en la página: {current_url}")

    for card in review_cards:
        try:
            review = extract_review_from_card(card)
            if is_valid_review(review):
                reviews.append(review)
            else:
                logger.warning(f"Reseña descartada por falta de información: {review}")
        except Exception as e:
            logger.warning(f"Error grave al extraer reseña: {e}")
            continue  # Continuar con la siguiente reseña incluso si falla una

    logger.info(f"Total de reseñas extraídas en la página: {len(reviews)}")
    return reviews


def extract_review_from_card(card: Selector) -> Dict:
    """
    Extrae los detalles de una reseña de una tarjeta con manejo robusto de errores.
    """
    # Inicializar todas las variables con valores por defecto
    contributions_text = "0"
    
    try:
        # Extraer nombre de usuario
        name = card.xpath(".//span[contains(@class, 'fiohW')]/a/text()").get() or "Sin nombre"

        # Extraer calificación
        rating_text = card.xpath(".//svg[contains(@class, 'UctUV')]//title/text()").get() or "0.0"
        rating = float(rating_text.split("of")[0].strip()) if "of" in rating_text else 0.0

        # Extraer título y texto (estos son obligatorios)
        title = card.xpath(".//div[contains(@class, 'biGQs')]//span[@class='yCeTE']/text()").get() or "Sin título"
        review_text = card.xpath(".//span[@class='JguWG']//span[@class='yCeTE']/text()").get() or "Sin texto"

        # Extraer ubicación y contribuciones (opcionales)
        user_info_block = card.xpath(".//div[@class='QIHsu Zb']")
        info_spans = user_info_block.xpath(".//div[@class='vYLts']//div[@class='biGQs _P pZUbB osNWb']/span/text()").getall()
        
        location = "Sin ubicación"
        contributions = 0
        
        if len(info_spans) >= 2:
            location = info_spans[0].strip() if info_spans[0] else location
            contributions_text = info_spans[1].strip() if info_spans[1] else "0"
        elif len(info_spans) == 1:
            contributions_text = info_spans[0].strip() if info_spans[0] else "0"

        # Limpieza robusta de contribuciones
        contributions_text = contributions_text.lower().replace("contributions", "").replace("contribuciones", "").strip()
        contributions_text = contributions_text.replace(",", "").replace(".", "")
        contributions = int(contributions_text) if contributions_text.isdigit() else 0

        # Extraer fechas (opcionales)
        visit_info = card.xpath(".//div[@class='RpeCd']/text()").get() or "Sin fecha"
        visit_date = "Sin fecha"
        companion_type = "Sin información"
        if "•" in visit_info:
            parts = [p.strip() for p in visit_info.split("•")]
            visit_date = parts[0] if parts else visit_date
            companion_type = parts[1] if len(parts) > 1 else companion_type

        written_date = card.xpath(".//div[contains(@class, 'ncFvv')]/text()").get() or "Sin fecha"
        if written_date.startswith("Written "):
            written_date = written_date[8:].strip()

        return {
            "username": name,
            "rating": rating,
            "title": title,
            "review_text": review_text,
            "location": location,
            "contributions": contributions,
            "visit_date": visit_date,
            "written_date": written_date,
            "companion_type": companion_type
        }

    except Exception as e:
        logger.error(f"Error al procesar tarjeta de reseña: {e}")
        # Devuelve una reseña con valores por defecto pero marcada como error
        return {
            "username": "Error en extracción",
            "rating": 0.0,
            "title": title if 'title' in locals() else "Error en extracción",
            "review_text": review_text if 'review_text' in locals() else "Error en extracción",
            "location": "Error en extracción",
            "contributions": 0,
            "visit_date": "Error en extracción",
            "written_date": "Error en extracción",
            "companion_type": "Error en extracción"
        }
    
    
def is_valid_review(review: Dict) -> bool:
    """
    Verifica solo los campos críticos (título y texto).
    """
    required_fields = {
        "title": ["", "sin título", "error en extracción"],
        "review_text": ["", "sin texto", "error en extracción"]
    }
    
    # Verificar que ni el título ni el texto estén vacíos o sean inválidos
    title_valid = review.get("title", "").lower() not in required_fields["title"]
    text_valid = review.get("review_text", "").lower() not in required_fields["review_text"]
    
    return title_valid and text_valid