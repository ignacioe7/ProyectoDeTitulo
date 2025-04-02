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
    Cuenta las métricas de una atracción.
    Retorna:
    - total_reviews: Todas las reseñas (de places.py)
    - english_reviews: Solo reseñas en inglés disponibles
    - english_pages: Páginas de reseñas en inglés
    """
    try:
        response = await client.get(url)
        selector = Selector(response.text)

        # Verificar si estamos en vista de inglés
        language_button = selector.css('button.Datwj[aria-haspopup="listbox"] .biGQs._P::text').get('')
        
        if "English" in language_button:
            # Extraer solo reseñas en inglés disponibles
            english_reviews = extract_available_reviews(selector, response.text)
            english_pages = (english_reviews + 9) // 10 if english_reviews > 0 else 0
            
            return {
                "total_reviews": reviews_count,
                "english_reviews": english_reviews,
                "english_pages": english_pages 
            }
        
        return {
            "total_reviews": reviews_count,
            "english_reviews": 0,
            "english_pages": 0
        }

    except Exception as e:
        logger.error(f"Error al obtener métricas: {e}")
        return {
            "total_reviews": reviews_count,
            "english_reviews": 0,
            "english_pages": 0
        }


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
        updated_metrics = await count_attraction_metrics(client, url, basic_metadata.get("reviews_count", 0))
        basic_metadata.update(updated_metrics)

        # Si no hay reseñas, retornar datos básicos
        if updated_metrics["reviews_count"] == 0:
            return {
                **basic_metadata,
                "reviews": [],
                "url": url,
                "scrape_status": "completed_no_reviews"
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
            "reviews": reviews_data,
            "scrape_status": "completed" if len(reviews_data) > 0 else "completed_partial"
        }

    except Exception as e:
        logger.error(f"Error al procesar {place_name}: {e}")
        return {
            **basic_metadata,
            "reviews": [],
            "url": url,
            "scrape_status": "failed"
        }


async def extract_reviews_from_all_pages(client: httpx.AsyncClient, base_url: str, metadata: Dict, max_pages: int) -> List[Dict]:
    """
    Extrae reseñas de todas las páginas de una atracción.
    Si falla 3 veces en una página, abandona completamente la atracción.
    """
    reviews_data = []
    place_name = metadata.get("place_name", "Desconocido")

    # Procesar cada página
    for page in range(1, max_pages + 1):
        page_url = get_page_url(base_url, page)
        logger.info(f"Procesando {place_name}: Página {page}/{max_pages}")

        # Configuración de reintentos
        max_retries = 3
        retry_delays = [300, 600, 900]  # segundos para cada reintento
        current_retry = 0
        page_success = False

        while current_retry < max_retries and not page_success:
            try:
                page_response = await client.get(page_url)
                page_data = await parse_things_to_do_page(page_response, client, metadata)

                if page_data["reviews"]:
                    reviews_data.extend(page_data["reviews"])
                    logger.info(f"{place_name}: Añadidas {len(page_data['reviews'])} reseñas de la página {page}")
                    page_success = True
                else:
                    current_retry += 1
                    if current_retry < max_retries:
                        delay = retry_delays[current_retry - 1]
                        logger.warning(
                            f"{place_name}: No se encontraron reseñas en la página {page} "
                            f"(Intento {current_retry}/{max_retries}). "
                            f"Reintentando en {delay} segundos..."
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error(
                            f"{place_name}: No se encontraron reseñas en la página {page} "
                            f"después de {max_retries} intentos. Abandonando atracción..."
                        )
                        return reviews_data 

                # Pausas inteligentes para evitar bloqueos
                if page_success:
                    await smart_sleep(page)

            except Exception as e:
                logger.error(f"Error procesando página {page} de {place_name}: {e}")
                current_retry += 1
                if current_retry < max_retries:
                    delay = retry_delays[current_retry - 1]
                    logger.warning(f"Reintentando en {delay} segundos...")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Fallo definitivo al procesar página {page} después de {max_retries} intentos. Abandonando atracción...")
                    return reviews_data 

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
    if page % 100 == 0:
        pause_time = 60
    elif page % 50 == 0:
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
        "is_last_page": len(reviews) < 10  
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

    for card in review_cards:
        try:
            review = extract_review_from_card(card)
            if is_valid_review(review):
                reviews.append(review)
            else:
                logger.warning(f"Reseña descartada por falta de información: {review}")
        except Exception as e:
            logger.warning(f"Error grave al extraer reseña: {e}")
            continue

    logger.info(f"Total de reseñas extraídas en la página: {len(reviews)}")
    return reviews


def extract_review_from_card(card: Selector) -> Dict:
    """Versión mejorada para manejar todas las variaciones de reseñas"""
    try:
        # 1. Extraer nombre de usuario
        name = card.xpath(".//a[contains(@class, 'BMQDV')]/span/text()").get()
        if not name:
            name = card.xpath(".//span[contains(@class, 'fiohW')]//text()").get()
        name = name.strip() if name else "Sin nombre"

        # 2. Extraer rating
        rating_text = card.xpath(".//*[local-name()='svg' and contains(@class, 'UctUV')]//title/text()").get("0.0")
        rating = float(rating_text.split("of")[0].strip()) if "of" in rating_text else 0.0

        # 3. Extraer título
        title = card.xpath(".//div[contains(@class, 'ncFvv')]//span[contains(@class, 'yCeTE')]/text()").get()
        if not title:
            title = card.xpath(".//a[contains(@class, 'BMQDV')]//span[contains(@class, 'yCeTE')]/text()").get()
        title = title.strip() if title else "Sin título"

        # 4. Extraer texto de la reseña
        review_text = " ".join([
            text.strip() for text in 
            card.xpath(".//div[contains(@class, 'KxBGd')]//span[contains(@class, 'yCeTE')]//text()").getall()
            if text.strip()
        ])
        if not review_text:
            review_text = " ".join([
                text.strip() for text in 
                card.xpath(".//span[contains(@class, 'JguWG')]//span[contains(@class, 'yCeTE')]//text()").getall()
                if text.strip()
            ])
        review_text = review_text if review_text else "Sin texto"

        # 5. Extraer ubicación y contribuciones
        location = "Sin ubicación"
        contributions = 0
        contrib_text = "0"  
        info_div = card.xpath(".//div[contains(@class, 'vYLts')]")
        
        if info_div:
            spans = info_div.xpath(".//span/text()").getall()
            if len(spans) >= 2:
                location = spans[0].strip()
                contrib_text = spans[1].replace("contributions", "").replace("contribuciones", "").strip()
            elif spans:
                contrib_text = spans[0].replace("contributions", "").replace("contribuciones", "").strip()
            
            # Limpieza de contribuciones
            contrib_text = contrib_text.replace(",", "").replace(".", "").strip()
            if contrib_text:  
                contributions = int(contrib_text) if contrib_text.isdigit() else 0

        # 6. Extraer fechas de visita
        visit_info = card.xpath(".//div[contains(@class, 'RpeCd')]/text()").get("")
        visit_date = "Sin fecha"
        companion_type = "Sin información"
        
        if "•" in visit_info:
            parts = [p.strip() for p in visit_info.split("•")]
            visit_date = parts[0] if parts else visit_date
            companion_type = parts[1] if len(parts) > 1 else companion_type
        elif visit_info.strip():
            visit_date = visit_info.strip()

        # 7. Fecha de escritura
        written_date = card.xpath(".//div[contains(@class, 'TreSq')]//div[contains(@class, 'ncFvv')]/text()").get("")
        if not written_date:
            written_date = card.xpath(".//div[contains(@class, 'ncFvv') and contains(@class, 'osNWb')]/text()").get("")
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
        return {
            "username": "Error en extracción",
            "rating": 0.0,
            "title": "Error en extracción",
            "review_text": "Error en extracción",
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