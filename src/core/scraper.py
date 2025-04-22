import asyncio
import random
import httpx
from typing import Dict, List, Optional
from loguru import logger as log
from parsel import Selector

from src.core.metrics import ReviewMetricsCalculator
from src.core.parsers.review_parser import ReviewParser
from src.utils.networking import smart_sleep
from ..models.attraction import Attraction
from ..utils.constants import BASE_URL, HEADERS, get_headers


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
        """Versión asíncrona para scraping de página de atracciones"""
        try:
            response = await self.client.get(url)
            selector = Selector(response.text)
            attractions = []
            
            for card in selector.xpath('//article[contains(@class, "GTuVU")]'):
                try:
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
                        attraction_data["url"] = f"{BASE_URL}{href.split('#')[0]}"
                    
                    # Extraer posición y nombre
                    name_div = card.xpath('.//div[contains(@class, "XfVdV") and contains(@class, "AIbhI")]')
                    if name_div:
                        name_text = name_div.xpath('string(.)').get().strip()
                        if '.' in name_text:
                            parts = name_text.split('.', 1)
                            try:
                                attraction_data["position"] = int(parts[0].strip())
                            except (ValueError, IndexError):
                                pass
                            attraction_data["place_name"] = parts[1].strip()
                        else:
                            attraction_data["place_name"] = name_text
                    
                    # Extraer puntuación
                    rating_div = card.xpath('.//div[contains(@class, "MyMKp")]//div[contains(@class, "biGQs") and contains(@class, "_P") and contains(@class, "hmDzD")]')
                    if rating_div:
                        rating_text = rating_div.xpath('text()').get()
                        if rating_text and '.' in rating_text:
                            try:
                                attraction_data["rating"] = float(rating_text.strip())
                            except ValueError:
                                pass
                            
                    # Extraer número de reseñas
                    reviews_div = card.xpath('.//a[contains(@class, "BMQDV")]//div[@class="f Q2"]/div[contains(@class, "biGQs") and contains(@class, "_P") and contains(@class, "hmDzD")][last()]')
                    if not reviews_div:
                        reviews_div = card.xpath('.//div[contains(@class, "Q2")]//div[contains(@class, "biGQs") and contains(@class, "_P") and contains(@class, "hmDzD")][last()]')
                    
                    if reviews_div:
                        reviews_text = reviews_div.xpath('text()').get()
                        if reviews_text:
                            cleaned_text = reviews_text.strip().replace('.', '').replace(',', '')
                            if cleaned_text.isdigit():
                                try:
                                    attraction_data["reviews_count"] = int(cleaned_text)
                                except ValueError:
                                    pass
                                
                    # Extraer tipo de lugar
                    type_section = card.xpath('.//div[contains(@class, "dxkoL")]')
                    if type_section:
                        type_div = type_section.xpath('.//div[contains(@class, "biGQs") and contains(@class, "_P") and contains(@class, "hmDzD")][1]')
                        if type_div:
                            type_text = type_div.xpath('text()').get()
                            if type_text and not any(c.isdigit() for c in type_text) and '.' not in type_text:
                                attraction_data["place_type"] = type_text.strip()
                    
                    attractions.append(attraction_data)
                
                except Exception as e:
                    log.warning(f"Error extrayendo datos de atracción: {e}")
                    continue
                    
            return attractions
            
        except Exception as e:
            log.error(f"Error en scrape_page: {e}")
            raise

    async def get_next_page_url(self, response_text: str) -> Optional[str]:
        """Obtiene URL para la siguiente página de atracciones"""
        selector = Selector(text=response_text)
        next_link = selector.css('a.BrOJk[data-smoke-attr="pagination-next-arrow"]::attr(href)').get()
        return f"{BASE_URL}{next_link}" if next_link else None

    async def get_all_attractions(self, region_url: str) -> List[Dict]:
        """Obtiene datos de todas las atracciones en una región específica"""
        all_attractions = []
        page_count = 1
        current_url = region_url
        
        while current_url:
            response = await self.client.get(current_url)
            page_attractions = await self.scrape_page(current_url)
            all_attractions.extend(page_attractions)
            log.info(f"Encontradas {len(page_attractions)} atracciones en página {page_count}")
            
            next_url = await self.get_next_page_url(response.text)
            if not next_url:
                break
                
            current_url = next_url
            page_count += 1
            await asyncio.sleep(2)
        
        return all_attractions
    
class ReviewScraper:
    """Scraper especializado en extracción de reseñas con:
    - Manejo de paginación
    - Detección automática de idioma
    - Reintentos inteligentes
    - Integración con el sistema de parsing
    """
    
    def __init__(self, max_retries: int = 3, max_concurrency: int = 3):
        self.client = None
        self.max_retries = max_retries
        self.max_concurrency = max_concurrency
        self.parser = ReviewParser()
        self.metrics = ReviewMetricsCalculator()

    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            headers=get_headers(),
            follow_redirects=True,
            timeout=httpx.Timeout(30.0),
            limits=httpx.Limits(max_connections=self.max_concurrency)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()

    async def scrape_reviews(self, attraction: Attraction, max_pages: Optional[int] = None) -> Dict:
        """Versión principal que mantienes en tu código actual"""
        english_url = self._ensure_english_view(attraction.url)
        metrics = await self._get_review_metrics(english_url, attraction.reviews_count)
        
        if metrics['english_reviews'] == 0:
            return self._build_empty_response(attraction, metrics)

        reviews = await self._scrape_paginated_reviews(
            base_url=english_url,
            attraction_name=attraction.place_name,
            total_pages=self._calculate_pages(metrics, max_pages)
        )

        return self._build_success_response(attraction, metrics, reviews)

    async def scrape_multiple_attractions(self, attractions: List[Attraction]) -> List[Dict]:
        """Devuelve lista de atracciones procesadas en formato dict"""
        semaphore = asyncio.Semaphore(self.max_concurrency)
        
        async def process_attraction(attraction):
            async with semaphore:
                return await self.scrape_reviews(attraction)
        
        return await asyncio.gather(*[process_attraction(a) for a in attractions])

    # -------------------- Métodos internos --------------------
    async def _scrape_paginated_reviews(self, base_url: str, attraction_name: str, total_pages: int) -> List[Dict]:
        """Maneja la paginación con retries y delays inteligentes"""
        all_reviews = []
        seen_reviews = set()

        for page in range(1, total_pages + 1):
            page_url = self._build_page_url(base_url, page)

            for attempt in range(1, self.max_retries + 1):
                try:
                    reviews = await self._scrape_single_page(page_url)
                    if reviews:
                        # Filtrar duplicados antes de agregar
                        for review in reviews:
                            review_hash = self._generate_review_hash(review)
                            if review_hash not in seen_reviews:
                                seen_reviews.add(review_hash)
                                all_reviews.append(review)

                        log.success(f"{attraction_name} - Página {page}: {len(reviews)} reseñas (únicas: {len(all_reviews)})")
                        await smart_sleep(page)
                        break
                except Exception as e:
                    log.error(f"Error en página {page} (intento {attempt}): {str(e)}")
                    if attempt == self.max_retries:
                        break
                    await self._exponential_backoff(attempt)

        return all_reviews

    def _generate_review_hash(self, review: Dict) -> str:
        """Genera un hash único para cada reseña basado en contenido clave"""
        key_fields = (
            review.get('username', ''),
            review.get('title', ''),
            review.get('written_date', ''),
            str(review.get('rating', 0))
        )
        return hash(key_fields)

    async def _scrape_single_page(self, url: str) -> List[Dict]:
        """Combina fetching y parsing con validación"""
        response = await self.client.get(url)
        reviews = self.parser.parse_reviews_page(response.text, url)
        return [r for r in reviews if self.parser.validate_review(r)]

    async def _get_review_metrics(self, url: str, default_count: int) -> Dict:
        response = await self.client.get(url)
        selector = Selector(response.text)

        total = self.parser.extract_total_reviews(selector) or default_count
        english = self.parser.extract_english_reviews(selector)

        return {
            "total_reviews": total,
            "english_reviews": english,
            "english_pages": (english + 9) // 10 if english > 0 else 0 # Redondeo hacia arriba
        }

    # -------------------- Helpers --------------------
    def _ensure_english_view(self, url: str) -> str:
        """Fuerza vista en inglés si no está en la URL"""
        if "filterLang=en" not in url:
            return f"{url}?filterLang=en" if "?" not in url else f"{url}&filterLang=en"
        return url

    def _build_page_url(self, base_url: str, page: int) -> str:
        """Construye URLs paginadas manteniendo parámetros"""
        if page == 1:
            return base_url
        offset = (page - 1) * 10
        separator = "&" if "?" in base_url else "?"
        return f"{base_url}{separator}reviewsOffset={offset}"

    def _calculate_pages(self, metrics: Dict, max_pages: Optional[int]) -> int:
        """Determina páginas a scrapear"""
        pages = metrics['english_pages']
        return min(pages, max_pages) if max_pages else pages

    def _build_empty_response(self, attraction: Attraction, metrics: Dict) -> Dict:
        """Respuesta para atracciones sin reseñas"""
        return {
            **attraction.__dict__,
            "total_reviews": metrics['total_reviews'],
            "english_reviews": 0,
            "reviews": [],
            "scrape_status": "no_english_reviews"
        }

    def _build_success_response(self, attraction: Attraction, metrics: Dict, reviews: List) -> Dict:
        """Estructura respuesta exitosa con todos los campos necesarios"""
        return {
            **attraction.__dict__,
            "place_name": attraction.place_name,
            "place_type": attraction.place_type,
            "rating": attraction.rating,
            "url": attraction.url,
            "total_reviews": metrics['total_reviews'],
            "english_reviews": metrics['english_reviews'],
            "reviews": reviews,
            "scrape_status": "completed" if reviews else "failed"
        }

    async def _exponential_backoff(self, attempt: int):
        """Delay exponencial para reintentos"""
        delay = min(2 ** attempt, 60)  # Cap a 60 segundos
        await asyncio.sleep(delay + random.random())