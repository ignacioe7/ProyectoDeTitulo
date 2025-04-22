from dataclasses import dataclass
from typing import List, Dict, Optional
from parsel import Selector
from loguru import logger as log
import re

@dataclass
class ReviewParserConfig:
    max_retries: int = 3
    min_delay: float = 1.0
    max_delay: float = 5.0

class ReviewParser:
    """Parser completo para extracción de reseñas de TripAdvisor"""
    
    def __init__(self, config: ReviewParserConfig = None):
        self.config = config or ReviewParserConfig()
        self.problematic_urls = []

    def parse_reviews_page(self, html: str, url: str) -> List[Dict]:
        selector = Selector(html)
        return [self._parse_review_card(card) for card in selector.xpath("//div[@data-automation='reviewCard']")]

    def _parse_review_card(self, card: Selector) -> Dict:
        """Extrae todos los campos de una reseña individual"""
        try:
            return {
                "username": self._extract_username(card),
                "rating": self._extract_rating(card),
                "title": self._extract_title(card),
                "review_text": self._extract_text(card),
                "location": self._extract_location(card),
                "contributions": self._extract_contributions(card),
                "visit_date": self._extract_visit_date(card),
                "written_date": self._extract_written_date(card),
                "companion_type": self._extract_companion(card),
                "is_translated": self._is_translated(card)
            }
        except Exception as e:
            log.error(f"Error parsing review card: {e}")
            return {}

    # Métodos de extracción específicos
    def _extract_username(self, card: Selector) -> str:
        name = card.xpath(".//a[contains(@class, 'BMQDV') and contains(@class, 'ukgoS')]/text()").get()
        if not name:
            name = card.xpath(".//span[contains(@class, 'fiohW')]/text()").get()
        if not name:
            name = card.xpath(".//a[contains(@class, 'BMQDV')]//text()").get()
        return name.strip() if name else "Anónimo"
    
    def _extract_rating(self, card: Selector) -> float:
        rating_text = card.xpath(".//svg[contains(@class, 'UctUV') or contains(@class, 'evwcZ')]//title/text()").get("0 of 5 bubbles")
        try:
            return float(rating_text.split("of")[0].strip())
        except (ValueError, IndexError):
            return 0.0
    
    def _extract_title(self, card: Selector) -> str:
        title = card.xpath(".//div[contains(@class, 'ncFvv')]//span[contains(@class, 'yCeTE')]/text()").get()
        if not title:
            title = card.xpath(".//a[contains(@class, 'BMQDV')]//span[contains(@class, 'yCeTE')]/text()").get()
        if not title:
            title = card.xpath(".//span[contains(@class, 'yCeTE') and not(ancestor::div[contains(@class, 'KxBGd')])]/text()").get()
        return title.strip() if title else "Sin título"

    def _extract_text(self, card: Selector) -> str:
        texts = card.xpath(".//div[contains(@class, 'KxBGd')]//text()").getall()
        return " ".join(t.strip() for t in texts if t.strip()) or "Sin texto"

    def _extract_location(self, card: Selector) -> str:
        location = card.xpath(".//div[contains(@class, 'vYLts')]//span[1]/text()").get("")
        return location.strip() if location and not any(c.isdigit() for c in location) else "Sin ubicación"

    def _extract_contributions(self, card: Selector) -> int:
        contrib_text = card.xpath(".//div[contains(@class, 'vYLts')]//span[contains(text(), 'contribut')]/text()").get("0")
        return int(re.sub(r'\D', '', contrib_text)) if contrib_text else 0

    def _extract_visit_date(self, card: Selector) -> str:
        date_info = card.xpath(".//div[contains(@class, 'RpeCd')]/text()").get("")
        return date_info.split('•')[0].strip() if '•' in date_info else date_info.strip() or "Sin fecha"

    def _extract_written_date(self, card: Selector) -> str:
        date_text = card.xpath(".//div[contains(@class, 'TreSq')]//div[contains(@class, 'ncFvv')]/text()").get("")
        return date_text.replace("Written ", "").strip() if date_text.startswith("Written ") else date_text.strip()

    def _extract_companion(self, card: Selector) -> str:
        companion_text = card.xpath(".//div[contains(@class, 'RpeCd')]/text()").get("")
        return companion_text.split('•')[1].strip() if '•' in companion_text else "Sin información"

    def _is_translated(self, card: Selector) -> bool:
        return "translated" in card.get().lower()

    def calculate_english_pages(self, selector: Selector) -> int:
        """Calcula el número de páginas en inglés basado en el total de reseñas"""
        total_reviews = self.extract_total_reviews(selector)
        english_reviews = self.extract_english_reviews(selector)
        if english_reviews == 0:
            return 0
        return (english_reviews + 9) // 10  # Redondeo hacia arriba

    def extract_total_reviews(self, selector: Selector) -> int:
        """Versión mejorada para extraer el total de reseñas"""
        # Intento 1: Extraer del texto de resultados
        results_text = selector.css('div.Ci::text').get('')
        if 'of' in results_text:
            match = re.search(r'of\s+([\d,]+)', results_text)
            if match:
                return int(match.group(1).replace(',', ''))

        # Intento 2: Buscar en todo el HTML como fallback
        all_matches = re.findall(r'showing.*?results.*?of.*?([\d,]+)', selector.get(), re.IGNORECASE)
        if all_matches:
            try:
                return int(all_matches[0].replace(',', ''))
            except (ValueError, IndexError):
                pass
              
        # Intento 3: Usar el contador de la pestaña de reseñas
        tab_count = selector.css('a[data-tab-name="Reviews"] span::text').get('0')
        return int(tab_count.replace(',', '')) if tab_count else 0

    def extract_english_reviews(self, selector: Selector) -> int:
        """Identifica reseñas en inglés (versión corregida)"""
        language_button = selector.css('button.Datwj[aria-haspopup="listbox"] .biGQs._P::text').get('')
        if "English" in language_button:
            return self.extract_total_reviews(selector)
        return 0

    def validate_review(self, review: Dict) -> bool:
        """Valida que la reseña tenga los campos mínimos requeridos"""
        return bool(review.get("title")) and bool(review.get("review_text")) and review.get("rating", 0) > 0