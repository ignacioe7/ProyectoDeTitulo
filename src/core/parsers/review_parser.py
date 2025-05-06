from dataclasses import dataclass
from typing import List, Dict, Optional
from parsel import Selector # Para parsear
from loguru import logger as log # Logs
import re # Regex

@dataclass
class ReviewParserConfig:
  max_retries: int = 3
  min_delay: float = 1.0
  max_delay: float = 5.0

class ReviewParser:
  """Parser completo para extracción de reseñas de TripAdvisor"""

  def __init__(self, config: ReviewParserConfig = None):
    self.config = config or ReviewParserConfig()
    self.problematic_urls = [] # Guardamos URLs que den problemas

  def parse_reviews_page(self, html: str, url: str) -> List[Dict]:
    """Parsea todas las reseñas de una página HTML"""
    selector = Selector(html)
    # Buscamos cada tarjeta de reseña y la parseamos
    return [self._parse_review_card(card) for card in selector.xpath("//div[@data-automation='reviewCard']")]

  def _parse_review_card(self, card: Selector) -> Dict:
    """Extrae todos los campos de una reseña individual"""
    try:
      # Llamamos a cada método de extracción
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
      log.error(f"Error parseando tarjeta de reseña: {e}")
      return {} # Devolvemos diccionario vacío si falla

  # --- Métodos de extracción específicos ---
  def _extract_username(self, card: Selector) -> str:
    # Intentamos varios selectores para el nombre
    name = card.xpath(".//a[contains(@class, 'BMQDV') and contains(@class, 'ukgoS')]/text()").get()
    if not name:
      name = card.xpath(".//span[contains(@class, 'fiohW')]/text()").get()
    if not name:
      name = card.xpath(".//a[contains(@class, 'BMQDV')]//text()").get()
    return name.strip() if name else "Anónimo" # Default si no encontramos

  def _extract_rating(self, card: Selector) -> float:
    # Sacamos el texto del título del SVG (ej: '4.0 of 5 bubbles')
    rating_text = card.xpath(".//svg[contains(@class, 'UctUV') or contains(@class, 'evwcZ')]//title/text()").get("0 of 5 bubbles")
    try:
      # Cogemos la parte antes de 'of' y la convertimos a float
      return float(rating_text.split("of")[0].strip())
    except (ValueError, IndexError):
      return 0.0 # Default 0 si falla

  def _extract_title(self, card: Selector) -> str:
    # Probamos varios selectores para el título
    title = card.xpath(".//div[contains(@class, 'ncFvv')]//span[contains(@class, 'yCeTE')]/text()").get()
    if not title:
      title = card.xpath(".//a[contains(@class, 'BMQDV')]//span[contains(@class, 'yCeTE')]/text()").get()
    if not title: # Último intento
      title = card.xpath(".//span[contains(@class, 'yCeTE') and not(ancestor::div[contains(@class, 'KxBGd')])]/text()").get()
    return title.strip() if title else "Sin título"

  def _extract_text(self, card: Selector) -> str:
    # Cogemos todo el texto dentro del div principal del texto
    texts = card.xpath(".//div[contains(@class, 'KxBGd')]//text()").getall()
    # Unimos los trozos, quitando espacios extra
    return " ".join(t.strip() for t in texts if t.strip()) or "Sin texto"

  def _extract_location(self, card: Selector) -> str:
    # Sacamos la ubicación del usuario
    location = card.xpath(".//div[contains(@class, 'vYLts')]//span[1]/text()").get("")
    # Validamos que no sea un número (a veces se cuela el número de contribuciones)
    return location.strip() if location and not any(c.isdigit() for c in location) else "Sin ubicación"

  def _extract_contributions(self, card: Selector) -> int:
    # Buscamos el texto que contiene 'contribut'
    contrib_text = card.xpath(".//div[contains(@class, 'vYLts')]//span[contains(text(), 'contribut')]/text()").get("0")
    # Quitamos todo lo que no sea dígito y convertimos a int
    return int(re.sub(r'\D', '', contrib_text)) if contrib_text else 0

  def _extract_visit_date(self, card: Selector) -> str:
    # La fecha de visita suele estar antes del '•'
    date_info = card.xpath(".//div[contains(@class, 'RpeCd')]/text()").get("")
    return date_info.split('•')[0].strip() if '•' in date_info else date_info.strip() or "Sin fecha"

  def _extract_written_date(self, card: Selector) -> str:
    # La fecha en que se escribió
    date_text = card.xpath(".//div[contains(@class, 'TreSq')]//div[contains(@class, 'ncFvv')]/text()").get("")
    # 'Written ' lo quitamos
    return date_text.replace("Written ", "").strip() if date_text.startswith("Written ") else date_text.strip()

  def _extract_companion(self, card: Selector) -> str:
    # El tipo de acompañante suele estar después del '•'
    companion_text = card.xpath(".//div[contains(@class, 'RpeCd')]/text()").get("")
    return companion_text.split('•')[1].strip() if '•' in companion_text else "Sin información"

  def _is_translated(self, card: Selector) -> bool:
    # Comprueba si la reseña indica que está traducida
    return "translated" in card.get().lower()

  # --- Métodos para métricas generales de la página ---
  def calculate_english_pages(self, selector: Selector) -> int:
    """Calcula el número de páginas en inglés basado en el total de reseñas"""
    total_reviews = self.extract_total_reviews(selector) # Total general
    english_reviews = self.extract_english_reviews(selector) # Total en inglés
    if english_reviews == 0:
      return 0
    # Dividimos entre 10 (reseñas por pág) y redondeamos arriba
    return (english_reviews + 9) // 10

  def extract_total_reviews(self, selector: Selector) -> int:
    """Versión mejorada para extraer el total de reseñas"""
    # Intento 1: Del texto '1-10 of 1,234 reviews'
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
        pass # Si falla, seguimos

    # Intento 3: Usar el contador de la pestaña de reseñas
    tab_count = selector.css('a[data-tab-name="Reviews"] span::text').get('0')
    # Quitamos comas y convertimos
    return int(tab_count.replace(',', '')) if tab_count else 0

  def extract_english_reviews(self, selector: Selector) -> int:
    """Identifica reseñas en inglés (versión corregida)"""
    # Buscamos el texto del botón de idioma
    language_button = selector.css('button.Datwj[aria-haspopup="listbox"] .biGQs._P::text').get('')
    # Si contiene 'English', asumimos que el total mostrado es en inglés
    if "English" in language_button:
      return self.extract_total_reviews(selector)
    return 0 # Si no, cero en inglés

  # --- Validación ---
  def validate_review(self, review: Dict) -> bool:
    """Valida que la reseña tenga los campos mínimos requeridos"""
    # Necesitamos título, texto y un rating > 0
    return bool(review.get("title")) and bool(review.get("review_text")) and review.get("rating", 0) > 0