from dataclasses import dataclass
from typing import List, Dict, Optional
from parsel import Selector 
from loguru import logger as log 
import re 

@dataclass
class ReviewParserConfig:
  """config basica del parser"""
  max_retries: int = 3 
  min_delay: float = 1.0 
  max_delay: float = 5.0 

class ReviewParser:
  """extrae reseñas de html de tripadvisor
  
  uso:
    parser = ReviewParser()
    reviews = parser.parse_reviews_page(html_content, url)
  """

  REVIEWS_PER_PAGE = 10 # numero de reseñas por pagina

  def __init__(self, config: Optional[ReviewParserConfig] = None): 
    self.config = config or ReviewParserConfig()
    self.problematic_urls: List[str] = [] # urls que fallan

  def parse_reviews_page(self, html: str, url: str) -> List[Dict]: 
    """extrae todas las reseñas de una pagina"""
    selector = Selector(html)
    review_cards = selector.xpath("//div[@data-automation='reviewCard']") 
    
    parsed_reviews: List[Dict] = []
    for card in review_cards:
      parsed_review = self._parse_review_card(card)
      if parsed_review: 
        parsed_reviews.append(parsed_review)
    
    log.debug(f"parseadas {len(parsed_reviews)} reseñas de {len(review_cards)} tarjetas")
    return parsed_reviews

  def _parse_review_card(self, card: Selector) -> Optional[Dict]:
    """extrae datos de una tarjeta de reseña individual"""
    try:
      return {
        "review_id": self._extract_review_id(card),
        "username": self._extract_username(card), 
        "rating": self._extract_rating(card), 
        "title": self._extract_title(card), 
        "review_text": self._extract_text(card), 
        "location": self._extract_location(card), 
        "contributions": self._extract_contributions(card), 
        "visit_date": self._extract_visit_date(card), 
        "written_date": self._extract_written_date(card), 
        "companion_type": self._extract_companion(card), 
      }
    except Exception as e:
      log.debug(f"fallo parseando tarjeta: {e}")
      return None 

  # === EXTRACTORES ===

  def _extract_review_id(self, card: Selector) -> str:
    """extrae el ID único de la reseña desde el enlace ShowUserReviews"""
    
    # busca el enlace que contiene el ID de la reseña
    review_link = card.xpath('.//a[contains(@href, "/ShowUserReviews-")]/@href').get()
    
    if review_link:
        match = re.search(r'-r(\d+)-', review_link)
        if match:
            return match.group(1)  # Devuelve solo el número
    
    return ""

  def _extract_username(self, card: Selector) -> str:
    """saca el nombre del usuario"""
    # selector principal
    name = card.xpath(".//a[contains(@class, 'BMQDV') and contains(@class, 'ukgoS')]/text()").get()
    if not name: 
      # fallback 1
      name = card.xpath(".//span[contains(@class, 'fiohW')]/text()").get()
    if not name: 
      # fallback 2
      name = card.xpath(".//a[contains(@class, 'BMQDV')]//text()").get()
    return name.strip() if name else "Anónimo"

  def _extract_rating(self, card: Selector) -> float:
    """extrae rating de 0-5 estrellas"""
    rating_text = card.xpath(".//svg[contains(@class, 'UctUV') or contains(@class, 'evwcZ')]//title/text()").get("0 of 5 bubbles")
    try:
      # formato: "4 of 5 bubbles" -> saca el 4
      rating_value = rating_text.split("of")[0].strip()
      return float(rating_value)
    except (ValueError, IndexError):
      return 0.0 

  def _extract_title(self, card: Selector) -> str:
    """titulo de la reseña"""
    title = card.xpath(".//div[contains(@class, 'ncFvv')]//span[contains(@class, 'yCeTE')]/text()").get()
    if not title: 
      title = card.xpath(".//a[contains(@class, 'BMQDV')]//span[contains(@class, 'yCeTE')]/text()").get()
    if not title: 
      title = card.xpath(".//span[contains(@class, 'yCeTE') and not(ancestor::div[contains(@class, 'KxBGd')])]/text()").get()
    return title.strip() if title else "Sin título"

  def _extract_text(self, card: Selector) -> str:
    """texto principal de la reseña"""
    texts = card.xpath(".//div[contains(@class, 'KxBGd')]//text()").getall()
    full_text = " ".join(t.strip() for t in texts if t.strip())
    return full_text or "Sin texto"

  def _extract_location(self, card: Selector) -> str:
    """ubicacion del usuario si esta disponible"""
    location = card.xpath(".//div[contains(@class, 'vYLts')]//span[1]/text()").get("")
    # evitar que se cuele el numero de contribuciones
    return location.strip() if location and not location.strip().isdigit() else "Sin ubicación"

  def _extract_contributions(self, card: Selector) -> int:
    """numero de contribuciones del usuario"""
    contrib_text = card.xpath(".//div[contains(@class, 'vYLts')]//span[contains(text(), 'contribut') or contains(text(), 'reseña') or contains(text(), 'review')]/text()").get("0")
    # solo digitos
    digits = re.sub(r'\D', '', contrib_text)
    return int(digits) if digits else 0

  def _extract_visit_date(self, card: Selector) -> str:
    """fecha que dice el usuario que visito"""
    date_info = card.xpath(".//div[contains(@class, 'RpeCd')]/text()").get("")
    # formato: "Date of visit: Mayo 2023 • En pareja"
    if '•' in date_info:
      return date_info.split('•')[0].strip().replace("Date of visit:", "").strip()
    return date_info.strip().replace("Date of visit:", "").strip() or "Sin fecha"

  def _extract_written_date(self, card: Selector) -> str:
    """fecha en que se escribio la reseña"""
    date_text = card.xpath(".//div[contains(@class, 'TreSq')]//div[contains(@class, 'ncFvv')]/text()").get("")
    # quitar prefijos comunes
    if date_text.startswith("Written "):
      return date_text.replace("Written ", "").strip()
    if date_text.startswith("Escrita el "): 
      return date_text.replace("Escrita el ", "").strip()
    return date_text.strip()

  def _extract_companion(self, card: Selector) -> str:
    """tipo de acompañante (pareja familia etc)"""
    companion_text = card.xpath(".//div[contains(@class, 'RpeCd')]/text()").get("")
    # formato: "Date of visit: Mayo 2023 • En pareja"
    if '•' in companion_text and len(companion_text.split('•')) > 1:
      return companion_text.split('•')[1].strip()
    return "Sin información"

  # === METRICAS DE PAGINA ===
  
  def calculate_english_pages(self, selector: Selector) -> int:
    """calcula cuantas paginas hay de reseñas en ingles"""
    english_reviews_count = self.extract_english_reviews_count(selector) 
    if english_reviews_count == 0:
      return 0
    # redondeo hacia arriba asumiendo 10 por pagina
    return (english_reviews_count + 9) // 10

  def extract_total_reviews_count(self, selector: Selector) -> int:
    """total de reseñas en todos los idiomas"""
    # intento 1: texto tipo "1-10 of 1,234 reviews"
    results_text = selector.css('div.Ci::text').get('') 
    if 'of' in results_text:
      match = re.search(r'of\s+([\d,]+)', results_text)
      if match:
        try:
          return int(match.group(1).replace(',', ''))
        except ValueError:
          pass 

    # intento 2: buscar en todo el html
    all_text = selector.get()
    matches = re.findall(r'showing results \d+-\d+ of ([\d,]+)', all_text, re.IGNORECASE)
    if not matches: 
        matches = re.findall(r'([\d,]+) reviews', all_text, re.IGNORECASE)

    if matches:
      try:
        # tomar el numero mas grande
        counts = [int(m.replace(',', '')) for m in matches]
        return max(counts) if counts else 0
      except ValueError:
        pass 

    return 0 

  def extract_english_reviews_count(self, selector: Selector) -> int:
    """numero de reseñas especificamente en ingles"""
    # busca el boton de idioma tipo 'English (1,234)'
    lang_button_text = selector.css('button.Datwj[aria-haspopup="listbox"]::attr(aria-label)').get('') 
    if not lang_button_text: 
        # fallback a texto visible
        lang_button_text = selector.css('button.Datwj[aria-haspopup="listbox"] .biGQs._P::text').get('')

    # buscar patron "English (1,234)"
    match = re.search(r'English\s*\((\d{1,3}(?:,\d{3})*)\)', lang_button_text, re.IGNORECASE)
    if match:
      try:
        return int(match.group(1).replace(',', ''))
      except ValueError:
        return 0 
    return 0 

  # === VALIDACION ===
  
  def validate_review(self, review: Dict) -> bool:
    """valida que la reseña tenga datos minimos"""
    # necesita id y texto no vacio
    has_id = bool(review.get("review_id"))
    has_text = bool(review.get("review_text", "").strip())
    return has_id and has_text