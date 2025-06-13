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
      
      # ✅ ACTUALIZADO: Múltiples selectores para diferentes estructuras
      review_link_selectors = [
          './/a[contains(@href, "/ShowUserReviews-")]/@href',          # Selector principal
          './/div[@class="biGQs _P fiohW qWPrE ncFvv fOtGX"]//a/@href', # Específico de b.html
          './/a[contains(@class, "BMQDV")]/@href',                     # Fallback
      ]
      
      review_link = ""
      for selector in review_link_selectors:
          review_link = card.xpath(selector).get()
          if review_link:
              break
      
      if review_link:
          # Buscar patrón: ShowUserReviews-g295425-d318302-r872425419-
          match = re.search(r'ShowUserReviews-[^-]+-[^-]+-r(\d+)-', review_link)
          if match:
              return match.group(1)  # Devuelve solo el número: "872425419"
          
          # Fallback: patrón anterior
          match = re.search(r'-r(\d+)-', review_link)
          if match:
              return match.group(1)
      
      # ⚠️ Log para debugging si no encuentra ID
      log.debug(f"No se pudo extraer review_id, href encontrado: {review_link}")
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
    """extrae rating de 0-5 estrellas (multilenguaje)"""
    # ✅ ACTUALIZADO: Soporte para múltiples clases CSS y formatos
    rating_selectors = [
        ".//svg[contains(@class, 'UctUV')]//title/text()",  # Formato original
        ".//svg[contains(@class, 'evwcZ')]//title/text()",  # Formato español
        ".//svg[@data-automation='bubbleRatingImage']//title/text()",  # Selector más específico
    ]
    
    rating_text = ""
    for selector in rating_selectors:
        rating_text = card.xpath(selector).get()
        if rating_text:
            break
    
    if not rating_text:
        rating_text = "0 of 5 bubbles"  # Fallback
    
    try:
        # ✅ SOPORTE MULTILENGUAJE para diferentes formatos
        rating_patterns = [
            r'(\d+)\s+of\s+5\s+bubbles',      # "4 of 5 bubbles" (inglés)
            r'(\d+)\s+de\s+5\s+burbujas',     # "3 de 5 burbujas" (español)
            r'(\d+)\s+sur\s+5\s+bulles',      # Francés
            r'(\d+)\s+von\s+5\s+blasen',      # Alemán
            r'(\d+)\s+di\s+5\s+bolle',        # Italiano
            r'(\d+)\s+van\s+5\s+bellen',      # Holandés
            r'(\d+)\s+av\s+5\s+bubblor',      # Sueco
            r'(\d+)\s+из\s+5\s+пузырей',      # Ruso
            r'(\d+)\s+من\s+5\s+فقاعات',       # Árabe
            r'(\d+)\s+de\s+5\s+bolhas',       # Portugués
            r'(\d+)\s+个\s+5\s+泡泡',          # Chino
            r'(\d+)\s+の\s+5\s+バブル',         # Japonés
            r'(\d+)\s+의\s+5\s+버블',          # Coreano
            r'(\d+)\s+/\s+5',                 # Formato genérico
            r'^(\d+)',                        # Solo el número al inicio
        ]
        
        for pattern in rating_patterns:
            match = re.search(pattern, rating_text, re.IGNORECASE)
            if match:
                return float(match.group(1))
                
    except (ValueError, IndexError, AttributeError):
        pass
    
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
    """numero de contribuciones del usuario (multilenguaje)"""
    # ✅ ACTUALIZADO: Múltiples selectores para diferentes estructuras
    contribution_selectors = [
        ".//div[contains(@class, 'vYLts')]//span[contains(@class, 'IugUm')]/text()",  # Español específico
        ".//div[contains(@class, 'vYLts')]//span[contains(text(), 'contribut') or contains(text(), 'reseña') or contains(text(), 'review')]/text()",
        ".//span[contains(text(), 'contribuciones')]/text()",  # Español
        ".//span[contains(text(), 'contributions')]/text()",   # Inglés
        ".//span[contains(text(), 'avis')]/text()",           # Francés
        ".//span[contains(text(), 'bewertungen')]/text()",    # Alemán
        ".//span[contains(text(), 'recensioni')]/text()",     # Italiano
        ".//span[contains(text(), 'beoordelingen')]/text()",  # Holandés
        ".//span[contains(text(), 'отзыв')]/text()",          # Ruso
        ".//span[contains(text(), 'مساهمات')]/text()",       # Árabe
        ".//span[contains(text(), 'contribuições')]/text()",  # Portugués
        ".//span[contains(text(), '贡献')]/text()",            # Chino
        ".//span[contains(text(), 'レビュー')]/text()",         # Japonés
        ".//span[contains(text(), '리뷰')]/text()",             # Coreano
    ]
    
    contrib_text = ""
    for selector in contribution_selectors:
        contrib_text = card.xpath(selector).get()
        if contrib_text:
            break
    
    if not contrib_text:
        contrib_text = "0"
    
    # ✅ EXTRAER NÚMEROS de diferentes formatos
    # Ejemplos: "278 contribuciones", "278 contributions", "278"
    digits = re.findall(r'\d+', contrib_text)
    if digits:
        # Tomar el primer número encontrado
        return int(digits[0])
    
    return 0

  def _extract_visit_date(self, card: Selector) -> str:
    """fecha que dice el usuario que visito (multilenguaje)"""
    date_info = card.xpath(".//div[contains(@class, 'RpeCd')]/text()").get("")
    
    if not date_info:
        # Selector alternativo
        date_info = card.xpath(".//div[contains(@class, 'ncFvv')]//text()[contains(., 'visit') or contains(., 'visita')]").get("")
    
    # ✅ LIMPIAR PREFIJOS EN MÚLTIPLES IDIOMAS
    prefixes_to_remove = [
        "Date of visit:",     # Inglés
        "Fecha de visita:",   # Español
        "Date de visite:",    # Francés
        "Besuchsdatum:",      # Alemán
        "Data della visita:", # Italiano
        "Datum bezoek:",      # Holandés
        "Дата посещения:",    # Ruso
        "تاريخ الزيارة:",      # Árabe
        "Data da visita:",    # Portugués
        "访问日期:",          # Chino
        "訪問日:",            # Japonés
        "방문 날짜:",          # Coreano
    ]
    
    for prefix in prefixes_to_remove:
        if date_info.startswith(prefix):
            date_info = date_info.replace(prefix, "").strip()
            break
    
    # Separar por • si hay información adicional
    if '•' in date_info:
        date_info = date_info.split('•')[0].strip()
    
    return date_info if date_info else "Sin fecha"
  
  def _extract_written_date(self, card: Selector) -> str:
    """fecha en que se escribio la reseña (multilenguaje)"""
    date_text = card.xpath(".//div[contains(@class, 'TreSq')]//div[contains(@class, 'ncFvv')]/text()").get("")
    
    if not date_text:
        # Selector alternativo
        date_text = card.xpath(".//div[contains(@class, 'ncFvv')]/text()[contains(., 'Written') or contains(., 'Escrita')]").get("")
    
    # ✅ QUITAR PREFIJOS EN MÚLTIPLES IDIOMAS
    prefixes_to_remove = [
        "Written ",           # Inglés
        "Escrita el ",        # Español
        "Écrit le ",          # Francés
        "Geschrieben ",       # Alemán
        "Scritta il ",        # Italiano
        "Geschreven ",        # Holandés
        "Написано ",          # Ruso
        "كتب في ",            # Árabe
        "Escrito em ",        # Portugués
        "撰写于 ",            # Chino
        "書かれた日 ",         # Japonés
        "작성일 ",            # Coreano
        
    ]
    
    for prefix in prefixes_to_remove:
        if date_text.startswith(prefix):
            date_text = date_text.replace(prefix, "").strip()
            break
    
    return date_text.strip() if date_text else "Sin fecha"

  def _extract_companion(self, card: Selector) -> str: 
    """tipo de acompañante (multilenguaje)"""
    companion_text = card.xpath(".//div[contains(@class, 'RpeCd')]/text()").get("")
    
    # Separar después del •
    if '•' in companion_text and len(companion_text.split('•')) > 1:
        companion = companion_text.split('•')[1].strip()
        
        # ✅ MAPEO DE TIPOS DE ACOMPAÑANTE a formato estándar (opcional)
        companion_mapping = {
            # Español
            "En pareja": "Pareja",
            "En familia": "Familia",
            "Con amigos": "Amigos",
            "Solo": "Solo",
            "Viaje de negocios": "Negocios",
            
            # Inglés
            "Couple": "Pareja",
            "Family": "Familia",
            "Friends": "Amigos",
            "Solo": "Solo",
            "Business": "Negocios",
            
            # Francés
            "En couple": "Pareja",
            "En famille": "Familia",
            "Entre amis": "Amigos",
            "Seul": "Solo",
            "Voyage d'affaires": "Negocios",

            # Alemán
            "Als Paar": "Pareja",
            "Mit der Familie": "Familia",
            "Mit Freunden": "Amigos",
            "Allein": "Solo",
            "Geschäftsreise": "Negocios",
            
            # Italiano
            "In coppia": "Pareja",
            "In famiglia": "Familia",
            "Con amici": "Amigos",
            "Da solo": "Solo",
            "Viaggio di lavoro": "Negocios",
            
            # Holandés
            "Als koppel": "Pareja",
            "Met familie": "Familia",
            "Met vrienden": "Amigos",
            "Alleen": "Solo",
            "Zakenreis": "Negocios",
            
            # Ruso
            "В паре": "Pareja",
            "С семьей": "Familia",
            "С друзьями": "Amigos",
            "Один": "Solo",
            "Деловая поездка": "Negocios",
            
            # Árabe
            "كزوجين": "Pareja",
            "مع العائلة": "Familia",
            "مع الأصدقاء": "Amigos",
            "وحدي": "Solo",
            "رحلة عمل": "Negocios",
            
            # Portugués
            "Em casal": "Pareja",
            "Em família": "Familia",
            "Com amigos": "Amigos",
            "Sozinho": "Solo",
            "Viagem de negócios": "Negocios",
            
            # Chino
            "作为伴侣": "Pareja",
            "与家人": "Familia",
            "与朋友": "Amigos",
            "独自一人": "Solo",
            "商务旅行": "Negocios",

            # Japonés
            "カップルで": "Pareja",
            "家族と": "Familia",
            "友人と": "Amigos",
            "一人で": "Solo",
            "ビジネス旅行": "Negocios",
            
            # Coreano
            "커플로": "Pareja",
            "가족과": "Familia",
            "친구와": "Amigos",
            "혼자": "Solo",
            "출장": "Negocios",
            
        }
        
        return companion_mapping.get(companion, companion)
    
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