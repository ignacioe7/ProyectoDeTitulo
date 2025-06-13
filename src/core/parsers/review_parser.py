from dataclasses import dataclass
from typing import List, Dict, Optional
from parsel import Selector 
from loguru import logger as log 
import re 

# Configuración para controlar el comportamiento del parser de reseñas
@dataclass
class ReviewParserConfig:
  max_retries: int = 3         # Intentos máximos antes de fallar
  min_delay: float = 1.0       # Tiempo mínimo entre requests
  max_delay: float = 5.0       # Tiempo máximo entre requests

# ========================================================================================================
#                                              PARSER PRINCIPAL
# ========================================================================================================

# Parser especializado para extraer reseñas de TripAdvisor
# Convierte HTML en datos estructurados de usuarios, ratings y contenido de texto
class ReviewParser:

  REVIEWS_PER_PAGE = 10 # Reseñas estándar por página en TripAdvisor

  def __init__(self, config: Optional[ReviewParserConfig] = None): 
    self.config = config or ReviewParserConfig()
    self.problematic_urls: List[str] = [] # URLs que han presentado errores

# ========================================================================================================
#                                              PARSEO DE PÁGINA
# ========================================================================================================

  # PROCESA UNA PÁGINA COMPLETA Y EXTRAE TODAS LAS RESEÑAS DISPONIBLES
  def parse_reviews_page(self, html: str, url: str) -> List[Dict]: 
    selector = Selector(html)
    # Localiza tarjetas de reseña por atributo data-automation
    review_cards = selector.xpath("//div[@data-automation='reviewCard']") 
    
    parsed_reviews: List[Dict] = []
    for card in review_cards:
      parsed_review = self._parse_review_card(card)
      if parsed_review: 
        parsed_reviews.append(parsed_review)
    
    log.debug(f"parseadas {len(parsed_reviews)} reseñas de {len(review_cards)} tarjetas")
    return parsed_reviews

# ========================================================================================================
#                                            PARSEO DE TARJETA
# ========================================================================================================

  # EXTRAE TODOS LOS CAMPOS DE DATOS DE UNA TARJETA DE RESEÑA
  def _parse_review_card(self, card: Selector) -> Optional[Dict]:
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

# ========================================================================================================
#                                            EXTRAER ID DE RESEÑA
# ========================================================================================================

  # OBTIENE EL IDENTIFICADOR NUMÉRICO ÚNICO DE LA RESEÑA
  def _extract_review_id(self, card: Selector) -> str:
    # Busca enlaces con patrón ShowUserReviews en el href
    review_link = card.xpath('.//a[contains(@href, "/ShowUserReviews-")]/@href').get()
    
    if review_link:
        # Extrae ID numérico usando expresión regular
        match = re.search(r'-r(\d+)-', review_link)
        if match:
            return match.group(1)
    
    return ""

# ========================================================================================================
#                                           EXTRAER NOMBRE DE USUARIO
# ========================================================================================================

  # OBTIENE EL NOMBRE DEL AUTOR DE LA RESEÑA
  def _extract_username(self, card: Selector) -> str:
    # Busca por clases CSS específicas de enlaces de usuario
    name = card.xpath(".//a[contains(@class, 'BMQDV') and contains(@class, 'ukgoS')]/text()").get()
    if not name: 
      # Alternativa con spans para layouts diferentes
      name = card.xpath(".//span[contains(@class, 'fiohW')]/text()").get()
    if not name: 
      # Fallback más general solo con clase BMQDV
      name = card.xpath(".//a[contains(@class, 'BMQDV')]//text()").get()
    return name.strip() if name else "Anónimo"

# ========================================================================================================
#                                            EXTRAER CALIFICACIÓN
# ========================================================================================================

  # OBTIENE LA PUNTUACIÓN EN ESCALA DE 0 A 5 ESTRELLAS
  def _extract_rating(self, card: Selector) -> float:
    rating_text = card.xpath(".//svg[contains(@class, 'UctUV') or contains(@class, 'evwcZ')]//title/text()").get("0 of 5 bubbles")
    try:
      # Parsea formato "4 of 5 bubbles" tomando el primer número
      rating_value = rating_text.split("of")[0].strip()
      return float(rating_value)
    except (ValueError, IndexError):
      return 0.0

# ========================================================================================================
#                                              EXTRAER TÍTULO
# ========================================================================================================

  # OBTIENE EL TÍTULO O ENCABEZADO DE LA RESEÑA
  def _extract_title(self, card: Selector) -> str:
    title = card.xpath(".//div[contains(@class, 'ncFvv')]//span[contains(@class, 'yCeTE')]/text()").get()
    if not title: 
      # Busca títulos dentro de enlaces
      title = card.xpath(".//a[contains(@class, 'BMQDV')]//span[contains(@class, 'yCeTE')]/text()").get()
    if not title: 
      # Busca spans excluyendo contenedores de texto
      title = card.xpath(".//span[contains(@class, 'yCeTE') and not(ancestor::div[contains(@class, 'KxBGd')])]/text()").get()
    return title.strip() if title else "Sin título"

# ========================================================================================================
#                                              EXTRAER TEXTO
# ========================================================================================================

  # OBTIENE TODO EL CONTENIDO DE TEXTO DE LA RESEÑA
  def _extract_text(self, card: Selector) -> str:
    # Extrae todos los nodos de texto del contenedor principal
    texts = card.xpath(".//div[contains(@class, 'KxBGd')]//text()").getall()
    full_text = " ".join(t.strip() for t in texts if t.strip())
    return full_text or "Sin texto"

# ========================================================================================================
#                                             EXTRAER UBICACIÓN
# ========================================================================================================

  # OBTIENE LA UBICACIÓN GEOGRÁFICA DEL USUARIO
  def _extract_location(self, card: Selector) -> str:
    location = card.xpath(".//div[contains(@class, 'vYLts')]//span[1]/text()").get("")
    # Filtra valores numéricos que son conteos de contribuciones
    return location.strip() if location and not location.strip().isdigit() else "Sin ubicación"

# ========================================================================================================
#                                           EXTRAER CONTRIBUCIONES
# ========================================================================================================

  # OBTIENE EL NÚMERO TOTAL DE CONTRIBUCIONES DEL USUARIO
  def _extract_contributions(self, card: Selector) -> int:
    # Busca texto que mencione contribuciones en múltiples idiomas
    contrib_text = card.xpath(".//div[contains(@class, 'vYLts')]//span[contains(text(), 'contribut') or contains(text(), 'reseña') or contains(text(), 'review')]/text()").get("0")
    # Extrae solo caracteres numéricos
    digits = re.sub(r'\D', '', contrib_text)
    return int(digits) if digits else 0

# ========================================================================================================
#                                            EXTRAER FECHA DE VISITA
# ========================================================================================================

  # OBTIENE LA FECHA EN QUE EL USUARIO VISITÓ EL LUGAR
  def _extract_visit_date(self, card: Selector) -> str:
    date_info = card.xpath(".//div[contains(@class, 'RpeCd')]/text()").get("")
    # Separa fecha de tipo de compañía usando el separador bullet
    if '•' in date_info:
      return date_info.split('•')[0].strip().replace("Date of visit:", "").strip()
    return date_info.strip().replace("Date of visit:", "").strip() or "Sin fecha"

# ========================================================================================================
#                                          EXTRAER FECHA DE ESCRITURA
# ========================================================================================================

  # OBTIENE LA FECHA EN QUE SE REDACTÓ LA RESEÑA
  def _extract_written_date(self, card: Selector) -> str:
    date_text = card.xpath(".//div[contains(@class, 'TreSq')]//div[contains(@class, 'ncFvv')]/text()").get("")
    # Limpia prefijos en inglés y español
    if date_text.startswith("Written "):
      return date_text.replace("Written ", "").strip()
    if date_text.startswith("Escrita el "): 
      return date_text.replace("Escrita el ", "").strip()
    return date_text.strip()

# ========================================================================================================
#                                            EXTRAER ACOMPAÑANTE
# ========================================================================================================

  # OBTIENE EL TIPO DE COMPAÑÍA DURANTE LA VISITA
  def _extract_companion(self, card: Selector) -> str:
    companion_text = card.xpath(".//div[contains(@class, 'RpeCd')]/text()").get("")
    # Toma la segunda parte después del separador bullet
    if '•' in companion_text and len(companion_text.split('•')) > 1:
      return companion_text.split('•')[1].strip()
    return "Sin información"

# ========================================================================================================
#                                          CALCULAR PÁGINAS EN INGLÉS
# ========================================================================================================

  # CALCULA EL NÚMERO DE PÁGINAS NECESARIAS PARA RESEÑAS EN INGLÉS
  def calculate_english_pages(self, selector: Selector) -> int:
    english_reviews_count = self.extract_english_reviews_count(selector) 
    if english_reviews_count == 0:
      return 0
    # División con redondeo hacia arriba para 10 reseñas por página
    return (english_reviews_count + 9) // 10

# ========================================================================================================
#                                            EXTRAER TOTAL DE RESEÑAS
# ========================================================================================================

  # OBTIENE EL CONTEO TOTAL DE RESEÑAS EN TODOS LOS IDIOMAS
  def extract_total_reviews_count(self, selector: Selector) -> int:
    # Estrategia 1: buscar en indicador de resultados
    results_text = selector.css('div.Ci::text').get('') 
    if 'of' in results_text:
      match = re.search(r'of\s+([\d,]+)', results_text)
      if match:
        try:
          return int(match.group(1).replace(',', ''))
        except ValueError:
          pass 

    # Estrategia 2: búsqueda por regex en todo el HTML
    all_text = selector.get()
    matches = re.findall(r'showing results \d+-\d+ of ([\d,]+)', all_text, re.IGNORECASE)
    if not matches: 
        matches = re.findall(r'([\d,]+) reviews', all_text, re.IGNORECASE)

    if matches:
      try:
        # Retorna el valor más alto encontrado
        counts = [int(m.replace(',', '')) for m in matches]
        return max(counts) if counts else 0
      except ValueError:
        pass 

    return 0

# ========================================================================================================
#                                            EXTRAER RESEÑAS EN INGLÉS
# ========================================================================================================

  # OBTIENE EL CONTEO ESPECÍFICO DE RESEÑAS EN IDIOMA INGLÉS
  def extract_english_reviews_count(self, selector: Selector) -> int:
    # Busca botón de filtro de idioma
    lang_button_text = selector.css('button.Datwj[aria-haspopup="listbox"]::attr(aria-label)').get('') 
    if not lang_button_text: 
        # Fallback al texto visible del botón
        lang_button_text = selector.css('button.Datwj[aria-haspopup="listbox"] .biGQs._P::text').get('')

    # Extrae número del formato "English (1,234)"
    match = re.search(r'English\s*\((\d{1,3}(?:,\d{3})*)\)', lang_button_text, re.IGNORECASE)
    if match:
      try:
        return int(match.group(1).replace(',', ''))
      except ValueError:
        return 0 
    return 0

# ========================================================================================================
#                                             VALIDAR RESEÑA
# ========================================================================================================

  # VERIFICA QUE UNA RESEÑA CONTENGA DATOS MÍNIMOS REQUERIDOS
  def validate_review(self, review: Dict) -> bool:
    # Requiere ID válido y contenido de texto para ser considerada útil
    has_id = bool(review.get("review_id"))
    has_text = bool(review.get("review_text", "").strip())
    return has_id and has_text