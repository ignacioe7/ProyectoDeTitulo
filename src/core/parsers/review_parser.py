from dataclasses import dataclass
from typing import List, Dict, Optional
from parsel import Selector 
from loguru import logger as log 
import re 

# CONFIGURACION DEL PARSER
@dataclass
class ReviewParserConfig:
  max_retries: int = 3 
  min_delay: float = 1.0 
  max_delay: float = 5.0 

# ===============================================================
# PARSER PRINCIPAL DE RESEÑAS
# ===============================================================

class ReviewParser:
  # EXTRAE RESEÑAS DE HTML DE TRIPADVISOR
  # 
  # USO:
  #   parser = ReviewParser()
  #   reviews = parser.parse_reviews_page(html_content, url)

  REVIEWS_PER_PAGE = 10

  def __init__(self, config: Optional[ReviewParserConfig] = None): 
    self.config = config or ReviewParserConfig()
    self.problematic_urls: List[str] = []

  # ===============================================================
  # EXTRAER TODAS LAS RESEÑAS DE PAGINA
  # ===============================================================

  def parse_reviews_page(self, html: str, url: str) -> List[Dict]: 
    # Extrae todas las reseñas de una pagina HTML
    # Devuelve lista de diccionarios con datos estructurados
    selector = Selector(html)
    review_cards = selector.xpath("//div[@data-automation='reviewCard']") 
    
    parsed_reviews: List[Dict] = []
    for card in review_cards:
      parsed_review = self._parse_review_card(card)
      if parsed_review: 
        parsed_reviews.append(parsed_review)
    
    log.debug(f"parseadas {len(parsed_reviews)} reseñas de {len(review_cards)} tarjetas")
    return parsed_reviews

  # ===============================================================
  # EXTRAER DATOS DE TARJETA INDIVIDUAL
  # ===============================================================

  def _parse_review_card(self, card: Selector) -> Optional[Dict]:
    # Extrae todos los datos de una tarjeta de reseña
    # Maneja errores y devuelve None si falla
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

  # ===============================================================
  # EXTRAER ID DE RESEÑA
  # ===============================================================

  def _extract_review_id(self, card: Selector) -> str:
    # Extrae ID unico desde enlace ShowUserReviews
    # Busca patron r872425419 en la URL
    
    review_link_selectors = [
      './/a[contains(@href, "/ShowUserReviews-")]/@href',
      './/div[@class="biGQs _P fiohW qWPrE ncFvv fOtGX"]//a/@href',
      './/a[contains(@class, "BMQDV")]/@href',
    ]
    
    review_link = ""
    for selector in review_link_selectors:
      review_link = card.xpath(selector).get()
      if review_link:
        break
    
    if review_link:
      # Buscar patron: ShowUserReviews-g295425-d318302-r872425419-
      match = re.search(r'ShowUserReviews-[^-]+-[^-]+-r(\d+)-', review_link)
      if match:
        return match.group(1)
        
      # Patron alternativo
      match = re.search(r'-r(\d+)-', review_link)
      if match:
        return match.group(1)
    
    log.debug(f"No se pudo extraer review_id, href encontrado: {review_link}")
    return ""

  # ===============================================================
  # EXTRAER NOMBRE DE USUARIO
  # ===============================================================

  def _extract_username(self, card: Selector) -> str:
    # Obtiene nombre del usuario que escribio la reseña
    # Usa multiples selectores como fallback
    name = card.xpath(".//a[contains(@class, 'BMQDV') and contains(@class, 'ukgoS')]/text()").get()
    if not name: 
      name = card.xpath(".//span[contains(@class, 'fiohW')]/text()").get()
    if not name: 
      name = card.xpath(".//a[contains(@class, 'BMQDV')]//text()").get()
    return name.strip() if name else "Anónimo"
  
  # ===============================================================
  # EXTRAER RATING DE ESTRELLAS
  # ===============================================================

  def _extract_rating(self, card: Selector) -> float:
    # Extrae rating de 0-5 estrellas en formato multilenguaje
    # Busca texto tipo "4 of 5 bubbles" o equivalente
    rating_selectors = [
      ".//svg[contains(@class, 'UctUV')]//title/text()",
      ".//svg[contains(@class, 'evwcZ')]//title/text()",
      ".//svg[@data-automation='bubbleRatingImage']//title/text()",
    ]
    
    rating_text = ""
    for selector in rating_selectors:
      rating_text = card.xpath(selector).get()
      if rating_text:
        break
    
    if not rating_text:
      rating_text = "0 of 5 bubbles"
    
    try:
      # Patrones para diferentes idiomas
      rating_patterns = [
        r'(\d+)\s+of\s+5\s+bubbles',      # Ingles
        r'(\d+)\s+de\s+5\s+burbujas',     # Español
        r'(\d+)\s+sur\s+5\s+bulles',      # Frances
        r'(\d+)\s+von\s+5\s+blasen',      # Aleman
        r'(\d+)\s+di\s+5\s+bolle',        # Italiano
        r'(\d+)\s+van\s+5\s+bellen',      # Holandes
        r'(\d+)\s+av\s+5\s+bubblor',      # Sueco
        r'(\d+)\s+из\s+5\s+пузырей',      # Ruso
        r'(\d+)\s+من\s+5\s+فقاعات',       # Arabe
        r'(\d+)\s+de\s+5\s+bolhas',       # Portugues
        r'(\d+)\s+个\s+5\s+泡泡',          # Chino
        r'(\d+)\s+の\s+5\s+バブル',         # Japones
        r'(\d+)\s+의\s+5\s+버블',          # Coreano
        r'(\d+)\s+/\s+5',                 # Formato generico
        r'^(\d+)',                        # Solo numero al inicio
      ]
      
      for pattern in rating_patterns:
        match = re.search(pattern, rating_text, re.IGNORECASE)
        if match:
          return float(match.group(1))
            
    except (ValueError, IndexError, AttributeError):
      pass
    
    return 0.0

  # ===============================================================
  # EXTRAER TITULO DE RESEÑA
  # ===============================================================

  def _extract_title(self, card: Selector) -> str:
    # Obtiene titulo principal de la reseña
    # Evita seleccionar elementos de areas no deseadas
    title = card.xpath(".//div[contains(@class, 'ncFvv')]//span[contains(@class, 'yCeTE')]/text()").get()
    if not title: 
      title = card.xpath(".//a[contains(@class, 'BMQDV')]//span[contains(@class, 'yCeTE')]/text()").get()
    if not title: 
      title = card.xpath(".//span[contains(@class, 'yCeTE') and not(ancestor::div[contains(@class, 'KxBGd')])]/text()").get()
    return title.strip() if title else "Sin título"

  # ===============================================================
  # EXTRAER TEXTO PRINCIPAL
  # ===============================================================

  def _extract_text(self, card: Selector) -> str:
      # Obtiene contenido completo de la reseña
      # Maneja múltiples estructuras posibles
      
      # Selector principal
      texts = card.xpath(".//div[contains(@class, 'KxBGd')]//text()").getall()
      full_text = " ".join(t.strip() for t in texts if t.strip())
      
      # Si no encuentra texto, probar selector alternativo para yCeTE
      if not full_text or full_text == "Sin texto":
          texts = card.xpath(".//span[contains(@class, 'yCeTE')]/text()").getall()
          full_text = " ".join(t.strip() for t in texts if t.strip())
      
      return full_text or "Sin texto"

  # ===============================================================
  # EXTRAER UBICACION DEL USUARIO
  # ===============================================================

  
  def _extract_location(self, card: Selector) -> str:
    # Obtiene ubicacion geografica del usuario
    # Maneja casos con y sin contribuciones
    
    # Buscar contenedor principal
    vYLts_container = card.xpath(".//div[contains(@class, 'vYLts')]")
    if not vYLts_container:
      return "Sin ubicación"
    
    # Obtener todos los spans dentro del contenedor
    spans = vYLts_container[0].xpath(".//span")
    
    for span in spans:
      text = span.xpath("./text()").get("")
      if text and text.strip():
        # Verificar si NO es un número de contribuciones
        # Patrones de contribuciones en diferentes idiomas
        contribution_patterns = [
          r'\d+\s*(contribut|contribuição|contribution|avis|bewertung|recensioni|beoordelingen|отзыв|مساهمات|贡献|レビュー|리뷰|件の投稿|beiträge)',
          r'\d+\s*contribut',  # General para contribuciones
          r'^\d+$',            # Solo números
        ]
        
        is_contribution = False
        for pattern in contribution_patterns:
          if re.search(pattern, text.strip(), re.IGNORECASE):
            is_contribution = True
            break
        
        # Si no es contribución y no es solo dígitos, es ubicación
        if not is_contribution and not text.strip().isdigit():
          return text.strip()
    
    return "Sin ubicación"
  
  # ===============================================================
  # EXTRAER NUMERO DE CONTRIBUCIONES
  # ===============================================================
  
  def _extract_contributions(self, card: Selector) -> int:
    # Obtiene numero total de contribuciones del usuario
    # Maneja múltiples idiomas y formatos
    
    # Selectores mejorados que cubren más casos
    contribution_selectors = [
      # Selector principal por clase
      ".//div[contains(@class, 'vYLts')]//span[contains(@class, 'IugUm')]/text()",
      
      # Selectores por contenido de texto - más específicos
      ".//div[contains(@class, 'vYLts')]//span[contains(text(), 'contribut')]/text()",
      ".//div[contains(@class, 'vYLts')]//span[contains(text(), 'contribution')]/text()",
      ".//div[contains(@class, 'vYLts')]//span[contains(text(), 'contribuição')]/text()",
      ".//div[contains(@class, 'vYLts')]//span[contains(text(), 'reseña')]/text()",
      ".//div[contains(@class, 'vYLts')]//span[contains(text(), 'review')]/text()",
      ".//div[contains(@class, 'vYLts')]//span[contains(text(), 'avis')]/text()",
      ".//div[contains(@class, 'vYLts')]//span[contains(text(), 'bewertung')]/text()",
      ".//div[contains(@class, 'vYLts')]//span[contains(text(), 'beiträge')]/text()",
      ".//div[contains(@class, 'vYLts')]//span[contains(text(), 'recensioni')]/text()",
      ".//div[contains(@class, 'vYLts')]//span[contains(text(), 'beoordelingen')]/text()",
      ".//div[contains(@class, 'vYLts')]//span[contains(text(), 'отзыв')]/text()",
      ".//div[contains(@class, 'vYLts')]//span[contains(text(), 'مساهمات')]/text()",
      ".//div[contains(@class, 'vYLts')]//span[contains(text(), '贡献')]/text()",
      ".//div[contains(@class, 'vYLts')]//span[contains(text(), 'レビュー')]/text()",
      ".//div[contains(@class, 'vYLts')]//span[contains(text(), '件の投稿')]/text()",
      ".//div[contains(@class, 'vYLts')]//span[contains(text(), '리뷰')]/text()",
      
      # Selectores más generales como fallback
      ".//span[contains(text(), 'contribut')]/text()",
      ".//span[contains(text(), 'contribution')]/text()",
      ".//span[contains(text(), 'avis')]/text()",
      ".//span[contains(text(), 'bewertung')]/text()",
      ".//span[contains(text(), 'beiträge')]/text()",
      ".//span[contains(text(), 'recensioni')]/text()",
      ".//span[contains(text(), 'beoordelingen')]/text()",
      ".//span[contains(text(), 'отзыв')]/text()",
      ".//span[contains(text(), 'مساهمات')]/text()",
      ".//span[contains(text(), 'contribuições')]/text()",
      ".//span[contains(text(), '贡献')]/text()",
      ".//span[contains(text(), 'レビュー')]/text()",
      ".//span[contains(text(), '件の投稿')]/text()",
      ".//span[contains(text(), '리뷰')]/text()",
    ]
    
    contrib_text = ""
    for selector in contribution_selectors:
      contrib_text = card.xpath(selector).get()
      if contrib_text and contrib_text.strip():
        break
    
    if not contrib_text:
      contrib_text = "0"
    
    # Extraer números, manejando separadores de miles
    # Remover caracteres no numéricos excepto comas y puntos
    clean_text = re.sub(r'[^\d,.\s]', ' ', contrib_text)
    
    # Buscar patrones de números con separadores de miles
    number_patterns = [
      r'(\d{1,3}(?:[,.\s]\d{3})*)',  # 1,158 o 1.158 o 1 158
      r'(\d+)',                       # Cualquier número
    ]
    
    for pattern in number_patterns:
      matches = re.findall(pattern, clean_text)
      if matches:
        # Tomar el primer número encontrado
        number_str = matches[0]
        # Limpiar separadores de miles
        number_str = re.sub(r'[,.\s]', '', number_str)
        try:
          return int(number_str)
        except ValueError:
          continue
    
    return 0
  
  # ===============================================================
  # EXTRAER FECHA DE VISITA
  # ===============================================================

  def _extract_visit_date(self, card: Selector) -> str:
    # Obtiene fecha que dice el usuario que visito
    # Resultado estandarizado en formato YYYY-MM-DD
    date_info = card.xpath(".//div[contains(@class, 'RpeCd')]/text()").get("")
    
    if not date_info:
      date_info = card.xpath(".//div[contains(@class, 'ncFvv')]//text()[contains(., 'visit') or contains(., 'visita')]").get("")
    
    # Limpiar prefijos en multiples idiomas
    prefixes_to_remove = [
      "Date of visit:",     # Ingles
      "Fecha de visita:",   # Español
      "Date de visite:",    # Frances
      "Besuchsdatum:",      # Aleman
      "Data della visita:", # Italiano
      "Datum bezoek:",      # Holandes
      "Дата посещения:",    # Ruso
      "تاريخ الزيارة:",      # Arabe
      "Data da visita:",    # Portugues
      "访问日期:",          # Chino
      "訪問日:",            # Japones
      "방문 날짜:",          # Coreano
    ]
    
    for prefix in prefixes_to_remove:
      if date_info.startswith(prefix):
        date_info = date_info.replace(prefix, "").strip()
        break
    
    # Separar informacion adicional
    if '•' in date_info:
      date_info = date_info.split('•')[0].strip()
    
    raw_date = date_info if date_info else "Sin fecha"
    
    return self._standardize_date(raw_date, "visit")
  
  # ===============================================================
  # EXTRAER FECHA DE ESCRITURA
  # ===============================================================

  def _extract_written_date(self, card: Selector) -> str:
    # Obtiene fecha en que se escribio la reseña
    # Resultado estandarizado en formato YYYY-MM-DD
    date_text = card.xpath(".//div[contains(@class, 'TreSq')]//div[contains(@class, 'ncFvv')]/text()").get("")
    
    if not date_text:
      date_text = card.xpath(".//div[contains(@class, 'ncFvv')]/text()[contains(., 'Written') or contains(., 'Escrita')]").get("")
    
    # Quitar prefijos en multiples idiomas
    prefixes_to_remove = [
      "Written ",              # Ingles
      "Escrita el ",           # Español
      "Escrita a ",            # Portugues
      "Écrit le ",             # Frances
      "Geschrieben ",          # Aleman
      "Verfasst am ",          # Aleman alternativo
      "Scritta il ",           # Italiano
      "Geschreven ",           # Holandes
      "Написано ",             # Ruso
      "كتب في ",               # Arabe
      "Escrito em ",           # Portugues
      "撰写于 ",               # Chino
      "書かれた日 ",            # Japones
      "작성일 ",               # Coreano
    ]
    
    for prefix in prefixes_to_remove:
      if date_text.startswith(prefix):
        date_text = date_text.replace(prefix, "").strip()
        break
    
    raw_date = date_text.strip() if date_text else "Sin fecha"
    
    return self._standardize_date(raw_date, "written")
  
  # ===============================================================
  # EXTRAER TIPO DE COMPAÑIA
  # ===============================================================

  def _extract_companion(self, card: Selector) -> str: 
    # Obtiene tipo de acompañante estandarizado
    # Devuelve uno de: Couples, Friends, Family, Solo, Business
    companion_text = card.xpath(".//div[contains(@class, 'RpeCd')]/text()").get("")
    
    # Separar despues del separador
    if '•' in companion_text and len(companion_text.split('•')) > 1:
      companion = companion_text.split('•')[1].strip()
      
      return self._standardize_companion_type(companion)
    
    return "NO INFO"
  
  # ===============================================================
  # CALCULAR PAGINAS EN INGLES
  # ===============================================================

  def calculate_english_pages(self, selector: Selector) -> int:
    # Calcula cuantas paginas hay de reseñas en ingles
    # Asume 10 reseñas por pagina
    english_reviews_count = self.extract_english_reviews_count(selector) 
    if english_reviews_count == 0:
      return 0
    return (english_reviews_count + 9) // 10

  # ===============================================================
  # EXTRAER TOTAL DE RESEÑAS
  # ===============================================================

  def extract_total_reviews_count(self, selector: Selector) -> int:
    # Obtiene total de reseñas en todos los idiomas
    # Busca patrones tipo "1-10 of 1,234 reviews"
    results_text = selector.css('div.Ci::text').get('') 
    if 'of' in results_text:
      match = re.search(r'of\s+([\d,]+)', results_text)
      if match:
        try:
          return int(match.group(1).replace(',', ''))
        except ValueError:
          pass 

    # Buscar en todo el HTML
    all_text = selector.get()
    matches = re.findall(r'showing results \d+-\d+ of ([\d,]+)', all_text, re.IGNORECASE)
    if not matches: 
      matches = re.findall(r'([\d,]+) reviews', all_text, re.IGNORECASE)

    if matches:
      try:
        # Tomar el numero mas grande
        counts = [int(m.replace(',', '')) for m in matches]
        return max(counts) if counts else 0
      except ValueError:
        pass 

    return 0 

  # ===============================================================
  # EXTRAER RESEÑAS EN INGLES
  # ===============================================================

  def extract_english_reviews_count(self, selector: Selector) -> int:
    # Obtiene numero de reseñas especificamente en ingles
    # Busca boton de idioma tipo 'English (1,234)'
    lang_button_text = selector.css('button.Datwj[aria-haspopup="listbox"]::attr(aria-label)').get('') 
    if not lang_button_text: 
      lang_button_text = selector.css('button.Datwj[aria-haspopup="listbox"] .biGQs._P::text').get('')

    # Buscar patron "English (1,234)"
    match = re.search(r'English\s*\((\d{1,3}(?:,\d{3})*)\)', lang_button_text, re.IGNORECASE)
    if match:
      try:
        return int(match.group(1).replace(',', ''))
      except ValueError:
        return 0 
    return 0 

  # ===============================================================
  # ESTANDARIZAR FECHAS
  # ===============================================================
  
  def _standardize_date(self, raw_date: str, date_type: str = "visit") -> str:
    # ESTANDARIZA FECHAS A FORMATO YYYY-MM-DD PARA EXCEL
    # Convierte fechas multilenguaje a formato ISO
    # Devuelve "No date" si no puede parsear
    
    if not raw_date or raw_date in ["Sin fecha", "", "No date"]:
      return "No date"
    
    raw_date = raw_date.strip()
    
    # Mapeo completo de meses en todos los idiomas
    month_mappings = {
      # Ingles
      'january': 1, 'jan': 1, 'february': 2, 'feb': 2, 'march': 3, 'mar': 3,
      'april': 4, 'apr': 4, 'may': 5, 'june': 6, 'jun': 6,
      'july': 7, 'jul': 7, 'august': 8, 'aug': 8, 'september': 9, 'sep': 9,
      'october': 10, 'oct': 10, 'november': 11, 'nov': 11, 'december': 12, 'dec': 12,
      
      # Español
      'enero': 1, 'ene': 1, 'febrero': 2, 'feb': 2, 'marzo': 3, 'mar': 3,
      'abril': 4, 'abr': 4, 'mayo': 5, 'may': 5, 'junio': 6, 'jun': 6,
      'julio': 7, 'jul': 7, 'agosto': 8, 'ago': 8, 'septiembre': 9, 'sep': 9,
      'octubre': 10, 'oct': 10, 'noviembre': 11, 'nov': 11, 'diciembre': 12, 'dic': 12,
      
      # Portugues
      'janeiro': 1, 'jan': 1, 'fevereiro': 2, 'fev': 2, 'março': 3, 'mar': 3,
      'abril': 4, 'abr': 4, 'maio': 5, 'mai': 5, 'junho': 6, 'jun': 6,
      'julho': 7, 'jul': 7, 'agosto': 8, 'ago': 8, 'setembro': 9, 'set': 9,
      'outubro': 10, 'out': 10, 'novembro': 11, 'nov': 11, 'dezembro': 12, 'dez': 12,
      
      # Frances
      'janvier': 1, 'janv': 1, 'février': 2, 'févr': 2, 'mars': 3, 'mar': 3,
      'avril': 4, 'avr': 4, 'mai': 5, 'mai': 5, 'juin': 6, 'juin': 6,
      'juillet': 7, 'juil': 7, 'août': 8, 'août': 8, 'septembre': 9, 'sept': 9,
      'octobre': 10, 'oct': 10, 'novembre': 11, 'nov': 11, 'décembre': 12, 'déc': 12,
      
      # Aleman
      'januar': 1, 'jan': 1, 'februar': 2, 'feb': 2, 'märz': 3, 'mär': 3,
      'april': 4, 'apr': 4, 'mai': 5, 'mai': 5, 'juni': 6, 'jun': 6,
      'juli': 7, 'jul': 7, 'august': 8, 'aug': 8, 'september': 9, 'sep': 9,
      'oktober': 10, 'okt': 10, 'november': 11, 'nov': 11, 'dezember': 12, 'dez': 12,
      
      # Italiano
      'gennaio': 1, 'gen': 1, 'febbraio': 2, 'feb': 2, 'marzo': 3, 'mar': 3,
      'aprile': 4, 'apr': 4, 'maggio': 5, 'mag': 5, 'giugno': 6, 'giu': 6,
      'luglio': 7, 'lug': 7, 'agosto': 8, 'ago': 8, 'settembre': 9, 'set': 9,
      'ottobre': 10, 'ott': 10, 'novembre': 11, 'nov': 11, 'dicembre': 12, 'dic': 12,
      
      # Holandes
      'januari': 1, 'jan': 1, 'februari': 2, 'feb': 2, 'maart': 3, 'mrt': 3,
      'april': 4, 'apr': 4, 'mei': 5, 'mei': 5, 'juni': 6, 'jun': 6,
      'juli': 7, 'jul': 7, 'augustus': 8, 'aug': 8, 'september': 9, 'sep': 9,
      'oktober': 10, 'okt': 10, 'november': 11, 'nov': 11, 'december': 12, 'dec': 12,
      
      # Ruso
      'января': 1, 'янв': 1, 'февраля': 2, 'фев': 2, 'марта': 3, 'мар': 3,
      'апреля': 4, 'апр': 4, 'мая': 5, 'май': 5, 'июня': 6, 'июн': 6,
      'июля': 7, 'июл': 7, 'августа': 8, 'авг': 8, 'сентября': 9, 'сен': 9,
      'октября': 10, 'окт': 10, 'ноября': 11, 'ноя': 11, 'декабря': 12, 'дек': 12,
      
      # Turco
      'ocak': 1, 'oca': 1, 'şubat': 2, 'şub': 2, 'mart': 3, 'mar': 3,
      'nisan': 4, 'nis': 4, 'mayıs': 5, 'may': 5, 'haziran': 6, 'haz': 6,
      'temmuz': 7, 'tem': 7, 'ağustos': 8, 'ağu': 8, 'eylül': 9, 'eyl': 9,
      'ekim': 10, 'eki': 10, 'kasım': 11, 'kas': 11, 'aralık': 12, 'ara': 12,
      
      # Arabe
      'يناير': 1, 'فبراير': 2, 'مارس': 3, 'أبريل': 4, 'مايو': 5, 'يونيو': 6,
      'يوليو': 7, 'أغسطس': 8, 'سبتمبر': 9, 'أكتوبر': 10, 'نوفمبر': 11, 'ديسمبر': 12,
    }
    
    try:
      # PATRON 1: "19 de abril de 2025" (Español/Portugues)
      pattern1 = r'(\d+)\s+de\s+(\w+)\s+de\s+(\d{4})'
      match = re.search(pattern1, raw_date, re.IGNORECASE)
      if match:
        day, month_name, year = match.groups()
        month_num = month_mappings.get(month_name.lower())
        if month_num:
          return f"{year}-{month_num:02d}-{int(day):02d}"
      
      # PATRON 2: "April 19, 2025" (Ingles)
      pattern2 = r'(\w+)\s+(\d+),?\s+(\d{4})'
      match = re.search(pattern2, raw_date, re.IGNORECASE)
      if match:
        month_name, day, year = match.groups()
        month_num = month_mappings.get(month_name.lower())
        if month_num:
          return f"{year}-{month_num:02d}-{int(day):02d}"
      
      # PATRON 3: "9. März 2020" (Aleman)
      pattern3 = r'(\d+)\.\s+(\w+)\s+(\d{4})'
      match = re.search(pattern3, raw_date, re.IGNORECASE)
      if match:
        day, month_name, year = match.groups()
        month_num = month_mappings.get(month_name.lower())
        if month_num:
          return f"{year}-{month_num:02d}-{int(day):02d}"
      
      # PATRON 4: "7 mai 2024" (Frances)
      pattern4 = r'(\d+)\s+(\w+)\s+(\d{4})'
      match = re.search(pattern4, raw_date, re.IGNORECASE)
      if match:
        day, month_name, year = match.groups()
        month_num = month_mappings.get(month_name.lower())
        if month_num:
          return f"{year}-{month_num:02d}-{int(day):02d}"
      
      # PATRON 5: "2025年4月19日" (Japones)
      pattern5 = r'(\d{4})年(\d+)月(\d+)日'
      match = re.search(pattern5, raw_date)
      if match:
        year, month, day = match.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
      
      # PATRON 6: "2020년 3월 7일" (Coreano)
      pattern6 = r'(\d{4})년\s*(\d+)월\s*(\d+)일'
      match = re.search(pattern6, raw_date)
      if match:
        year, month, day = match.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
      
      # PATRON 7: "2019-10-07" (ISO)
      pattern7 = r'(\d{4})-(\d{2})-(\d{2})'
      match = re.search(pattern7, raw_date)
      if match:
        year, month, day = match.groups()
        return f"{year}-{month}-{day}"
      
      # PATRON 8: "9 августа 2019 г." (Ruso)
      pattern8 = r'(\d+)\s+(\w+)\s+(\d{4})\s*г\.'
      match = re.search(pattern8, raw_date, re.IGNORECASE)
      if match:
        day, month_name, year = match.groups()
        month_num = month_mappings.get(month_name.lower())
        if month_num:
          return f"{year}-{month_num:02d}-{int(day):02d}"
      
      # PATRON 9: "19 Nisan 2025" (Turco)
      pattern9 = r'(\d+)\s+(\w+)\s+(\d{4})'
      match = re.search(pattern9, raw_date, re.IGNORECASE)
      if match:
        day, month_name, year = match.groups()
        month_num = month_mappings.get(month_name.lower())
        if month_num:
          return f"{year}-{month_num:02d}-{int(day):02d}"
      
      # PATRON 10: "14 maart 2022" (Holandes)
      pattern10 = r'(\d+)\s+(\w+)\s+(\d{4})'
      match = re.search(pattern10, raw_date, re.IGNORECASE)
      if match:
        day, month_name, year = match.groups()
        month_num = month_mappings.get(month_name.lower())
        if month_num:
          return f"{year}-{month_num:02d}-{int(day):02d}"
      
      # PATRON 11: "19 aprile 2025" (Italiano)
      pattern11 = r'(\d+)\s+(\w+)\s+(\d{4})'
      match = re.search(pattern11, raw_date, re.IGNORECASE)
      if match:
        day, month_name, year = match.groups()
        month_num = month_mappings.get(month_name.lower())
        if month_num:
          return f"{year}-{month_num:02d}-{int(day):02d}"
      
      # PATRON 12: Solo mes y año - varios formatos
      # Portugues/Español: "abr. de 2025"
      pattern12a = r'(\w+)\.?\s+de\s+(\d{4})'
      match = re.search(pattern12a, raw_date, re.IGNORECASE)
      if match:
        month_name, year = match.groups()
        month_num = month_mappings.get(month_name.lower())
        if month_num:
          return f"{year}-{month_num:02d}-01"
      
      # Ingles: "Apr 2025"
      pattern12b = r'(\w+)\s+(\d{4})'
      match = re.search(pattern12b, raw_date, re.IGNORECASE)
      if match:
        month_name, year = match.groups()
        month_num = month_mappings.get(month_name.lower())
        if month_num:
          return f"{year}-{month_num:02d}-01"
      
      # Japones: "2025年4月"
      pattern12c = r'(\d{4})年(\d+)月'
      match = re.search(pattern12c, raw_date)
      if match:
        year, month = match.groups()
        return f"{year}-{int(month):02d}-01"
      
      # Coreano: "2020년 2월"
      pattern12d = r'(\d{4})년\s*(\d+)월'
      match = re.search(pattern12d, raw_date)
      if match:
        year, month = match.groups()
        return f"{year}-{int(month):02d}-01"
      
      # Ruso: "авг. 2019 г."
      pattern12e = r'(\w+)\.?\s+(\d{4})\s*г\.'
      match = re.search(pattern12e, raw_date, re.IGNORECASE)
      if match:
        month_name, year = match.groups()
        month_num = month_mappings.get(month_name.lower())
        if month_num:
          return f"{year}-{month_num:02d}-01"
      
      # Arabe: "أبريل 2025"
      pattern12f = r'(\w+)\s+(\d{4})'
      match = re.search(pattern12f, raw_date, re.IGNORECASE)
      if match:
        month_name, year = match.groups()
        month_num = month_mappings.get(month_name.lower())
        if month_num:
          return f"{year}-{month_num:02d}-01"
      
    except Exception as e:
      log.debug(f"Error parsing date '{raw_date}': {e}")
    
    return "No date"
  
  # ===============================================================
  # ESTANDARIZAR TIPO DE COMPAÑIA
  # ===============================================================

  def _standardize_companion_type(self, raw_companion: str) -> str:
    # ESTANDARIZA TIPOS DE COMPAÑIA AL FORMATO INGLES
    # Convierte texto multilenguaje a categorias estandar:
    # Couples, Friends, Family, Solo, Business
    
    if not raw_companion or raw_companion.strip() == "":
      return "No information"
    
    companion = raw_companion.strip()
    
    # Mapeo completo de tipos de compañia
    companion_mapping = {
      # Ingles (ya estandarizado)
      "Couples": "Couples",
      "Friends": "Friends", 
      "Family": "Family",
      "Solo": "Solo",
      "Business": "Business",
      
      # Español
      "Parejas": "Couples",
      "Amigos": "Friends",
      "Familia": "Family",
      "En solitario": "Solo",
      "Negocios": "Business",
      "En pareja": "Couples",
      "En familia": "Family",
      "Con amigos": "Friends",
      "Viaje de negocios": "Business",
      
      # Portugues
      "Casais": "Couples",
      "Amigos": "Friends",
      "Família": "Family",
      "A sós": "Solo",
      "Negócios": "Business",
      "Em casal": "Couples",
      "Em família": "Family",
      "Com amigos": "Friends",
      "Sozinho": "Solo",
      "Viagem de negócios": "Business",
      
      # Aleman
      "Familie": "Family",
      "Paare": "Couples",
      "Freunde": "Friends",
      "Allein/Single": "Solo",
      "Geschäftsreise": "Business",
      "Als Paar": "Couples",
      "Mit der Familie": "Family",
      "Mit Freunden": "Friends",
      "Allein": "Solo",
      "Single": "Solo",
      
      # Frances
      "En solo": "Solo",
      "En couple": "Couples",
      "Entre amis": "Friends",
      "En famille": "Family",
      "Affaires": "Business",
      "Voyage d'affaires": "Business",
      "Seul": "Solo",
      
      # Japones
      "カップル・夫婦": "Couples",
      "友達": "Friends",
      "ファミリー": "Family",
      "一人": "Solo",
      "同僚・仕事関連": "Business",
      "カップルで": "Couples",
      "家族と": "Family",
      "友人と": "Friends",
      "一人で": "Solo",
      "ビジネス旅行": "Business",
      
      # Ruso
      "Для двоих": "Couples",
      "С друзьями": "Friends",
      "Путешествие в одиночку": "Solo",
      "Семейный отдых": "Family",
      "Бизнес": "Business",
      "В паре": "Couples",
      "С семьей": "Family",
      "Один": "Solo",
      "Деловая поездка": "Business",
      
      # Turco
      "Çiftler": "Couples",
      "Arkadaşlar": "Friends",
      "Aile": "Family",
      "Yalnız": "Solo",
      "İşletme": "Business",
      
      # Holandes
      "Gezinnen": "Family",
      "Stellen": "Couples",
      "Vrienden": "Friends",
      "Alleen": "Solo",
      "Zakelijk": "Business",
      "Als koppel": "Couples",
      "Met familie": "Family",
      "Met vrienden": "Friends",
      "Zakenreis": "Business",
      
      # Arabe
      "العائلة": "Family",
      "زوجان": "Couples",
      "الأصدقاء": "Friends",
      "بمفردك": "Solo",
      "العمل": "Business",
      "كزوجين": "Couples",
      "مع العائلة": "Family",
      "مع الأصدقاء": "Friends",
      "وحدي": "Solo",
      "رحلة عمل": "Business",
      
      # Italiano
      "Coppie": "Couples",
      "Amici": "Friends",
      "Famiglia": "Family",
      "Solo": "Solo",
      "Per affari": "Business",
      "In coppia": "Couples",
      "In famiglia": "Family",
      "Con amici": "Friends",
      "Da solo": "Solo",
      "Viaggio di lavoro": "Business",
      
      # Coreano
      "친구": "Friends",
      "개인": "Solo",
      "커플": "Couples",
      "가족": "Family",
      "비지니스": "Business",
      "커플로": "Couples",
      "가족과": "Family",
      "친구와": "Friends",
      "혼자": "Solo",
      "출장": "Business",
      
      # Chino
      "情侣游": "Couples",
      "全家游": "Family",
      "独自旅行": "Solo",
      "结伴旅行": "Friends",
      "商务行": "Business",
      "作为伴侣": "Couples",
      "与家人": "Family",
      "与朋友": "Friends",
      "独自一人": "Solo",
      "商务旅行": "Business",
    }
    
    # Buscar coincidencia exacta
    standardized = companion_mapping.get(companion, companion)
    
    # Si no encuentra, buscar coincidencia parcial
    if standardized == companion:
      companion_lower = companion.lower()
      for key, value in companion_mapping.items():
        if key.lower() in companion_lower or companion_lower in key.lower():
          return value
    
    return standardized if standardized in ["Couples", "Friends", "Family", "Solo", "Business"] else "NO INFO"