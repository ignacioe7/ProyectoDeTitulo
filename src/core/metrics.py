from parsel import Selector 
import re 

class ReviewMetricsCalculator:
  """Calcula métricas sobre las reseñas disponibles"""

  @staticmethod
  def extract_total_reviews(selector: Selector) -> int:
    """Extrae el número total de reseñas"""
    # Buscamos el texto que dice algo como '1-10 of 1,234 reviews'
    count_text = selector.css('div.Ci::text').get('')
    # Usamos regex para sacar el número después de 'of'
    match = re.search(r'of\s+([\d,]+)', count_text)
    # Si encontramos algo, lo limpiamos (quitamos comas) y lo convertimos a número
    return int(match.group(1).replace(',', '')) if match else 0

  @staticmethod
  def extract_english_reviews(selector: Selector) -> int:
    """Identifica reseñas en inglés"""
    # Buscamos el botón del filtro de idioma
    lang_selector = selector.css('button.Datwj[aria-haspopup="listbox"]')
    # Si existe y dice 'English', asumimos que todas las reseñas mostradas son en inglés
    if lang_selector and "English" in lang_selector.get(''):
      # En ese caso, el total de reseñas en inglés es el total mostrado
      return ReviewMetricsCalculator.extract_total_reviews(selector)
    # Si no, pues cero reseñas en inglés
    return 0