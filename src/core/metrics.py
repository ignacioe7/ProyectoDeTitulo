from typing import Dict, Optional, Tuple
from parsel import Selector
from loguru import logger as log
import re

# ===============================================================
# CALCULADORA DE METRICAS DE RESEÑAS
# ===============================================================

class ReviewMetricsCalculator:
  # CALCULADORA DE METRICAS DE RESEÑAS CON SOPORTE MULTILENGUAJE
  # Extrae conteos y estadisticas desde HTML de TripAdvisor
  # Soporta 13 idiomas diferentes con patrones especificos

  # Mapeo de idiomas a dominios
  LANGUAGE_DOMAINS = {
    "english": "www.tripadvisor.com",
    "spanish": "www.tripadvisor.es", 
    "portuguese": "www.tripadvisor.pt",
    "french": "www.tripadvisor.fr",
    "german": "www.tripadvisor.de",
    "italian": "www.tripadvisor.it",
    "dutch": "www.tripadvisor.nl",
    "russian": "www.tripadvisor.ru",
    "japanese": "www.tripadvisor.jp",
    "korean": "www.tripadvisor.co.kr",
    "arabic": "ar.tripadvisor.com",
    "turkish": "www.tripadvisor.com.tr",
    "chinese": "www.tripadvisor.cn"
  }

  # Nombres de idiomas para deteccion en botones
  LANGUAGE_BUTTON_NAMES = {
    "english": ["English"],
    "spanish": ["Español (España)", "Español", "Spanish"],
    "portuguese": ["Português", "Portuguese"],
    "french": ["Français", "French"],
    "german": ["Deutsch", "German"],
    "italian": ["Italiano", "Italian"],
    "dutch": ["Nederlands", "Dutch"],
    "russian": ["Русский", "Russian"],
    "japanese": ["日本語", "Japanese"],
    "korean": ["한국어", "Korean"],
    "arabic": ["العربية", "Arabic"],
    "turkish": ["Türkçe", "Turkish"],
    "chinese": ["中文简体", "Chinese", "中文"]
  }

  # Patrones de paginacion para cada idioma
  PAGINATION_PATTERNS = {
    "english": [
      r'showing results \d+-\d+ of ([\d,]+)',
      r'(\d+) reviews',
      r'of ([\d,]+) results'
    ],
    "spanish": [
      r'se muestran los resultados \d+-\d+ de ([\d,]+)',
      r'(\d+) opiniones',
      r'de ([\d,]+) resultados'
    ],
    "portuguese": [
      r'a mostrar resultados \d+ a \d+ de ([\d,]+)',
      r'(\d+) avaliações',
      r'de ([\d,]+) resultados'
    ],
    "french": [
      r'résultats \d+-\d+ sur ([\d,\.]+)',
      r'(\d+) avis',
      r'sur ([\d,\.]+) résultats'
    ],
    "german": [
      r'von insgesamt\s*([\d,\.]+)\s*angezeigt',
      r'(\d+) bewertungen',
      r'von insgesamt ([\d,\.]+)'
    ],
    "italian": [
      r'(\d+-\d+)\s*di\s*([\d,\.]+)\s*risultati mostrati',
      r'(\d+) recensioni',
      r'di ([\d,\.]+) risultati'
    ],
    "dutch": [
      r'(\d+-\d+)\s*van de\s*([\d,\.]+)\s*resultaten worden getoond',
      r'(\d+) beoordelingen',
      r'van de ([\d,\.]+) resultaten'
    ],
    "russian": [
      r'показаны результаты \d+–\d+ из\s*([\d,\.]+)',
      r'(\d+) отзывов',
      r'из ([\d,\.]+) результатов'
    ],
    "japanese": [
      r'([\d,\.]+)件中\d+～\d+件の結果を表示中',
      r'(\d+) 件のレビュー',
      r'([\d,\.]+)件中'
    ],
    "korean": [
      r'검색 결과 전체\s*([\d,]+)\s*중\s*\d+-\d+',
      r'(\d+) 리뷰',
      r'전체 ([\d,]+) 중'
    ],
    "arabic": [
      r'عرض نتائج \d+-\d+ من أصل ([\d,]+)',
      r'(\d+) مراجعات',
      r'من أصل ([\d,]+)'
    ],
    "turkish": [
      r'([\d,\.]+)\s*sonuçtan \d+-\d+ arasındakiler gösteriliyor',
      r'(\d+) yorum',
      r'([\d,\.]+) sonuçtan'
    ],
    "chinese": [
      r'显示第 \d+ 至 \d+ 位的结果，共 ([\d,]+) 位',
      r'(\d+) 条点评',
      r'共 ([\d,]+) 位'
    ]
  }

  # Patrones aria-label para botones de idioma
  ARIA_LABEL_PATTERNS = {
    "english": [
      r'English:\s*English\s*\((\d+(?:[,\.]\d+)*)\)',
      r'English\s*\((\d+(?:[,\.]\d+)*)\)'
    ],
    "spanish": [
      r'Español \(España\):\s*Español \(España\)\s*\((\d+(?:[,\.]\d+)*)\)',
      r'Español.*?\((\d+(?:[,\.]\d+)*)\)'
    ],
    "portuguese": [
      r'Português:\s*Português\s*\((\d+(?:[,\.]\d+)*)\)',
      r'Português\s*\((\d+(?:[,\.]\d+)*)\)'
    ],
    "french": [
      r'Français:\s*Français\s*\((\d+(?:[,\.]\d+)*)\)',
      r'Français\s*\((\d+(?:[,\.]\d+)*)\)'
    ],
    "german": [
      r'Deutsch:\s*Deutsch\s*\((\d+(?:[,\.]\d+)*)\)',
      r'Deutsch\s*\((\d+(?:[,\.]\d+)*)\)'
    ],
    "italian": [
      r'Italiano:\s*Italiano\s*\((\d+(?:[,\.]\d+)*)\)',
      r'Italiano\s*\((\d+(?:[,\.]\d+)*)\)'
    ],
    "dutch": [
      r'Nederlands:\s*Nederlands\s*\((\d+(?:[,\.]\d+)*)\)',
      r'Nederlands\s*\((\d+(?:[,\.]\d+)*)\)'
    ],
    "russian": [
      r'Русский:\s*Русский\s*\((\d+(?:[,\.]\d+)*)\)',
      r'Русский\s*\((\d+(?:[,\.]\d+)*)\)'
    ],
    "japanese": [
      r'日本語:\s*日本語\s*\((\d+(?:[,\.]\d+)*)\)',
      r'日本語\s*\((\d+(?:[,\.]\d+)*)\)'
    ],
    "korean": [
      r'한국어:\s*한국어\s*\((\d+(?:[,\.]\d+)*)\)',
      r'한국어\s*\((\d+(?:[,\.]\d+)*)\)'
    ],
    "arabic": [
      r'العربية:\s*العربية\s*\((\d+(?:[,\.]\d+)*)\)',
      r'العربية\s*\((\d+(?:[,\.]\d+)*)\)'
    ],
    "turkish": [
      r'Türkçe:\s*Türkçe\s*\((\d+(?:[,\.]\d+)*)\)',
      r'Türkçe\s*\((\d+(?:[,\.]\d+)*)\)'
    ],
    "chinese": [
      r'中文简体.*?\((\d+(?:[,\.]\d+)*)\)',
      r'中文\s*\((\d+(?:[,\.]\d+)*)\)'
    ]
  }

  # ===============================================================
  # GENERAR URL DE IDIOMA
  # ===============================================================

  @classmethod
  def generate_language_url(cls, base_url: str, language_code: str) -> str:
    # GENERA URL PARA IDIOMA ESPECIFICO
    # Convierte URL entre dominios de TripAdvisor
    # Mantiene la ruta original pero cambia el dominio
    if language_code not in cls.LANGUAGE_DOMAINS:
      log.warning(f"Idioma no soportado: {language_code}, usando inglés por defecto")
      language_code = "english"
    
    target_domain = cls.LANGUAGE_DOMAINS[language_code]
    
    # Extraer la parte especifica de la URL despues del dominio
    if "tripadvisor" in base_url:
      # Buscar el patron /Attraction_Review- o similar
      match = re.search(r'tripadvisor\.[^/]+(/.*)', base_url)
      if match:
        path = match.group(1)
        return f"https://{target_domain}{path}"
    
    # Si no se puede convertir, retornar la URL original
    log.warning(f"No se pudo convertir URL al idioma {language_code}: {base_url}")
    return base_url

  # ===============================================================
  # VERIFICAR IDIOMA ACTUAL
  # ===============================================================

  @classmethod
  def is_current_view_language(cls, selector: Selector, expected_language_code: str) -> bool:
    # VERIFICA SI EL IDIOMA VISIBLE COINCIDE CON EL ESPERADO
    # Busca texto en botones de seleccion de idioma
    # Compara con nombres conocidos para cada idioma
    if expected_language_code not in cls.LANGUAGE_BUTTON_NAMES:
      log.warning(f"Idioma no reconocido: {expected_language_code}")
      return False
    
    expected_names = cls.LANGUAGE_BUTTON_NAMES[expected_language_code]
    
    # Selectores basados en HTML real
    language_button_selectors = [
      'button[aria-haspopup="listbox"]',
      'button.Datwj[aria-haspopup="listbox"]',
      'button.bHgte[aria-haspopup="listbox"]'
    ]
    
    for selector_path in language_button_selectors:
      buttons = selector.css(selector_path)
      for button in buttons:
        # Verificar aria-label (mas confiable)
        aria_label = button.css('::attr(aria-label)').get('').strip()
        
        # Verificar texto visible
        button_text = button.css('span::text').get('').strip()
        if not button_text:
          button_text = button.xpath('.//span/text()').get('').strip()
        
        # Comprobar si coincide con algun nombre esperado
        for expected_name in expected_names:
          if (expected_name in button_text or 
              expected_name in aria_label):
            log.debug(f"Idioma verificado: {expected_name} encontrado en botón")
            return True
    
    log.warning(f"Idioma {expected_language_code} no verificado en la página")
    return False

  # ===============================================================
  # EXTRAER TOTAL DE RESEÑAS
  # ===============================================================

  @classmethod
  def extract_total_reviews(cls, selector: Selector, language_code: str = "english") -> Optional[int]:
    # EXTRAE NUMERO TOTAL DE RESEÑAS DESDE PAGINACION
    # Usa patrones especificos del idioma para encontrar conteos
    # Busca en elementos de paginacion y texto descriptivo
    
    # Selectores basados en HTML real
    pagination_selectors = [
      'div.Ci',
      'div.cIUfa.Ci',
      'div[class*="pagination"]',
      'div[data-automation="reviewsResults"]',
      'span[class*="results"]'
    ]
    
    # Obtener patrones especificos del idioma
    patterns_to_try = cls.PAGINATION_PATTERNS.get(language_code, cls.PAGINATION_PATTERNS["english"])
    
    for selector_path in pagination_selectors:
      elements = selector.css(selector_path)
      for element in elements:
        # Obtener texto completo incluyendo &nbsp; y espacios
        text = element.xpath('string(.)').get('').strip()
        text = re.sub(r'\s+', ' ', text)
        text = text.replace('\u00a0', ' ')
        
        if text:
          log.debug(f"Texto de paginación encontrado: '{text}'")
          
          for pattern in patterns_to_try:
            try:
              match = re.search(pattern, text, re.IGNORECASE)
              if match:
                # Obtener el ultimo grupo que contenga numeros
                groups = match.groups()
                number_str = None
                
                # Buscar el grupo con el numero mas grande (que deberia ser el total)
                for group in reversed(groups):
                  if group and re.search(r'\d', group):
                    number_str = group
                    break
                
                if number_str:
                  # Limpiar el numero
                  clean_number = re.sub(r'[^\d]', '', number_str)
                  if clean_number:
                    count = int(clean_number)
                    log.debug(f"Total extraído de paginación: {count} (patrón: {pattern[:50]}...)")
                    return count
                    
            except (ValueError, IndexError) as e:
              log.debug(f"Error procesando patrón {pattern}: {e}")
              continue
    
    log.warning(f"No se pudo extraer total de reseñas para idioma {language_code}")
    return None
  
  # ===============================================================
  # EXTRAER CONTEO POR IDIOMA
  # ===============================================================

  @classmethod
  def extract_language_specific_review_count(cls, selector: Selector, language_code: str) -> Optional[int]:
    # EXTRAE NUMERO DE RESEÑAS ESPECIFICO DEL IDIOMA DESDE BOTON
    # Lee atributos aria-label de botones de seleccion
    # Usa patrones regex para extraer numeros del texto
    if language_code not in cls.ARIA_LABEL_PATTERNS:
      log.warning(f"No hay patrones definidos para idioma: {language_code}")
      return None
    
    patterns = cls.ARIA_LABEL_PATTERNS[language_code]
    
    # Selectores basados en HTML real
    button_selectors = [
      'button[aria-haspopup="listbox"]',
      'button.Datwj[aria-haspopup="listbox"]',
      'button.bHgte[aria-haspopup="listbox"]'
    ]
    
    for selector_path in button_selectors:
      buttons = selector.css(selector_path)
      
      for button in buttons:
        # Obtener aria-label
        aria_label = button.css('::attr(aria-label)').get('').strip()
        
        if aria_label:
          log.debug(f"Verificando aria-label: '{aria_label}'")
          
          for pattern in patterns:
            try:
              match = re.search(pattern, aria_label, re.IGNORECASE)
              if match:
                number_str = match.group(1).replace(',', '').replace('.', '').replace(' ', '')
                if number_str.isdigit():
                  count = int(number_str)
                  log.debug(f"Conteo de {language_code} desde botón: {count}")
                  return count
            except (ValueError, IndexError) as e:
              log.debug(f"Error procesando patrón {pattern}: {e}")
              continue
    
    log.debug(f"No se encontró conteo específico para idioma {language_code}")
    return None
  
  # ===============================================================
  # OBTENER METRICAS COMPLETAS
  # ===============================================================

  @classmethod
  def get_review_metrics_for_language(cls, selector: Selector, language_code: str) -> Dict[str, Optional[int]]:
    # OBTIENE METRICAS COMPLETAS PARA IDIOMA ESPECIFICO
    # Combina verificacion de idioma con extraccion de conteos
    # Prioriza fuentes mas confiables y maneja discrepancias
    
    # Verificar si estamos en la vista del idioma correcto
    is_correct_language = cls.is_current_view_language(selector, language_code)
    
    # Extraer total desde paginacion
    total_from_pagination = cls.extract_total_reviews(selector, language_code)
    
    # Extraer especifico del idioma desde boton
    language_specific_count = cls.extract_language_specific_review_count(selector, language_code)
    
    # Logica de priorizacion mejorada
    final_count = 0
    source = "none"
    
    if is_correct_language and total_from_pagination is not None:
      # Caso ideal: Vista correcta + paginacion disponible
      final_count = total_from_pagination
      source = "pagination"
      log.debug(f"Usando paginación para {language_code}: {final_count}")
      
    elif language_specific_count is not None:
      # Caso alternativo: Boton especifico disponible
      final_count = language_specific_count
      source = "language_button"
      log.debug(f"Usando botón de idioma para {language_code}: {final_count}")
      
      # Advertencia si hay discrepancia con paginacion
      if total_from_pagination is not None and abs(total_from_pagination - language_specific_count) > 10:
        log.warning(f"Discrepancia en {language_code}: paginación={total_from_pagination}, botón={language_specific_count}")
        
    elif total_from_pagination is not None:
      # Fallback: Solo paginacion, pero sin verificar idioma
      final_count = total_from_pagination
      source = "pagination_fallback"
      log.warning(f"Usando paginación sin verificar idioma para {language_code}: {final_count}")
    
    return {
      "total_reviews": final_count,
      "is_correct_language_view": is_correct_language,
      "pagination_count": total_from_pagination,
      "language_button_count": language_specific_count,
      "source": source
    }

  # ===============================================================
  # VERIFICAR VISTA EN INGLES
  # ===============================================================

  @classmethod
  def is_current_view_english(cls, selector: Selector) -> bool:
    # METODO DE COMPATIBILIDAD PARA VERIFICAR VISTA EN INGLES
    # Wrapper para mantener compatibilidad con codigo existente
    return cls.is_current_view_language(selector, "english")

  # ===============================================================
  # EXTRAER CONTEO ESPECIFICO EN INGLES
  # ===============================================================

  @classmethod
  def extract_specific_english_review_count(cls, selector: Selector) -> Optional[int]:
    # METODO DE COMPATIBILIDAD PARA EXTRAER CONTEO EN INGLES
    # Wrapper para mantener compatibilidad con codigo existente
    return cls.extract_language_specific_review_count(selector, "english")