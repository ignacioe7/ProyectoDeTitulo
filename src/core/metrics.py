from parsel import Selector
import re
from typing import Optional
from loguru import logger as log


# Calculadora de métricas para extraer conteos y estadísticas de páginas de reseñas
# Maneja extracción de totales, detección de idioma y conteos específicos
class ReviewMetricsCalculator:

# ========================================================================================================
#                                         EXTRAER TOTAL RESEÑAS
# ========================================================================================================

  @staticmethod
  def extract_total_reviews(selector: Selector) -> Optional[int]:
    # EXTRAE EL NÚMERO TOTAL DE RESEÑAS DESDE LA PAGINACIÓN
    # Busca elementos de paginación con clase Ci
    pagination_text_element = selector.css('div.Ci')

    if not pagination_text_element:
      return None

    # Obtiene todo el texto del elemento de paginación
    pagination_text = pagination_text_element.xpath('string(.)').get()

    if not pagination_text:
      return None

    pagination_text = pagination_text.strip()

    # Busca patrón "of NÚMERO" en el texto de paginación
    match = re.search(r'of\s+([\d,]+)', pagination_text)
    if match:
      try:
        total_reviews_str = match.group(1).replace(',', '')
        total_reviews = int(total_reviews_str)
        return total_reviews
      except ValueError:
        log.warning(f"no se pudo convertir '{match.group(1)}' a número")
        return None
    else:
      return None

# ========================================================================================================
#                                       DETECTAR VISTA EN INGLÉS
# ========================================================================================================

  @staticmethod
  def is_current_view_english(selector: Selector) -> bool:
    # DETERMINA SI LA VISTA ACTUAL DE RESEÑAS ESTÁ EN INGLÉS
    try:
      # Busca el aria-label del botón de selección de idioma
      lang_button_aria_label = selector.css('button.Datwj[aria-haspopup="listbox"]::attr(aria-label)').get()

      if lang_button_aria_label:
        if "english" in lang_button_aria_label.lower():
          if "English" in lang_button_aria_label: # Verificación simple de mayúsculas
            return True

    except Exception as e:
      log.error(f"error determinando idioma: {e}")

    return False

# ========================================================================================================
#                                    EXTRAER CONTEO ESPECÍFICO INGLÉS
# ========================================================================================================

  @staticmethod
  def extract_specific_english_review_count(selector: Selector) -> Optional[int]:
    # EXTRAE EL CONTEO DE RESEÑAS EN INGLÉS DEL BOTÓN DE IDIOMA
    try:
      # Busca el aria-label del botón de filtro de idioma
      lang_button_aria_label = selector.css('button.Datwj[aria-haspopup="listbox"]::attr(aria-label)').get()
      if lang_button_aria_label:
        # Busca patrón "English (NÚMERO)" en el aria-label
        match = re.search(r"English(?::\s*English)?\s*\((\d+)\)", lang_button_aria_label, re.IGNORECASE)
        if match:
          count_str = match.group(1)
          try:
            count = int(count_str)
            return count
          except ValueError:
            log.warning(f"no se pudo convertir '{count_str}' a numero")
            return None
    except Exception as e:
      log.error(f"error extrayendo contador de ingles: {e}")
    return None