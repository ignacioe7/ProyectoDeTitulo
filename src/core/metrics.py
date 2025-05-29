from parsel import Selector
import re
from typing import Optional
from loguru import logger as log

class ReviewMetricsCalculator:
  """calcula métricas de reseñas en una página"""

  @staticmethod
  def extract_total_reviews(selector: Selector) -> Optional[int]:
    """extrae el total de reseñas de la paginación"""
    # buscar texto de paginación
    pagination_text_element = selector.css('div.Ci')

    if not pagination_text_element:
      return None

    # obtener texto del div
    pagination_text = pagination_text_element.xpath('string(.)').get()

    if not pagination_text:
      return None

    pagination_text = pagination_text.strip()

    # buscar numero despues de "of "
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

  @staticmethod
  def is_current_view_english(selector: Selector) -> bool:
    """determina si la vista actual de reseñas esta en ingles"""
    try:
      # buscar aria-label del boton de idioma
      lang_button_aria_label = selector.css('button.Datwj[aria-haspopup="listbox"]::attr(aria-label)').get()

      if lang_button_aria_label:
        if "english" in lang_button_aria_label.lower():
          if "English" in lang_button_aria_label: # chequeo simple
            return True

    except Exception as e:
      log.error(f"error determinando idioma: {e}")

    return False

  @staticmethod
  def extract_specific_english_review_count(selector: Selector) -> Optional[int]:
    """extrae conteo de reseñas en ingles del boton de idioma"""
    try:
      # buscar aria-label del boton de idioma
      lang_button_aria_label = selector.css('button.Datwj[aria-haspopup="listbox"]::attr(aria-label)').get()
      if lang_button_aria_label:
        # buscar patron "English (NUM)"
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