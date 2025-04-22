from parsel import Selector
import re

class ReviewMetricsCalculator:
    """Calcula métricas sobre las reseñas disponibles"""
    
    @staticmethod
    def extract_total_reviews(selector: Selector) -> int:
        """Extrae el número total de reseñas"""
        count_text = selector.css('div.Ci::text').get('')
        match = re.search(r'of\s+([\d,]+)', count_text)
        return int(match.group(1).replace(',', '')) if match else 0

    @staticmethod
    def extract_english_reviews(selector: Selector) -> int:
        """Identifica reseñas en inglés"""
        lang_selector = selector.css('button.Datwj[aria-haspopup="listbox"]')
        if lang_selector and "English" in lang_selector.get(''):
            return ReviewMetricsCalculator.extract_total_reviews(selector)
        return 0