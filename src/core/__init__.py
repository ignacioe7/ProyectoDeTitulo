# ===============================================================
# MODULO CORE - EXPORTACIONES PRINCIPALES
# ===============================================================

# Importa todas las clases principales del sistema
from .data_handler import DataHandler
from .scraper import AttractionScraper, ReviewScraper
from .parsers import ReviewParser, ReviewParserConfig
from .analyzer import SentimentAnalyzer
from .metrics import ReviewMetricsCalculator

# ===============================================================
# EXPORTACIONES PUBLICAS
# ===============================================================

# Define que clases estan disponibles cuando se importa el modulo core
# Permite usar: from src.core import DataHandler, ReviewScraper, etc
__all__ = [
  'DataHandler',
  'AttractionScraper', 
  'ReviewScraper',
  'ReviewParser',
  'ReviewParserConfig',
  'SentimentAnalyzer',
  'ReviewMetricsCalculator'
]