# EXPORTACIONES PRINCIPALES DEL PAQUETE
# Clases principales para scraping y manejo de datos de TripAdvisor
# Incluye scrapers de atracciones y reseñas con soporte multilenguaje
from .core import DataHandler, AttractionScraper, ReviewScraper

__version__ = "1.0.0"
__all__ = ['DataHandler', 'AttractionScraper', 'ReviewScraper']