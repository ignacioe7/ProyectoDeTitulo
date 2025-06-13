# MÓDULO DE INICIALIZACIÓN PRINCIPAL DEL PAQUETE SRC
# Centraliza acceso a componentes core del sistema de scraping
# Define API pública para importación externa de clases principales

# importar clases fundamentales del sistema desde módulo core
from .core import DataHandler, AttractionScraper, ReviewScraper

# versión actual del proyecto para tracking de releases
__version__ = "1.0.0"

# lista de elementos públicos disponibles para importación externa
# define API pública del paquete completo
__all__ = ['DataHandler', 'AttractionScraper', 'ReviewScraper']