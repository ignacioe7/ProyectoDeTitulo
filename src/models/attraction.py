from dataclasses import dataclass
from typing import Optional

# ========================================================================================================
#                                         MODELO DE ATRACCIÓN
# ========================================================================================================

@dataclass
class Attraction:
  # Modelo que representa una atracción turística con sus datos básicos
  # Contiene información de ranking, tipo, rating y estado de scraping
  
  position: Optional[int] = None # Posición en el ranking de atracciones
  place_name: str = "Lugar Sin Nombre" # Nombre oficial del lugar turístico
  place_type: str = "Sin categoría" # Categoría o tipo de atracción
  rating: float = 0.0 # Calificación promedio en escala de 0 a 5
  reviews_count: int = 0 # Número total de reseñas disponibles
  url: str = "" # URL completa de la página de TripAdvisor
  previously_visited: bool = False # Indica si ya se scrapearon datos de esta atracción