from dataclasses import dataclass
from typing import Optional

@dataclass
class Attraction:
  """modelo que representa una atraccion turistica"""
  position: Optional[int] = None # posición en el ranking
  place_name: str = "Lugar Sin Nombre" # nombre del lugar
  place_type: str = "Sin categoría" # tipo de lugar
  rating: float = 0.0 # calificación promedio
  reviews_count: int = 0 # número total de reseñas
  url: str = "" # URL de la atracción
  previously_visited: bool = False # indica si ya se scrapeó la atracción