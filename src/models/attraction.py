from dataclasses import dataclass
from typing import Optional

@dataclass
class Attraction:
    """Modelo que representa una atracción turística"""
    position: Optional[int] = None
    place_name: str = "Lugar Sin Nombre"
    place_type: str = "Sin categoría"
    rating: float = 0.0
    reviews_count: int = 0
    url: str = ""