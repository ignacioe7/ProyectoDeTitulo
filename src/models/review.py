from dataclasses import dataclass
from typing import Optional

@dataclass
class Review:
    """Modelo de datos para una reseña"""
    username: str
    rating: float
    title: str
    review_text: str
    location: Optional[str] = None
    contributions: Optional[int] = None
    visit_date: Optional[str] = None
    written_date: Optional[str] = None
    companion_type: Optional[str] = None