from dataclasses import dataclass
from typing import Optional

@dataclass
class Review:
  """Modelo de datos para una reseña"""
  username: str = "Anónimo"  # Nombre de usuario
  rating: float = 0.0  # Calificación dada por el usuario
  title: str = "Sin título"  # Título de la reseña
  review_text: str = ""  # Texto completo de la reseña
  location: Optional[str] = None  # Ubicación del usuario
  contributions: Optional[int] = None  # Número de contribuciones del usuario
  visit_date: Optional[str] = None  # Fecha de la visita
  written_date: Optional[str] = None  # Fecha en que escribió la reseña
  companion_type: Optional[str] = None  # Tipo de acompañante
  sentiment: Optional[str] = None  # Sentimiento detectado por IA
  sentiment_score: Optional[float] = None  # Puntuación del sentimiento (0-4)
  analyzed_at: Optional[str] = None  # Fecha del análisis de sentimiento