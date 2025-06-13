from dataclasses import dataclass
from typing import Optional

# ========================================================================================================
#                                          MODELO DE RESEÑA
# ========================================================================================================

@dataclass
class Review:
  # Modelo de datos para una reseña individual
  # Incluye datos del usuario, contenido y análisis de sentimientos
  
  username: str = "Anónimo"  # Nombre de usuario que escribió la reseña
  rating: float = 0.0  # Calificación dada en escala de 0 a 5 estrellas
  title: str = "Sin título"  # Título 
  review_text: str = ""  # Contenido completo del texto
  location: Optional[str] = None  # Lugar de procedencia
  contributions: Optional[int] = None  # Número total de contribuciones
  visit_date: Optional[str] = None  # Fecha en que visitó el lugar
  written_date: Optional[str] = None  # Fecha en que escribió la reseña
  companion_type: Optional[str] = None  # Tipo de acompañante durante la visita
  sentiment: Optional[str] = None  # Sentimiento detectado por análisis
  sentiment_score: Optional[float] = None  # Puntuación numérica del sentimiento en escala 0-4
  analyzed_at: Optional[str] = None  # Timestamp de cuándo se realizó el análisis de sentimiento