from dataclasses import dataclass
from typing import Optional

@dataclass
class Review:
  """Modelo de datos para una reseña con soporte multilenguaje"""
  review_id: Optional[str] = None  
  username: str = "Anónimo"  # Nombre de usuario
  rating: float = 0.0  # Calificación dada por el usuario
  title: str = "Sin título"  # Título de la reseña
  review_text: str = ""  # Texto completo de la reseña
  location: Optional[str] = None  # Ubicación del usuario
  contributions: Optional[int] = None  # Número de contribuciones del usuario
  visit_date: Optional[str] = None  # Fecha de la visita
  written_date: Optional[str] = None  # Fecha en que escribió la reseña
  companion_type: Optional[str] = None  # Tipo de acompañante
  language: Optional[str] = None  # Idioma de la reseña (ej: "english", "spanish")
  original_language: Optional[str] = None  # Idioma original si es traducción
  is_translated: bool = False  # Indica si es una traducción automática
  sentiment: Optional[str] = None  # Sentimiento detectado por IA
  sentiment_score: Optional[float] = None  # Puntuación del sentimiento (0-4)
  analyzed_at: Optional[str] = None  # Fecha del análisis de sentimiento
  scraped_at: Optional[str] = None  # Fecha cuando se extrajo la reseña
  source_url: Optional[str] = None  # URL de donde se extrajo
  
  def __post_init__(self):
    """Validaciones y ajustes automáticos después de la inicialización"""
    # Generar ID si no existe
    if not self.review_id:
      self.review_id = self._generate_fallback_id()
    
    # Validar rating
    if self.rating < 0:
      self.rating = 0.0
    elif self.rating > 5:
      self.rating = 5.0
  
  def _generate_fallback_id(self) -> str:
    """Genera un ID de respaldo basado en contenido único"""
    import hashlib
    
    # Usar campos únicos para generar hash
    content = f"{self.username}_{self.written_date}_{self.title[:50]}"
    hash_obj = hashlib.md5(content.encode('utf-8'))
    return f"fallback_{hash_obj.hexdigest()[:12]}"
  
  def to_dict(self) -> dict:
    """Convierte la reseña a diccionario para JSON"""
    return {
      "review_id": self.review_id,
      "username": self.username,
      "rating": self.rating,
      "title": self.title,
      "review_text": self.review_text,
      "location": self.location,
      "contributions": self.contributions,
      "visit_date": self.visit_date,
      "written_date": self.written_date,
      "companion_type": self.companion_type,
      "language": self.language,
      "original_language": self.original_language,
      "is_translated": self.is_translated,
      "sentiment": self.sentiment,
      "sentiment_score": self.sentiment_score,
      "analyzed_at": self.analyzed_at,
      "scraped_at": self.scraped_at,
      "source_url": self.source_url
    }
  
  @classmethod
  def from_dict(cls, data: dict) -> 'Review':
    """Crea una instancia de Review desde un diccionario"""
    return cls(
      review_id=data.get("review_id"),
      username=data.get("username", "Anónimo"),
      rating=float(data.get("rating", 0.0)),
      title=data.get("title", "Sin título"),
      review_text=data.get("review_text", ""),
      location=data.get("location"),
      contributions=data.get("contributions"),
      visit_date=data.get("visit_date"),
      written_date=data.get("written_date"),
      companion_type=data.get("companion_type"),
      language=data.get("language"),
      original_language=data.get("original_language"),
      is_translated=data.get("is_translated", False),
      sentiment=data.get("sentiment"),
      sentiment_score=data.get("sentiment_score"),
      analyzed_at=data.get("analyzed_at"),
      scraped_at=data.get("scraped_at"),
      source_url=data.get("source_url")
    )
  
  def is_duplicate_of(self, other: 'Review') -> bool:
    """Determina si esta reseña es duplicada de otra"""
    if not isinstance(other, Review):
      return False
    
    # Comparar por ID si ambos lo tienen
    if self.review_id and other.review_id:
      return self.review_id == other.review_id
    
    # Comparar por contenido único
    return (
      self.username == other.username and
      self.written_date == other.written_date and
      self.title == other.title
    )
  
  def get_unique_hash(self) -> str:
    """Obtiene un hash único para esta reseña"""
    if self.review_id and self.review_id.startswith("review_"):
      return self.review_id
    
    content = f"{self.username}_{self.written_date}_{self.title}"
    import hashlib
    return f"hash_{hashlib.md5(content.encode()).hexdigest()[:12]}"