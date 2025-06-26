from dataclasses import dataclass
from typing import Optional

@dataclass
class Review:
  # MODELO DE DATOS PARA RESEÑA SIMPLIFICADO
  # Almacena informacion basica de una reseña de TripAdvisor
  # Compatible con estructura actual de consolidated_data.json
  review_id: Optional[str] = None  
  username: str = "Anónimo" # nombre de usuario
  rating: float = 0.0 # calificacion dada por el usuario 1-5
  title: str = "Sin título" # titulo de la reseña
  review_text: str = "" # texto completo de la reseña
  location: Optional[str] = None # ubicacion del usuario
  contributions: Optional[int] = None # numero de contribuciones del usuario
  visit_date: Optional[str] = None # fecha de la visita
  written_date: Optional[str] = None # fecha en que escribio la reseña
  companion_type: Optional[str] = None # tipo de acompañante
  sentiment: Optional[str] = None # sentimiento detectado por IA
  sentiment_score: Optional[float] = None # puntuacion del sentimiento 0-4
  analyzed_at: Optional[str] = None # fecha del analisis de sentimiento
  
  # ===============================================================
  # VALIDACIONES POST INICIALIZACION
  # ===============================================================
  
  def __post_init__(self):
    # VALIDACIONES Y AJUSTES AUTOMATICOS DESPUES DE LA INICIALIZACION
    # Genera ID si no existe y valida valores numericos
    if not self.review_id:
      self.review_id = self._generate_fallback_id()
    
    # Validar rating entre 0 y 5
    if self.rating < 0:
      self.rating = 0.0
    elif self.rating > 5:
      self.rating = 5.0
  
  # ===============================================================
  # GENERAR ID DE RESPALDO
  # ===============================================================
  
  def _generate_fallback_id(self) -> str:
    # GENERA UN ID DE RESPALDO BASADO EN CONTENIDO UNICO
    # Usa hash MD5 de campos identificadores principales
    import hashlib
    
    content = f"{self.username}_{self.written_date}_{self.title[:50]}"
    hash_obj = hashlib.md5(content.encode('utf-8'))
    return f"fallback_{hash_obj.hexdigest()[:12]}"
  
  # ===============================================================
  # CONVERTIR A DICCIONARIO
  # ===============================================================
  
  def to_dict(self) -> dict:
    # CONVIERTE LA RESEÑA A DICCIONARIO PARA JSON
    # Retorna todos los campos en formato serializable
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
      "sentiment": self.sentiment,
      "sentiment_score": self.sentiment_score,
      "analyzed_at": self.analyzed_at
    }
  
  # ===============================================================
  # CREAR DESDE DICCIONARIO
  # ===============================================================
  
  @classmethod
  def from_dict(cls, data: dict) -> 'Review':
    # CREA UNA INSTANCIA DE REVIEW DESDE UN DICCIONARIO
    # Maneja valores por defecto y conversion de tipos
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
      sentiment=data.get("sentiment"),
      sentiment_score=data.get("sentiment_score"),
      analyzed_at=data.get("analyzed_at")
    )
  
  # ===============================================================
  # VERIFICAR SI ES DUPLICADA
  # ===============================================================
  
  def is_duplicate_of(self, other: 'Review') -> bool:
    # DETERMINA SI ESTA RESEÑA ES DUPLICADA DE OTRA
    # Compara por ID primero luego por contenido unico
    if not isinstance(other, Review):
      return False
    
    # Comparar por ID si ambos lo tienen
    if self.review_id and other.review_id:
      return self.review_id == other.review_id
    
    # Comparar por contenido unico
    return (
      self.username == other.username and
      self.written_date == other.written_date and
      self.title == other.title
    )
  
  # ===============================================================
  # OBTENER HASH UNICO
  # ===============================================================
  
  def get_unique_hash(self) -> str:
    # OBTIENE UN HASH UNICO PARA ESTA RESEÑA
    # Usa review_id si es valido sino genera hash de contenido
    if self.review_id and self.review_id.startswith("review_"):
      return self.review_id
    
    content = f"{self.username}_{self.written_date}_{self.title}"
    import hashlib
    return f"hash_{hashlib.md5(content.encode()).hexdigest()[:12]}"