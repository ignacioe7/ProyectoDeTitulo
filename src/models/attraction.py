from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime

@dataclass
class Attraction:
  # MODELO DE ATRACCION TURISTICA CON SOPORTE MULTILENGUAJE
  # Almacena informacion completa de una atraccion con estructura jerarquica
  # Incluye metadatos de scraping y analisis de sentimientos
  position: Optional[int] = None # posicion en el ranking
  attraction_name: str = "Lugar Sin Nombre" # nombre del lugar
  url: str = "" # URL de la atraccion en TripAdvisor
  rating: float = 0.0 # calificacion promedio general
  reviews_count: int = 0 # numero total de reseñas de todos los idiomas
  place_type: str = "Sin categoría" # tipo de lugar (National Parks, Museums, etc)
  languages: Optional[Dict[str, Any]] = None # diccionario con datos por idioma
  scraped_reviews_count: int = 0 # total de reseñas scrapeadas
  last_analyzed_date: Optional[str] = None # fecha del ultimo analisis de sentimientos
  previously_visited: bool = False # indica si ya se scrapeo la atraccion

  # ===============================================================
  # VALIDACIONES POST INICIALIZACION
  # ===============================================================

  def __post_init__(self):
    # VALIDACIONES Y AJUSTES AUTOMATICOS DESPUES DE LA INICIALIZACION
    # Inicializa languages si esta vacio y valida valores numericos
    if self.languages is None:
      self.languages = {}
    
    # Validar rating entre 0 y 5
    if self.rating < 0:
      self.rating = 0.0
    elif self.rating > 5:
      self.rating = 5.0

  # ===============================================================
  # CONVERTIR A DICCIONARIO
  # ===============================================================

  def to_dict(self) -> dict:
    # CONVIERTE LA ATRACCION A DICCIONARIO PARA JSON
    # Retorna todos los campos en formato serializable
    return {
      "position": self.position,
      "attraction_name": self.attraction_name,
      "url": self.url,
      "rating": self.rating,
      "reviews_count": self.reviews_count,
      "place_type": self.place_type,
      "languages": self.languages,
      "scraped_reviews_count": self.scraped_reviews_count,
      "last_analyzed_date": self.last_analyzed_date
    }

  # ===============================================================
  # CREAR DESDE DICCIONARIO
  # ===============================================================

  @classmethod
  def from_dict(cls, data: dict) -> 'Attraction':
    # CREA UNA INSTANCIA DE ATTRACTION DESDE UN DICCIONARIO
    # Maneja valores por defecto y conversion de tipos
    return cls(
      position=data.get("position"),
      attraction_name=data.get("attraction_name", "Lugar Sin Nombre"),
      url=data.get("url", ""),
      rating=float(data.get("rating", 0.0)),
      reviews_count=int(data.get("reviews_count", 0)),
      place_type=data.get("place_type", "Sin categoría"),
      languages=data.get("languages", {}),
      scraped_reviews_count=int(data.get("scraped_reviews_count", 0)),
      last_analyzed_date=data.get("last_analyzed_date")
    )

  # ===============================================================
  # OBTENER RESEÑAS POR IDIOMA
  # ===============================================================

  def get_reviews_by_language(self, language: str) -> list:
    # OBTIENE LAS RESEÑAS DE UN IDIOMA ESPECIFICO
    # Retorna lista vacia si el idioma no existe
    if not self.languages or language not in self.languages:
      return []
    
    return self.languages[language].get("reviews", [])

  # ===============================================================
  # AGREGAR RESEÑA A IDIOMA
  # ===============================================================

  def add_review_to_language(self, language: str, review_data: dict):
    # AGREGA UNA RESEÑA A UN IDIOMA ESPECIFICO
    # Crea la estructura del idioma si no existe
    if not self.languages:
      self.languages = {}
    
    if language not in self.languages:
      self.languages[language] = {
        "reviews": [],
        "reviews_count": 0,
        "stored_reviews": 0,
        "skipped_duplicates": [],
        "previously_scraped": False,
        "last_scrape_date": None
      }
    
    self.languages[language]["reviews"].append(review_data)
    self.languages[language]["stored_reviews"] = len(self.languages[language]["reviews"])

  # ===============================================================
  # OBTENER TOTAL DE RESEÑAS ALMACENADAS
  # ===============================================================

  def get_total_stored_reviews(self) -> int:
    # CUENTA EL TOTAL DE RESEÑAS ALMACENADAS EN TODOS LOS IDIOMAS
    # Suma las reseñas de cada idioma disponible
    total = 0
    if self.languages:
      for lang_data in self.languages.values():
        total += len(lang_data.get("reviews", []))
    return total

  # ===============================================================
  # OBTENER IDIOMAS DISPONIBLES
  # ===============================================================

  def get_available_languages(self) -> list:
    # RETORNA LISTA DE IDIOMAS DISPONIBLES PARA ESTA ATRACCION
    # Lista vacia si no hay idiomas configurados
    return list(self.languages.keys()) if self.languages else []