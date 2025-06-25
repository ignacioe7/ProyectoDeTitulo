import asyncio
import json
import aiofiles
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
from loguru import logger as log

from ..utils.constants import PathConfig


class DataHandler:
  """Maneja carga, guardado y manipulación de datos JSON"""

  def __init__(self):
    self.paths = PathConfig()
    self._ensure_dirs()
    
    # Configuración de regiones
    self.regions_data: Dict[str, Dict] = {}
    self.regions: List[str] = []
    self._load_regions_config()
    
    # Datos principales
    self.consolidated_file: Path = self.paths.CONSOLIDATED_JSON
    self.data: Dict[str, List[Dict[str, Any]]] = self._load_data()

  # ==================== CONFIGURACIÓN ====================
  
  def _ensure_dirs(self):
    """Crea directorios necesarios"""
    Path(self.paths.REGIONS_DIR).mkdir(parents=True, exist_ok=True)
    Path(self.paths.LOGS_DIR).mkdir(parents=True, exist_ok=True)

  def _load_regions_config(self):
    """Carga configuración desde regions.json"""
    try:
      if not self.paths.REGIONS_FILE.exists():
        log.error(f"Archivo no encontrado: {self.paths.REGIONS_FILE}")
        return

      with open(self.paths.REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions_list = json.load(f)

      temp_data = {}
      temp_names = []
      
      for region in regions_list:
        if isinstance(region, dict) and "nombre" in region:
          name = region["nombre"]
          temp_data[name] = region
          temp_names.append(name)

      self.regions_data = temp_data
      self.regions = sorted(list(set(temp_names)))
      log.info(f"Cargadas {len(self.regions)} regiones")
      
    except Exception as e:
      log.error(f"Error cargando regiones: {e}")
      self.regions_data = {}
      self.regions = []

  def _load_data(self) -> Dict[str, List[Dict[str, Any]]]:
    """Carga datos consolidados"""
    try:
      if self.consolidated_file.exists():
        with open(self.consolidated_file, 'r', encoding='utf-8') as f:
          data = json.load(f)
          
        if isinstance(data, dict) and "regions" in data:
          log.info("Datos cargados desde archivo")
          return data
          
    except Exception as e:
      log.error(f"Error cargando datos: {e}")
      
    log.info("Creando estructura nueva")
    return {"regions": []}

  # ==================== ACCESO A DATOS ====================
  
  def get_region_config(self, region_name: str) -> Optional[Dict]:
    """Obtiene configuración de región"""
    return self.regions_data.get(region_name)

  def get_region_data(self, region_name: str) -> Optional[Dict]:
    """Obtiene datos de región"""
    for region in self.data.get("regions", []):
      if region.get("region_name") == region_name:
        return region
    return None

  def get_regions_with_data(self) -> List[str]:
    """Obtiene nombres de regiones con datos"""
    return [
      region.get("region_name")
      for region in self.data.get("regions", [])
      if region.get("region_name")
    ]

  # ==================== GUARDADO ====================
  
  async def save_data(self, data_to_save: Optional[Dict] = None) -> Path:
    """Guarda datos de forma asíncrona"""
    data = data_to_save or self.data
    
    if "regions" not in data:
      data["regions"] = []

    async with aiofiles.open(self.consolidated_file, 'w', encoding='utf-8') as f:
      await f.write(json.dumps(data, indent=2, ensure_ascii=False))
    
    log.info("Datos guardados")
    return self.consolidated_file

  def reload_data(self):
    """Recarga datos desde archivo"""
    self.data = self._load_data()

  # ==================== ATRACCIONES ====================
  
  async def save_attractions(self, region_name: str, attractions: List[Dict]) -> Optional[Path]:
    """Guarda atracciones para una región"""
    region_data = self._find_or_create_region(region_name)
    
    for attraction in attractions:
      self._process_attraction(region_data, attraction)
    
    region_data["last_attractions_scrape_date"] = datetime.now(timezone.utc).isoformat()
    return await self.save_data()

  def _find_or_create_region(self, region_name: str) -> Dict:
    """Busca región o la crea"""
    for region in self.data["regions"]:
      if region.get("region_name") == region_name:
        return region
    
    # Crear nueva región
    new_region = {
      "region_name": region_name,
      "attractions": [],
      "last_attractions_scrape_date": None
    }
    self.data["regions"].append(new_region)
    return new_region
  
  def _process_attraction(self, region_data: Dict, attraction_data: Dict):
      """Procesa una atracción - SOLO URL COMO CLAVE ÚNICA (CORREGIDO)"""
      url = attraction_data.get("url", "").strip()
      name = attraction_data.get("attraction_name", "").strip()
      
      # ✅ VALIDACIÓN CORREGIDA: Solo validar que tenga URL y nombre válidos
      if not url or not name:
          log.warning(f"Atracción sin datos básicos omitida: name='{name}', url='{url}'")
          return
      
      # ✅ VALIDACIÓN MEJORADA: Solo rechazar si es un placeholder obvio
      if name in ["Lugar Desconocido", "Atracción Desconocida", "Sin nombre"] and not url.startswith("https://"):
          log.warning(f"Atracción placeholder omitida: {name} - {url}")
          return
      
      # ✅ BUSCAR EXISTENTE SOLO POR URL (esto ya estaba bien)
      for existing_attraction in region_data.get("attractions", []):
          existing_url = existing_attraction.get("url", "").strip()
          
          # ✅ COMPARACIÓN EXACTA POR URL ÚNICAMENTE
          if existing_url == url and url != "":
              log.debug(f"Actualizando atracción existente por URL: {name}")
              
              # Preservar datos importantes existentes
              preserved_data = {
                  "languages": existing_attraction.get("languages", {}),
                  "scraped_reviews_count": existing_attraction.get("scraped_reviews_count", 0),
                  "last_analyzed_date": existing_attraction.get("last_analyzed_date")
              }
              
              # ✅ FUSIÓN INTELIGENTE: Mantener los mejores datos
              updated_attraction = {}
              
              # Posición: preferir la nueva si existe
              updated_attraction["position"] = (
                  attraction_data.get("position") or 
                  existing_attraction.get("position")
              )
              
              # Nombre: preferir el nuevo si es más específico
              new_name = attraction_data.get("attraction_name", "").strip()
              existing_name = existing_attraction.get("attraction_name", "").strip()
              
              if new_name and new_name not in ["Lugar Desconocido", "Atracción Desconocida"]:
                  updated_attraction["attraction_name"] = new_name
              elif existing_name and existing_name not in ["Lugar Desconocido", "Atracción Desconocida"]:
                  updated_attraction["attraction_name"] = existing_name
              else:
                  updated_attraction["attraction_name"] = new_name or existing_name
              
              # URL: mantener la misma
              updated_attraction["url"] = url
              
              # Rating: usar el más alto
              new_rating = float(attraction_data.get("rating", 0.0))
              existing_rating = float(existing_attraction.get("rating", 0.0))
              updated_attraction["rating"] = max(new_rating, existing_rating)
              
              # Reviews count: usar el más alto
              new_reviews = int(attraction_data.get("reviews_count", 0))
              existing_reviews = int(existing_attraction.get("reviews_count", 0))
              updated_attraction["reviews_count"] = max(new_reviews, existing_reviews)
              
              # Place type: preferir el más específico
              new_type = attraction_data.get("place_type", "").strip()
              existing_type = existing_attraction.get("place_type", "").strip()
              
              if new_type and new_type not in ["Sin Categoría", "Unknown"]:
                  updated_attraction["place_type"] = new_type
              elif existing_type and existing_type not in ["Sin Categoría", "Unknown"]:
                  updated_attraction["place_type"] = existing_type
              else:
                  updated_attraction["place_type"] = "Sin Categoría"
              
              # Restaurar datos preservados
              updated_attraction.update(preserved_data)
              
              # ✅ ACTUALIZAR la atracción existente
              existing_attraction.clear()
              existing_attraction.update(updated_attraction)
              
              log.info(f"✅ Atracción actualizada por URL: {updated_attraction['attraction_name']} "
                       f"(rating: {updated_attraction['rating']}, reseñas: {updated_attraction['reviews_count']})")
              return  # ✅ CRUCIAL: Return para NO crear duplicado
      
      # ✅ CREAR NUEVA ATRACCIÓN (URL no encontrada - NOMBRES REPETIDOS PERMITIDOS)
      new_attraction = {
          "position": attraction_data.get("position"),
          "attraction_name": name,
          "place_type": attraction_data.get("place_type", "Sin Categoría"),
          "rating": float(attraction_data.get("rating", 0.0)),
          "reviews_count": int(attraction_data.get("reviews_count", 0)),
          "url": url,
          "languages": {},
          "scraped_reviews_count": 0,
      }
      
      region_data["attractions"].append(new_attraction)
      log.info(f"✅ Nueva atracción creada: {name} (URL: {url[:50]}...) "
               f"(rating: {new_attraction['rating']}, reseñas: {new_attraction['reviews_count']})")

  # ==================== RESEÑAS ====================
  
  async def update_reviews(self, region_name: str, attraction_url: str, 
                         new_reviews: List[Dict], english_count: Optional[int] = None,
                         language: str = "english") -> Optional[Path]:
      """Actualiza reseñas de una atracción - MULTILENGUAJE"""
      region_data = self.get_region_data(region_name)
      if not region_data:
          return None
  
      attraction = self._find_attraction_by_url(region_data, attraction_url)
      if not attraction:
          return None
  
      # ✅ NUEVO: Inicializar estructura multilenguaje si no existe
      if "languages" not in attraction:
          attraction["languages"] = {}
      
      # ✅ CORREGIDO: Calcular valores reales si el idioma no existe
      if language not in attraction["languages"]:
          # Verificar si hay reseñas existentes en structure antigua
          existing_reviews_count = 0
          existing_reviews = []
          is_previously_scraped = False
          
          # Buscar reseñas en estructura antigua que coincidan con el idioma
          old_reviews = attraction.get("reviews", [])
          for review in old_reviews:
              if review.get("language") == language:
                  existing_reviews.append(review)
                  existing_reviews_count += 1
                  is_previously_scraped = True
          
          attraction["languages"][language] = {
              "reviews": existing_reviews,  # ✅ Usar reseñas existentes
              "reviews_count": english_count if english_count is not None else existing_reviews_count,
              "stored_reviews": existing_reviews_count,  # ✅ Contar reales
              "skipped_duplicates": [],
              "previously_scraped": is_previously_scraped,  # ✅ Calcular real
              "last_scrape_date": datetime.now(timezone.utc).isoformat() if is_previously_scraped else None
          }
      else:
          # ✅ Idioma ya existe, solo actualizar reviews_count si se proporciona
          if english_count is not None:
              attraction["languages"][language]["reviews_count"] = english_count
  
      # Resto del método permanece igual...
      existing_reviews = attraction["languages"][language].get("reviews", [])
      merged_reviews = self._merge_reviews(existing_reviews, new_reviews)
  
      attraction["languages"][language]["reviews"] = merged_reviews
      attraction["languages"][language]["stored_reviews"] = len(merged_reviews)
      attraction["languages"][language]["last_scrape_date"] = datetime.now(timezone.utc).isoformat()
      attraction["languages"][language]["previously_scraped"] = True
      
      # Calcular total único entre idiomas
      all_unique_reviews = []
      all_unique_hashes = set()
      
      for lang, lang_data in attraction.get("languages", {}).items():
          for review in lang_data.get("reviews", []):
              review_key = self._get_review_key(review)
              if review_key not in all_unique_hashes:
                  all_unique_reviews.append(review)
                  all_unique_hashes.add(review_key)
      
      attraction["scraped_reviews_count"] = len(all_unique_reviews)
      
      return await self.save_data()

  def _find_attraction_by_url(self, region_data: Dict, url: str) -> Optional[Dict]:
    """Busca atracción por URL"""
    for attraction in region_data.get("attractions", []):
      if attraction.get("url") == url:
        return attraction
    return None

  def _merge_reviews(self, existing: List[Dict], new: List[Dict]) -> List[Dict]:
    """Fusiona listas de reseñas sin duplicados"""
    existing_map = {}
    
    # Mapear existentes
    for review in existing:
      key = self._get_review_key(review)
      existing_map[key] = review

    # Añadir nuevas
    for review in new:
      key = self._get_review_key(review)
      existing_map[key] = review

    return list(existing_map.values())

  def _get_review_key(self, review: Dict) -> str:
    """Obtiene clave única para reseña"""
    if review_id := review.get("review_id"):
      return str(review_id)
    
    # Fallback a hash de contenido
    content = (
      review.get('username', '').strip().lower(),
      review.get('title', '').strip().lower(),
      review.get('written_date', ''),
      str(review.get('rating', ''))
    )
    return str(hash(content))

  def update_skipped_duplicates(self, region_name: str, attraction_url: str, 
                              skipped_ids: List[str], language: str = "english") -> None:
      """Actualiza IDs de reseñas duplicadas para un idioma específico"""
      region_data = self.get_region_data(region_name)
      if not region_data:
          return

      attraction = self._find_attraction_by_url(region_data, attraction_url)
      if not attraction:
          return

      if "languages" not in attraction:
          attraction["languages"] = {}
      
      if language not in attraction["languages"]:
          attraction["languages"][language] = {
              "reviews": [],
              "reviews_count": 0,
              "stored_reviews": 0,
              "skipped_duplicates": [],
              "previously_scraped": False,
              "last_scrape_date": None
          }

      # Fusionar IDs duplicadas sin repetir
      existing_skipped = set(attraction["languages"][language].get("skipped_duplicates", []))
      existing_skipped.update(skipped_ids)
      attraction["languages"][language]["skipped_duplicates"] = list(existing_skipped)

  # ==================== ANÁLISIS DE SENTIMIENTOS ====================

  def get_all_languages_in_region(self, region_name: str) -> List[str]:
    """Obtiene lista de idiomas disponibles en una región - NUEVO"""
    region_data = self.get_region_data(region_name)
    if not region_data:
      return []

    languages = set()

    for attraction in region_data.get("attractions", []):
      # De estructura multilenguaje
      languages_data = attraction.get("languages", {})
      languages.update(languages_data.keys())

      # De estructura antigua
      old_reviews = attraction.get("reviews", [])
      for review in old_reviews:
        if lang := review.get("language"):
          languages.add(lang)

    return sorted(list(languages))

  def get_all_languages_in_data(self) -> List[str]:
    """Obtiene lista de todos los idiomas disponibles en todos los datos - NUEVO"""
    all_languages = set()

    for region in self.data.get("regions", []):
      region_name = region.get("region_name")
      if region_name:
        region_languages = self.get_all_languages_in_region(region_name)
        all_languages.update(region_languages)

    return sorted(list(all_languages))

  def get_multilingual_stats_summary(self) -> Dict:
    """Obtiene resumen de estadísticas multilenguaje - NUEVO"""
    total_regions = len(self.data.get("regions", []))
    total_attractions = 0
    total_reviews = 0
    total_analyzed = 0
    language_breakdown = {}

    for region in self.data.get("regions", []):
      attractions = region.get("attractions", [])
      total_attractions += len(attractions)

      for attraction in attractions:
        # Estructura multilenguaje
        languages_data = attraction.get("languages", {})
        for lang, lang_data in languages_data.items():
          if lang not in language_breakdown:
            language_breakdown[lang] = {"total": 0, "analyzed": 0}

          reviews = lang_data.get("reviews", [])
          total_reviews += len(reviews)
          language_breakdown[lang]["total"] += len(reviews)

          for review in reviews:
            if review.get("sentiment"):
              total_analyzed += 1
              language_breakdown[lang]["analyzed"] += 1

        # Estructura antigua (compatibilidad)
        old_reviews = attraction.get("reviews", [])
        for review in old_reviews:
          lang = review.get("language", "unknown")
          if lang not in language_breakdown:
            language_breakdown[lang] = {"total": 0, "analyzed": 0}

          total_reviews += 1
          language_breakdown[lang]["total"] += 1

          if review.get("sentiment"):
            total_analyzed += 1
            language_breakdown[lang]["analyzed"] += 1

    return {
      "total_regions": total_regions,
      "total_attractions": total_attractions,
      "total_reviews": total_reviews,
      "total_analyzed": total_analyzed,
      "pending_analysis": total_reviews - total_analyzed,
      "coverage_percentage": (total_analyzed / total_reviews * 100) if total_reviews > 0 else 0,
      "available_languages": sorted(language_breakdown.keys()),
      "language_breakdown": language_breakdown
    }

  def update_region_attractions(self, region_name: str, attractions_data: List[Dict]) -> None:
    """Actualiza las atracciones de una región específica después del análisis"""
    try:
      # Buscar la región en los datos
      for region in self.data.get("regions", []):
        if region.get("region_name") == region_name:
          region["attractions"] = attractions_data
          log.debug(f"Región '{region_name}' actualizada con {len(attractions_data)} atracciones")
          return
      
      log.warning(f"Región '{region_name}' no encontrada para actualizar")
    except Exception as e:
      log.error(f"Error actualizando atracciones de '{region_name}': {e}")

  def update_region_analysis_date(self, region_name: str, analysis_date: str) -> None:
    """Actualiza la fecha de último análisis de sentimientos de una región"""
    try:
      for region in self.data.get("regions", []):
        if region.get("region_name") == region_name:
          region["last_analyzed_date"] = analysis_date
          log.debug(f"Fecha de análisis actualizada para '{region_name}'")
          return
      
      log.warning(f"Región '{region_name}' no encontrada para fecha")
    except Exception as e:
      log.error(f"Error actualizando fecha de '{region_name}': {e}")

  def get_region_analysis_stats(self, region_name: str, language: str = "all") -> Dict:  # ✅ NUEVO parámetro
    """Obtiene estadísticas de análisis para una región - MULTILENGUAJE"""
    region_data = self.get_region_data(region_name)
    if not region_data:
      return {
        "total_reviews": 0,
        "analyzed_reviews": 0,
        "pending_reviews": 0,
        "last_analyzed_date": None,
        "language_breakdown": {}  # ✅ NUEVO
      }
    
    total_reviews = 0
    analyzed_reviews = 0
    language_breakdown = {}  # ✅ NUEVO
    
    for attraction in region_data.get("attractions", []):
      # ✅ NUEVO: Procesar estructura multilenguaje
      if language == "all":
        # Compatibilidad con estructura antigua
        old_reviews = attraction.get("reviews", [])
        for review in old_reviews:
          total_reviews += 1
          review_lang = review.get("language", "unknown")
          if review_lang not in language_breakdown:
            language_breakdown[review_lang] = {"total": 0, "analyzed": 0}
          language_breakdown[review_lang]["total"] += 1
          
          if review.get("sentiment"):
            analyzed_reviews += 1
            language_breakdown[review_lang]["analyzed"] += 1
        
        # Nueva estructura multilenguaje
        languages_data = attraction.get("languages", {})
        for lang, lang_data in languages_data.items():
          if lang not in language_breakdown:
            language_breakdown[lang] = {"total": 0, "analyzed": 0}
            
          for review in lang_data.get("reviews", []):
            total_reviews += 1
            language_breakdown[lang]["total"] += 1
            
            if review.get("sentiment"):
              analyzed_reviews += 1
              language_breakdown[lang]["analyzed"] += 1
      else:
        # Filtrar por idioma específico
        language_data = attraction.get("languages", {}).get(language, {})
        for review in language_data.get("reviews", []):
          total_reviews += 1
          if review.get("sentiment"):
            analyzed_reviews += 1
        
        # Compatibilidad: procesar reseñas antiguas del idioma específico
        old_reviews = attraction.get("reviews", [])
        for review in old_reviews:
          if review.get("language") == language:
            total_reviews += 1
            if review.get("sentiment"):
              analyzed_reviews += 1
    
    return {
      "total_reviews": total_reviews,
      "analyzed_reviews": analyzed_reviews,
      "pending_reviews": total_reviews - analyzed_reviews,
      "last_analyzed_date": region_data.get("last_analyzed_date"),
      "language_breakdown": language_breakdown  # ✅ NUEVO
    }

  # ==================== EXPORTACIÓN ====================
  
  async def export_regions(self, region_names: List[str], format: str = "excel", 
                          language: str = "all") -> Optional[Path]:  # ✅ NUEVO parámetro
    """Exporta regiones seleccionadas - MULTILENGUAJE"""
    selected_regions = [
      region for region in self.data.get("regions", [])
      if region.get("region_name") in region_names
    ]

    if not selected_regions:
      return None

    # ✅ NUEVO: Filtrar por idioma si no es "all"
    if language != "all":
      import copy
      filtered_regions = copy.deepcopy(selected_regions)
      
      for region in filtered_regions:
        for attraction in region.get("attractions", []):
          # Filtrar reseñas por idioma
          filtered_reviews = []
          
          # Nueva estructura multilenguaje
          language_data = attraction.get("languages", {}).get(language, {})
          filtered_reviews.extend(language_data.get("reviews", []))
          
          # Compatibilidad con estructura antigua
          old_reviews = attraction.get("reviews", [])
          for review in old_reviews:
            if review.get("language") == language:
              filtered_reviews.append(review)
          
          # Reemplazar con reseñas filtradas
          attraction["reviews"] = filtered_reviews
          attraction["languages"] = {language: {"reviews": filtered_reviews}}
      
      selected_regions = filtered_regions

    from ..utils.exporters import DataExporter
    exporter = DataExporter()
    data_package = {"regions": selected_regions}

    if format == "excel":
      return await exporter.save_to_excel(data_package)
    elif format == "json":
      return await exporter.save_to_json(data_package)
    else:
      raise ValueError(f"Formato no soportado: {format}")
