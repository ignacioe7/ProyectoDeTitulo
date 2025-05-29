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
    """Procesa una atracción"""
    url = attraction_data.get("url")
    name = attraction_data.get("place_name")
    
    # Buscar existente
    for attraction in region_data.get("attractions", []):
      if attraction.get("url") == url or attraction.get("attraction_name") == name:
        # Actualizar existente
        attraction.update(attraction_data)
        return
    
    # Añadir nueva
    new_attraction = {
      "position": attraction_data.get("position"),
      "attraction_name": name or "Atracción Desconocida",
      "place_type": attraction_data.get("place_type", "Sin Categoría"),
      "rating": attraction_data.get("rating", 0.0),
      "reviews_count": attraction_data.get("reviews_count", 0),
      "url": url or "",
      "reviews": [],
      "scraped_reviews_count": 0,
      "english_reviews_count": 0,
      "last_reviews_scrape_date": None
    }
    region_data["attractions"].append(new_attraction)

  # ==================== RESEÑAS ====================
  
  async def update_reviews(self, region_name: str, attraction_url: str, 
                         new_reviews: List[Dict], english_count: Optional[int] = None) -> Optional[Path]:
    """Actualiza reseñas de una atracción"""
    region_data = self.get_region_data(region_name)
    if not region_data:
      return None

    attraction = self._find_attraction_by_url(region_data, attraction_url)
    if not attraction:
      return None

    # Fusionar reseñas
    existing_reviews = attraction.get("reviews", [])
    merged_reviews = self._merge_reviews(existing_reviews, new_reviews)

    # Actualizar
    attraction["reviews"] = merged_reviews
    attraction["scraped_reviews_count"] = len(merged_reviews)
    attraction["last_reviews_scrape_date"] = datetime.now(timezone.utc).isoformat()
    
    if english_count is not None:
      attraction["english_reviews_count"] = english_count

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

  # ==================== ANÁLISIS DE SENTIMIENTOS ====================

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

  def get_region_analysis_stats(self, region_name: str) -> Dict:
    """Obtiene estadísticas de análisis para una región"""
    region_data = self.get_region_data(region_name)
    if not region_data:
      return {
        "total_reviews": 0,
        "analyzed_reviews": 0,
        "pending_reviews": 0,
        "last_analyzed_date": None
      }
    
    total_reviews = 0
    analyzed_reviews = 0
    
    for attraction in region_data.get("attractions", []):
      for review in attraction.get("reviews", []):
        total_reviews += 1
        if review.get("sentiment"):
          analyzed_reviews += 1
    
    return {
      "total_reviews": total_reviews,
      "analyzed_reviews": analyzed_reviews,
      "pending_reviews": total_reviews - analyzed_reviews,
      "last_analyzed_date": region_data.get("last_analyzed_date")
    }

  # ==================== EXPORTACIÓN ====================
  
  async def export_regions(self, region_names: List[str], format: str = "excel") -> Optional[Path]:
    """Exporta regiones seleccionadas"""
    selected_regions = [
      region for region in self.data.get("regions", [])
      if region.get("region_name") in region_names
    ]

    if not selected_regions:
      return None

    from ..utils.exporters import DataExporter
    exporter = DataExporter()
    data_package = {"regions": selected_regions}

    if format == "excel":
      return await exporter.save_to_excel(data_package)
    elif format == "json":
      return await exporter.save_to_json(data_package)
    else:
      raise ValueError(f"Formato no soportado: {format}")