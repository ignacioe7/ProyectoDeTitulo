import asyncio
import json
import aiofiles
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
from loguru import logger as log

from ..utils.constants import PathConfig

# ========================================================================================================
#                                            MANEJADOR DE DATOS
# ========================================================================================================

# Clase principal para manejar carga, guardado y manipulación de datos JSON
# Gestiona archivos de regiones, atracciones y reseñas de forma asíncrona
class DataHandler:

  def __init__(self):
    self.paths = PathConfig()
    self._ensure_dirs()
    
    # Configuración de regiones cargada desde archivo
    self.regions_data: Dict[str, Dict] = {}
    self.regions: List[str] = []
    self._load_regions_config()
    
    # Estructura principal de datos consolidados
    self.consolidated_file: Path = self.paths.CONSOLIDATED_JSON
    self.data: Dict[str, List[Dict[str, Any]]] = self._load_data()

# ========================================================================================================
#                                         ASEGURAR DIRECTORIOS
# ========================================================================================================

  def _ensure_dirs(self):
    # CREA LOS DIRECTORIOS NECESARIOS SI NO EXISTEN
    Path(self.paths.REGIONS_DIR).mkdir(parents=True, exist_ok=True)
    Path(self.paths.LOGS_DIR).mkdir(parents=True, exist_ok=True)

# ========================================================================================================
#                                        CARGAR CONFIGURACIÓN
# ========================================================================================================

  def _load_regions_config(self):
    # CARGA LA CONFIGURACIÓN DE REGIONES DESDE EL ARCHIVO JSON
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

# ========================================================================================================
#                                           CARGAR DATOS
# ========================================================================================================

  def _load_data(self) -> Dict[str, List[Dict[str, Any]]]:
    # CARGA LOS DATOS CONSOLIDADOS DESDE EL ARCHIVO PRINCIPAL
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

# ========================================================================================================
#                                      OBTENER CONFIGURACIÓN
# ========================================================================================================

  def get_region_config(self, region_name: str) -> Optional[Dict]:
    # OBTIENE LA CONFIGURACIÓN DE UNA REGIÓN ESPECÍFICA
    return self.regions_data.get(region_name)

# ========================================================================================================
#                                        OBTENER DATOS REGIÓN
# ========================================================================================================

  def get_region_data(self, region_name: str) -> Optional[Dict]:
    # OBTIENE LOS DATOS COMPLETOS DE UNA REGIÓN ESPECÍFICA
    for region in self.data.get("regions", []):
      if region.get("region_name") == region_name:
        return region
    return None

# ========================================================================================================
#                                    OBTENER REGIONES CON DATOS
# ========================================================================================================

  def get_regions_with_data(self) -> List[str]:
    # OBTIENE LA LISTA DE NOMBRES DE REGIONES QUE TIENEN DATOS
    return [
      region.get("region_name")
      for region in self.data.get("regions", [])
      if region.get("region_name")
    ]

# ========================================================================================================
#                                           GUARDAR DATOS
# ========================================================================================================

  async def save_data(self, data_to_save: Optional[Dict] = None) -> Path:
    # GUARDA LOS DATOS DE FORMA ASÍNCRONA EN EL ARCHIVO CONSOLIDADO
    data = data_to_save or self.data
    
    if "regions" not in data:
      data["regions"] = []

    async with aiofiles.open(self.consolidated_file, 'w', encoding='utf-8') as f:
      await f.write(json.dumps(data, indent=2, ensure_ascii=False))
    
    log.info("Datos guardados")
    return self.consolidated_file

# ========================================================================================================
#                                           RECARGAR DATOS
# ========================================================================================================

  def reload_data(self):
    # RECARGA LOS DATOS DESDE EL ARCHIVO CONSOLIDADO
    self.data = self._load_data()

# ========================================================================================================
#                                        GUARDAR ATRACCIONES
# ========================================================================================================

  async def save_attractions(self, region_name: str, attractions: List[Dict]) -> Optional[Path]:
    # GUARDA LAS ATRACCIONES PARA UNA REGIÓN ESPECÍFICA
    region_data = self._find_or_create_region(region_name)
    
    for attraction in attractions:
      self._process_attraction(region_data, attraction)
    
    region_data["last_attractions_scrape_date"] = datetime.now(timezone.utc).isoformat()
    return await self.save_data()

# ========================================================================================================
#                                     BUSCAR O CREAR REGIÓN
# ========================================================================================================

  def _find_or_create_region(self, region_name: str) -> Dict:
    # BUSCA UNA REGIÓN EXISTENTE O LA CREA SI NO EXISTE
    for region in self.data["regions"]:
      if region.get("region_name") == region_name:
        return region
    
    # Crear nueva región con estructura básica
    new_region = {
      "region_name": region_name,
      "attractions": [],
      "last_attractions_scrape_date": None
    }
    self.data["regions"].append(new_region)
    return new_region

# ========================================================================================================
#                                        PROCESAR ATRACCIÓN
# ========================================================================================================

  def _process_attraction(self, region_data: Dict, attraction_data: Dict):
    # PROCESA UNA ATRACCIÓN INDIVIDUAL ACTUALIZANDO O CREANDO
    url = attraction_data.get("url")
    name = attraction_data.get("place_name")
    
    # Buscar atracción existente por URL o nombre
    for attraction in region_data.get("attractions", []):
      if attraction.get("url") == url or attraction.get("attraction_name") == name:
        # Actualizar datos de atracción existente
        attraction.update(attraction_data)
        return
    
    # Crear 
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

# ========================================================================================================
#                                        ACTUALIZAR RESEÑAS
# ========================================================================================================

  async def update_reviews(self, region_name: str, attraction_url: str, 
                         new_reviews: List[Dict], english_count: Optional[int] = None) -> Optional[Path]:
    # ACTUALIZA LAS RESEÑAS DE UNA ATRACCIÓN ESPECÍFICA
    region_data = self.get_region_data(region_name)
    if not region_data:
      return None

    attraction = self._find_attraction_by_url(region_data, attraction_url)
    if not attraction:
      return None

    # Fusionar reseñas existentes con nuevas evitando duplicados
    existing_reviews = attraction.get("reviews", [])
    merged_reviews = self._merge_reviews(existing_reviews, new_reviews)

    # Actualizar datos de la atracción
    attraction["reviews"] = merged_reviews
    attraction["scraped_reviews_count"] = len(merged_reviews)
    attraction["last_reviews_scrape_date"] = datetime.now(timezone.utc).isoformat()
    
    if english_count is not None:
      attraction["english_reviews_count"] = english_count

    return await self.save_data()

# ========================================================================================================
#                                      BUSCAR ATRACCIÓN POR URL
# ========================================================================================================

  def _find_attraction_by_url(self, region_data: Dict, url: str) -> Optional[Dict]:
    # BUSCA UNA ATRACCIÓN POR SU URL DENTRO DE UNA REGIÓN
    for attraction in region_data.get("attractions", []):
      if attraction.get("url") == url:
        return attraction
    return None

# ========================================================================================================
#                                         FUSIONAR RESEÑAS
# ========================================================================================================

  def _merge_reviews(self, existing: List[Dict], new: List[Dict]) -> List[Dict]:
    # FUSIONA LISTAS DE RESEÑAS ELIMINANDO DUPLICADOS
    existing_map = {}
    
    # Mapear reseñas existentes por clave única
    for review in existing:
      key = self._get_review_key(review)
      existing_map[key] = review

    # Añadir o actualizar con reseñas nuevas
    for review in new:
      key = self._get_review_key(review)
      existing_map[key] = review

    return list(existing_map.values())

# ========================================================================================================
#                                        OBTENER CLAVE RESEÑA
# ========================================================================================================

  def _get_review_key(self, review: Dict) -> str:
    # GENERA UNA CLAVE ÚNICA PARA IDENTIFICAR RESEÑAS
    if review_id := review.get("review_id"):
      return str(review_id)
    
    # Fallback usando hash del contenido principal
    content = (
      review.get('username', '').strip().lower(),
      review.get('title', '').strip().lower(),
      review.get('written_date', ''),
      str(review.get('rating', ''))
    )
    return str(hash(content))

# ========================================================================================================
#                                    ACTUALIZAR ATRACCIONES REGIÓN
# ========================================================================================================

  def update_region_attractions(self, region_name: str, attractions_data: List[Dict]) -> None:
    # ACTUALIZA LAS ATRACCIONES DE UNA REGIÓN DESPUÉS DEL ANÁLISIS
    try:
      # Buscar la región específica en los datos
      for region in self.data.get("regions", []):
        if region.get("region_name") == region_name:
          region["attractions"] = attractions_data
          log.debug(f"Región '{region_name}' actualizada con {len(attractions_data)} atracciones")
          return
      
      log.warning(f"Región '{region_name}' no encontrada para actualizar")
    except Exception as e:
      log.error(f"Error actualizando atracciones de '{region_name}': {e}")

# ========================================================================================================
#                                   ACTUALIZAR FECHA ANÁLISIS
# ========================================================================================================

  def update_region_analysis_date(self, region_name: str, analysis_date: str) -> None:
    # ACTUALIZA LA FECHA DE ÚLTIMO ANÁLISIS DE SENTIMIENTOS
    try:
      for region in self.data.get("regions", []):
        if region.get("region_name") == region_name:
          region["last_analyzed_date"] = analysis_date
          log.debug(f"Fecha de análisis actualizada para '{region_name}'")
          return
      
      log.warning(f"Región '{region_name}' no encontrada para fecha")
    except Exception as e:
      log.error(f"Error actualizando fecha de '{region_name}': {e}")

# ========================================================================================================
#                                   OBTENER ESTADÍSTICAS ANÁLISIS
# ========================================================================================================

  def get_region_analysis_stats(self, region_name: str) -> Dict:
    # OBTIENE ESTADÍSTICAS DE ANÁLISIS DE SENTIMIENTOS PARA UNA REGIÓN
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
    
    # Contar reseñas totales y analizadas
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

# ========================================================================================================
#                                         EXPORTAR REGIONES
# ========================================================================================================

  async def export_regions(self, region_names: List[str], format: str = "excel") -> Optional[Path]:
    # EXPORTA REGIONES SELECCIONADAS EN EL FORMATO ESPECIFICADO
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