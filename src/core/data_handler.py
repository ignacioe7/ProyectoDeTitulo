import asyncio
import json
import aiofiles
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
from loguru import logger as log

from ..utils.constants import PathConfig

# ===============================================================
# MANEJADOR DE DATOS
# ===============================================================

class DataHandler:
  # MANEJA CARGA, GUARDADO Y MANIPULACION DE DATOS JSON
  # Centraliza operaciones de persistencia y acceso a datos
  # Soporta estructura multilenguaje para reseñas

  def __init__(self):
    self.paths = PathConfig()
    self._ensure_dirs()
    
    # Configuracion de regiones
    self.regions_data: Dict[str, Dict] = {}
    self.regions: List[str] = []
    self._load_regions_config()
    
    # Datos principales
    self.consolidated_file: Path = self.paths.CONSOLIDATED_JSON
    self.data: Dict[str, List[Dict[str, Any]]] = self._load_data()

  # ===============================================================
  # ASEGURAR DIRECTORIOS
  # ===============================================================
  
  def _ensure_dirs(self):
    # CREA DIRECTORIOS NECESARIOS SI NO EXISTEN
    # Configura estructura basica de carpetas del proyecto
    Path(self.paths.REGIONS_DIR).mkdir(parents=True, exist_ok=True)
    Path(self.paths.LOGS_DIR).mkdir(parents=True, exist_ok=True)

  # ===============================================================
  # CARGAR CONFIGURACION DE REGIONES
  # ===============================================================

  def _load_regions_config(self):
    # CARGA CONFIGURACION DESDE ARCHIVO REGIONS.JSON
    # Lee definiciones de regiones disponibles para scraping
    # Inicializa estructura basica si no existe archivo
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

  # ===============================================================
  # CARGAR DATOS CONSOLIDADOS
  # ===============================================================

  def _load_data(self) -> Dict[str, List[Dict[str, Any]]]:
    # CARGA DATOS CONSOLIDADOS DESDE ARCHIVO PRINCIPAL
    # Lee estructura completa de regiones y atracciones
    # Retorna estructura vacia si no existe archivo
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

  # ===============================================================
  # OBTENER CONFIGURACION DE REGION
  # ===============================================================
  
  def get_region_config(self, region_name: str) -> Optional[Dict]:
    # OBTIENE CONFIGURACION DE REGION ESPECIFICA
    # Retorna metadatos de region desde archivo de configuracion
    return self.regions_data.get(region_name)

  # ===============================================================
  # OBTENER DATOS DE REGION
  # ===============================================================

  def get_region_data(self, region_name: str) -> Optional[Dict]:
    # OBTIENE DATOS COMPLETOS DE REGION
    # Incluye atracciones, reseñas y metadatos de scraping
    for region in self.data.get("regions", []):
      if region.get("region_name") == region_name:
        return region
    return None

  # ===============================================================
  # OBTENER REGIONES CON DATOS
  # ===============================================================

  def get_regions_with_data(self) -> List[str]:
    # OBTIENE NOMBRES DE REGIONES QUE TIENEN DATOS
    # Lista regiones que ya fueron procesadas y tienen contenido
    return [
      region.get("region_name")
      for region in self.data.get("regions", [])
      if region.get("region_name")
    ]

  # ===============================================================
  # GUARDAR DATOS
  # ===============================================================
  
  async def save_data(self, data_to_save: Optional[Dict] = None) -> Path:
    # GUARDA DATOS DE FORMA ASINCRONA
    # Persiste estructura completa en archivo JSON
    # Usa datos internos si no se especifica estructura
    data = data_to_save or self.data
    
    if "regions" not in data:
      data["regions"] = []

    async with aiofiles.open(self.consolidated_file, 'w', encoding='utf-8') as f:
      await f.write(json.dumps(data, indent=2, ensure_ascii=False))
    
    log.info("Datos guardados")
    return self.consolidated_file

  # ===============================================================
  # RECARGAR DATOS
  # ===============================================================

  def reload_data(self):
    # RECARGA DATOS DESDE ARCHIVO
    # Actualiza estructura interna con contenido del disco
    self.data = self._load_data()

  # ===============================================================
  # GUARDAR ATRACCIONES
  # ===============================================================
  
  async def save_attractions(self, region_name: str, attractions: List[Dict]) -> Optional[Path]:
    # GUARDA ATRACCIONES PARA UNA REGION
    # Procesa lista de atracciones y actualiza region
    # Maneja fusion inteligente de datos duplicados
    region_data = self._find_or_create_region(region_name)
    
    for attraction in attractions:
      self._process_attraction(region_data, attraction)
    
    region_data["last_attractions_scrape_date"] = datetime.now(timezone.utc).isoformat()
    return await self.save_data()

  # ===============================================================
  # BUSCAR O CREAR REGION
  # ===============================================================

  def _find_or_create_region(self, region_name: str) -> Dict:
    # BUSCA REGION EXISTENTE O CREA NUEVA
    # Inicializa estructura basica para nueva region
    for region in self.data["regions"]:
      if region.get("region_name") == region_name:
        return region
    
    # Crear nueva region
    new_region = {
      "region_name": region_name,
      "attractions": [],
      "last_attractions_scrape_date": None
    }
    self.data["regions"].append(new_region)
    return new_region
  
  # ===============================================================
  # PROCESAR ATRACCION
  # ===============================================================

  def _process_attraction(self, region_data: Dict, attraction_data: Dict):
      # PROCESA UNA ATRACCION USANDO URL COMO CLAVE UNICA
      # Fusiona datos inteligentemente para evitar duplicados
      # Preserva informacion importante de reseñas existentes
      url = attraction_data.get("url", "").strip()
      name = attraction_data.get("attraction_name", "").strip()
      
      # Validacion basica de datos requeridos
      if not url or not name:
          log.warning(f"Atracción sin datos básicos omitida: name='{name}', url='{url}'")
          return
      
      # Filtrar placeholders obvios
      if name in ["Lugar Desconocido", "Atracción Desconocida", "Sin nombre"] and not url.startswith("https://"):
          log.warning(f"Atracción placeholder omitida: {name} - {url}")
          return
      
      # Buscar atraccion existente por URL
      for existing_attraction in region_data.get("attractions", []):
          existing_url = existing_attraction.get("url", "").strip()
          
          # Comparacion exacta por URL
          if existing_url == url and url != "":
              log.debug(f"Actualizando atracción existente por URL: {name}")
              
              # Preservar datos importantes
              preserved_data = {
                  "languages": existing_attraction.get("languages", {}),
                  "scraped_reviews_count": existing_attraction.get("scraped_reviews_count", 0),
                  "last_analyzed_date": existing_attraction.get("last_analyzed_date")
              }
              
              # Fusion inteligente de metadatos
              updated_attraction = {}
              
              # Posicion: preferir nueva si existe
              updated_attraction["position"] = (
                  attraction_data.get("position") or 
                  existing_attraction.get("position")
              )
              
              # Nombre: preferir el mas especifico
              new_name = attraction_data.get("attraction_name", "").strip()
              existing_name = existing_attraction.get("attraction_name", "").strip()
              
              if new_name and new_name not in ["Lugar Desconocido", "Atracción Desconocida"]:
                  updated_attraction["attraction_name"] = new_name
              elif existing_name and existing_name not in ["Lugar Desconocido", "Atracción Desconocida"]:
                  updated_attraction["attraction_name"] = existing_name
              else:
                  updated_attraction["attraction_name"] = new_name or existing_name
              
              # URL se mantiene igual
              updated_attraction["url"] = url
              
              # Rating: usar el mas alto
              new_rating = float(attraction_data.get("rating", 0.0))
              existing_rating = float(existing_attraction.get("rating", 0.0))
              updated_attraction["rating"] = max(new_rating, existing_rating)
              
              # Reviews count: usar el mas alto
              new_reviews = int(attraction_data.get("reviews_count", 0))
              existing_reviews = int(existing_attraction.get("reviews_count", 0))
              updated_attraction["reviews_count"] = max(new_reviews, existing_reviews)
              
              # Place type: preferir el mas especifico
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
              
              # Actualizar atraccion existente
              existing_attraction.clear()
              existing_attraction.update(updated_attraction)
              
              log.info(f"Atracción actualizada por URL: {updated_attraction['attraction_name']} "
                       f"(rating: {updated_attraction['rating']}, reseñas: {updated_attraction['reviews_count']})")
              return
      
      # Crear nueva atraccion
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
      log.info(f"Nueva atracción creada: {name} (URL: {url[:50]}...) "
               f"(rating: {new_attraction['rating']}, reseñas: {new_attraction['reviews_count']})")

  # ===============================================================
  # ACTUALIZAR RESEÑAS
  # ===============================================================
  
  async def update_reviews(self, region_name: str, attraction_url: str, 
                         new_reviews: List[Dict], english_count: Optional[int] = None,
                         language: str = "english") -> Optional[Path]:
      # ACTUALIZA RESEÑAS DE UNA ATRACCION CON SOPORTE MULTILENGUAJE
      # Maneja estructura de idiomas y fusion de reseñas
      # Calcula estadisticas automaticamente
      region_data = self.get_region_data(region_name)
      if not region_data:
          return None
  
      attraction = self._find_attraction_by_url(region_data, attraction_url)
      if not attraction:
          return None
  
      # Inicializar estructura multilenguaje
      if "languages" not in attraction:
          attraction["languages"] = {}
      
      # Calcular valores reales si el idioma no existe
      if language not in attraction["languages"]:
          # Verificar reseñas en estructura antigua
          existing_reviews_count = 0
          existing_reviews = []
          is_previously_scraped = False
          
          # Buscar reseñas antiguas del idioma
          old_reviews = attraction.get("reviews", [])
          for review in old_reviews:
              if review.get("language") == language:
                  existing_reviews.append(review)
                  existing_reviews_count += 1
                  is_previously_scraped = True
          
          attraction["languages"][language] = {
              "reviews": existing_reviews,
              "reviews_count": english_count if english_count is not None else existing_reviews_count,
              "stored_reviews": existing_reviews_count,
              "skipped_duplicates": [],
              "previously_scraped": is_previously_scraped,
              "last_scrape_date": datetime.now(timezone.utc).isoformat() if is_previously_scraped else None
          }
      else:
          # Idioma ya existe, actualizar count si se proporciona
          if english_count is not None:
              attraction["languages"][language]["reviews_count"] = english_count
  
      # Fusionar reseñas nuevas con existentes
      existing_reviews = attraction["languages"][language].get("reviews", [])
      merged_reviews = self._merge_reviews(existing_reviews, new_reviews)
  
      attraction["languages"][language]["reviews"] = merged_reviews
      attraction["languages"][language]["stored_reviews"] = len(merged_reviews)
      attraction["languages"][language]["last_scrape_date"] = datetime.now(timezone.utc).isoformat()
      attraction["languages"][language]["previously_scraped"] = True
      
      # Calcular total unico entre idiomas
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

  # ===============================================================
  # BUSCAR ATRACCION POR URL
  # ===============================================================

  def _find_attraction_by_url(self, region_data: Dict, url: str) -> Optional[Dict]:
    # BUSCA ATRACCION POR URL EN DATOS DE REGION
    # Retorna estructura completa de atraccion si existe
    for attraction in region_data.get("attractions", []):
      if attraction.get("url") == url:
        return attraction
    return None

  # ===============================================================
  # FUSIONAR RESEÑAS
  # ===============================================================

  def _merge_reviews(self, existing: List[Dict], new: List[Dict]) -> List[Dict]:
    # FUSIONA LISTAS DE RESEÑAS SIN DUPLICADOS
    # Usa claves unicas para detectar contenido repetido
    # Prioriza reseñas nuevas en caso de conflicto
    existing_map = {}
    
    # Mapear reseñas existentes
    for review in existing:
      key = self._get_review_key(review)
      existing_map[key] = review

    # Añadir reseñas nuevas
    for review in new:
      key = self._get_review_key(review)
      existing_map[key] = review

    return list(existing_map.values())

  # ===============================================================
  # OBTENER CLAVE DE RESEÑA
  # ===============================================================

  def _get_review_key(self, review: Dict) -> str:
    # OBTIENE CLAVE UNICA PARA RESEÑA
    # Usa review_id si existe, sino genera hash de contenido
    # Garantiza deteccion confiable de duplicados
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

  # ===============================================================
  # ACTUALIZAR DUPLICADOS OMITIDOS
  # ===============================================================

  def update_skipped_duplicates(self, region_name: str, attraction_url: str, 
                              skipped_ids: List[str], language: str = "english") -> None:
      # ACTUALIZA IDS DE RESEÑAS DUPLICADAS PARA IDIOMA ESPECIFICO
      # Mantiene registro de contenido ya procesado
      # Evita reprocesamiento innecesario en futuros scraping
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

  # ===============================================================
  # OBTENER IDIOMAS EN REGION
  # ===============================================================

  def get_all_languages_in_region(self, region_name: str) -> List[str]:
    # OBTIENE LISTA DE IDIOMAS DISPONIBLES EN UNA REGION
    # Escanea estructura multilenguaje y antigua
    # Retorna lista ordenada de codigos de idioma
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

  # ===============================================================
  # OBTENER TODOS LOS IDIOMAS
  # ===============================================================

  def get_all_languages_in_data(self) -> List[str]:
    # OBTIENE LISTA DE TODOS LOS IDIOMAS EN TODOS LOS DATOS
    # Agrega idiomas de todas las regiones
    # Util para estadisticas globales y filtros
    all_languages = set()

    for region in self.data.get("regions", []):
      region_name = region.get("region_name")
      if region_name:
        region_languages = self.get_all_languages_in_region(region_name)
        all_languages.update(region_languages)

    return sorted(list(all_languages))

  # ===============================================================
  # OBTENER RESUMEN ESTADISTICAS MULTILENGUAJE
  # ===============================================================

  def get_multilingual_stats_summary(self) -> Dict:
    # OBTIENE RESUMEN DE ESTADISTICAS MULTILENGUAJE
    # Calcula totales por idioma y estado de analisis
    # Incluye porcentajes de cobertura y progreso
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

        # Estructura antigua compatibilidad
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

  # ===============================================================
  # ACTUALIZAR ATRACCIONES DE REGION
  # ===============================================================

  def update_region_attractions(self, region_name: str, attractions_data: List[Dict]) -> None:
    # ACTUALIZA ATRACCIONES DE REGION ESPECIFICA DESPUES DEL ANALISIS
    # Reemplaza lista completa de atracciones con datos actualizados
    # Usado tras procesar sentimientos o actualizar metadatos
    try:
      # Buscar la region en los datos
      for region in self.data.get("regions", []):
        if region.get("region_name") == region_name:
          region["attractions"] = attractions_data
          log.debug(f"Región '{region_name}' actualizada con {len(attractions_data)} atracciones")
          return
      
      log.warning(f"Región '{region_name}' no encontrada para actualizar")
    except Exception as e:
      log.error(f"Error actualizando atracciones de '{region_name}': {e}")

  # ===============================================================
  # ACTUALIZAR FECHA DE ANALISIS
  # ===============================================================

  def update_region_analysis_date(self, region_name: str, analysis_date: str) -> None:
    # ACTUALIZA FECHA DE ULTIMO ANALISIS DE SENTIMIENTOS
    # Registra timestamp de procesamiento mas reciente
    # Util para tracking de estado y planificacion
    try:
      for region in self.data.get("regions", []):
        if region.get("region_name") == region_name:
          region["last_analyzed_date"] = analysis_date
          log.debug(f"Fecha de análisis actualizada para '{region_name}'")
          return
      
      log.warning(f"Región '{region_name}' no encontrada para fecha")
    except Exception as e:
      log.error(f"Error actualizando fecha de '{region_name}': {e}")

  # ===============================================================
  # OBTENER ESTADISTICAS DE ANALISIS
  # ===============================================================

  def get_region_analysis_stats(self, region_name: str, language: str = "all") -> Dict:
    # OBTIENE ESTADISTICAS DE ANALISIS PARA UNA REGION CON SOPORTE MULTILENGUAJE
    # Cuenta reseñas totales vs analizadas por idioma
    # Incluye desglose detallado y fechas de procesamiento
    region_data = self.get_region_data(region_name)
    if not region_data:
      return {
        "total_reviews": 0,
        "analyzed_reviews": 0,
        "pending_reviews": 0,
        "last_analyzed_date": None,
        "language_breakdown": {}
      }
    
    total_reviews = 0
    analyzed_reviews = 0
    language_breakdown = {}
    
    for attraction in region_data.get("attractions", []):
      # Procesar estructura multilenguaje
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
        # Filtrar por idioma especifico
        language_data = attraction.get("languages", {}).get(language, {})
        for review in language_data.get("reviews", []):
          total_reviews += 1
          if review.get("sentiment"):
            analyzed_reviews += 1
        
        # Compatibilidad: procesar reseñas antiguas del idioma
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
      "language_breakdown": language_breakdown
    }

  # ===============================================================
  # EXPORTAR REGIONES
  # ===============================================================
  
  async def export_regions(self, region_names: List[str], format: str = "excel", 
                          language: str = "all") -> Optional[Path]:
    # EXPORTA REGIONES SELECCIONADAS CON SOPORTE MULTILENGUAJE
    # Filtra datos por idioma si se especifica
    # Soporta formatos Excel y JSON
    selected_regions = [
      region for region in self.data.get("regions", [])
      if region.get("region_name") in region_names
    ]

    if not selected_regions:
      return None

    # Filtrar por idioma si no es "all"
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