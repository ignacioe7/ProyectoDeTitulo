import json
import aiofiles 
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from unidecode import unidecode 
from loguru import logger as log # Logs
import re 
from ..models.attraction import Attraction # Modelo
from ..utils.constants import PathConfig # Rutas
from ..utils.exporters import DataExporter # Para exportar
from .analyzer import load_analyzer
import streamlit as st 

class DataHandler:
  """Maneja la carga y exportación de datos"""
  def __init__(self):
    self.paths = PathConfig() # Config de rutas
    self.exporter = self._get_exporter() # El exportador
    self._ensure_dirs() # Asegura que existan las carpetas
    self.regions_data: Dict[str, Dict] = {} 
    self.regions: List[str] = [] 
    self.load_regions_on_init() 


  def _ensure_dirs(self):
    """Garantiza que existen todos los directorios necesarios"""
    Path(self.paths.ATTRACTIONS_DIR).mkdir(parents=True, exist_ok=True)
    Path(self.paths.REGIONS_DIR).mkdir(parents=True, exist_ok=True)
    Path(self.paths.OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    Path(self.paths.LOGS_DIR).mkdir(parents=True, exist_ok=True)

  # ---- Operaciones con Regiones ----

  def load_regions_on_init(self):
    """Carga las regiones desde el archivo JSON y las almacena en el objeto."""
    try:
        regions_file = Path(self.paths.DATA_DIR) / "regions" / "regions.json" 
        if not regions_file.exists():
            log.error(f"Archivo de regiones no encontrado en: {regions_file}")
            self.regions_data = {}
            self.regions = []
            return

        with open(regions_file, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f) 
        
        self.regions_data = {}
        temp_regions_list = []

        if isinstance(loaded_data, list): 
            for region_dict in loaded_data:
                if isinstance(region_dict, dict) and "nombre" in region_dict:
                    region_name = region_dict["nombre"] 
                    self.regions_data[region_name] = region_dict
                    temp_regions_list.append(region_name)
                else:
                    log.warning(f"Elemento en regions.json no es un diccionario válido o no tiene 'nombre': {region_dict}")

        self.regions = sorted(list(set(temp_regions_list))) 
        
        if self.regions:
            log.info(f"Regiones cargadas exitosamente al inicializar DataHandler: {len(self.regions)} regiones.")
        else:
            log.warning("No se cargaron regiones o el archivo está vacío/mal formateado.")

    except FileNotFoundError:
        log.error(f"El archivo de configuración de regiones no se encontró: {regions_file}")
        self.regions_data = {}
        self.regions = []
    except json.JSONDecodeError:
        log.error(f"Error al decodificar el JSON del archivo de regiones: {regions_file}")
        self.regions_data = {}
        self.regions = []
    except Exception as e:
        log.error(f"Error inesperado al cargar regiones en __init__: {e}")
        self.regions_data = {}
        self.regions = []

  def load_regions(self) -> List[Dict]:
    if not self.regions_data:
        log.warning("load_regions llamado pero self.regions_data está vacío. Intentando recargar.")
        self.load_regions_on_init()

    return list(self.regions_data.values())
    
  

  # ---- Operaciones con Atracciones ----
  async def save_attractions(self, region_name: str, attractions: List[Dict]) -> Path:
    """Guarda atracciones en formato JSON (versión asíncrona)"""
    try:
      filename = self.get_attraction_filepath(region_name) # Nombre del archivo
      data = {
        "region": region_name,
        "attractions": attractions,
        "scrape_date": datetime.now().isoformat() # Fecha actual
      }

      # Usar aiofiles para escritura asíncrona
      async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
        await f.write(json.dumps(data, indent=2, ensure_ascii=False)) # Guardamos bonito

      log.success(f"Atracciones guardadas en {filename}")
      return filename

    except Exception as e:
      log.error(f"Error guardando atracciones: {e}")
      raise # Relanzamos el error

  def load_attractions(self, region_name: str) -> List[Attraction]:
    """Carga atracciones y las convierte a objetos Attraction"""
    filename = self.get_attraction_filepath(region_name)
    try:
      with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
        # Convertimos cada diccionario a un objeto Attraction
        return [Attraction(**attr) for attr in data.get('attractions', [])]
    except FileNotFoundError:
      log.error(f"Archivo no encontrado: {filename}")
      return []
    except json.JSONDecodeError:
      log.error(f"Error leyendo archivo JSON: {filename}")
      return []

  # ---- Operaciones con Reviews ----
  async def export_reviews(self, data: Dict, format: str = "excel") -> Path:
    """Exporta reseñas, puede ser a excel o json"""
    if not isinstance(data, dict) or 'region' not in data:
      log.error("Data debe ser un diccionario con clave 'region'") 
      raise ValueError("Data debe ser un diccionario con clave 'region'")
    
    original_region_name = data['region']
    log.debug(f"Exportando reseñas para la región original: '{original_region_name}', formato: '{format}'")

    sanitized_region_name_for_file = self._sanitize_region_name(original_region_name)
    log.info(f"Nombre de región sanitizado para el archivo de exportación: '{sanitized_region_name_for_file}'")

    export_data_package = {
        "region_original": original_region_name,
        "region_for_filename": sanitized_region_name_for_file, 
        "attractions_data": data.get("attractions_data", data.get("attractions", []))
    }

    if format == "excel":
      return await self.exporter.save_to_excel(export_data_package)
    elif format == "json":
      return await self.exporter.save_to_json(export_data_package)
    
    log.error(f"Formato no soportado para exportación: {format}")
    raise ValueError(f"Formato no soportado: {format}")

  # ---- Análisis de Sentimientos ----

  async def analyze_and_update_excel(self, region_name: str) -> bool:
    """Aplica análisis de sentimientos a un Excel existente y lo actualiza"""
    try:
      log.info("analyze_and_update_excel: Intentando obtener el analizador...")
      # Obtenemos el analizador como variable LOCAL
      analyzer = load_analyzer()

      if analyzer is None or analyzer.nlp is None:
        log.error("analyze_and_update_excel: El análisis no puede continuar, el analizador no está disponible.")
        st.error("Error crítico: El modelo de análisis de sentimiento no está disponible o no se cargó correctamente. Revisa los logs.")
        return False # Indica fallo
      
      # Buscamos el archivo Excel...
      filename = self._find_matching_file(region_name)
      if not filename:
        st.error(f"No se encontró archivo Excel de reseñas para {region_name}")
        return False # Indica fallo

      log.info(f"Procesando archivo para análisis: {filename}")

      # Leer las hojas del Excel
      with pd.ExcelFile(filename) as xls:
        if 'Summary' not in xls.sheet_names or 'Reviews' not in xls.sheet_names:
            st.error(f"El archivo {filename} no tiene las hojas 'Summary' y 'Reviews'")
            raise ValueError(f"El archivo {filename} no tiene las hojas 'Summary' y 'Reviews'")
        summary_df = pd.read_excel(xls, sheet_name='Summary')
        reviews_df = pd.read_excel(xls, sheet_name='Reviews')

      # Verificar columnas requeridas
      required_cols = {'Attraction', 'Title', 'Review Text'}
      if not required_cols.issubset(reviews_df.columns):
        missing = required_cols - set(reviews_df.columns)
        st.error(f"Faltan columnas requeridas en la hoja 'Reviews': {missing}")
        raise ValueError(f"Faltan columnas requeridas en la hoja 'Reviews': {missing}")

      # Aplicar análisis si no existe
      if 'Sentiment' not in reviews_df.columns or 'Sentiment Score' not in reviews_df.columns:
        log.info("Aplicando análisis de sentimiento a las reseñas...")
        # --- Barra de progreso en Streamlit ---
        progress_bar = st.progress(0.0, text="Analizando reseñas...")
        total_rows = len(reviews_df)
        sentiment_results = []

        for i, row in reviews_df.iterrows():
            result_tuple = analyzer.analyze_review(
                row.get('Title', ''),
                row.get('Review Text', '')
            )
            sentiment_results.append(pd.Series(result_tuple))
            # Actualizar barra de progreso
            progress = (i + 1) / total_rows
            progress_bar.progress(progress, text=f"Analizando reseña {i+1}/{total_rows}")

        # Asignar resultados y eliminar barra
        reviews_df[['Sentiment', 'Sentiment Score']] = pd.DataFrame(sentiment_results, index=reviews_df.index)
        progress_bar.empty() # O progress_bar.progress(1.0, text="Análisis completado")
        # --- Fin Barra de progreso ---
      else:
        log.info("Las columnas de sentimiento ya existen, omitiendo análisis")
        st.info("Las columnas de sentimiento ya existen en el archivo. Omitiendo re-análisis.")

      # Calcular estadísticas
      stats = self._calculate_sentiment_stats(reviews_df)
      summary_df = self._update_summary_stats(summary_df, stats)

      # Guardar cambios
      log.info(f"Guardando archivo actualizado: {filename}")
      with pd.ExcelWriter(filename, engine='openpyxl', mode='w') as writer:
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        reviews_df.to_excel(writer, sheet_name='Reviews', index=False)
        self._adjust_column_widths(writer, summary_df, reviews_df)

      log.success(f"Análisis y actualización completados: {filename}")
      return True

    except FileNotFoundError as e:
         log.error(f"Error archivo no encontrado en analyze_and_update_excel: {e}")
         # El error ya se mostró en st.error donde se lanzó o en _find_matching_file
         return False
    except ValueError as e:
         log.error(f"Error de valor (ej: hojas/columnas faltantes) en analyze_and_update_excel: {e}")
         # El error ya se mostró en st.error donde se lanzó
         return False
    except Exception as e:
      log.error(f"Error inesperado en analyze_and_update_excel para {region_name}: {str(e)}", exc_info=True)
      st.error(f"Ocurrió un error inesperado durante el análisis. Revisa los logs. Detalles: {str(e)}")
      return False

  def _calculate_sentiment_stats(self, df: pd.DataFrame) -> pd.DataFrame:
    """Calcula estadísticas de sentimiento por atracción"""

    if 'Sentiment' not in df.columns or df['Sentiment'].isnull().all(): # Añadido chequeo si toda la columna es null
         log.warning("_calculate_sentiment_stats: Columna 'Sentiment' no encontrada o vacía.")
         # Devolver un DataFrame vacío con las columnas esperadas para evitar errores en el merge
         return pd.DataFrame(columns=['Attraction', 'POSITIVE', 'NEGATIVE', 'NEUTRAL', 'ERROR', # Añadir otros posibles
                                      'Total Reviews Analyzed', 'POSITIVE %', 'NEGATIVE %']).set_index('Attraction')

    # Agrupamos por atracción y sentimiento, contamos cuántos hay de cada uno
    # Usamos dropna=False para contar explícitamente los NaN en Sentiment si los hubiera
    stats = df.groupby(['Attraction', 'Sentiment'], dropna=False).size().unstack(fill_value=0)

    # Aseguramos que existan las columnas POSITIVE, NEGATIVE, NEUTRAL, ERROR (y otras si aplica)
    # para cálculos consistentes, incluso si no hay reseñas de ese tipo.
    for sentiment_type in ['POSITIVE', 'NEGATIVE', 'NEUTRAL', 'ERROR']: # Asegurar columnas base
      if sentiment_type not in stats.columns:
        stats[sentiment_type] = 0

    # Calculamos el total de reseñas ANALIZADAS VÁLIDAMENTE (excluimos ERROR y NaN si existe como columna)
    valid_sentiments = [s for s in ['POSITIVE', 'NEGATIVE', 'NEUTRAL'] if s in stats.columns]
    stats['Total Reviews Analyzed'] = stats[valid_sentiments].sum(axis=1)

    # Calculamos porcentajes, evitando división por cero.
    # El denominador es el total de reseñas analizadas válidamente.
    stats['POSITIVE %'] = (stats['POSITIVE'].astype(float) / stats['Total Reviews Analyzed'].astype(float) * 100).round(1)
    stats['NEGATIVE %'] = (stats['NEGATIVE'].astype(float) / stats['Total Reviews Analyzed'].astype(float) * 100).round(1)

    # Si Total Reviews Analyzed fue 0, la división da NaN. Reemplazamos NaN con 0.
    stats['POSITIVE %'] = stats['POSITIVE %'].fillna(0)
    stats['NEGATIVE %'] = stats['NEGATIVE %'].fillna(0)


    # Seleccionar y devolver solo las columnas necesarias para el merge
    return stats[['POSITIVE %', 'NEGATIVE %', 'Total Reviews Analyzed']] # Devolvemos solo lo necesario

  def _update_summary_stats(self, summary_df: pd.DataFrame, stats: pd.DataFrame) -> pd.DataFrame:
    """Actualiza el DataFrame de resumen con las estadísticas calculadas"""

    # Columnas que esperamos añadir/actualizar
    stat_cols = ['POSITIVE %', 'NEGATIVE %', 'Total Reviews Analyzed']

    # Caso 1: No hay estadísticas calculadas (ej: no había reseñas)
    if stats.empty:
        log.warning("_update_summary_stats: DataFrame de estadísticas vacío. Rellenando con 0.")
        # Aseguramos que las columnas existan en el summary_df y las llenamos con 0
        for col in stat_cols:
            if col not in summary_df.columns:
                summary_df[col] = 0
            else:
                # Si ya existe (de una ejecución anterior?), la llenamos con 0 por si acaso
                summary_df[col] = summary_df[col].fillna(0)
        # Asegurar tipo correcto para Total Reviews Analyzed
        if 'Total Reviews Analyzed' in summary_df.columns:
             summary_df['Total Reviews Analyzed'] = summary_df['Total Reviews Analyzed'].astype(int)
        return summary_df

    # Caso 2: Hay estadísticas calculadas
    stats_to_merge = stats.reset_index() # Ahora 'Attraction' es una columna

    # Las filas sin coincidencia en stats_to_merge tendrán NaN en las columnas de stats.
    updated_summary = summary_df.merge(
      stats_to_merge,
      left_on='Attraction Name', # Columna en summary_df
      right_on='Attraction',    # Columna en stats_to_merge (después de reset_index)
      how='left'                # Mantiene todas las atracciones de summary_df
    )

    # Eliminamos la columna 'Attraction' duplicada que viene de stats_to_merge
    if 'Attraction' in updated_summary.columns:
        updated_summary = updated_summary.drop(columns=['Attraction'])

    # Rellenamos con 0 los valores NaN que resultaron del merge para las atracciones
    # que NO estaban en 'stats'. Esto es clave para tu requerimiento.
    for col in stat_cols:
        if col in updated_summary.columns:
            updated_summary[col] = updated_summary[col].fillna(0)
            # Convertimos a int si es la columna de total, después de llenar NaNs
            if col == 'Total Reviews Analyzed':
                 updated_summary[col] = updated_summary[col].astype(int)
        else:
             # Si por alguna razón la columna no se creó en el merge (muy raro), la creamos con 0s
             log.warning(f"La columna '{col}' no existía después del merge. Creándola con ceros.")
             updated_summary[col] = 0


    return updated_summary

  def _adjust_column_widths(self, writer, summary_df, reviews_df):
    """Ajusta automáticamente los anchos de columna en el Excel"""
    # Iteramos por las hojas que queremos ajustar
    for sheet_name, df in [('Summary', summary_df), ('Reviews', reviews_df)]:
      worksheet = writer.sheets[sheet_name] # Obtenemos la hoja de trabajo

      # Iteramos por cada columna del DataFrame
      for idx, col in enumerate(df.columns):
        # Encontramos la longitud máxima del contenido de la columna o del título
        max_len = max(
          df[col].astype(str).map(len).max(), # Longitud máxima del contenido
          len(str(col)) # Longitud del nombre de la columna
        )

        # Establecemos el ancho de la columna 
        worksheet.column_dimensions[chr(65 + idx)].width = min(max_len + 2, 50)

  # ---- Métodos de ayuda ----
  def _sanitize_name(self, name: str) -> str:
    """Limpia nombres para usarlos en archivos (versión simple y obsoleta, preferir _sanitize_region_name)"""
    log.warning("Usando _sanitize_name (simple). Considerar usar _sanitize_region_name.")
    name = name.lower()
    name = re.sub(r'[\s-]+', '_', name)
    name = re.sub(r'[^\w_]+', '', name) 
    return name.strip('_')

  def _sanitize_region_name(self, region_name: str) -> str:
    """Normaliza el nombre de la región para nombres de archivo usando unidecode."""
    if not isinstance(region_name, str):
        log.warning(f"Se recibió un tipo no string para sanitizar: {type(region_name)}. Devolviendo string vacío.")
        return ""
    
    log.debug(f"Sanitizando nombre de región original: '{region_name}'")

    # Eliminar prefijos como "XV-", "I-", etc.
    name = re.sub(r'^[XIV]+-?\s*', '', region_name, flags=re.IGNORECASE).strip()
    log.debug(f"Después de quitar prefijo numérico: '{name}'")

    try:
        name_unidecoded = unidecode(name)
        log.debug(f"Después de unidecode: '{name_unidecoded}'")
    except Exception as e:
        log.warning(f"Falló unidecode para '{name}', usando fallback manual. Error: {e}")
        name_unidecoded = (name.lower().replace('á', 'a').replace('é', 'e').replace('í', 'i')
                .replace('ó', 'o').replace('ú', 'u').replace('ñ', 'n'))
        name_unidecoded = re.sub(r'[^\w\s-]', '', name_unidecoded) # Quitar caracteres no deseados después del fallback
        log.debug(f"Después de fallback de unidecode: '{name_unidecoded}'")

    name_lower = name_unidecoded.lower()
    log.debug(f"Después de convertir a minúsculas: '{name_lower}'")

    # Reemplazar cualquier cosa que no sea alfanumérica por guion bajo
    name_alphanum = re.sub(r'[^\w]+', '_', name_lower, flags=re.ASCII)
    log.debug(f"Después de reemplazar no alfanuméricos por '_': '{name_alphanum}'")

    # Reemplazar múltiples guiones bajos por uno solo y quitar los de los extremos
    name_final = re.sub(r'_+', '_', name_alphanum).strip('_')
    log.info(f"Nombre de región sanitizado final: '{region_name}' -> '{name_final}'")
    return name_final

  def get_attraction_filepath(self, region_name: str) -> Path:
    """Genera la ruta esperada para archivos JSON de atracciones"""
    log.debug(f"Obteniendo ruta para archivo de atracciones. Región original: '{region_name}'")
    sanitized = self._sanitize_region_name(region_name)
    filepath = Path(self.paths.ATTRACTIONS_DIR) / f"{sanitized}_attractions.json"
    log.info(f"Ruta generada para archivo JSON de atracciones: '{filepath}'")
    return filepath

  def get_reviews_filepath(self, region_name: str, format: str = "excel") -> Path:
    """Genera la ruta esperada para archivos de reseñas (Excel o JSON)"""
    log.debug(f"Obteniendo ruta para archivo de reseñas. Región original: '{region_name}', Formato: '{format}'")
    sanitized_name = self._sanitize_region_name(region_name)
    ext = "xlsx" if format == "excel" else "json"
    filepath = Path(self.paths.OUTPUT_DIR) / f"{sanitized_name}_reviews.{ext}"
    log.info(f"Ruta generada para archivo de reseñas ({format}): '{filepath}'")
    return filepath

  def _find_matching_file(self, region_name: str) -> Optional[Path]:
    """Busca el archivo Excel de reseñas que coincida con el nombre de la región"""
    output_dir = Path(self.paths.OUTPUT_DIR)
    if not output_dir.exists():
      log.warning(f"El directorio de salida no existe: {output_dir}")
      return None

    # Nombre objetivo sanitizado
    target_name = self._sanitize_region_name(region_name)
    target_filename = f"{target_name}_reviews.xlsx"
    exact_match_path = output_dir / target_filename

    # 1 Intenta coincidencia exacta con el nombre sanitizado
    if exact_match_path.exists():
      log.debug(f"Encontrada coincidencia exacta: {exact_match_path}")
      return exact_match_path

    # 2 Si no, busca archivos que *contengan* el nombre sanitizado
    log.debug(f"No hubo coincidencia exacta, buscando archivos que contengan '{target_name}'")
    possible_matches = list(output_dir.glob(f"*{target_name}*_reviews.xlsx"))

    if len(possible_matches) == 1:
      log.info(f"Encontrada una coincidencia aproximada: {possible_matches[0]}")
      return possible_matches[0]
    elif len(possible_matches) > 1:
      log.warning(f"Múltiples archivos coinciden con '{target_name}': {possible_matches}")
      return possible_matches[0] # Devolvemos el primero por ahora

    log.error(f"No se encontró ningún archivo Excel de reseñas para '{region_name}' (buscando '{target_name}')")
    return None # No se encontró nada

  def _get_exporter(self):
    # Importamos aquí para evitar problemas de importación circular
    from ..utils.exporters import DataExporter
    return DataExporter()