import json
import re
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from loguru import logger as log

from .constants import PathConfig

class DataExporter:
  """
  Clase encargada de exportar los datos recolectados
  a formatos como JSON o Excel.
  """

  def __init__(self):
    self.paths = PathConfig()
    self._ensure_dirs()

  def _ensure_dirs(self):
    """Asegura la existencia de los directorios de salida"""
    Path(self.paths.ATTRACTIONS_DIR).mkdir(parents=True, exist_ok=True)
    Path(self.paths.OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

  async def save_to_json(self, data: Dict, filename: str = None) -> Path:
    """Guarda datos en un archivo JSON"""
    # Timestamp para nombre de archivo único si no se provee
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = filename or f"attractions_{timestamp}.json"
    filepath = Path(self.paths.ATTRACTIONS_DIR) / filename

    try:
      # Escritura del archivo JSON
      with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
      log.success(f"Datos JSON guardados en {filepath}")
      return filepath
    except Exception as e:
      log.error(f"Problema al guardar JSON: {e}")
      raise # Re-lanza la excepción para manejo superior

  async def save_to_excel(self, data_package: Dict) -> Path:
    region_name_for_file = data_package.get("region_for_filename")
    if not region_name_for_file:
      log.error("Falta 'region_for_filename' en data_package para save_to_excel")
      raise ValueError("Se requiere 'region_for_filename' para nombrar el archivo Excel")

    attractions_data = data_package.get("attractions_data", [])
    
    # Construcción del nombre de archivo con nombre sanitizado
    filename = Path(self.paths.OUTPUT_DIR) / f"{region_name_for_file}_reviews.xlsx"
    log.info(f"DataExporter: Nombre de archivo Excel a generar: {filename}")

    # Preparación de datos para los métodos helper
    data_for_helpers = {'attractions': attractions_data}
    summary_df = self._create_summary_df(data_for_helpers)
    reviews_df = self._create_reviews_df(data_for_helpers)

    try:
      with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        reviews_df.to_excel(writer, sheet_name='Reviews', index=False)
        self._adjust_column_widths(writer, [summary_df, reviews_df]) 
      log.success(f"Archivo Excel generado: {filename}")
      return filename
    except Exception as e:
      log.error(f"Error al guardar Excel {filename}: {e}")
      raise

  def _create_summary_df(self, data: Dict) -> pd.DataFrame:
    """Crea el DataFrame para la hoja de resumen"""
    rows = []
    for attraction in data['attractions']:
      # Extracción de datos relevantes por atracción
      rows.append({
        'Attraction Name': attraction.get('place_name'),
        'Type': attraction.get('place_type'),
        'Rating': attraction.get('rating', 0.0), # Default 0.0 si no existe rating
        'Total Reviews': attraction.get('total_reviews', 0),
        'Total English Reviews': attraction.get('english_reviews', 0), # Reviews en inglés
        'URL': attraction.get('url', '') # URL de la atracción
      })
    # Conversión de lista de dicts a DataFrame
    return pd.DataFrame(rows)

  def _create_reviews_df(self, data: Dict) -> pd.DataFrame:
    """Crea el DataFrame para la hoja de reseñas, evitando duplicados"""
    reviews = []
    seen_hashes = set() # Set para control de duplicados mediante hash

    for attraction in data['attractions']:
      for review in attraction.get('reviews', []): # Itera sobre reseñas de la atracción
        # Genera un hash para la reseña basado en campos clave
        # Permite identificar duplicados de forma eficiente
        review_hash = hash((
          review.get('username', ''),
          review.get('title', ''),
          review.get('written_date', ''),
          str(review.get('rating', 0)) # Rating a string para el hash
        ))

        # Si el hash no existe, se agrega la reseña
        if review_hash not in seen_hashes:
          seen_hashes.add(review_hash)
          reviews.append({
            'Attraction': attraction.get('place_name'), # Atracción asociada
            'Username': review.get('username'), # Autor de la reseña
            'Rating': review.get('rating'), # Calificación
            'Location': review.get('location'), # Ubicación del autor
            'Contributions': review.get('contributions'), # N° de contribuciones
            'Visit Date': review.get('visit_date'), # Fecha de visita
            'Written Date': review.get('written_date'), # Fecha de escritura
            'Companion Type': review.get('companion_type'), # Tipo de acompañante
            'Title': review.get('title'), # Título de la reseña
            'Review Text': review.get('review_text'), # Texto completo
          })

    df = pd.DataFrame(reviews)

    return df.drop_duplicates(
      subset=['Username', 'Title', 'Written Date', 'Rating'],
      keep='first'
    )

  def _adjust_column_widths(self, writer, dfs: list):
    """Ajusta el ancho de columnas en Excel para mejor visualización"""
    # Itera sobre cada hoja y su DataFrame asociado
    for sheet_name, df in zip(writer.sheets.keys(), dfs):
      worksheet = writer.sheets[sheet_name]
      # Itera sobre cada columna del DataFrame
      for idx, col in enumerate(df.columns):
        # Calcula ancho: máximo entre contenido y nombre de columna, más margen
        max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
        # Asigna ancho a columna, con tope de 50 para evitar exceso
        column_letter = chr(65 + idx) # Letra de columna (A, B, C...)
        worksheet.column_dimensions[column_letter].width = min(max_len, 50)