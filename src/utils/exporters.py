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
  Se encarga de guardar los datos que hemos recopilado
  en diferentes formatos como JSON o Excel.
  """

  def __init__(self):
    self.paths = PathConfig()
    self._ensure_dirs()

  def _ensure_dirs(self):
    """Asegura que las carpetas donde guardaremos los archivos existan."""
    Path(self.paths.ATTRACTIONS_DIR).mkdir(parents=True, exist_ok=True)
    Path(self.paths.OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

  async def save_to_json(self, data: Dict, filename: str = None) -> Path:
    """Guarda un diccionario de datos en un archivo JSON."""
    # Usamos la fecha y hora para crear un nombre de archivo único si no se da uno.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = filename or f"attractions_{timestamp}.json"
    filepath = Path(self.paths.ATTRACTIONS_DIR) / filename

    try:
      # Abrimos el archivo y guardamos los datos.
      with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
      log.success(f"Datos JSON guardados correctamente en {filepath}")
      return filepath
    except Exception as e:
      log.error(f"Hubo un problema al guardar el JSON: {e}")
      raise # Re-lanzamos la excepción para que se maneje más arriba si es necesario

  async def save_to_excel(self, region_data: Dict, filename: str = None) -> Path:
    """
    Exporta los datos de una región a un archivo Excel con dos hojas:
    una de atracciones y otra con todas las reseñas.
    """
    region_name = region_data.get('region', 'unknown') # Nombre por defecto si no existe.

    def sanitize_region_name(name: str) -> str:
      """Limpia el nombre de la región para usarlo en el nombre del archivo."""
      # Quita números romanos (como 'Region IV-').
      name = re.sub(r'[XIV]+-?\s*', '', name, flags=re.IGNORECASE)
      # Quita caracteres que no sean letras, números o espacios.
      name = re.sub(r'[^\w\s]', '', name.lower())
      # Reemplaza espacios y guiones por guiones bajos.
      name = re.sub(r'[\s-]+', '_', name).strip('_')
      # Reemplaza vocales con tildes y la ñ por sus versiones sin tilde.
      name = (name.replace('á', 'a').replace('é', 'e').replace('í', 'i')
             .replace('ó', 'o').replace('ú', 'u').replace('ñ', 'n'))
      return name

    sanitized_name = sanitize_region_name(region_name)
    # Crea el nombre del archivo si no se proporciona uno.
    filename = filename or f"{sanitized_name}_reviews.xlsx"
    filepath = Path(self.paths.OUTPUT_DIR) / filename

    try:
      # Usamos ExcelWriter para poder escribir en múltiples hojas.
      with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        # Hoja 1: todas las atracciones.
        summary_df = self._create_summary_df(region_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)

        # Hoja 2: Todas las reseñas.
        reviews_df = self._create_reviews_df(region_data)
        reviews_df.to_excel(writer, sheet_name='Reviews', index=False)

        # Ajustamos el ancho de las columnas para que se lean mejor.
        self._adjust_column_widths(writer, [summary_df, reviews_df])

      log.success(f"Archivo Excel creado con éxito: {filepath}")
      return filepath
    except Exception as e:
      log.error(f"Error al generar el archivo Excel: {e}")
      raise

  def _create_summary_df(self, data: Dict) -> pd.DataFrame:
    """Prepara los datos para la hoja de resumen del Excel."""
    rows = []
    for attraction in data['attractions']:
      # Extraemos la información clave de cada atracción.
      rows.append({
        'Attraction Name': attraction.get('place_name'),
        'Type': attraction.get('place_type'),
        'Rating': attraction.get('rating', 0.0), # Valor por defecto si no hay rating.
        'Total Reviews': attraction.get('total_reviews', 0),
        'Total English Reviews': attraction.get('english_reviews', 0),
        'URL': attraction.get('url', '') # Enlace a la página de la atracción.
      })
    # Convertimos la lista de diccionarios en un DataFrame de pandas.
    return pd.DataFrame(rows)

  def _create_reviews_df(self, data: Dict) -> pd.DataFrame:
    """Prepara los datos para la hoja de reseñas, evitando duplicados."""
    reviews = []
    seen_hashes = set() # Usamos un set para guardar hashes y detectar duplicados rápidamente.

    for attraction in data['attractions']:
      for review in attraction.get('reviews', []): # Iteramos sobre las reseñas de cada atracción.
        # Creamos un 'hash' (una firma única) para cada reseña basado en algunos de sus datos.
        # Esto ayuda a identificar si ya hemos visto esta reseña antes.
        review_hash = hash((
          review.get('username', ''),
          review.get('title', ''),
          review.get('written_date', ''),
          str(review.get('rating', 0)) # Convertimos el rating a string para el hash.
        ))

        # Si no hemos visto este hash antes, añadimos la reseña.
        if review_hash not in seen_hashes:
          seen_hashes.add(review_hash)
          reviews.append({
            'Attraction': attraction.get('place_name'), # Nombre de la atracción a la que pertenece.
            'Username': review.get('username'), # Nombre del usuario que escribió la reseña.
            'Rating': review.get('rating'), # Calificación de la reseña.
            'Location': review.get('location'), # Ubicación del usuario que escribió la reseña.
            'Contributions': review.get('contributions'), # Número de contribuciones del usuario.
            'Visit Date': review.get('visit_date'), # Cuándo visitó el lugar.
            'Written Date': review.get('written_date'), # Cuándo escribió la reseña.
            'Companion Type': review.get('companion_type'), # Con quién viajaba.
            'Title': review.get('title'), # Título de la reseña.
            'Review Text': review.get('review_text'), # El texto completo de la reseña.
          })

    df = pd.DataFrame(reviews)

    # Como medida extra, eliminamos duplicados basados en un conjunto de columnas clave.
    # 'keep=first' significa que si hay duplicados, nos quedamos con el primero que encontramos.
    return df.drop_duplicates(
      subset=['Username', 'Title', 'Written Date', 'Rating'],
      keep='first'
    )

  def _adjust_column_widths(self, writer, dfs: list):
    """Ajusta el ancho de las columnas en las hojas de Excel para mejorar la legibilidad."""
    # Iteramos sobre cada hoja y su DataFrame correspondiente.
    for sheet_name, df in zip(writer.sheets.keys(), dfs):
      worksheet = writer.sheets[sheet_name]
      # Iteramos sobre cada columna del DataFrame.
      for idx, col in enumerate(df.columns):
        # Calculamos el ancho necesario: el máximo entre el texto más largo de la columna
        # y el nombre de la columna, más un pequeño margen.
        max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
        # Establecemos el ancho de la columna, con un máximo de 50 para evitar columnas excesivamente anchas.
        worksheet.set_column(idx, idx, min(max_len, 50))