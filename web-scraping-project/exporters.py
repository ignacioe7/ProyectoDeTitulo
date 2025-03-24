import json
import pandas as pd
from datetime import datetime
from typing import Dict, List
from loguru import logger as log

async def save_to_json(data: Dict, filename: str = None) -> None:
  """Guarda datos en formato JSON"""
  if filename is None:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'valparaiso_attractions_{timestamp}.json'
  
  with open(filename, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
    
  log.info(f"Datos guardados en {filename}")

async def save_to_excel(valparaiso_data: Dict, filename: str = None) -> None:
    """
    Guarda los datos en un archivo Excel.
    """
    excel_file = filename or 'valparaiso_reviews.xlsx'

    try:
        # Crear archivo Excel con nombre personalizable
        with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
            # Crear hoja de resumen principal
            summary_data = {
                'Attraction Name': [],
                'Type': [],
                'Score': [],
                'Total Reviews': [],  # Total de reseñas según el JSON
                'Total Available Reviews': [],  # Total de reseñas extraídas
                'URL': []
            }

            # Crear lista de reseñas para hoja detallada
            all_reviews = []

            for attraction in valparaiso_data['attractions']:
                # Añadir a resumen
                summary_data['Attraction Name'].append(attraction.get('place_name', 'Desconocido'))
                summary_data['Type'].append(attraction.get('place_type', 'Desconocido')) 
                summary_data['Score'].append(attraction.get('rating', 0.0))
                summary_data['Total Reviews'].append(attraction.get('reviews_count', 0))
                summary_data['Total English Reviews'].append(len(attraction.get('reviews', [])))
                summary_data['URL'].append(attraction.get('url', ''))

                # Añadir todas las reseñas con nombre de atracción
                for review in attraction.get('reviews', []): 
                    review_data = {
                        'Attraction': attraction.get('place_name', 'Desconocido'),
                        'User': review.get('username', 'SIN NOMBRE'),
                        'Location': review.get('location', 'SIN UBICACIÓN'),
                        'Contributions': review.get('contributions', 0),
                        'Rating': review.get('rating', 0.0),
                        'Title': review.get('title', 'SIN TÍTULO'),
                        'Text': review.get('review_text', 'SIN TEXTO DE RESEÑA'),
                        'Visit Date': review.get('visit_date', 'SIN FECHA'),
                        'Written Date': review.get('written_date', 'SIN FECHA DE ESCRITURA'),
                        'Companion Type': review.get('companion_type', 'SIN INFORMACIÓN')
                    }
                    all_reviews.append(review_data)

            # Crear DataFrames
            summary_df = pd.DataFrame(summary_data)
            reviews_df = pd.DataFrame(all_reviews)

            # Guardar en hojas de Excel
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            reviews_df.to_excel(writer, sheet_name='Reviews', index=False)

            # Ajustar automáticamente el ancho de las columnas
            for sheet_name, df in zip(['Summary', 'Reviews'], [summary_df, reviews_df]):
                worksheet = writer.sheets[sheet_name]
                for idx, col in enumerate(df.columns):
                    max_len = max(df[col].astype(str).map(len).max(), len(col)) + 1
                    worksheet.set_column(idx, idx, max_len)

        log.info(f"Excel guardado: {excel_file}")
    except Exception as e:
        log.error(f"Error al guardar el archivo Excel: {e}")

async def load_json(filename: str, default_value=None) -> Dict:
  try:
    with open(filename, 'r', encoding='utf-8') as f:
      return json.load(f)
  except FileNotFoundError:
      log.error(f"Archivo {filename} no encontrado")
      return default_value if default_value is not None else {}
  except json.JSONDecodeError:
      log.error(f"Error al decodificar JSON en {filename}")
      return default_value if default_value is not None else {}