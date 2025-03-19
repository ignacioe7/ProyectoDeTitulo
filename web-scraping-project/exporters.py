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

async def save_to_excel(valparaiso_data: Dict) -> None:
  """Guarda datos extraídos en Excel con múltiples hojas"""
  
  # Crear archivo Excel
  excel_file = 'valparaiso_reviews.xlsx'
  writer = pd.ExcelWriter(excel_file, engine='xlsxwriter')

  # Crear hoja de resumen principal
  summary_data = {
    'Attraction Name': [],
    'Type': [],
    'Score': [],
    'Total Reviews': [],
    'URL': []
  }

  # Crear lista de reseñas para hoja detallada
  all_reviews = []

  for attraction in valparaiso_data['attractions']:
    # Añadir a resumen
    summary_data['Attraction Name'].append(attraction['place_name'])
    summary_data['Type'].append(attraction['attraction'])
    summary_data['Score'].append(attraction['score'])
    summary_data['Total Reviews'].append(attraction['total_reviews'])
    summary_data['URL'].append(attraction['url'])

    # Añadir todas las reseñas con nombre de atracción
    for review in attraction['reviews']:
      review_data = {
        'Attraction': attraction['place_name'],
        'User': review['username'],
        'Location': review['location'],
        'Rating': review['rating'],
        'Title': review['title'],
        'Text': review['review_text'],
        'Visit Date': review['visit_date'],
        'Written Date': review['written_date'],
        'Companion Type': review['companion_type']
      }
      all_reviews.append(review_data)

  # Crear DataFrames
  summary_df = pd.DataFrame(summary_data)
  reviews_df = pd.DataFrame(all_reviews)

  # Guardar en hojas de Excel
  summary_df.to_excel(writer, sheet_name='Summary', index=False)
  reviews_df.to_excel(writer, sheet_name='Reviews', index=False)

  # Ajustar automáticamente ancho de columnas
  for sheet in writer.sheets:
    worksheet = writer.sheets[sheet]
    for idx, col in enumerate(summary_df if sheet == 'Summary' else reviews_df):
      series = summary_df[col] if sheet == 'Summary' else reviews_df[col]
      max_len = max((
        series.astype(str).map(len).max(),
        len(str(series.name))
      )) + 1
      worksheet.set_column(idx, idx, max_len)

  writer.close()
  log.info(f"Excel actualizado: {excel_file}")