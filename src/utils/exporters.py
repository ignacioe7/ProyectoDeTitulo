import json
from typing import Dict, Optional
from io import BytesIO
import pandas as pd
from loguru import logger as log

class DataExporter:
  # EXPORTADOR DE DATOS A EXCEL Y JSON
  # Maneja conversion de datos consolidados a formatos de archivo
  # Genera archivos en memoria para descarga directa

  def __init__(self):
    pass

  # ===============================================================
  # EXPORTAR A EXCEL EN BYTES
  # ===============================================================

  def export_to_excel_bytes(self, data_package: Dict) -> Optional[bytes]:
    # GENERA EXCEL EN MEMORIA Y DEVUELVE BYTES
    # Crea dos hojas: resumen de atracciones y detalle de reseñas
    # Ajusta ancho de columnas automaticamente segun contenido
    if not data_package.get("regions"):
      return None

    output = BytesIO()
    try:
      with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # hoja resumen de atracciones
        summary_data = []
        for region in data_package.get("regions", []):
          region_name = region.get("region_name", "Región Desconocida")
          for attraction in region.get("attractions", []):
            summary_data.append({
              "Región": region_name,
              "Atracción": attraction.get("attraction_name", "N/A"),
              "Tipo": attraction.get("place_type", "N/A"),
              "Rating": attraction.get("rating", 0),
              "Total Reseñas": attraction.get("reviews_count", 0),
              "Reseñas Inglés": attraction.get("english_reviews_count", 0),
              "Reseñas Scrapeadas": len(attraction.get("reviews", [])),
              "URL": attraction.get("url", "N/A"),
              "Última Actualización": attraction.get("last_reviews_scrape_date", "N/A")
            })
        
        if summary_data:
          df_summary = pd.DataFrame(summary_data)
          df_summary.to_excel(writer, sheet_name="Resumen_Atracciones", index=False)
          
          # ajustar ancho columnas
          worksheet_summary = writer.sheets["Resumen_Atracciones"]
          for idx, col in enumerate(df_summary.columns):
            series = df_summary[col]
            max_len = max(
              series.astype(str).map(len).max(),
              len(str(col))
            ) + 3
            worksheet_summary.set_column(idx, idx, min(max_len, 50)) # max 50 chars

        # hoja detalle reseñas
        reviews_data = []
        for region in data_package.get("regions", []):
          region_name = region.get("region_name", "Región Desconocida")
          for attraction in region.get("attractions", []):
            attraction_name = attraction.get("attraction_name", "Atracción Desconocida")
            for review in attraction.get("reviews", []):
              reviews_data.append({
                "Región": region_name,
                "Atracción": attraction_name,
                "Usuario": review.get("username", "N/A"),
                "Rating": review.get("rating", 0),
                "Título": review.get("title", "N/A"),
                "Texto": review.get("review_text", "N/A"),
                "Fecha Escrita": review.get("written_date", "N/A"),
                "Fecha Visita": review.get("visit_date", "N/A"),
                "Compañía": review.get("companion_type", "N/A"),
                "Sentimiento": review.get("sentiment", "N/A"),
              })
        
        if reviews_data:
          df_reviews = pd.DataFrame(reviews_data)
          df_reviews.to_excel(writer, sheet_name="Detalle_Reseñas", index=False)
          
          # ajustar ancho columnas segun contenido
          worksheet_reviews = writer.sheets["Detalle_Reseñas"]
          for idx, col in enumerate(df_reviews.columns):
            series = df_reviews[col]
            max_len = max(
              series.astype(str).map(len).max(),
              len(str(col))
            ) + 3
            # limitar texto largo
            if col in ["Texto", "Título"]:
              max_len = min(max_len, 80)
            else:
              max_len = min(max_len, 30)
            worksheet_reviews.set_column(idx, idx, max_len)
            
      processed_data = output.getvalue()
      log.info(f"excel generado: {len(processed_data)} bytes")
      return processed_data

    except Exception as e:
      log.error(f"error generando excel: {e}")
      return None

  # ===============================================================
  # GUARDAR A JSON
  # ===============================================================

  async def save_to_json(self, data_package: Dict) -> Optional[bytes]:
    # GENERA JSON EN MEMORIA Y DEVUELVE BYTES
    # Convierte datos consolidados a formato JSON con encoding UTF-8
    # Mantiene caracteres especiales y formato legible
    if not data_package.get("regions"):
      return None

    try:
      json_content = json.dumps(data_package, indent=2, ensure_ascii=False)
      json_bytes = json_content.encode('utf-8')
      log.info(f"json generado: {len(json_bytes)} bytes")
      return json_bytes
    except Exception as e:
      log.error(f"error generando json: {e}")
      return None