# MÓDULO DE EXPORTACIÓN DE DATOS A MÚLTIPLES FORMATOS
# Implementa generación de archivos Excel y JSON desde estructura de datos consolidada
# Proporciona funciones para convertir datos jerárquicos a formatos de descarga

import json
from typing import Dict, Optional
from io import BytesIO
import pandas as pd
from loguru import logger as log

# ====================================================================================================================
#                                           CLASE PRINCIPAL DE EXPORTACIÓN
# ====================================================================================================================

class DataExporter:
  # EXPORTA DATOS A MÚLTIPLES FORMATOS DE ARCHIVO PARA DESCARGA
  # Genera archivos Excel con múltiples hojas y JSON estructurado
  # Maneja procesamiento en memoria sin crear archivos temporales

  def __init__(self):
    # INICIALIZA INSTANCIA DE EXPORTADOR SIN CONFIGURACIÓN ADICIONAL
    # No requiere parámetros de configuración en el constructor
    pass

  # ====================================================================================================================
  #                                         GENERAR ARCHIVO EXCEL EN MEMORIA
  # ====================================================================================================================

  def export_to_excel_bytes(self, data_package: Dict) -> Optional[bytes]:
    # GENERA ARCHIVO EXCEL COMPLETO CON MÚLTIPLES HOJAS EN MEMORIA
    # Crea hoja resumen de atracciones y hoja detallada de reseñas
    # Retorna bytes del archivo Excel o None en caso de error
    
    # validar que existen datos de regiones para exportar
    if not data_package.get("regions"):
      return None

    output = BytesIO()
    try:
      with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        
        # construir datos para hoja resumen de atracciones
        summary_data = []
        for region in data_package.get("regions", []):
          region_name = region.get("region_name", "Región Desconocida")
          
          # procesar cada atracción en la región actual
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
        
        # crear y escribir hoja resumen si hay datos disponibles
        if summary_data:
          df_summary = pd.DataFrame(summary_data)
          df_summary.to_excel(writer, sheet_name="Resumen_Atracciones", index=False)
          
          # ajustar ancho de columnas automáticamente según contenido
          worksheet_summary = writer.sheets["Resumen_Atracciones"]
          for idx, col in enumerate(df_summary.columns):
            series = df_summary[col]
            max_len = max(
              series.astype(str).map(len).max(),
              len(str(col))
            ) + 3
            # limitar ancho máximo para evitar columnas excesivamente anchas
            worksheet_summary.set_column(idx, idx, min(max_len, 50))

        # construir datos para hoja detallada de reseñas individuales
        reviews_data = []
        for region in data_package.get("regions", []):
          region_name = region.get("region_name", "Región Desconocida")
          
          for attraction in region.get("attractions", []):
            attraction_name = attraction.get("attraction_name", "Atracción Desconocida")
            
            # procesar cada reseña individual con metadatos completos
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
        
        # crear y escribir hoja de reseñas si hay datos disponibles
        if reviews_data:
          df_reviews = pd.DataFrame(reviews_data)
          df_reviews.to_excel(writer, sheet_name="Detalle_Reseñas", index=False)
          
          # ajustar ancho de columnas con límites específicos por tipo
          worksheet_reviews = writer.sheets["Detalle_Reseñas"]
          for idx, col in enumerate(df_reviews.columns):
            series = df_reviews[col]
            max_len = max(
              series.astype(str).map(len).max(),
              len(str(col))
            ) + 3
            
            # aplicar límites específicos según tipo de columna
            if col in ["Texto", "Título"]:
              max_len = min(max_len, 80)  # columnas de texto largo
            else:
              max_len = min(max_len, 30)  # columnas de metadatos
            worksheet_reviews.set_column(idx, idx, max_len)
            
      # obtener bytes del archivo Excel generado
      processed_data = output.getvalue()
      log.info(f"Excel generado exitosamente: {len(processed_data)} bytes")
      return processed_data

    except Exception as e:
      log.error(f"Error generando archivo Excel: {e}")
      return None

  # ====================================================================================================================
  #                                         GENERAR ARCHIVO JSON EN MEMORIA
  # ====================================================================================================================

  async def save_to_json(self, data_package: Dict) -> Optional[bytes]:
    # GENERA ARCHIVO JSON ESTRUCTURADO EN MEMORIA CON CODIFICACIÓN UTF-8
    # Mantiene estructura jerárquica original de datos consolidados
    # Retorna bytes del archivo JSON o None en caso de error
    
    # validar que existen datos de regiones para exportar
    if not data_package.get("regions"):
      return None

    try:
      # serializar datos a JSON con formato legible y caracteres especiales
      json_content = json.dumps(data_package, indent=2, ensure_ascii=False)
      json_bytes = json_content.encode('utf-8')
      log.info(f"JSON generado exitosamente: {len(json_bytes)} bytes")
      return json_bytes
    except Exception as e:
      log.error(f"Error generando archivo JSON: {e}")
      return None