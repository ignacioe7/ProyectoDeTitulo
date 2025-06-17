# MÓDULO DE FILTRADO Y EXPORTACIÓN DE RESEÑAS DETALLADAS
# Proporciona interfaz completa para filtrar, visualizar y exportar reseñas
# Implementa múltiples criterios de filtrado y opciones de exportación en varios formatos

import sys
import os
import streamlit as st
import pandas as pd
from datetime import datetime, timezone, timedelta
import json 
from io import BytesIO 
from ...utils.constants import PathConfig

# orden predefinido para categorías de sentimiento en interfaz
SENTIMENT_ORDER = ["VERY_NEGATIVE", "NEGATIVE", "NEUTRAL", "POSITIVE", "VERY_POSITIVE"]

# mapeo de nombres técnicos a nombres amigables para interfaz
UI_COLUMN_MAPPING = {
    "region_name": "Región",
    "attraction_name": "Atracción",
    "username": "Usuario",
    "rating_review": "Rating (Reseña)",
    "title": "Título",
    "review_text": "Texto Reseña",
    "written_date": "Fecha Escrita",
    "visit_date": "Fecha Visita",
    "companion_type": "Compañía",
    "sentiment": "Sentimiento",
    "sentiment_score": "Score Sentimiento"
}

# columnas seleccionadas por defecto al inicializar filtros
DEFAULT_SELECTED_COLUMNS = list(UI_COLUMN_MAPPING.keys())

# ====================================================================================================================
#                                        CARGAR Y PROCESAR DATOS CONSOLIDADOS
# ====================================================================================================================

@st.cache_data
def load_and_process_data():
  # CARGA Y PROCESA DATOS DESDE ARCHIVO JSON CONSOLIDADO
  # Lee estructura jerárquica de regiones/atracciones/reseñas y aplana a DataFrame
  # Retorna DataFrame con todas las reseñas o DataFrame vacío en caso de error
  path_config = PathConfig()
  consolidated_file_path = path_config.CONSOLIDATED_JSON

  try:
    with open(consolidated_file_path, 'r', encoding='utf-8') as f:
      data = json.load(f)
  except FileNotFoundError:
    st.error(f"Error: No se encontró el archivo de datos en {consolidated_file_path}")
    return pd.DataFrame()
  except json.JSONDecodeError:
    st.error(f"Error: El archivo {consolidated_file_path} no es un JSON válido.")
    return pd.DataFrame()
  except Exception as e:
    st.error(f"Error inesperado al cargar datos: {e}")
    return pd.DataFrame()

  all_reviews_list = []
  if "regions" not in data:
    st.warning("El archivo JSON no contiene la clave 'regions'.")
    return pd.DataFrame()

  # aplanar estructura jerárquica a lista de reseñas individuales
  for region in data.get("regions", []):
    region_name = region.get("region_name", "Región Desconocida")
    for attraction in region.get("attractions", []):
      attraction_name = attraction.get("attraction_name")
      if not attraction_name:
        attraction_name = attraction.get("place_name", "Atracción Desconocida")

      # extraer cada reseña con metadatos de región y atracción
      for review in attraction.get("reviews", []):
        review_data = {
          "region_name": region_name,
          "attraction_name": attraction_name,
          "username": review.get("username", "N/A"),
          "rating_review": review.get("rating"),
          "title": review.get("title", "N/A"),
          "review_text": review.get("review_text", "N/A"),
          "written_date": review.get("written_date"),
          "visit_date": review.get("visit_date"),
          "companion_type": review.get("companion_type"),
          "sentiment": review.get("sentiment"), 
          "sentiment_score": review.get("sentiment_score") 
        }
        all_reviews_list.append(review_data)
  
  if not all_reviews_list:
    st.info("No se encontraron reseñas en el archivo de datos.")
    return pd.DataFrame()
      
  return pd.DataFrame(all_reviews_list)

# ====================================================================================================================
#                                        CONVERTIR DATAFRAME A BYTES EXCEL
# ====================================================================================================================

def to_excel_bytes(df):
  # CONVIERTE DATAFRAME A FORMATO EXCEL EN MEMORIA
  # Utiliza openpyxl para generar archivo Excel sin guardar en disco
  # Retorna bytes del archivo Excel listo para descarga
  output = BytesIO()
  # requiere openpyxl instalado para funcionalidad completa
  with pd.ExcelWriter(output, engine='openpyxl') as writer:
    df.to_excel(writer, index=False, sheet_name='Reseñas')
  processed_data = output.getvalue()
  return processed_data

# ====================================================================================================================
#                                            RENDERIZAR PÁGINA PRINCIPAL
# ====================================================================================================================

def render(data_handler=None):
  # RENDERIZA INTERFAZ COMPLETA DE FILTRADO Y EXPORTACIÓN DE RESEÑAS
  # Maneja carga de datos, aplicación de filtros múltiples y opciones de descarga
  # Proporciona tabla interactiva con columnas configurables y exportación multiformat
  st.header("📊 Visor de Reseñas Detalladas")

  # cargar datos completos usando cache para optimizar rendimiento
  reviews_df_full = load_and_process_data()

  if reviews_df_full.empty:
    return

  st.sidebar.header("Filtros de Datos")

  # aplicar filtros secuenciales para refinar datos mostrados
  # 1. filtro por región con selección múltiple
  available_regions = sorted(reviews_df_full["region_name"].unique())
  selected_regions = st.sidebar.multiselect(
    "Región(es)",
    options=available_regions,
    default=available_regions[0] if available_regions else [] 
  )

  if not selected_regions:
    filtered_df_rows = reviews_df_full.copy()
  else:
    filtered_df_rows = reviews_df_full[reviews_df_full["region_name"].isin(selected_regions)]

  # 2. filtro por atracción dependiente de regiones seleccionadas
  if not filtered_df_rows.empty:
    available_attractions = sorted(filtered_df_rows["attraction_name"].unique())
    selected_attractions = st.sidebar.multiselect(
      "Atracción(es)",
      options=available_attractions,
      default=[] 
    )
    if selected_attractions: 
      filtered_df_rows = filtered_df_rows[filtered_df_rows["attraction_name"].isin(selected_attractions)]
  else:
    selected_attractions = []

  # 3. filtro por rango de rating con slider dinámico
  if not filtered_df_rows.empty and "rating_review" in filtered_df_rows.columns:
    valid_ratings = filtered_df_rows["rating_review"].dropna()
    if not valid_ratings.empty:
      min_val_rating = float(valid_ratings.min())
      max_val_rating = float(valid_ratings.max())
      
      selected_rating_range = st.sidebar.slider(
        "Rating de Reseña",
        min_value=min_val_rating,
        max_value=max_val_rating,
        value=(min_val_rating, max_val_rating),
        step=0.1 
      )
      filtered_df_rows = filtered_df_rows[
        (filtered_df_rows["rating_review"] >= selected_rating_range[0]) &
        (filtered_df_rows["rating_review"] <= selected_rating_range[1])
      ]

  # 4. filtro por categorías de sentimiento disponibles
  if not filtered_df_rows.empty and "sentiment" in filtered_df_rows.columns:
    available_sentiments = [s for s in SENTIMENT_ORDER if s in filtered_df_rows["sentiment"].unique()]
    if available_sentiments: 
      selected_sentiments = st.sidebar.multiselect(
        "Sentimiento(s)",
        options=available_sentiments,
        default=available_sentiments 
      )
      if selected_sentiments:
         filtered_df_rows = filtered_df_rows[filtered_df_rows["sentiment"].isin(selected_sentiments)]
          
  # 5. filtro de búsqueda textual en contenido de reseñas
  if not filtered_df_rows.empty and "review_text" in filtered_df_rows.columns:
    search_term = st.sidebar.text_input("Buscar en texto de reseña")
    if search_term:
      filtered_df_rows = filtered_df_rows[filtered_df_rows["review_text"].str.contains(search_term, case=False, na=False)]

  # configuración de columnas visibles en tabla y descarga
  st.sidebar.header("Selección de Columnas")
  all_available_columns = reviews_df_full.columns.tolist()
  
  valid_default_columns = [col for col in DEFAULT_SELECTED_COLUMNS if col in all_available_columns]

  selected_columns_raw_names = st.sidebar.multiselect(
    "Columnas a mostrar/descargar",
    options=all_available_columns,
    default=valid_default_columns
  )

  if not selected_columns_raw_names:
    selected_columns_raw_names = all_available_columns
    return 

  # dataframe final para descarga mantiene nombres técnicos originales
  df_for_download = filtered_df_rows[selected_columns_raw_names]

  # presentación de tabla con datos filtrados
  st.subheader("Tabla de Reseñas")
  
  if df_for_download.empty:
    st.info("No hay reseñas que coincidan con los filtros seleccionados.")
  else:
    st.markdown(f"Mostrando **{len(df_for_download)}** de **{len(reviews_df_full)}** reseñas totales (considerando filtros de datos y columnas).")
    
    # crear copia para mostrar con nombres amigables
    df_for_display = df_for_download.copy()
    
    rename_map_for_display = {k: v for k, v in UI_COLUMN_MAPPING.items() if k in df_for_display.columns}
    df_for_display.rename(columns=rename_map_for_display, inplace=True)
    
    # configuración dinámica de columnas según datos disponibles
    dynamic_column_config = {}
    if "Texto Reseña" in df_for_display.columns:
      dynamic_column_config["Texto Reseña"] = st.column_config.TextColumn(width="large")
    if "Rating (Reseña)" in df_for_display.columns:
      dynamic_column_config["Rating (Reseña)"] = st.column_config.NumberColumn(format="%.1f ⭐")

    st.dataframe(
      df_for_display, 
      height=500, 
      use_container_width=True,
      column_config=dynamic_column_config 
    )
    
    # opciones de descarga en múltiples formatos con timestamp único
    st.markdown("---")
    st.subheader("Descargar Datos Filtrados")
    
    col1, col2, col3 = st.columns(3)

    # generar timestamp con zona horaria ajustada para nombres únicos
    current_time_str = (datetime.now(timezone.utc) + timedelta(hours=-3)).strftime('%Y%m%d_%H%M%S')

    with col1:
      # DataFrame para limpiar los datos antes de exportar
      df_for_csv = df_for_download.copy()
      
      # Limpia saltos de línea y caracteres que generan problemas
      text_columns = ['review_text', 'title', 'username', 'attraction_name', 'region_name']
      for col in text_columns:
        if col in df_for_csv.columns:
          df_for_csv[col] = df_for_csv[col].astype(str).str.replace(r'\n', ' ', regex=True)
          df_for_csv[col] = df_for_csv[col].str.replace(r'\r', ' ', regex=True)
          df_for_csv[col] = df_for_csv[col].str.replace(r'"', '""', regex=True)  
          df_for_csv[col] = df_for_csv[col].str.replace(r'\t', ' ', regex=True) 
          df_for_csv[col] = df_for_csv[col].str.replace(r'\s+', ' ', regex=True).str.strip()
      
      # Exportar CSV con configuración específica 
      csv_export = df_for_csv.to_csv(
        index=False,
        encoding='utf-8',
        quoting=1,  
        lineterminator='\n', 
        escapechar=None  
      ).encode('utf-8')
      
      st.download_button(
        label="📥 Descargar CSV",
        data=csv_export,
        file_name=f"reseñas_filtradas_{current_time_str}.csv",
        mime='text/csv',
        use_container_width=True
      )
    
    with col2:
      # exportación a formato Excel usando función de conversión
      excel_export = to_excel_bytes(df_for_download) 
      st.download_button(
        label="📥 Descargar Excel",
        data=excel_export,
        file_name=f"reseñas_filtradas_{current_time_str}.xlsx",
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        use_container_width=True
      )

    with col3:
      # exportación a formato JSON con estructura de registros
      json_export = df_for_download.to_json(orient='records', indent=2).encode('utf-8')
      st.download_button(
        label="📥 Descargar JSON",
        data=json_export,
        file_name=f"reseñas_filtradas_{current_time_str}.json",
        mime='application/json',
        use_container_width=True
      )

# ====================================================================================================================
#                                              PUNTO DE ENTRADA PRINCIPAL
# ====================================================================================================================

if __name__ == '__main__':
  # PERMITE EJECUCIÓN DIRECTA DEL MÓDULO PARA TESTING Y DESARROLLO
  # Renderiza interfaz completa sin dependencias externas del data handler
  render()