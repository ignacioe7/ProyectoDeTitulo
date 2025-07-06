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

# Mapeo de códigos de idioma a nombres legibles
AVAILABLE_LANGUAGES = {
  "all": "Todos los idiomas",
  "english": "Inglés",
  "spanish": "Español", 
  "portuguese": "Portugués",
  "french": "Francés",
  "german": "Alemán",
  "italian": "Italiano",
  "dutch": "Holandés",
  "japanese": "Japonés",
  "chinese": "Chino"
}

# Mapeo de idiomas por sufijo
LANGUAGE_SUFFIX_MAPPING = {
  "all": "all",
  "english": "en",
  "spanish": "es",
  "portuguese": "pt",
  "french": "fr",
  "german": "de",
  "italian": "it",
  "dutch": "nl",
  "japanese": "ja",
  "chinese": "zh"
}

# Mapeo de regiones
REGION_MAPPING = {
  "XV- Región de Arica y Parinacota": "XV",
  "I- Región de Tarapacá": "I", 
  "II- Región de Antofagasta": "II",
  "III- Región de Atacama": "III",
  "IV- Región de Coquimbo": "IV",
  "V- Región de Valparaíso": "V",
  "XIII- Región Metropolitana": "XIII",
  "VI- Región de O'Higgins": "VI",
  "VII- Región del Maule": "VII",
  "XVI- Región de Ñuble": "XVI",
  "VIII- Región del Biobío": "VIII",
  "IX- Región de la Araucanía": "IX",
  "XIV- Región de Los Ríos": "XIV",
  "X- Región de Los Lagos": "X",
  "XI- Región de Aysén": "XI",
  "XII- Región de Magallanes": "XII"
}



# mapeo de nombres técnicos a nombres amigables para interfaz
UI_COLUMN_MAPPING = {
  "region_name": "Región",
  "attraction_name": "Atracción",
  "language": "Idioma",
  "username": "Usuario",
  "rating_review": "Rating (Reseña)",
  "title": "Título",
  "review_text": "Texto Reseña",
  "location": "Ubicación",
  "written_date": "Fecha Escrita",
  "visit_date": "Fecha Visita",
  "companion_type": "Compañía",
  "contributions": "Contribuciones",
  "sentiment": "Sentimiento",
  "sentiment_score": "Score Sentimiento",
  "analyzed_at": "Analizado En"
}

# columnas seleccionadas por defecto al inicializar filtros
DEFAULT_SELECTED_COLUMNS = [
  "region_name", "attraction_name", "language", "username", "rating_review", 
  "title", "review_text", "written_date", "visit_date", "companion_type", "contributions", "sentiment",
  "sentiment_score"
]



# ====================================================================================================================
#                                        CARGAR Y PROCESAR DATOS CONSOLIDADOS
# ====================================================================================================================

@st.cache_data
def load_and_process_data():
  # CARGA Y PROCESA DATOS DESDE ARCHIVO JSON CONSOLIDADO CON SOPORTE MULTIIDIOMA
  # Lee estructura jerárquica de regiones/atracciones/idiomas/reseñas y aplana a DataFrame
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

  # aplanar estructura jerárquica con soporte multiidioma
  for region in data.get("regions", []):
    region_name = region.get("region_name", "Región Desconocida")
    
    for attraction in region.get("attractions", []):
      attraction_name = attraction.get("attraction_name")
      if not attraction_name:
        attraction_name = attraction.get("place_name", "Atracción Desconocida")

      # Procesar estructura multiidioma: attraction["languages"][language_code]["reviews"]
      languages_data = attraction.get("languages", {})
      
      if languages_data:
        # Nueva estructura multiidioma
        for language_code, language_data in languages_data.items():
          reviews = language_data.get("reviews", [])
          
          for review in reviews:
            review_data = {
              "region_name": region_name,
              "attraction_name": attraction_name,
              "language": language_code,
              "username": review.get("username", "N/A"),
              "rating_review": review.get("rating"),
              "title": review.get("title", "N/A"),
              "review_text": review.get("review_text", "N/A"),
              "location": review.get("location", "N/A"),
              "written_date": review.get("written_date"),
              "visit_date": review.get("visit_date"),
              "companion_type": review.get("companion_type"),
              "contributions": review.get("contributions"),
              "sentiment": review.get("sentiment"), 
              "sentiment_score": review.get("sentiment_score"),
              "analyzed_at": review.get("analyzed_at")
            }
            all_reviews_list.append(review_data)
      else:
        # Estructura antigua de compatibilidad (reviews directamente en attraction)
        reviews = attraction.get("reviews", [])
        for review in reviews:
          review_data = {
            "region_name": region_name,
            "attraction_name": attraction_name,
            "language": review.get("language", "unknown"),
            "username": review.get("username", "N/A"),
            "rating_review": review.get("rating"),
            "title": review.get("title", "N/A"),
            "review_text": review.get("review_text", "N/A"),
            "location": review.get("location", "N/A"),
            "written_date": review.get("written_date"),
            "visit_date": review.get("visit_date"),
            "companion_type": review.get("companion_type"),
            "contributions": review.get("contributions"),
            "sentiment": review.get("sentiment"), 
            "sentiment_score": review.get("sentiment_score"),
            "analyzed_at": review.get("analyzed_at")
          }
          all_reviews_list.append(review_data)
  
  if not all_reviews_list:
    st.info("No se encontraron reseñas en el archivo de datos.")
    return pd.DataFrame()
      
  return pd.DataFrame(all_reviews_list)

def get_available_languages_from_data(df):
  """Obtiene los idiomas disponibles en los datos."""
  if df.empty or "language" not in df.columns:
    return ["all"]
  
  available_languages = ["all"]
  languages_in_data = sorted(df["language"].unique())
  
  for lang_code in languages_in_data:
    if lang_code != "unknown" and lang_code in AVAILABLE_LANGUAGES:
      available_languages.append(lang_code)
  
  return available_languages

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

  # --- Filtros de Filas (Datos) ---
  
  # 1. Filtro por Idioma (primer filtro como en reviews.py)
  available_languages = get_available_languages_from_data(reviews_df_full)
  
  if len(available_languages) > 1:
    language_options = []
    for lang_code in available_languages:
      if lang_code in AVAILABLE_LANGUAGES:
        language_options.append(AVAILABLE_LANGUAGES[lang_code])
      else:
        language_options.append(lang_code.title())
    
    selected_language_display = st.sidebar.selectbox(
      "Idioma",
      options=language_options,
      index=0,  # Por defecto "Todos los idiomas"
      help="Selecciona el idioma de las reseñas a mostrar"
    )
    
    # Convertir de vuelta al código de idioma
    selected_language = None
    for lang_code, lang_name in AVAILABLE_LANGUAGES.items():
      if lang_name == selected_language_display:
        selected_language = lang_code
        break
    
    if selected_language is None:
      # Si no se encuentra en el mapeo, usar el texto directamente
      selected_language = selected_language_display.lower()
  else:
    selected_language = "all"

  # Aplicar filtro de idioma
  if selected_language != "all":
    filtered_df_rows = reviews_df_full[reviews_df_full["language"] == selected_language]
  else:
    filtered_df_rows = reviews_df_full.copy()

  # 2. filtro por región con selección múltiple
  if not filtered_df_rows.empty:
    available_regions = sorted(filtered_df_rows["region_name"].unique())
    selected_regions = st.sidebar.multiselect(
      "Región(es)",
      options=available_regions,
      default=available_regions[0:1] if available_regions else []
    )

    if selected_regions:
      filtered_df_rows = filtered_df_rows[filtered_df_rows["region_name"].isin(selected_regions)]

  # 3. filtro por atracción dependiente de regiones seleccionadas
  if not filtered_df_rows.empty:
    available_attractions = sorted(filtered_df_rows["attraction_name"].unique())
    selected_attractions = st.sidebar.multiselect(
      "Atracción(es)",
      options=available_attractions,
      default=[] 
    )
    if selected_attractions: 
      filtered_df_rows = filtered_df_rows[filtered_df_rows["attraction_name"].isin(selected_attractions)]

  # 4. filtro por rango de rating con slider dinámico
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

  # 5. filtro por categorías de sentimiento disponibles
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
          
  # 6. filtro de búsqueda textual en contenido de reseñas
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
    st.warning("Por favor, selecciona al menos una columna para mostrar.")
    return

  # dataframe final para descarga mantiene nombres técnicos originales
  df_for_download = filtered_df_rows[selected_columns_raw_names]

  # presentación de tabla con datos filtrados
  st.subheader("Tabla de Reseñas")
  
  if df_for_download.empty:
    st.info("No hay reseñas que coincidan con los filtros seleccionados.")
  else:
    # Mostrar estadísticas multiidioma
    total_reviews = len(reviews_df_full)
    filtered_reviews = len(df_for_download)
    
    col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
    with col_stat1:
      st.metric("Total reseñas", total_reviews)
    with col_stat2:
      st.metric("Reseñas filtradas", filtered_reviews)
    with col_stat3:
      if selected_language != "all":
        language_name = AVAILABLE_LANGUAGES.get(selected_language, selected_language.title())
        st.metric("Idioma seleccionado", language_name)
      else:
        unique_languages = df_for_download["language"].nunique() if "language" in df_for_download.columns else 0
        st.metric("Idiomas únicos", unique_languages)
    with col_stat4:
      unique_attractions = df_for_download["attraction_name"].nunique() if "attraction_name" in df_for_download.columns else 0
      st.metric("Atracciones únicas", unique_attractions)
    
    st.markdown(f"Mostrando **{filtered_reviews}** de **{total_reviews}** reseñas totales.")
    
    # crear copia para mostrar con nombres amigables
    df_for_display = df_for_download.copy()
    
    rename_map_for_display = {k: v for k, v in UI_COLUMN_MAPPING.items() if k in df_for_display.columns}
    df_for_display.rename(columns=rename_map_for_display, inplace=True)
    
    # Convertir códigos de idioma a nombres legibles en la visualización
    if "Idioma" in df_for_display.columns:
      df_for_display["Idioma"] = df_for_display["Idioma"].map(
        lambda x: AVAILABLE_LANGUAGES.get(x, x.title()) if pd.notna(x) else "N/A"
      )
    
    # configuración dinámica de columnas según datos disponibles
    dynamic_column_config = {}
    if "Texto Reseña" in df_for_display.columns:
      dynamic_column_config["Texto Reseña"] = st.column_config.TextColumn(width="large")
    if "Rating (Reseña)" in df_for_display.columns:
      dynamic_column_config["Rating (Reseña)"] = st.column_config.NumberColumn(format="%.1f ⭐")
    if "Idioma" in df_for_display.columns:
      dynamic_column_config["Idioma"] = st.column_config.TextColumn(width="small")

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
    language_suffix = f"_{selected_language}" if selected_language != "all" else "_todos_idiomas"

    with col1:
      # DataFrame para limpiar los datos antes de exportar
      df_for_csv = df_for_download.copy()
      
      # Limpia saltos de línea y caracteres que generan problemas
      text_columns = ['review_text', 'title', 'username', 'attraction_name', 'region_name', 'location']
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
        file_name=f"reseñas_filtradas{language_suffix}_{current_time_str}.csv",
        mime='text/csv',
        use_container_width=True,
        help="CSV limpio sin saltos de línea problemáticos"
      )
    
    with col2:
      # exportación a formato Excel usando función de conversión
      excel_export = to_excel_bytes(df_for_download)
      
      # Generar nombre de archivo personalizado para Excel
      region_number = ""
      if selected_regions and len(selected_regions) == 1:
        # Usar el mapeo de regiones existente
        region_number = REGION_MAPPING.get(selected_regions[0], "all")
      else:
        region_number = "all"
        
      language_suffix = LANGUAGE_SUFFIX_MAPPING.get(selected_language, selected_language)
      excel_filename = f"{region_number}_{language_suffix}.xlsx"
      
      st.download_button(
        label="📥 Descargar Excel",
        data=excel_export,
        file_name=excel_filename,
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        use_container_width=True,
        help="Excel con todas las columnas disponibles"
      )

    """ with col3:
      # Para JSON, limpiar caracteres problemáticos manteniendo estructura
      df_for_json = df_for_download.copy()
      text_columns = ['review_text', 'title', 'username', 'attraction_name', 'region_name', 'location']
      for col in text_columns:
        if col in df_for_json.columns:
          df_for_json[col] = df_for_json[col].astype(str).str.replace(r'\r', '', regex=True)
          df_for_json[col] = df_for_json[col].str.replace(r'\t', ' ', regex=True)
      
      json_export = df_for_json.to_json(orient='records', indent=2, ensure_ascii=False).encode('utf-8')
      st.download_button(
        label="📥 Descargar JSON",
        data=json_export,
        file_name=f"reseñas_filtradas{language_suffix}_{current_time_str}.json",
        mime='application/json',
        use_container_width=True,
        help="JSON estructurado con datos filtrados"
      ) """

    # Información adicional con detalles multiidioma
    st.info(f"""
    **Información de exportación:**
    - **Idioma seleccionado**: {AVAILABLE_LANGUAGES.get(selected_language, selected_language.title())}
    - **Total de columnas**: {len(selected_columns_raw_names)}
    - **Formatos disponibles**: CSV (compatible), Excel (completo), JSON (estructurado)
    """)

# ====================================================================================================================
#                                              PUNTO DE ENTRADA PRINCIPAL
# ====================================================================================================================

if __name__ == '__main__':
  # PERMITE EJECUCIÓN DIRECTA DEL MÓDULO PARA TESTING Y DESARROLLO
  # Renderiza interfaz completa sin dependencias externas del data handler
  render()