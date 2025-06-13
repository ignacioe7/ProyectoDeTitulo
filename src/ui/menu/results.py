# MÓDULO DE ANÁLISIS COMPARATIVO ENTRE RATING Y SENTIMIENTO IA
# Implementa visualizaciones y estadísticas para comparar ratings de usuarios con análisis de sentimiento
# Proporciona exportación de datos y métricas de correlación entre puntuaciones manuales y automáticas

from datetime import datetime
from typing import List, Dict, Optional
import streamlit as st
import pandas as pd
import plotly.express as px
from loguru import logger as log

from src.utils.exporters import DataExporter

# configuración de colores para sentimientos
SENTIMENT_COLORS = {
  "VERY_NEGATIVE": "#ff4444",
  "NEGATIVE": "#ff8866", 
  "NEUTRAL": "#ffdd44",
  "POSITIVE": "#66dd88",
  "VERY_POSITIVE": "#44ff66"
}

# mapeo de sentimientos a valores numéricos
SENTIMENT_VALUES = {
  "VERY_NEGATIVE": 0,
  "NEGATIVE": 1,
  "NEUTRAL": 2,
  "POSITIVE": 3,
  "VERY_POSITIVE": 4
}

# colores para ratings de estrellas
RATING_COLORS = {
  1: "#ff4444",
  2: "#ff8866",
  3: "#ffdd44", 
  4: "#66dd88",
  5: "#44ff66"
}

# mapeo de columnas internas a nombres de interfaz
UI_COLUMN_MAPPING = {
  "region_display_name": "Región",
  "attraction_display_name": "Atracción",
  "rating": "Rating (Estrellas)",
  "sentiment": "Sentimiento",
  "sentiment_score": "Score Sentimiento (0-4)",
  "review_text": "Texto Reseña",
  "written_date": "Fecha Escrita"
}

# ====================================================================================================================
#                                        OBTENER RESEÑAS PARA INTERFAZ
# ====================================================================================================================

def get_all_reviews_for_ui(data_handler, selected_region_name: str) -> List[Dict]:
  # EXTRAE Y FILTRA RESEÑAS CON ANÁLISIS DE SENTIMIENTO VÁLIDO
  # Procesa datos por región y valida que tengan análisis de sentimiento completo
  # Retorna lista de reseñas enriquecidas con metadatos de región y atracción
  reviews_for_display = []
  
  if not hasattr(data_handler, 'data') or not data_handler.data:
    log.warning("DataHandler sin datos")
    return reviews_for_display
  
  all_regions_data = data_handler.data.get("regions", [])
  if not all_regions_data:
    log.warning("Sin datos de regiones")
    return reviews_for_display
  
  # filtrar por región seleccionada
  if selected_region_name == "Todas las regiones":
    reviews_for_display = _get_all_regions_reviews(all_regions_data)
  else:
    reviews_for_display = _get_single_region_reviews(all_regions_data, selected_region_name)
  
  # filtrar solo reseñas con análisis válido
  analyzed_reviews = [
    review for review in reviews_for_display 
    if _has_sentiment_analysis(review)
  ]
  
  log.info(f"Filtradas {len(analyzed_reviews)} reseñas de {len(reviews_for_display)} totales")
  return analyzed_reviews

# ====================================================================================================================
#                                       VERIFICAR ANÁLISIS DE SENTIMIENTO
# ====================================================================================================================

def _has_sentiment_analysis(review: Dict) -> bool:
  # VERIFICA SI UNA RESEÑA TIENE ANÁLISIS DE SENTIMIENTO VÁLIDO
  # Valida presencia de sentimiento y score en rangos correctos
  # Retorna True si la reseña tiene datos de análisis completos y válidos
  sentiment = review.get("sentiment")
  sentiment_score = review.get("sentiment_score")
  
  has_sentiment = (
    sentiment and 
    isinstance(sentiment, str) and 
    sentiment.strip() != "" and
    sentiment.upper() in ["VERY_NEGATIVE", "NEGATIVE", "NEUTRAL", "POSITIVE", "VERY_POSITIVE"]
  )
  
  has_score = (
    sentiment_score is not None and
    isinstance(sentiment_score, (int, float)) and
    0 <= sentiment_score <= 4
  )
  
  return has_sentiment and has_score

# ====================================================================================================================
#                                      EXTRAER RESEÑAS DE TODAS LAS REGIONES
# ====================================================================================================================

def _get_all_regions_reviews(all_regions_data: List[Dict]) -> List[Dict]:
  # EXTRAE RESEÑAS DE TODAS LAS REGIONES CON METADATOS ADICIONALES
  # Itera por todas las regiones y atracciones agregando información contextual
  # Retorna lista plana de reseñas con datos de región y atracción
  reviews_for_display = []
  
  for region_item in all_regions_data:
    region_name = region_item.get("region_name", "Región Desconocida")
    
    for attraction_item in region_item.get("attractions", []):
      attraction_name = attraction_item.get("attraction_name", "Atracción Desconocida")
      
      for review_item in attraction_item.get("reviews", []):
        review_copy = review_item.copy()
        review_copy["region_display_name"] = region_name
        review_copy["attraction_display_name"] = attraction_name
        reviews_for_display.append(review_copy)
  
  return reviews_for_display

# ====================================================================================================================
#                                      EXTRAER RESEÑAS DE REGIÓN ESPECÍFICA
# ====================================================================================================================

def _get_single_region_reviews(all_regions_data: List[Dict], region_name: str) -> List[Dict]:
  # EXTRAE RESEÑAS DE UNA REGIÓN ESPECÍFICA CON METADATOS
  # Busca la región solicitada y procesa solo sus atracciones y reseñas
  # Retorna lista de reseñas de la región con información contextual
  reviews_for_display = []
  
  region_data = next(
    (r for r in all_regions_data if r.get("region_name") == region_name), 
    None
  )
  
  if not region_data:
    return reviews_for_display
  
  region_display_name = region_data.get("region_name", "Región Desconocida")
  
  for attraction_item in region_data.get("attractions", []):
    attraction_name = attraction_item.get("attraction_name", "Atracción Desconocida")
    
    for review_item in attraction_item.get("reviews", []):
      review_copy = review_item.copy()
      review_copy["region_display_name"] = region_display_name
      review_copy["attraction_display_name"] = attraction_name
      reviews_for_display.append(review_copy)
  
  return reviews_for_display

# ====================================================================================================================
#                                        CALCULAR ESTADÍSTICAS DE SENTIMIENTO
# ====================================================================================================================

def calculate_sentiment_stats(reviews_list: List[Dict]) -> Dict:
  # CALCULA ESTADÍSTICAS COMPLETAS DE SENTIMIENTO Y RATINGS
  # Procesa reseñas válidas para generar métricas de distribución y correlación
  # Retorna diccionario con estadísticas detalladas y análisis de alineación
  valid_reviews = [review for review in reviews_list if _has_sentiment_analysis(review)]
  
  stats = {
    "total_reviews": len(reviews_list),
    "valid_analyzed_reviews": len(valid_reviews),
    "rating_individual_summary": {1: 0, 2: 0, 3: 0, 4: 0, 5: 0},
    "sentiment_summary": {
      "VERY_NEGATIVE": 0, "NEGATIVE": 0, "NEUTRAL": 0, 
      "POSITIVE": 0, "VERY_POSITIVE": 0
    },
    "sentiment_scores": [],
    "average_sentiment": 2.0,
    "rating_sentiment_breakdown": {i: {
      "VERY_NEGATIVE": 0, "NEGATIVE": 0, "NEUTRAL": 0, 
      "POSITIVE": 0, "VERY_POSITIVE": 0
    } for i in range(1, 6)},
    "rating_sentiment_correlation": {
      "discrepancies": [],
      "alignment_score": 0.0
    }
  }
  
  # procesar cada reseña válida para estadísticas
  for review in valid_reviews:
    _process_review_for_stats(review, stats)
  
  # calcular promedio de scores de sentimiento
  if stats["sentiment_scores"]:
    stats["average_sentiment"] = sum(stats["sentiment_scores"]) / len(stats["sentiment_scores"])
  
  # calcular correlaciones entre rating y sentimiento
  _calculate_rating_sentiment_correlation(stats)
  
  return stats

# ====================================================================================================================
#                                       PROCESAR RESEÑA INDIVIDUAL PARA ESTADÍSTICAS
# ====================================================================================================================

def _process_review_for_stats(review: Dict, stats: Dict) -> None:
  # PROCESA UNA RESEÑA INDIVIDUAL PARA ACUMULAR ESTADÍSTICAS
  # Extrae rating y sentimiento para categorizar en contadores apropiados
  # Actualiza diccionario de estadísticas con datos de la reseña procesada
  rating = review.get("rating", 0.0)
  sentiment_value = review.get("sentiment", "")
  sentiment_score = review.get("sentiment_score")
  
  # procesar sentimiento multilingual estándar
  if sentiment_value in stats["sentiment_summary"]:
    stats["sentiment_summary"][sentiment_value] += 1
    
    if sentiment_score is not None:
      stats["sentiment_scores"].append(float(sentiment_score))
    elif sentiment_value in SENTIMENT_VALUES:
      stats["sentiment_scores"].append(SENTIMENT_VALUES[sentiment_value])
  else:
    # compatibilidad con formato anterior de sentimientos
    processed_sentiment = _normalize_sentiment(sentiment_value)
    if processed_sentiment and processed_sentiment in stats["sentiment_summary"]:
      stats["sentiment_summary"][processed_sentiment] += 1
      stats["sentiment_scores"].append(3.0 if processed_sentiment == "POSITIVE" else 1.0)
  
  # procesar rating individual en escala 1-5
  if isinstance(rating, (int, float)) and 1 <= rating <= 5:
    rating_int = int(round(rating))
    stats["rating_individual_summary"][rating_int] += 1
    
    # breakdown detallado por combinación rating-sentimiento
    if sentiment_value in stats["sentiment_summary"]:
      stats["rating_sentiment_breakdown"][rating_int][sentiment_value] += 1

# ====================================================================================================================
#                                         NORMALIZAR VALORES DE SENTIMIENTO
# ====================================================================================================================

def _normalize_sentiment(sentiment_value) -> Optional[str]:
  # NORMALIZA VALORES DE SENTIMIENTO PARA COMPATIBILIDAD CON VERSIONES ANTERIORES
  # Convierte formatos antiguos de sentimiento a formato estándar
  # Retorna valor normalizado o None si no se puede procesar
  if not isinstance(sentiment_value, str):
    return None
  
  sentiment_upper = sentiment_value.upper()
  if "POSITIVE" in sentiment_upper:
    return "POSITIVE"
  elif "NEGATIVE" in sentiment_upper:
    return "NEGATIVE"
  
  return None

# ====================================================================================================================
#                                    CALCULAR CORRELACIÓN RATING-SENTIMIENTO
# ====================================================================================================================

def _calculate_rating_sentiment_correlation(stats: Dict) -> None:
  # CALCULA CORRELACIÓN ENTRE RATING DE USUARIO Y SENTIMIENTO DETECTADO POR IA
  # Analiza alineación esperada entre puntuación manual y análisis automático
  # Actualiza estadísticas con score de alineación y discrepancias detectadas
  total_with_both = 0
  aligned_count = 0
  discrepancies = []
  
  for rating, sentiment_counts in stats["rating_sentiment_breakdown"].items():
    total_reviews_for_rating = sum(sentiment_counts.values())
    if total_reviews_for_rating == 0:
      continue
      
    total_with_both += total_reviews_for_rating
    
    # definir sentimientos esperados según rating
    expected_sentiments = []
    if rating in [1, 2]:
      expected_sentiments = ["VERY_NEGATIVE", "NEGATIVE"]
    elif rating == 3:
      expected_sentiments = ["NEGATIVE", "NEUTRAL", "POSITIVE"]
    elif rating in [4, 5]:
      expected_sentiments = ["POSITIVE", "VERY_POSITIVE"]
    
    # contar reseñas alineadas con expectativa
    aligned_for_rating = sum(sentiment_counts.get(sent, 0) for sent in expected_sentiments)
    aligned_count += aligned_for_rating
    
    # detectar y registrar discrepancias significativas
    misaligned = total_reviews_for_rating - aligned_for_rating
    if misaligned > 0:
      discrepancies.append({
        "rating": rating,
        "total_reviews": total_reviews_for_rating,
        "aligned": aligned_for_rating,
        "misaligned": misaligned,
        "misalignment_percentage": (misaligned / total_reviews_for_rating) * 100
      })
  
  # calcular score global de alineación
  alignment_score = (aligned_count / total_with_both * 100) if total_with_both > 0 else 0
  
  stats["rating_sentiment_correlation"]["discrepancies"] = discrepancies
  stats["rating_sentiment_correlation"]["alignment_score"] = round(alignment_score, 1)

# ====================================================================================================================
#                                      MOSTRAR GRÁFICO DE DISTRIBUCIÓN DE RATINGS
# ====================================================================================================================

def display_individual_ratings_bar_chart(stats_data: Dict) -> None:
  # MUESTRA GRÁFICO DE BARRAS CON DISTRIBUCIÓN DE RATINGS 1-5 ESTRELLAS
  # Crea visualización colorizada según valor de rating con conteos
  # Utiliza colores consistentes y muestra valores en las barras
  data = stats_data.get("rating_individual_summary", {})
  
  df_data = {
    "Rating": [f"{i}★" for i in range(1, 6)],
    "Cantidad": [data.get(i, 0) for i in range(1, 6)],
    "RatingValue": list(range(1, 6))
  }
  df = pd.DataFrame(df_data)

  if df["Cantidad"].sum() == 0:
    st.info("No hay datos de ratings para mostrar")
    return

  colors = [RATING_COLORS[i] for i in range(1, 6)]
  
  fig = px.bar(
    df, 
    x="Rating", 
    y="Cantidad",
    title="Distribución de Ratings Individuales (1-5★)",
    color="RatingValue",
    color_discrete_sequence=colors,
    labels={"Cantidad": "Número de Reseñas"},
    text="Cantidad"
  )
  
  fig.update_traces(
    texttemplate='%{text}',
    textposition='outside',
    showlegend=False
  )
  
  fig.update_layout(showlegend=False, height=400, coloraxis_showscale=False) 
  st.plotly_chart(fig, use_container_width=True)

# ====================================================================================================================
#                                    MOSTRAR GRÁFICO DE SENTIMIENTOS MULTILINGUAL
# ====================================================================================================================

def display_multilingual_sentiment_chart(stats_data: Dict) -> None:
  # MUESTRA GRÁFICO DE SENTIMIENTOS DETECTADOS POR IA EN ESCALA 0-4
  # Visualiza distribución de sentimientos con emojis y colores apropiados
  # Incluye información de score numérico en hover y etiquetas descriptivas
  data = stats_data.get("sentiment_summary", {})
  
  sentiment_order = ["VERY_NEGATIVE", "NEGATIVE", "NEUTRAL", "POSITIVE", "VERY_POSITIVE"]
  sentiment_labels = ["Muy Negativo (0)", "Negativo (1)", "Neutral (2)", "Positivo (3)", "Muy Positivo (4)"]
  sentiment_emojis = ["😞", "😕", "😐", "😊", "🤩"]
  
  df_data = {
    "Sentimiento": [f"{emoji} {label}" for emoji, label in zip(sentiment_emojis, sentiment_labels)],
    "Cantidad": [data.get(sentiment, 0) for sentiment in sentiment_order],
    "Score": [SENTIMENT_VALUES[sentiment] for sentiment in sentiment_order]
  }
  df = pd.DataFrame(df_data)

  if df["Cantidad"].sum() == 0:
    st.info("No hay datos de sentimiento para mostrar")
    return

  colors = [SENTIMENT_COLORS[sentiment] for sentiment in sentiment_order]
  
  fig = px.bar(
    df, 
    x="Sentimiento", 
    y="Cantidad",
    title="Distribución de Sentimientos IA (0-4)",
    color="Sentimiento",
    color_discrete_sequence=colors,
    labels={"Cantidad": "Número de Reseñas"},
    text="Cantidad"
  )
  
  fig.update_traces(
    texttemplate='%{text}',
    textposition='outside',
    hovertemplate='<b>%{x}</b><br>Cantidad: %{y}<br>Score: %{customdata}<extra></extra>',
    customdata=df["Score"]
  )
  
  fig.update_layout(
    showlegend=False, 
    height=400,
    xaxis_title="Categoría de Sentimiento",
    yaxis_title="Número de Reseñas"
  )
  st.plotly_chart(fig, use_container_width=True)

# ====================================================================================================================
#                                  MOSTRAR ANÁLISIS DE CORRELACIÓN RATING VS SENTIMIENTO
# ====================================================================================================================

def display_rating_sentiment_correlation_analysis(stats_data: Dict) -> None:
  # MUESTRA ANÁLISIS DE CORRELACIÓN ENTRE RATING USUARIO Y SENTIMIENTO IA
  # Presenta métricas de alineación y detecta discrepancias significativas
  # Proporciona interpretación cualitativa del nivel de correlación
  correlation_data = stats_data.get("rating_sentiment_correlation", {})
  alignment_score = correlation_data.get("alignment_score", 0)
  discrepancies = correlation_data.get("discrepancies", [])
  
  st.markdown("#### Análisis de Correlación Rating vs Sentimiento IA")
  
  col1, col2 = st.columns(2)
  with col1:
    st.metric(
      "Alineación General", 
      f"{alignment_score:.1f}%",
      help="Porcentaje de reseñas donde rating coincide con sentimiento IA"
    )
  
  with col2:
    # interpretación cualitativa de nivel de alineación
    if alignment_score >= 80:
      interpretation = "Excelente correlación"
    elif alignment_score >= 60:
      interpretation = "Buena correlación"
    elif alignment_score >= 40:
      interpretation = "Correlación moderada"
    else:
      interpretation = "Baja correlación"
    
    st.metric("Interpretación", interpretation)
  
  # mostrar discrepancias significativas detectadas
  if discrepancies:
    st.markdown("##### Discrepancias Detectadas:")
    for disc in discrepancies:
      if disc["misalignment_percentage"] >= 20:
        st.warning(
          f"**Rating {disc['rating']}★**: {disc['misaligned']}/{disc['total_reviews']} reseñas "
          f"({disc['misalignment_percentage']:.1f}%) no coinciden con sentimiento esperado"
        )
  else:
    st.success("No se detectaron discrepancias significativas")

# ====================================================================================================================
#                                MOSTRAR COMPARACIÓN DETALLADA RATING VS SENTIMIENTO
# ====================================================================================================================

def display_rating_sentiment_detailed_comparison(stats_data: Dict) -> None:
  # MUESTRA COMPARACIÓN DETALLADA ENTRE RATING USUARIO Y SENTIMIENTO IA
  # Crea gráfico agrupado mostrando distribución de sentimientos por rating
  # Incluye zonas de correlación esperada y anotaciones explicativas
  data_for_df = []
  sentiment_order = ["VERY_NEGATIVE", "NEGATIVE", "NEUTRAL", "POSITIVE", "VERY_POSITIVE"]
  sentiment_display = ["Muy Negativo (0)", "Negativo (1)", "Neutral (2)", "Positivo (3)", "Muy Positivo (4)"]
  
  for rating_value, sentiment_counts in stats_data.get("rating_sentiment_breakdown", {}).items():
    for sentiment_key, display_name in zip(sentiment_order, sentiment_display):
      count = sentiment_counts.get(sentiment_key, 0)
      data_for_df.append({
        "Rating Usuario": f"{rating_value}★",
        "Sentimiento IA": display_name,
        "Cantidad": count,
        "SentimentKey": sentiment_key,
        "RatingValue": rating_value
      })
  
  if not data_for_df:
    st.info("No hay datos suficientes para mostrar el gráfico Rating vs Sentimiento")
    return
  
  df_grouped = pd.DataFrame(data_for_df)
  if df_grouped.empty or df_grouped["Cantidad"].sum() == 0:
    st.info("No hay datos suficientes para mostrar el gráfico Rating vs Sentimiento")
    return

  fig = px.bar(
    df_grouped,
    x="Rating Usuario",
    y="Cantidad",
    color="Sentimiento IA",
    barmode='group',
    title="Comparación Detallada: Rating Usuario (1-5★) vs Sentimiento IA (0-4)",
    labels={
      "Rating Usuario": "Puntuación dada por el Usuario",
      "Cantidad": "Número de Reseñas",
      "Sentimiento IA": "Sentimiento detectado por IA"
    },
    color_discrete_map={
      display_name: SENTIMENT_COLORS[sentiment_key] 
      for sentiment_key, display_name in zip(sentiment_order, sentiment_display)
    },
    category_orders={
      "Rating Usuario": [f"{i}★" for i in range(1, 6)],
      "Sentimiento IA": sentiment_display
    }
  )
  
  # añadir zonas de correlación esperada como fondo
  max_count = df_grouped["Cantidad"].max() if not df_grouped.empty else 100
  
  # zona negativa esperada para ratings 1-2
  fig.add_shape(
    type="rect", x0=-0.5, x1=1.5, y0=0, y1=max_count,
    fillcolor="red", opacity=0.1, line_width=0
  )
  
  # zona positiva esperada para ratings 4-5
  fig.add_shape(
    type="rect", x0=2.5, x1=4.5, y0=0, y1=max_count,
    fillcolor="green", opacity=0.1, line_width=0
  )
  
  fig.update_layout(
    xaxis_title="Puntuación del Usuario",
    yaxis_title="Número de Reseñas",
    height=700,
    legend_title="Sentimiento detectado por IA",
    annotations=[
      dict(
        text="Correlación esperada: Ratings 1-2★ → Sentimientos negativos | Ratings 4-5★ → Sentimientos positivos",
        showarrow=False, x=0.5, y=1.05, xref="paper", yref="paper",
        font=dict(size=12, color="gray")
      )
    ]
  )
  
  st.plotly_chart(fig, use_container_width=True)

# ====================================================================================================================
#                                   MOSTRAR HISTOGRAMA DE SCORES DE SENTIMIENTO
# ====================================================================================================================

def display_sentiment_score_histogram(stats_data: Dict) -> None:
  # MUESTRA HISTOGRAMA DE DISTRIBUCIÓN DE SCORES DE SENTIMIENTO 0-4
  # Visualiza frecuencia de scores con línea de promedio superpuesta
  # Proporciona visión detallada de la distribución continua de sentimientos
  scores = stats_data.get("sentiment_scores", [])
  
  if not scores:
    st.info("No hay scores de sentimiento para mostrar")
    return
  
  df_scores = pd.DataFrame({"Score": scores})
  
  fig = px.histogram(
    df_scores,
    x="Score",
    nbins=20,
    title="Distribución de Scores de Sentimiento (0-4)",
    labels={"Score": "Score de Sentimiento", "count": "Frecuencia"},
    color_discrete_sequence=["skyblue"]
  )
  
  # agregar línea vertical del promedio
  avg_score = stats_data.get("average_sentiment", 2.0)
  fig.add_vline(
    x=avg_score, 
    line_dash="dash", 
    line_color="red",
    annotation_text=f"Promedio: {avg_score:.2f}"
  )
  
  fig.update_layout(height=400)
  st.plotly_chart(fig, use_container_width=True)

# ====================================================================================================================
#                                         MANEJAR EXPORTACIÓN A EXCEL
# ====================================================================================================================

def handle_excel_export(data_handler, selected_region_name: str, reviews_count: int) -> None:
  # MANEJA EL PROCESO DE EXPORTACIÓN DE DATOS A FORMATO EXCEL
  # Valida disponibilidad de datos, genera archivo y proporciona descarga
  # Muestra progreso y maneja errores durante la generación del archivo
  if reviews_count == 0:
    st.error("No hay reseñas para exportar")
    return
  
  with st.spinner("Generando archivo Excel..."):
    try:
      data_for_export = _prepare_export_data(data_handler, selected_region_name)
      exporter = DataExporter()
      excel_bytes = exporter.export_to_excel_bytes(data_for_export)
      
      if excel_bytes:
        filename = _generate_filename(selected_region_name)
        st.download_button(
          label="Descargar Excel",
          data=excel_bytes,
          file_name=filename,
          mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          key="immediate_download_button"
        )
        st.success("Archivo Excel generado exitosamente")
      else:
        st.error("Error generando el archivo Excel")
        
    except Exception as e:
      log.error(f"Error exportación Excel: {e}")
      st.error(f"Error generando archivo: {str(e)}")

# ====================================================================================================================
#                                        PREPARAR DATOS PARA EXPORTACIÓN
# ====================================================================================================================

def _prepare_export_data(data_handler, selected_region_name: str) -> Dict:
  # PREPARA Y FILTRA DATOS SEGÚN REGIÓN SELECCIONADA PARA EXPORTACIÓN
  # Construye estructura de datos apropiada para el exportador
  # Retorna diccionario con regiones filtradas según selección del usuario
  if selected_region_name == "Todas las regiones":
    return {"regions": data_handler.data.get("regions", [])}
  else:
    filtered_regions = [
      r for r in data_handler.data.get("regions", [])
      if r.get("region_name") == selected_region_name
    ]
    return {"regions": filtered_regions}

# ====================================================================================================================
#                                         GENERAR NOMBRE DE ARCHIVO
# ====================================================================================================================

def _generate_filename(region_name: str) -> str:
  # GENERA NOMBRE SEGURO DE ARCHIVO PARA EXPORTACIÓN CON TIMESTAMP
  # Limpia caracteres especiales y agrega marca temporal única
  # Retorna nombre de archivo válido para sistema de archivos
  safe_region_name = region_name.replace(' ', '_').replace('/', '_')
  timestamp = datetime.now().strftime('%Y%m%d_%H%M')
  return f"datos_{safe_region_name}_{timestamp}.xlsx"

# ====================================================================================================================
#                                            RENDERIZAR PÁGINA PRINCIPAL
# ====================================================================================================================

def render(data_handler):
  # RENDERIZA INTERFAZ PRINCIPAL DE ANÁLISIS COMPARATIVO
  # Coordina validación de datos, filtros, exportación y visualizaciones
  # Maneja flujo completo desde carga de datos hasta presentación de resultados
  st.header("Análisis Comparativo: Rating vs Texto")
  st.markdown("Comparación entre puntuación (1-5 estrellas) y análisis de sentimiento por IA")
  st.caption("Modelo multilingual (escala 0-4): tabularisai/multilingual-sentiment-analysis")
  
  if not _validate_data_availability(data_handler):
    return
  
  region_names = _get_available_regions(data_handler)
  if not region_names:
    st.warning("No hay regiones disponibles para mostrar")
    return
  
  selected_region = _render_filters_section(region_names)
  reviews_data = get_all_reviews_for_ui(data_handler, selected_region)
  _render_export_section(data_handler, selected_region, len(reviews_data))
  
  if reviews_data:
    _render_analysis_section(reviews_data)
    _render_detailed_data_section(reviews_data)
  else:
    st.info(f"No hay reseñas disponibles para '{selected_region}'")

# ====================================================================================================================
#                                        VALIDAR DISPONIBILIDAD DE DATOS
# ====================================================================================================================

def _validate_data_availability(data_handler) -> bool:
  # VALIDA QUE EL DATA HANDLER CONTENGA DATOS VÁLIDOS PARA PROCESAMIENTO
  # Verifica estructura básica de datos requerida para análisis
  # Retorna True si los datos están disponibles y son procesables
  if not hasattr(data_handler, 'data') or not data_handler.data:
    st.error("No hay datos disponibles en el sistema")
    return False
  
  if "regions" not in data_handler.data:
    st.error("No se encontraron datos de regiones")
    return False
  
  return True

# ====================================================================================================================
#                                          OBTENER REGIONES DISPONIBLES
# ====================================================================================================================

def _get_available_regions(data_handler) -> List[str]:
  # EXTRAE LISTA DE NOMBRES DE REGIONES DISPONIBLES EN LOS DATOS
  # Filtra regiones válidas y elimina duplicados
  # Retorna lista ordenada de nombres de regiones únicos
  regions_data = data_handler.data.get("regions", [])
  region_names = [
    r.get("region_name") 
    for r in regions_data 
    if r.get("region_name")
  ]
  return sorted(list(set(region_names)))

# ====================================================================================================================
#                                           RENDERIZAR SECCIÓN DE FILTROS
# ====================================================================================================================

def _render_filters_section(region_names: List[str]) -> str:
  # RENDERIZA CONTROLES DE FILTRO PARA SELECCIÓN DE REGIÓN
  # Proporciona dropdown con opción de todas las regiones
  # Retorna nombre de región seleccionada por el usuario
  st.subheader("Filtros y Descarga")
  
  col_filter, col_download = st.columns([3, 1])
  
  with col_filter:
    selected_region = st.selectbox(
      "Selecciona una región:",
      options=["Todas las regiones"] + region_names,
      key="region_selector_results"
    )
  
  return selected_region

# ====================================================================================================================
#                                         RENDERIZAR SECCIÓN DE EXPORTACIÓN
# ====================================================================================================================

def _render_export_section(data_handler, selected_region: str, reviews_count: int) -> None:
  # RENDERIZA CONTROLES DE EXPORTACIÓN DE DATOS
  # Proporciona botón para generar y descargar archivo Excel
  # Maneja la llamada a función de exportación con parámetros apropiados
  col_filter, col_download = st.columns([3, 1])
  
  with col_download:
    if st.button("Generar Excel", key="main_download_button"):
      handle_excel_export(data_handler, selected_region, reviews_count)

# ====================================================================================================================
#                                         RENDERIZAR SECCIÓN DE ANÁLISIS
# ====================================================================================================================

def _render_analysis_section(reviews_data: List[Dict]) -> None:
  # RENDERIZA SECCIÓN PRINCIPAL CON MÉTRICAS Y VISUALIZACIONES DE ANÁLISIS
  # Calcula estadísticas, muestra métricas clave y genera gráficos comparativos
  # Coordina presentación completa de resultados de análisis de sentimiento
  stats = calculate_sentiment_stats(reviews_data)
  
  # mostrar métricas clave en columnas
  st.subheader("Métricas Clave")
  col1, col2, col3, col4 = st.columns(4)
  
  total_reviews = stats.get("total_reviews", 0)
  valid_analyzed = stats.get("valid_analyzed_reviews", 0)
  avg_sentiment = stats.get("average_sentiment", 2.0)
  alignment_score = stats.get("rating_sentiment_correlation", {}).get("alignment_score", 0)
  
  col1.metric("Total Reseñas Filtradas", f"{total_reviews:,}")
  col2.metric("Reseñas Válidas", f"{valid_analyzed:,}")
  col3.metric("Sentimiento Promedio", f"{avg_sentiment:.2f}/4.0")
  col4.metric("Correlación Rating-IA", f"{alignment_score:.1f}%")
  
  # mostrar métricas por categoría de sentimiento
  st.markdown("---")
  sentiment_summary = stats.get("sentiment_summary", {})
  positive_count = sentiment_summary.get("POSITIVE", 0) + sentiment_summary.get("VERY_POSITIVE", 0)
  negative_count = sentiment_summary.get("NEGATIVE", 0) + sentiment_summary.get("VERY_NEGATIVE", 0)
  neutral_count = sentiment_summary.get("NEUTRAL", 0)
  
  col5, col6, col7 = st.columns(3)
  col5.metric("Positivas", f"{positive_count:,}", 
              f"{(positive_count/valid_analyzed*100):.1f}%" if valid_analyzed > 0 else "0%")
  col6.metric("Neutrales", f"{neutral_count:,}", 
              f"{(neutral_count/valid_analyzed*100):.1f}%" if valid_analyzed > 0 else "0%")
  col7.metric("Negativas", f"{negative_count:,}", 
              f"{(negative_count/valid_analyzed*100):.1f}%" if valid_analyzed > 0 else "0%")
  
  # información sobre reseñas excluidas del análisis
  if total_reviews != valid_analyzed:
    excluded_count = total_reviews - valid_analyzed
    st.info(f"Se excluyeron {excluded_count:,} reseñas sin análisis de sentimiento válido")
  
  # mostrar distribuciones individuales lado a lado
  st.markdown("---")
  st.subheader("Distribuciones Individuales")
  
  col_chart1, col_chart2 = st.columns(2)
  
  with col_chart1:
    display_individual_ratings_bar_chart(stats)
  
  with col_chart2:
    display_multilingual_sentiment_chart(stats)
  
  # análisis de correlación entre rating y sentimiento
  st.markdown("---")
  display_rating_sentiment_correlation_analysis(stats)
  
  # histograma de distribución de scores
  st.markdown("---")
  display_sentiment_score_histogram(stats)
  
  # comparación detallada con gráfico agrupado
  st.markdown("---")
  st.subheader("Comparación Detallada")
  display_rating_sentiment_detailed_comparison(stats)

# ====================================================================================================================
#                                      RENDERIZAR SECCIÓN DE DATOS DETALLADOS
# ====================================================================================================================

def _render_detailed_data_section(reviews_data: List[Dict]) -> None:
  # RENDERIZA SECCIÓN OPCIONAL CON DATOS DETALLADOS DE RESEÑAS
  # Muestra tabla expandible con primeras 50 reseñas para inspección manual
  # Permite validación visual de datos procesados y análisis realizados
  if st.checkbox("Mostrar datos detallados de reseñas", key="show_detailed_data"):
    st.subheader("Datos Detallados de Reseñas")
    
    try:
      df_reviews = pd.DataFrame(reviews_data)
      
      # seleccionar columnas disponibles para mostrar
      cols_to_show = [
        col for col in UI_COLUMN_MAPPING.keys() 
        if col in df_reviews.columns
      ]
      
      if cols_to_show:
        df_display = df_reviews[cols_to_show].rename(columns=UI_COLUMN_MAPPING)
        st.caption(f"Mostrando las primeras 50 reseñas de {len(df_reviews)} total")
        st.dataframe(
          df_display.head(50),
          use_container_width=True,
          hide_index=True
        )
      else:
        st.info("No hay columnas de datos detallados para mostrar")
        
    except Exception as e:
      log.error(f"Error mostrando datos detallados: {e}")
      st.error("Error al mostrar los datos detallados")