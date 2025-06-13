from datetime import datetime, timezone
import torch
from transformers import pipeline
from loguru import logger as log 
from typing import Dict, Tuple, Optional, List
import streamlit as st

# ========================================================================================================
#                                           CARGAR ANALIZADOR
# ========================================================================================================

@st.cache_resource
def load_analyzer(use_cpu: bool = False):
  # CARGA EL ANALIZADOR DE SENTIMIENTOS CON CACHE DE STREAMLIT
  log.info("Cargando analizador")
  try:
    analyzer = SentimentAnalyzer(use_cpu=use_cpu)
    if analyzer.nlp is None:
      log.error("Fallo inicialización del analizador")
      return None 
    return analyzer
  except Exception as e:
    log.error(f"Error crítico cargando analizador: {e}")
    return None 

# ========================================================================================================
#                                        ANALIZADOR PRINCIPAL
# ========================================================================================================

class SentimentAnalyzer:
  # Analizador de sentimientos multiidioma usando modelo de Hugging Face
  # Maneja carga del modelo, análisis de texto y generación de estadísticas
  
  def __init__(self, use_cpu: bool = False):
    self.model_name = "tabularisai/multilingual-sentiment-analysis"
    self.nlp = None

    try:
      # Detecta si usar CPU o GPU según disponibilidad
      device = -1 if use_cpu or not torch.cuda.is_available() else 0
      device_name = 'CPU' if device == -1 else 'GPU'
      
      # Inicializa pipeline de transformers para clasificación de texto
      self.nlp = pipeline(
        "text-classification", 
        model=self.model_name,
        device=device,
        truncation=True
      )

      log.info(f"Modelo cargado en {device_name}")
    except Exception as e:
      log.error(f"Error cargando modelo: {e}")
      self.nlp = None

# ========================================================================================================
#                                           ANALIZAR TEXTO
# ========================================================================================================

  def analyze_text(self, text: str) -> Tuple[str, float]:
    # ANALIZA UN TEXTO Y DEVUELVE SENTIMIENTO CLASIFICADO CON SCORE NUMÉRICO
    if self.nlp is None:
      log.warning("Modelo no disponible")
      return "ERROR", 2.0

    try:
      # Limita texto a 512 caracteres para evitar errores del modelo
      processed_text = str(text).strip()[:512]
      
      if not processed_text:
        return "NEUTRAL", 2.0
        
      result = self.nlp(processed_text)[0]
      
      label = result['label']
      confidence = float(result['score'])
      
      # Mapea etiquetas del modelo a sentimientos consistentes
      sentiment_mapping = {
        # Formato estándar del modelo multilingual
        "LABEL_0": ("VERY_NEGATIVE", 0.0),
        "LABEL_1": ("NEGATIVE", 1.0),
        "LABEL_2": ("NEUTRAL", 2.0),
        "LABEL_3": ("POSITIVE", 3.0),
        "LABEL_4": ("VERY_POSITIVE", 4.0),
        
        # Variaciones en inglés
        "Very Negative": ("VERY_NEGATIVE", 0.0),
        "Negative": ("NEGATIVE", 1.0),
        "Neutral": ("NEUTRAL", 2.0),
        "Positive": ("POSITIVE", 3.0),
        "Very Positive": ("VERY_POSITIVE", 4.0),
        
        # Variaciones con mayúsculas
        "VERY_NEGATIVE": ("VERY_NEGATIVE", 0.0),
        "NEGATIVE": ("NEGATIVE", 1.0),
        "NEUTRAL": ("NEUTRAL", 2.0),
        "POSITIVE": ("POSITIVE", 3.0),
        "VERY_POSITIVE": ("VERY_POSITIVE", 4.0)
      }
      
      if label in sentiment_mapping:
        sentiment_name, sentiment_value = sentiment_mapping[label]
        log.debug(f"Texto analizado: {sentiment_name} ({sentiment_value}) confianza: {confidence:.3f}")
        return sentiment_name, sentiment_value
      else:
        log.warning(f"Label no reconocido: {label}")
        return "NEUTRAL", 2.0

    except Exception as e:
      log.error(f"Error analizando texto: {e}")
      return "ERROR", 2.0

# ========================================================================================================
#                                          ANALIZAR RESEÑA
# ========================================================================================================

  def analyze_review(self, title: Optional[str], text: Optional[str]) -> Tuple[str, float]:
    # ANALIZA TÍTULO Y TEXTO DE RESEÑA COMBINADOS PARA MEJOR PRECISIÓN
    try:
      # Limpia y valida ambos campos de entrada
      title_clean = str(title).strip() if title else ""
      text_clean = str(text).strip() if text else ""

      # Combina título y texto de manera inteligente
      if title_clean and text_clean:
        combined_text = f"{title_clean}. {text_clean}"
      elif title_clean:
        combined_text = title_clean
      elif text_clean:
        combined_text = text_clean
      else:
        return "NEUTRAL", 2.0

      # Procesa el texto combinado usando el analizador principal
      return self.analyze_text(combined_text)
      
    except Exception as e:
      log.error(f"Error en analyze_review: {e}")
      return "ERROR", 2.0

# ========================================================================================================
#                                      ANALIZAR RESEÑAS DE ATRACCIÓN
# ========================================================================================================

  async def analyze_attraction_reviews(self, attraction_data: Dict) -> Dict:
    # PROCESA TODAS LAS RESEÑAS NO ANALIZADAS DE UNA ATRACCIÓN ESPECÍFICA
    if self.nlp is None:
      log.warning("Modelo no disponible")
      return attraction_data
    
    analyzed_reviews = []
    reviews = attraction_data.get("reviews", [])
    newly_analyzed = 0
    
    for review in reviews:
      # Omite reseñas que ya tienen análisis previo
      if review.get("sentiment") and review.get("sentiment_score") is not None:
        analyzed_reviews.append(review)
        continue
      
      # Analiza reseña nueva
      sentiment, score = self.analyze_review(
        review.get("title"), 
        review.get("review_text")
      )
      
      # Actualiza reseña con resultados del análisis
      updated_review = {
        **review,
        "sentiment": sentiment,
        "sentiment_score": score,
        "analyzed_at": datetime.now(timezone.utc).isoformat()
      }
      analyzed_reviews.append(updated_review)
      newly_analyzed += 1
    
    if newly_analyzed > 0:
      attraction_name = attraction_data.get('attraction_name', 'Atracción')
      log.info(f"{newly_analyzed} reseñas analizadas para {attraction_name}")
    
    # Retorna atracción con todas las reseñas actualizadas
    return {
      **attraction_data,
      "reviews": analyzed_reviews,
      "last_analyzed_date": datetime.now(timezone.utc).isoformat()
    }

# ========================================================================================================
#                                       ANALIZAR RESEÑAS DE REGIÓN
# ========================================================================================================

  async def analyze_region_reviews(
    self, 
    region_data: Dict, 
    progress_callback: Optional[callable] = None
  ) -> Dict:
    # PROCESA TODAS LAS RESEÑAS DE TODAS LAS ATRACCIONES EN UNA REGIÓN
    if self.nlp is None:
      log.error("Modelo no disponible")
      if progress_callback:
        progress_callback(1.0, "Modelo no disponible")
      return region_data
        
    attractions = region_data.get("attractions", [])
    total_attractions = len(attractions)
    region_name = region_data.get('region_name', 'Región')
    
    if total_attractions == 0:
      log.info(f"Sin atracciones en {region_name}")
      if progress_callback:
        progress_callback(1.0, "Sin atracciones")
      return {
        **region_data,
        "last_analyzed_date": datetime.now(timezone.utc).isoformat()
      }

    analyzed_attractions = []
    
    for i, attraction in enumerate(attractions):
      attraction_name = attraction.get("attraction_name", f"Atracción #{i+1}")
      
      # Procesa cada atracción individualmente
      analyzed_attraction = await self.analyze_attraction_reviews(attraction)
      analyzed_attractions.append(analyzed_attraction)
      
      # Actualiza callback de progreso si está disponible
      if progress_callback:
        progress = (i + 1) / total_attractions
        status = f"{attraction_name} ({i+1}/{total_attractions})"
        progress_callback(progress, status)
    
    log.info(f"Análisis completado para {region_name}")
    
    if progress_callback:
      progress_callback(1.0, f"{region_name} completado")

    return {
      **region_data,
      "attractions": analyzed_attractions,
      "last_analyzed_date": datetime.now(timezone.utc).isoformat()
    }

# ========================================================================================================
#                                        ESTADÍSTICAS DE SENTIMIENTOS
# ========================================================================================================

  def get_sentiment_stats(self, reviews: List[Dict]) -> Dict:
    # CALCULA MÉTRICAS Y DISTRIBUCIÓN DE SENTIMIENTOS DE UNA LISTA DE RESEÑAS
    if not reviews:
      return {
        "total_reviews": 0,
        "sentiment_distribution": {},
        "average_sentiment": 2.0,
        "sentiment_counts": {}
      }
    
    # Contabiliza sentimientos y scores para estadísticas
    sentiment_counts = {}
    sentiment_scores = []
    
    for review in reviews:
      sentiment = review.get("sentiment")
      score = review.get("sentiment_score")
      
      if sentiment and score is not None:
        sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
        sentiment_scores.append(float(score))
    
    total_analyzed = len(sentiment_scores)
    average_score = sum(sentiment_scores) / total_analyzed if sentiment_scores else 2.0
    
    # Calcula distribución porcentual de cada categoría
    distribution = {}
    for sentiment, count in sentiment_counts.items():
      distribution[sentiment] = round((count / total_analyzed) * 100, 1) if total_analyzed > 0 else 0
    
    return {
      "total_reviews": len(reviews),
      "analyzed_reviews": total_analyzed,
      "sentiment_distribution": distribution,
      "average_sentiment": round(average_score, 2),
      "sentiment_counts": sentiment_counts
    }