from datetime import datetime, timezone
import torch
from transformers import pipeline
from loguru import logger as log 
from typing import Dict, Tuple, Optional, List
import streamlit as st

@st.cache_resource
def load_analyzer(use_cpu: bool = False):
  """Carga analizador de sentimientos"""
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

class SentimentAnalyzer:
  """Analizador de sentimientos multilingual"""
  
  def __init__(self, use_cpu: bool = False):
    self.model_name = "tabularisai/multilingual-sentiment-analysis"
    self.nlp = None
    self.data_handler = None

    try:
      # Detectar dispositivo
      device = -1 if use_cpu or not torch.cuda.is_available() else 0
      device_name = 'CPU' if device == -1 else 'GPU'
      
      # Crear pipeline del modelo
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

  def analyze_text(self, text: str) -> Tuple[str, float]:
    """Analiza texto y devuelve sentimiento y score (0-4)"""
    if self.nlp is None:
      log.warning("Modelo no disponible")
      return "ERROR", 2.0

    try:
      processed_text = str(text).strip()[:512]
      
      if not processed_text:
        return "NEUTRAL", 2.0
        
      result = self.nlp(processed_text)[0]
      
      label = result['label']
      confidence = float(result['score'])
      
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

  def analyze_review(self, title: Optional[str], text: Optional[str]) -> Tuple[str, float]:
    """Analiza título y texto de reseña combinados"""
    try:
      # Limpiar y combinar título + texto
      title_clean = str(title).strip() if title else ""
      text_clean = str(text).strip() if text else ""

      # Combinar inteligentemente
      if title_clean and text_clean:
        combined_text = f"{title_clean}. {text_clean}"
      elif title_clean:
        combined_text = title_clean
      elif text_clean:
        combined_text = text_clean
      else:
        return "NEUTRAL", 2.0

      # Analizar texto combinado
      return self.analyze_text(combined_text)
      
    except Exception as e:
      log.error(f"Error en analyze_review: {e}")
      return "ERROR", 2.0

  async def analyze_attraction_reviews_multilenguage(self, attraction_data: Dict, language: str = "spanish") -> Dict:
    """Analiza reseñas de una atracción en un idioma específico - ACTUALIZADO MULTILENGUAJE"""
    if self.nlp is None:
      log.warning("Modelo no disponible")
      return attraction_data
    
    # ✅ NUEVO: Trabajar con estructura multilenguaje
    if "languages" not in attraction_data:
      return attraction_data
    
    if language not in attraction_data["languages"]:
      return attraction_data
    
    language_data = attraction_data["languages"][language]
    reviews = language_data.get("reviews", [])
    
    if not reviews:
      return attraction_data
    
    analyzed_reviews = []
    newly_analyzed = 0
    
    for review in reviews:
      # Saltar si ya tiene análisis
      if review.get("sentiment") and review.get("sentiment_score") is not None:
        analyzed_reviews.append(review)
        continue
      
      # Analizar nueva reseña
      sentiment, score = self.analyze_review(
        review.get("title"), 
        review.get("review_text")
      )
      
      # Actualizar reseña
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
      log.info(f"{newly_analyzed} reseñas analizadas para {attraction_name} en {language}")
    
    # ✅ ACTUALIZAR: Estructura multilenguaje
    updated_attraction = {**attraction_data}
    updated_attraction["languages"][language]["reviews"] = analyzed_reviews
    updated_attraction["last_analyzed_date"] = datetime.now(timezone.utc).isoformat()
    
    return updated_attraction

  def analyze_region_reviews(self, data_handler, region_name: str, language: str = "spanish") -> Dict:
    """Analiza reseñas de una región específica - CORREGIDO"""
    
    log.info(f"Iniciando análisis para región: {region_name}, idioma: {language}")
    
    # ✅ CORREGIDO: Obtener datos de la región desde data_handler
    region_data = data_handler.get_region_data(region_name)
    if not region_data:
      log.error(f"Región '{region_name}' no encontrada")
      return {"total_reviews": 0, "analyzed_reviews": 0}
    
    attractions = region_data.get("attractions", [])
    log.info(f"Atracciones encontradas: {len(attractions)}")
    
    all_reviews = []
    processed_attractions = 0
    total_analyzed = 0
    
    for attraction in attractions:
      try:
        # ✅ CORREGIDO: Trabajar con estructura multilenguage
        languages_data = attraction.get("languages", {})
        
        if language not in languages_data:
          log.debug(f"Atracción '{attraction.get('attraction_name', 'Sin nombre')}': no tiene reseñas en {language}")
          continue
        
        reviews = languages_data[language].get("reviews", [])
        
        if reviews:
          log.debug(f"Atracción '{attraction.get('attraction_name', 'Sin nombre')}': {len(reviews)} reseñas en {language}")
          all_reviews.extend(reviews)
          processed_attractions += 1
          
          # Contar reseñas ya analizadas
          analyzed_count = sum(1 for r in reviews if r.get("sentiment") and r.get("sentiment_score") is not None)
          total_analyzed += analyzed_count
        else:
          log.debug(f"Atracción '{attraction.get('attraction_name', 'Sin nombre')}': 0 reseñas en {language}")
          
      except Exception as e:
        log.error(f"Error procesando atracción {attraction.get('attraction_name', 'Sin nombre')}: {e}")
        continue
    
    log.info(f"Total reseñas encontradas: {len(all_reviews)} de {processed_attractions} atracciones")
    log.info(f"Reseñas ya analizadas: {total_analyzed}")
    
    if not all_reviews:
      log.warning(f"No se encontraron reseñas en {language} para la región {region_name}")
      return {
        'total_reviews': 0,
        'analyzed_reviews': 0,
        'sentiment_distribution': {'POSITIVE': 0, 'NEUTRAL': 0, 'NEGATIVE': 0},
        'processed_attractions': 0
      }
    
    # ✅ NUEVO: Obtener estadísticas de sentimientos
    stats = self.get_sentiment_stats(all_reviews)
    
    return {
      'total_reviews': len(all_reviews),
      'analyzed_reviews': stats["analyzed_reviews"],
      'sentiment_distribution': stats["sentiment_distribution"],
      'average_sentiment': stats["average_sentiment"],
      'processed_attractions': processed_attractions
    }

  async def analyze_and_save_region(self, data_handler, region_name: str, language: str = "spanish") -> Dict:
    """Analiza todas las reseñas de una región y guarda los resultados"""
    
    log.info(f"Iniciando análisis completo para región: {region_name}, idioma: {language}")
    
    region_data = data_handler.get_region_data(region_name)
    if not region_data:
      return {"error": f"Región '{region_name}' no encontrada"}
    
    attractions = region_data.get("attractions", [])
    total_newly_analyzed = 0
    processed_attractions = 0
    
    for i, attraction in enumerate(attractions):
      try:
        attraction_name = attraction.get("attraction_name", f"Atracción {i+1}")
        log.debug(f"Procesando atracción {i+1}/{len(attractions)}: {attraction_name}")
        
        # Analizar reseñas de la atracción
        updated_attraction = await self.analyze_attraction_reviews_multilenguage(attraction, language)
        
        # Contar nuevas reseñas analizadas
        if language in updated_attraction.get("languages", {}):
          reviews = updated_attraction["languages"][language].get("reviews", [])
          newly_analyzed = sum(1 for r in reviews if r.get("analyzed_at") and 
                             r["analyzed_at"].startswith(datetime.now().strftime("%Y-%m-%d")))
          total_newly_analyzed += newly_analyzed
          processed_attractions += 1
          
          # Actualizar la atracción en la región
          attractions[i] = updated_attraction
        
      except Exception as e:
        log.error(f"Error analizando atracción {attraction.get('attraction_name', 'Sin nombre')}: {e}")
        continue
    
    # Guardar cambios
    try:
      await data_handler.save_data()
      log.info(f"Análisis completado: {total_newly_analyzed} nuevas reseñas analizadas en {processed_attractions} atracciones")
    except Exception as e:
      log.error(f"Error guardando datos: {e}")
    
    return {
      'total_newly_analyzed': total_newly_analyzed,
      'processed_attractions': processed_attractions,
      'region_name': region_name,
      'language': language
    }
      
  def get_sentiment_stats(self, reviews: List[Dict]) -> Dict:
    """Obtiene estadísticas de sentimientos de reseñas"""
    if not reviews:
      return {
        "total_reviews": 0,
        "sentiment_distribution": {},
        "average_sentiment": 2.0,
        "sentiment_counts": {}
      }
    
    # Contar sentimientos
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
    
    # Calcular distribución porcentual
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
  
  async def analyze_region_reviews_ui(self, region_data: Dict, progress_callback=None, language: str = "spanish") -> Dict:
      """Analiza reseñas de una región específica - VERSIÓN PARA UI CON CALLBACK"""
      
      region_name = region_data.get("region_name", "Región desconocida")
      log.info(f"Iniciando análisis UI para región: {region_name}, idioma: {language}")
      
      attractions = region_data.get("attractions", [])
      log.info(f"Atracciones encontradas: {len(attractions)}")
      
      total_attractions = len(attractions)
      processed_attractions = 0
      total_newly_analyzed = 0
      
      for i, attraction in enumerate(attractions):
          try:
              # ✅ CALLBACK: Actualizar progreso si se proporciona
              if progress_callback:
                  progress = i / total_attractions if total_attractions > 0 else 0
                  attraction_name = attraction.get('attraction_name', f'Atracción {i+1}')
                  status = f"Analizando: {attraction_name} ({i+1}/{total_attractions})"
                  
                  # Verificar si el callback retorna False (señal de detención)
                  should_continue = progress_callback(progress, status)
                  if should_continue is False:
                      log.info(f"Análisis detenido por callback en atracción {i+1}")
                      break
              
              # ✅ CORREGIDO: Trabajar con estructura multilenguaje
              languages_data = attraction.get("languages", {})
              
              if language not in languages_data:
                  log.debug(f"Atracción '{attraction.get('attraction_name', 'Sin nombre')}': no tiene reseñas en {language}")
                  continue
              
              # Analizar reseñas de la atracción para el idioma específico
              updated_attraction = await self.analyze_attraction_reviews_multilenguage(attraction, language)
              
              # Contar nuevas reseñas analizadas
              if language in updated_attraction.get("languages", {}):
                  reviews = updated_attraction["languages"][language].get("reviews", [])
                  newly_analyzed_today = sum(1 for r in reviews if r.get("analyzed_at") and 
                                           r["analyzed_at"].startswith(datetime.now().strftime("%Y-%m-%d")))
                  total_newly_analyzed += newly_analyzed_today
                  processed_attractions += 1
                  
                  # Actualizar la atracción en la región
                  attractions[i] = updated_attraction
                  
                  if newly_analyzed_today > 0:
                      log.debug(f"Atracción '{attraction.get('attraction_name', 'Sin nombre')}': {newly_analyzed_today} nuevas reseñas analizadas")
              
          except Exception as e:
              log.error(f"Error analizando atracción {attraction.get('attraction_name', 'Sin nombre')}: {e}")
              continue
      
      # ✅ CALLBACK: Progreso final
      if progress_callback:
          progress_callback(1.0, f"Completado: {total_newly_analyzed} reseñas analizadas")
      
      log.info(f"Análisis completado para {region_name}: {total_newly_analyzed} nuevas reseñas analizadas en {processed_attractions} atracciones")
      
      # ✅ RETORNAR: Región actualizada con las atracciones modificadas
      updated_region = {**region_data}
      updated_region["attractions"] = attractions
      updated_region["last_analyzed_date"] = datetime.now(timezone.utc).isoformat()
      
      return updated_region
  
  async def analyze_region_all_languages_ui(self, region_data: Dict, progress_callback=None) -> Dict:
      """Analiza reseñas de una región en TODOS los idiomas disponibles - VERSIÓN PARA UI"""
      
      region_name = region_data.get("region_name", "Región desconocida")
      log.info(f"Iniciando análisis multilenguaje UI para región: {region_name}")
      
      # ✅ NUEVO: Encontrar todos los idiomas disponibles en la región
      available_languages = set()
      for attraction in region_data.get("attractions", []):
          for lang_code in attraction.get("languages", {}).keys():
              available_languages.add(lang_code)
      
      if not available_languages:
          log.warning(f"No se encontraron idiomas en la región {region_name}")
          return region_data
      
      log.info(f"Idiomas encontrados: {sorted(available_languages)}")
      
      # ✅ PROCESAR: Cada idioma secuencialmente
      updated_region = {**region_data}
      total_newly_analyzed = 0
      
      for i, language in enumerate(sorted(available_languages)):
          try:
              # ✅ CALLBACK: Progreso por idioma
              if progress_callback:
                  lang_progress = i / len(available_languages)
                  status = f"Procesando idioma: {language} ({i+1}/{len(available_languages)})"
                  should_continue = progress_callback(lang_progress, status)
                  if should_continue is False:
                      log.info(f"Análisis detenido en idioma {language}")
                      break
              
              # Analizar idioma específico usando el método UI
              result = await self.analyze_region_reviews_ui(updated_region, progress_callback, language)
              updated_region = result
              
              # Contar nuevas reseñas analizadas en este idioma
              for attraction in updated_region.get("attractions", []):
                  if language in attraction.get("languages", {}):
                      reviews = attraction["languages"][language].get("reviews", [])
                      newly_analyzed_today = sum(1 for r in reviews if r.get("analyzed_at") and 
                                               r["analyzed_at"].startswith(datetime.now().strftime("%Y-%m-%d")))
                      total_newly_analyzed += newly_analyzed_today
              
          except Exception as e:
              log.error(f"Error analizando idioma {language}: {e}")
              continue
      
      # ✅ CALLBACK: Progreso final
      if progress_callback:
          progress_callback(1.0, f"Análisis multilenguaje completado: {total_newly_analyzed} reseñas")
      
      log.info(f"Análisis multilenguaje completado para {region_name}: {total_newly_analyzed} reseñas en {len(available_languages)} idiomas")
      return updated_region