import torch # Para PyTorch
from transformers import pipeline # La magia de Hugging Face
from loguru import logger as log 
from typing import Tuple, Optional 
import pandas as pd # Para manejar NaNs
import streamlit as st

@st.cache_resource
def load_analyzer(use_cpu: bool = False):
    """Carga y devuelve una instancia única del SentimentAnalyzer."""
    log.info("Intentando cargar SentimentAnalyzer desde caché o inicializando...")
    try:
        analyzer = SentimentAnalyzer(use_cpu=use_cpu)
        if analyzer.nlp is None:
             log.error("load_analyzer: La inicialización de SentimentAnalyzer falló (nlp es None).")
             return None 
        log.success("SentimentAnalyzer cargado/recuperado con éxito.")
        return analyzer
    except Exception as e:
        log.error(f"Excepción crítica durante load_analyzer: {e}")
        return None 

class SentimentAnalyzer:
  def __init__(self, use_cpu: bool = False):
    self.model_name = "distilbert-base-uncased-finetuned-sst-2-english" # Modelo elegido
    self.nlp = None # Aquí irá el pipeline

    try:
      # Forzar CPU si se pide o si no hay GPU
      device = -1 if use_cpu or not torch.cuda.is_available() else 0

      log.info(f"Inicializando modelo: {self.model_name} en {'CPU' if device == -1 else 'GPU'}")

      # Creamos el pipeline de análisis de sentimiento
      self.nlp = pipeline(
        "sentiment-analysis",
        model=self.model_name,
        device=device,
        truncation=True # Importante para textos largos
      )

      # Prueba rápida para ver si funciona
      test_result = self.nlp("This is great!", truncation=True)[0]
      log.success(f"Modelo inicializado correctamente Prueba: {test_result}")

    except Exception as e:
      log.error(f"Error al inicializar el modelo: {e}")
      self._fallback_initialization() # Intentamos el plan B

  def _fallback_initialization(self):
    """Intenta con un enfoque más simple si falla la inicialización principal"""
    if self.nlp is not None: # Si ya se inicializó, no hacer nada
         return
    try:
      log.warning("Intentando inicialización alternativa...")
      # Usamos el pipeline genérico, forzando CPU
      self.nlp = pipeline(
        "sentiment-analysis",
        framework="pt", # PyTorch
        device=-1  # Forzar CPU
      )
      log.success("Modelo alternativo inicializado (puede tener menor precisión)")
    except Exception as e:
      log.error("No se pudo inicializar ningún modelo de análisis de sentimiento")
      self.nlp = None # Nos rendimos

  def analyze_text(self, text: str) -> Tuple[str, float]:
    """Versión robusta del análisis de texto"""
    if self.nlp is None:
      return "ERROR", 0.0 # Si no hay modelo, error

    try:

      # Analizamos, truncando si es muy largo
      result = self.nlp(
        str(text)[:512], # Convertimos a str y cortamos
        truncation=True,
        max_length=512 # Límite del modelo
      )[0] # Cogemos el primer resultado

      # Normalizar etiquetas a POSITIVE/NEGATIVE
      label = result['label'].upper()
      if "POS" in label:
        return "POSITIVE", result['score']
      elif "NEG" in label:
        return "NEGATIVE", result['score']

    except Exception as e:
      log.error(f"Error analizando texto: {e}")
      return "ERROR", 0.0 # Error durante el análisis

  def analyze_review(self, title: Optional[str], text: Optional[str]) -> Tuple[str, float]:
    """Analiza combinación de título y reseña con manejo mejorado de valores nulos"""
    try:
      # Limpiamos título y texto
      title_clean = str(title).strip() if title else ""
      text_clean = str(text).strip() if text else ""

      # Combinamos título y texto, si hay título
      combined = f"{title_clean}. {text_clean}" if title_clean else text_clean

      # Analizamos el texto combinado, limitado
      return self.analyze_text(combined[:512])
    except Exception as e:
      log.error(f"Error en analyze_review: {str(e)}")
      return "ERROR", 0.0 # Error aquí también