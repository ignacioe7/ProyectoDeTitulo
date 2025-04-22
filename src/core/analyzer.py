import torch
from transformers import pipeline
from loguru import logger as log
from typing import Tuple, Optional
import pandas as pd

class SentimentAnalyzer:
    def __init__(self, use_cpu: bool = False):
        self.model_name = "distilbert-base-uncased-finetuned-sst-2-english"
        self.nlp = None
        
        try:
            # Forzar CPU si se solicita o si no hay GPU disponible
            device = -1 if use_cpu or not torch.cuda.is_available() else 0
            
            log.info(f"Inicializando modelo: {self.model_name} en {'CPU' if device == -1 else 'GPU'}")
            
            self.nlp = pipeline(
                "sentiment-analysis",
                model=self.model_name,
                device=device,
                truncation=True
            )
            
            # Prueba básica para verificar que funciona
            test_result = self.nlp("This is great!", truncation=True)[0]
            log.success(f"Modelo inicializado correctamente. Prueba: {test_result}")
            
        except Exception as e:
            log.error(f"Error al inicializar el modelo: {e}")
            self._fallback_initialization()

    def _fallback_initialization(self):
        """Intenta con un enfoque más simple si falla la inicialización principal"""
        try:
            log.warning("Intentando inicialización alternativa...")
            self.nlp = pipeline(
                "sentiment-analysis",
                framework="pt",
                device=-1  # Forzar CPU
            )
            log.success("Modelo alternativo inicializado (puede tener menor precisión)")
        except Exception as e:
            log.error("No se pudo inicializar ningún modelo de análisis de sentimiento")
            self.nlp = None

    def analyze_text(self, text: str) -> Tuple[str, float]:
        """Versión robusta del análisis de texto"""
        if self.nlp is None:
            return "ERROR", 0.0
            
        try:
            if not text or pd.isna(text):
                return "NEUTRAL", 0.5
                
            result = self.nlp(
                str(text)[:512],
                truncation=True,
                max_length=512
            )[0]
            
            # Normalizar etiquetas
            label = result['label'].upper()
            if "POS" in label:
                return "POSITIVE", result['score']
            elif "NEG" in label:
                return "NEGATIVE", result['score']
            return "NEUTRAL", result['score']
            
        except Exception as e:
            log.error(f"Error analizando texto: {e}")
            return "ERROR", 0.0

    def analyze_review(self, title: Optional[str], text: Optional[str]) -> Tuple[str, float]:
        """Analiza combinación de título y reseña con manejo mejorado de valores nulos"""
        try:
            # Preprocesamiento de texto más robusto
            title_clean = str(title).strip() if title else ""
            text_clean = str(text).strip() if text else ""

            # Combinar título y texto con puntuación adecuada
            combined = f"{title_clean}. {text_clean}" if title_clean else text_clean

            # Manejo de strings vacíos
            if not combined.strip():
                return "NEUTRAL", 0.5

            return self.analyze_text(combined[:512])  # Limitar a longitud máxima del modelo
        except Exception as e:
            log.error(f"Error en analyze_review: {str(e)}")
            return "ERROR", 0.0