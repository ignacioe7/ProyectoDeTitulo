import streamlit as st 
import asyncio # Para manejar las cosas asíncronas
from src.core.analyzer import load_analyzer
from loguru import logger as log

def render(data_handler):
  st.header("Análisis de Sentimientos") # El título de esta sección

  if data_handler is None:
      st.error("Error crítico: El gestor de datos (DataHandler) no está disponible.")
      log.error("analisis_sentimientos.render: data_handler es None.")
      return # No continuar si no hay data_handler
  
  regions = data_handler.load_regions()
  if not regions:
      st.warning("No se encontraron regiones. Verifica el archivo 'regions.json'.")
      return

  region_names = [region["nombre"] for region in regions]
  selected_region = st.selectbox("Selecciona una región para analizar", region_names)

  analyzer_instance = None
  analyzer_ready = False

  try:
      analyzer_instance = load_analyzer()
      if analyzer_instance and analyzer_instance.nlp:
          analyzer_ready = True
          log.debug("Analizador de sentimientos listo.")
      else:
          log.warning("El analizador de sentimientos no está listo (load_analyzer devolvió None o nlp es None).")
          st.warning("El modelo de análisis de sentimientos no está listo o falló al cargar. El botón está deshabilitado.")
  except Exception as e:
      log.error(f"Error al intentar cargar el analizador: {e}")
      st.error(f"Ocurrió un error al intentar cargar el modelo de análisis: {e}")
      analyzer_ready = False 

  if st.button("Iniciar Análisis", disabled=not analyzer_ready):
    st.info(f"Iniciando análisis de sentimientos para la región: {selected_region}...")
    log.info(f"Botón 'Iniciar Análisis' presionado para {selected_region}")
    with st.spinner(f"Analizando reseñas de {selected_region}... Esto puede tardar varios minutos."):
        success = asyncio.run(data_handler.analyze_and_update_excel(selected_region))

    if success:
      st.success(f"Análisis completado y archivo Excel actualizado para {selected_region}")
      log.info(f"Análisis completado con éxito para {selected_region}")
    else:
      st.error(f"El análisis para {selected_region} falló. Revisa los mensajes anteriores y los logs para más detalles.")
      log.error(f"Análisis fallido para {selected_region}")