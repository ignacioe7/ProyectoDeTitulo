from typing import List, Tuple
import streamlit as st 
import asyncio
from src.core.analyzer import load_analyzer
from loguru import logger as log
import pandas as pd
from datetime import datetime, timezone
import re

def get_relative_time(iso_date_string: str) -> str:
  """Convierte fecha ISO a tiempo relativo"""
  if not iso_date_string or iso_date_string == "Nunca":
    return "Nunca"
  
  try:
    # Parsear fecha ISO
    if iso_date_string.endswith('Z'):
      iso_date_string = iso_date_string[:-1] + '+00:00'
    
    # Intentar parsear con diferentes formatos
    try:
      past_date = datetime.fromisoformat(iso_date_string)
    except ValueError:
      # Fallback para formatos sin timezone
      past_date = datetime.fromisoformat(iso_date_string.replace('Z', ''))
      past_date = past_date.replace(tzinfo=timezone.utc)
    
    # Asegurar que tenga timezone
    if past_date.tzinfo is None:
      past_date = past_date.replace(tzinfo=timezone.utc)
    
    now = datetime.now(timezone.utc)
    diff = now - past_date
    
    # Convertir a segundos totales
    total_seconds = int(diff.total_seconds())
    
    if total_seconds < 0:
      return "En el futuro"
    elif total_seconds < 60:
      return f"hace {total_seconds} segundos"
    elif total_seconds < 3600:
      minutes = total_seconds // 60
      return f"hace {minutes} minuto{'s' if minutes != 1 else ''}"
    elif total_seconds < 86400:
      hours = total_seconds // 3600
      return f"hace {hours} hora{'s' if hours != 1 else ''}"
    elif total_seconds < 2592000:  # 30 días
      days = total_seconds // 86400
      return f"hace {days} día{'s' if days != 1 else ''}"
    elif total_seconds < 31536000:  # 365 días
      months = total_seconds // 2592000
      return f"hace {months} mes{'es' if months != 1 else ''}"
    else:
      years = total_seconds // 31536000
      return f"hace {years} año{'s' if years != 1 else ''}"
      
  except Exception as e:
    log.warning(f"Error parseando fecha: {e}")
    return "Fecha inválida"

def render(data_handler):
  """Renderiza página de análisis de sentimientos"""
  st.header("Análisis de Sentimientos")
  
  # Verificar estado de análisis
  analysis_active = st.session_state.get('analysis_active', False)
  
  st.markdown("---")

  if data_handler is None:
    st.error("Error crítico: Gestor de datos no disponible")
    log.error("render análisis: data_handler es None")
    return

  # Recargar datos frescos del archivo
  try:
    data_handler.reload_data()
  except Exception as e:
    log.warning(f"Error recargando datos: {e}")

  # Cargar regiones disponibles desde data_handler
  regions_data = data_handler.data.get("regions", []) 
  region_names_for_ui = [r.get("region_name") for r in regions_data if r.get("region_name")]
  
  if not region_names_for_ui:
    st.warning("No hay regiones con datos disponibles para analizar")
    return

  # Selector de región - deshabilitar si hay análisis activo
  selected_region_ui = st.selectbox(
    "Selecciona una región para analizar:",
    options=["Todas las regiones"] + region_names_for_ui,
    disabled=analysis_active
  )

  # Cargar analizador de sentimientos
  analyzer_instance = load_analyzer()
  if not analyzer_instance or not analyzer_instance.nlp:
    st.warning("Modelo de análisis de sentimientos no está listo")
    st.caption("Verifica la instalación del modelo multilingual si el problema persiste")
    return

  # Estadísticas antes de analizar - deshabilitar si hay análisis activo
  show_stats = st.checkbox(
    "Mostrar estadísticas actuales de análisis", 
    disabled=analysis_active
  )
  
  if show_stats:
    regions_to_stat = region_names_for_ui if selected_region_ui == "Todas las regiones" else [selected_region_ui]
    display_current_stats(data_handler, regions_to_stat)

  # Inicializar estados de sesión
  if 'analysis_active' not in st.session_state:
    st.session_state.analysis_active = False
  if 'should_stop_analysis' not in st.session_state:
    st.session_state.should_stop_analysis = False

  # Botones de control
  st.markdown("### Control de Análisis")
  col1, col2 = st.columns(2)
  
  with col1:
    if st.button("Iniciar Análisis", disabled=analysis_active, key="analyze_btn"):
      st.session_state.analysis_active = True
      st.session_state.should_stop_analysis = False
      log.info(f"Iniciando análisis para: {selected_region_ui}")
      st.rerun()
  
  with col2:
    if st.button("Detener Análisis", disabled=not analysis_active, key="stop_analysis_btn"):
      st.session_state.should_stop_analysis = True
      log.info("Solicitud de detención recibida")
      st.warning("Deteniendo análisis... Por favor espera")

  # Mostrar estado actual
  if analysis_active:
    if st.session_state.should_stop_analysis:
      st.warning("Deteniendo análisis... Por favor espera")
    else:
      current_region = st.session_state.get('current_analysis_region', selected_region_ui)
      st.info(f"Análisis activo para: **{current_region}**")
      
      # Guardar región actual en session_state
      st.session_state.current_analysis_region = selected_region_ui

  # Ejecutar análisis si está activo
  if analysis_active:
    st.markdown("### Progreso del Análisis")
    
    # Ejecutar función async usando asyncio.run
    asyncio.run(analyze_reviews_ui(data_handler, analyzer_instance, selected_region_ui))
    
    # Después de que termine el análisis forzar rerun para actualizar UI
    if not st.session_state.analysis_active:
      st.rerun()

def display_current_stats(data_handler, region_names_to_show: List[str]):
  """Muestra estadísticas de reseñas analizadas/no analizadas con fechas relativas"""
  
  # Recargar datos frescos antes de mostrar estadísticas
  try:
    data_handler.reload_data()
  except Exception as e:
    log.warning(f"Error recargando datos para estadísticas: {e}")
  
  stats = []
  all_regions_data = data_handler.data.get("regions", [])

  for region_data_item in all_regions_data:
    current_region_name_spanish = region_data_item.get("region_name")
    if not current_region_name_spanish or current_region_name_spanish not in region_names_to_show:
      continue
            
    analyzed_count = 0
    not_analyzed_count = 0
    for attraction_item in region_data_item.get("attractions", []):
      for review_item in attraction_item.get("reviews", []):
        if review_item.get("sentiment"):
          analyzed_count += 1
        else:
          not_analyzed_count += 1
    
    # Obtener fecha de último análisis y convertir a tiempo relativo
    last_analyzed_date = region_data_item.get("last_analyzed_date", "Nunca")
    relative_time = get_relative_time(last_analyzed_date)
        
    stats.append({
      "Región": current_region_name_spanish,
      "Reseñas analizadas": analyzed_count,
      "Reseñas pendientes": not_analyzed_count,
      "Último análisis": relative_time
    })
    
  if stats:
    # Configurar el dataframe con formato mejorado
    df = pd.DataFrame(stats)
    st.dataframe(
      df,
      use_container_width=True,
      hide_index=True
    )
  else:
    st.info("No hay datos de análisis disponibles para las regiones seleccionadas")

async def run_analysis_for_one_region(data_handler, analyzer, region_name_spanish: str, progress_callback=None, stop_event=None) -> Tuple[bool, int]:
  """Analiza una región específica y actualiza data_handler"""
  reviews_processed_count = 0
  try:
    # Verificar si se debe detener
    if stop_event and stop_event.is_set():
      log.info(f"Análisis detenido antes de procesar región '{region_name_spanish}'")
      return False, 0
    
    # Obtener datos de la región usando el nombre en español
    region_data_to_analyze = data_handler.get_region_data(region_name_spanish)

    if not region_data_to_analyze:
      log.error(f"Región '{region_name_spanish}' no encontrada")
      return False, 0

    pending_reviews_in_region = 0
    for attraction in region_data_to_analyze.get("attractions", []):
      for review in attraction.get("reviews", []):
        if not review.get("sentiment"):
          pending_reviews_in_region += 1

    # Crear callback que actualice el progreso de la UI y verifique detención
    def ui_progress_callback(progress_value, status_text):
      # Verificar si se debe detener
      if stop_event and stop_event.is_set():
        return False  # Señal para detener
      
      if progress_callback:
        progress_callback(progress_value, status_text)
      return True  # Continuar

    # analyzer.analyze_region_reviews con callback de progreso
    analyzed_region_data_dict = await analyzer.analyze_region_reviews(
      region_data_to_analyze, 
      progress_callback=ui_progress_callback,
    )

    # Verificar si se detuvo durante el análisis
    if stop_event and stop_event.is_set():
      log.info(f"Análisis detenido durante procesamiento de región '{region_name_spanish}'")
      return False, 0

    if not isinstance(analyzed_region_data_dict, dict) or "attractions" not in analyzed_region_data_dict:
      log.error(f"Resultado de análisis para '{region_name_spanish}' no es dict válido")
      return False, 0
    
    # Actualizar fecha de último análisis
    current_time = datetime.now(timezone.utc).isoformat()
    
    # Actualizar fecha de último análisis en la región
    data_handler.update_region_analysis_date(region_name_spanish, current_time)

    # Actualizar datos en data_handler
    data_handler.update_region_attractions(region_name_spanish, analyzed_region_data_dict["attractions"])
    await data_handler.save_data()
    
    reviews_processed_count = pending_reviews_in_region
    log.info(f"Región '{region_name_spanish}' analizada: {reviews_processed_count} reseñas")
    return True, reviews_processed_count

  except Exception as e:
    log.error(f"Error analizando '{region_name_spanish}': {e}")
    return False, reviews_processed_count

async def analyze_reviews_ui(data_handler, analyzer, selected_region_ui: str):
  """Maneja el proceso de análisis con feedback en UI y barra de progreso funcional"""
  
  # Crear evento de detención
  stop_event = asyncio.Event()
  
  try:
    regions_to_process_names_spanish = []
    if selected_region_ui == "Todas las regiones":
      regions_to_process_names_spanish = data_handler.get_regions_with_data()
    else:
      regions_to_process_names_spanish = [selected_region_ui]

    if not regions_to_process_names_spanish:
      st.warning("No hay regiones seleccionadas o válidas para analizar")
      return

    # Función para verificar detención en bucle
    async def check_stop_signal():
      while not stop_event.is_set():
        if st.session_state.get('should_stop_analysis', False):
          log.info("Detectada señal de detención desde UI")
          stop_event.set()
          break
        await asyncio.sleep(0.1)

    # Ejecutar verificación de detención en paralelo
    check_task = asyncio.create_task(check_stop_signal())

    # Contar total de reseñas pendientes
    total_reviews_to_analyze_overall = 0
    for region_name_iter_spanish in regions_to_process_names_spanish:
      region_data_iter = data_handler.get_region_data(region_name_iter_spanish)
      if region_data_iter:
        for attraction_iter in region_data_iter.get("attractions", []):
          for review_iter in attraction_iter.get("reviews", []):
            if not review_iter.get("sentiment"):
              total_reviews_to_analyze_overall += 1
        
    if total_reviews_to_analyze_overall == 0:
      st.info("No hay reseñas pendientes de análisis en la selección")
      display_current_stats(data_handler, regions_to_process_names_spanish)
      return

    # Crear elementos de progreso
    progress_bar = st.progress(0.0)
    status_text = st.empty()
    
    # Variables para tracking de progreso
    processed_reviews_count_overall = 0
    current_region_index = 0
    num_total_regions_to_process = len(regions_to_process_names_spanish)
    
    def update_overall_progress(region_progress, region_status):
      """Callback para actualizar progreso general"""
      nonlocal processed_reviews_count_overall, current_region_index
      
      # Calcular progreso general basado en regiones completadas + progreso de región actual
      region_weight = 1.0 / num_total_regions_to_process
      overall_progress = (current_region_index * region_weight) + (region_progress * region_weight)
      
      # Actualizar barra de progreso
      progress_bar.progress(min(1.0, overall_progress))
      
      # Actualizar texto de estado
      region_name = regions_to_process_names_spanish[current_region_index] if current_region_index < len(regions_to_process_names_spanish) else "Completado"
      status_text.text(
        f"Región {current_region_index + 1}/{num_total_regions_to_process}: {region_name}\n"
        f"{region_status}\n"
        f"Progreso general: {overall_progress:.1%}"
      )
    
    try:
      status_text.text(f"Preparando análisis... {total_reviews_to_analyze_overall} reseñas en total")
      
      for i, current_region_name_spanish in enumerate(regions_to_process_names_spanish):
        # Verificar si se debe detener
        if stop_event.is_set():
          log.info("Análisis detenido por solicitud del usuario")
          break
          
        current_region_index = i
        
        # Callback específico para esta región
        def region_progress_callback(progress, status):
          update_overall_progress(progress, status)
        
        # Llamar a la función de análisis para la región actual
        success, num_processed_in_call = await run_analysis_for_one_region(
          data_handler, 
          analyzer, 
          current_region_name_spanish,
          progress_callback=region_progress_callback,
          stop_event=stop_event
        )
                  
        if success:
          processed_reviews_count_overall += num_processed_in_call
                  
        if not success:
          if stop_event.is_set():
            break
          st.error(f"Error analizando '{current_region_name_spanish}' Continuando...")
      
      # Progreso final
      progress_bar.progress(1.0)
      
      # Mensaje final
      if stop_event.is_set():
        final_message = (
          f"Análisis DETENIDO por el usuario!\n"
          f"Regiones procesadas: {current_region_index}/{num_total_regions_to_process}\n"
          f"Total reseñas procesadas: {processed_reviews_count_overall}"
        )
        st.warning(final_message)
      else:
        final_message = f"Análisis completado! Total reseñas procesadas: {processed_reviews_count_overall}"
        if processed_reviews_count_overall == total_reviews_to_analyze_overall:
          st.success(final_message)
        else:
          st.warning(final_message + f" (Esperadas: {total_reviews_to_analyze_overall})")
                              
    except Exception as e:
      st.error(f"Error inesperado durante el proceso de análisis: {str(e)}")
      log.error(f"Error en analyze_reviews_ui: {e}")
    finally:
      # Cancelar tarea de verificación
      check_task.cancel()
      try:
        await check_task
      except asyncio.CancelledError:
        pass
    
  except Exception as e:
    st.error(f"Error crítico en análisis: {str(e)}")
    log.error(f"Error crítico en analyze_reviews_ui: {e}")
  finally:
    # Importante: resetear el estado al finalizar
    log.info("Sesión de análisis finalizada")
    st.session_state.analysis_active = False
    st.session_state.should_stop_analysis = False
    
    # Forzar recarga de datos después del análisis
    try:
      data_handler.reload_data()
    except Exception as e:
      log.warning(f"Error recargando datos después del análisis: {e}")
          
    # Mostrar estadísticas actualizadas
    regions_to_show = []
    if selected_region_ui == "Todas las regiones":
      regions_to_show = data_handler.get_regions_with_data()
    else:
      regions_to_show = [selected_region_ui]
    
    display_current_stats(data_handler, regions_to_show)