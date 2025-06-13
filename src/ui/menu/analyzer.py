# MÓDULO DE INTERFAZ PARA ANÁLISIS DE SENTIMIENTOS USANDO IA
# Implementa procesamiento asíncrono de reseñas con modelo multilingual
# Proporciona control de progreso, detención y estadísticas en tiempo real

from typing import List, Tuple
import streamlit as st 
import asyncio
from src.core.analyzer import load_analyzer
from loguru import logger as log
import pandas as pd
from datetime import datetime, timezone
import re

# ====================================================================================================================
#                                          OBTENER TIEMPO RELATIVO
# ====================================================================================================================

def get_relative_time(iso_date_string: str) -> str:
  # CONVIERTE FECHA ISO A FORMATO RELATIVO LEGIBLE PARA HUMANOS
  # Maneja múltiples formatos de fecha y calcula tiempo transcurrido
  # Retorna string descriptivo del tiempo relativo o mensaje de error
  if not iso_date_string or iso_date_string == "Nunca":
    return "Nunca"
  
  try:
    # parsear fecha ISO con manejo de zona horaria Z
    if iso_date_string.endswith('Z'):
      iso_date_string = iso_date_string[:-1] + '+00:00'
    
    # intentar parsear con diferentes formatos de fecha
    try:
      past_date = datetime.fromisoformat(iso_date_string)
    except ValueError:
      # fallback para formatos sin timezone explícito
      past_date = datetime.fromisoformat(iso_date_string.replace('Z', ''))
      past_date = past_date.replace(tzinfo=timezone.utc)
    
    # asegurar que fecha tenga información de timezone
    if past_date.tzinfo is None:
      past_date = past_date.replace(tzinfo=timezone.utc)
    
    # calcular diferencia con momento actual en UTC
    now = datetime.now(timezone.utc)
    diff = now - past_date
    
    # convertir a segundos totales para cálculo de unidades
    total_seconds = int(diff.total_seconds())
    
    # determinar unidad temporal apropiada para mostrar
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

# ====================================================================================================================
#                                            RENDERIZAR PÁGINA PRINCIPAL
# ====================================================================================================================

def render(data_handler):
  # RENDERIZA INTERFAZ PRINCIPAL PARA ANÁLISIS DE SENTIMIENTOS
  # Coordina validación de datos, carga de modelo y control de proceso
  # Maneja flujo completo desde selección de región hasta visualización de resultados
  st.header("🤖 Análisis de Sentimientos")
  
  # verificar estado de análisis activo desde session state
  analysis_active = st.session_state.get('analysis_active', False)
  
  st.markdown("---")

  # validación crítica de disponibilidad del data handler
  if data_handler is None:
    st.error("Error crítico: Gestor de datos no disponible")
    log.error("render análisis: data_handler es None")
    return

  # recargar datos frescos del archivo para garantizar estado actual
  try:
    data_handler.reload_data()
  except Exception as e:
    log.warning(f"Error recargando datos: {e}")

  # cargar regiones disponibles desde data handler validado
  regions_data = data_handler.data.get("regions", []) 
  region_names_for_ui = [r.get("region_name") for r in regions_data if r.get("region_name")]
  
  # validar que existen regiones con datos para analizar
  if not region_names_for_ui:
    st.warning("No hay regiones con datos disponibles para analizar")
    return

  # selector de región con deshabilitación durante análisis activo
  selected_region_ui = st.selectbox(
    "Selecciona una región para analizar:",
    options=["Todas las regiones"] + region_names_for_ui,
    disabled=analysis_active
  )

  # cargar instancia del analizador de sentimientos
  analyzer_instance = load_analyzer()
  if not analyzer_instance or not analyzer_instance.nlp:
    st.warning("Modelo de análisis de sentimientos no está listo")
    st.caption("Verifica la instalación del modelo multilingual si el problema persiste")
    return

  # checkbox para estadísticas con deshabilitación durante análisis
  show_stats = st.checkbox(
    "Mostrar estadísticas actuales de análisis", 
    disabled=analysis_active
  )
  
  # mostrar estadísticas según selección del usuario
  if show_stats:
    regions_to_stat = region_names_for_ui if selected_region_ui == "Todas las regiones" else [selected_region_ui]
    display_current_stats(data_handler, regions_to_stat)

  # inicializar estados de sesión para control de proceso
  if 'analysis_active' not in st.session_state:
    st.session_state.analysis_active = False
  if 'should_stop_analysis' not in st.session_state:
    st.session_state.should_stop_analysis = False

  # sección de botones de control con validación de estados
  st.markdown("### Control de Análisis")
  col1, col2 = st.columns(2)
  
  with col1:
    # botón de inicio con deshabilitación durante proceso activo
    if st.button("Iniciar Análisis", disabled=analysis_active, key="analyze_btn"):
      st.session_state.analysis_active = True
      st.session_state.should_stop_analysis = False
      log.info(f"Iniciando análisis para: {selected_region_ui}")
      st.rerun()
  
  with col2:
    # botón de detener solo disponible durante análisis activo
    if st.button("Detener Análisis", disabled=not analysis_active, key="stop_analysis_btn"):
      st.session_state.should_stop_analysis = True
      log.info("Solicitud de detención recibida")
      st.warning("Deteniendo análisis... Por favor espera")

  # mostrar estado actual del proceso en la interfaz
  if analysis_active:
    if st.session_state.should_stop_analysis:
      st.warning("Deteniendo análisis... Por favor espera")
    else:
      current_region = st.session_state.get('current_analysis_region', selected_region_ui)
      st.info(f"Análisis activo para: **{current_region}**")
      
      # persistir región actual en session state para continuidad
      st.session_state.current_analysis_region = selected_region_ui

  # ejecutar análisis si está marcado como activo
  if analysis_active:
    st.markdown("### Progreso del Análisis")
    
    # ejecutar función asíncrona usando asyncio en contexto síncrono
    asyncio.run(analyze_reviews_ui(data_handler, analyzer_instance, selected_region_ui))
    
    # forzar actualización de UI después de completar análisis
    if not st.session_state.analysis_active:
      st.rerun()

# ====================================================================================================================
#                                        MOSTRAR ESTADÍSTICAS ACTUALES
# ====================================================================================================================

def display_current_stats(data_handler, region_names_to_show: List[str]):
  # MUESTRA ESTADÍSTICAS DE RESEÑAS ANALIZADAS Y PENDIENTES
  # Presenta tabla con conteos por región y fechas de último análisis
  # Incluye información de tiempo relativo para contexto temporal
  
  # recargar datos frescos antes de mostrar estadísticas
  try:
    data_handler.reload_data()
  except Exception as e:
    log.warning(f"Error recargando datos para estadísticas: {e}")
  
  stats = []
  all_regions_data = data_handler.data.get("regions", [])

  # procesar cada región para construir estadísticas
  for region_data_item in all_regions_data:
    current_region_name_spanish = region_data_item.get("region_name")
    if not current_region_name_spanish or current_region_name_spanish not in region_names_to_show:
      continue
            
    # contar reseñas analizadas vs pendientes por región
    analyzed_count = 0
    not_analyzed_count = 0
    for attraction_item in region_data_item.get("attractions", []):
      for review_item in attraction_item.get("reviews", []):
        if review_item.get("sentiment"):
          analyzed_count += 1
        else:
          not_analyzed_count += 1
    
    # obtener fecha de último análisis y convertir a formato relativo
    last_analyzed_date = region_data_item.get("last_analyzed_date", "Nunca")
    relative_time = get_relative_time(last_analyzed_date)
        
    # agregar fila completa con estadísticas procesadas
    stats.append({
      "Región": current_region_name_spanish,
      "Reseñas analizadas": analyzed_count,
      "Reseñas pendientes": not_analyzed_count,
      "Último análisis": relative_time
    })
    
  # renderizar tabla con datos o mensaje informativo
  if stats:
    # configurar dataframe con formato mejorado para visualización
    df = pd.DataFrame(stats)
    st.dataframe(
      df,
      use_container_width=True,
      hide_index=True
    )
  else:
    st.info("No hay datos de análisis disponibles para las regiones seleccionadas")

# ====================================================================================================================
#                                    EJECUTAR ANÁLISIS PARA UNA REGIÓN
# ====================================================================================================================

async def run_analysis_for_one_region(data_handler, analyzer, region_name_spanish: str, progress_callback=None, stop_event=None) -> Tuple[bool, int]:
  # ANALIZA UNA REGIÓN ESPECÍFICA Y ACTUALIZA DATA HANDLER
  # Procesa reseñas pendientes usando analizador de sentimientos
  # Retorna tupla con éxito del proceso y número de reseñas procesadas
  reviews_processed_count = 0
  try:
    # verificar si se debe detener antes de iniciar
    if stop_event and stop_event.is_set():
      log.info(f"Análisis detenido antes de procesar región '{region_name_spanish}'")
      return False, 0
    
    # obtener datos de la región usando el nombre en español
    region_data_to_analyze = data_handler.get_region_data(region_name_spanish)

    # validar que la región existe y tiene datos
    if not region_data_to_analyze:
      log.error(f"Región '{region_name_spanish}' no encontrada")
      return False, 0

    # contar reseñas pendientes de análisis en región
    pending_reviews_in_region = 0
    for attraction in region_data_to_analyze.get("attractions", []):
      for review in attraction.get("reviews", []):
        if not review.get("sentiment"):
          pending_reviews_in_region += 1

    # crear callback que actualice progreso de UI y verifique detención
    def ui_progress_callback(progress_value, status_text):
      # verificar si se debe detener durante procesamiento
      if stop_event and stop_event.is_set():
        return False  # señal para detener
      
      # actualizar callback externo si está disponible
      if progress_callback:
        progress_callback(progress_value, status_text)
      return True  # continuar procesamiento

    # ejecutar análisis de región con callback de progreso
    analyzed_region_data_dict = await analyzer.analyze_region_reviews(
      region_data_to_analyze, 
      progress_callback=ui_progress_callback,
    )

    # verificar si se detuvo durante el análisis
    if stop_event and stop_event.is_set():
      log.info(f"Análisis detenido durante procesamiento de región '{region_name_spanish}'")
      return False, 0

    # validar estructura de datos resultado del análisis
    if not isinstance(analyzed_region_data_dict, dict) or "attractions" not in analyzed_region_data_dict:
      log.error(f"Resultado de análisis para '{region_name_spanish}' no es dict válido")
      return False, 0
    
    # actualizar fecha de último análisis con timestamp actual
    current_time = datetime.now(timezone.utc).isoformat()
    
    # persistir fecha de último análisis en la región
    data_handler.update_region_analysis_date(region_name_spanish, current_time)

    # actualizar datos analizados en data handler
    data_handler.update_region_attractions(region_name_spanish, analyzed_region_data_dict["attractions"])
    await data_handler.save_data()
    
    # contabilizar reseñas procesadas para estadísticas
    reviews_processed_count = pending_reviews_in_region
    log.info(f"Región '{region_name_spanish}' analizada: {reviews_processed_count} reseñas")
    return True, reviews_processed_count

  except Exception as e:
    log.error(f"Error analizando '{region_name_spanish}': {e}")
    return False, reviews_processed_count

# ====================================================================================================================
#                                      MANEJAR ANÁLISIS CON INTERFAZ DE USUARIO
# ====================================================================================================================

async def analyze_reviews_ui(data_handler, analyzer, selected_region_ui: str):
  # MANEJA PROCESO DE ANÁLISIS CON FEEDBACK EN UI Y BARRA DE PROGRESO
  # Coordina análisis múltiple de regiones con control de detención
  # Proporciona actualizaciones en tiempo real y cleanup de estado
  
  # crear evento de detención para control asíncrono
  stop_event = asyncio.Event()
  
  try:
    # determinar regiones a procesar según selección
    regions_to_process_names_spanish = []
    if selected_region_ui == "Todas las regiones":
      regions_to_process_names_spanish = data_handler.get_regions_with_data()
    else:
      regions_to_process_names_spanish = [selected_region_ui]

    # validar que hay regiones válidas para procesar
    if not regions_to_process_names_spanish:
      st.warning("No hay regiones seleccionadas o válidas para analizar")
      return

    # función para verificar señal de detención en bucle asíncrono
    async def check_stop_signal():
      while not stop_event.is_set():
        if st.session_state.get('should_stop_analysis', False):
          log.info("Detectada señal de detención desde UI")
          stop_event.set()
          break
        await asyncio.sleep(0.1)

    # ejecutar verificación de detención en paralelo
    check_task = asyncio.create_task(check_stop_signal())

    # contar total de reseñas pendientes para barra de progreso
    total_reviews_to_analyze_overall = 0
    for region_name_iter_spanish in regions_to_process_names_spanish:
      region_data_iter = data_handler.get_region_data(region_name_iter_spanish)
      if region_data_iter:
        for attraction_iter in region_data_iter.get("attractions", []):
          for review_iter in attraction_iter.get("reviews", []):
            if not review_iter.get("sentiment"):
              total_reviews_to_analyze_overall += 1
        
    # validar que hay reseñas pendientes de análisis
    if total_reviews_to_analyze_overall == 0:
      st.info("No hay reseñas pendientes de análisis en la selección")
      display_current_stats(data_handler, regions_to_process_names_spanish)
      return

    # crear elementos de progreso visual para UI
    progress_bar = st.progress(0.0)
    status_text = st.empty()
    
    # variables para tracking de progreso durante análisis
    processed_reviews_count_overall = 0
    current_region_index = 0
    num_total_regions_to_process = len(regions_to_process_names_spanish)
    
    # función callback para actualizar progreso general
    def update_overall_progress(region_progress, region_status):
      # ACTUALIZA PROGRESO GENERAL BASADO EN REGIONES COMPLETADAS
      nonlocal processed_reviews_count_overall, current_region_index
      
      # calcular progreso general basado en regiones completadas más progreso actual
      region_weight = 1.0 / num_total_regions_to_process
      overall_progress = (current_region_index * region_weight) + (region_progress * region_weight)
      
      # actualizar barra de progreso visual
      progress_bar.progress(min(1.0, overall_progress))
      
      # actualizar texto de estado con información detallada
      region_name = regions_to_process_names_spanish[current_region_index] if current_region_index < len(regions_to_process_names_spanish) else "Completado"
      status_text.text(
        f"Región {current_region_index + 1}/{num_total_regions_to_process}: {region_name}\n"
        f"{region_status}\n"
        f"Progreso general: {overall_progress:.1%}"
      )
    
    try:
      # mostrar estado inicial del proceso
      status_text.text(f"Preparando análisis... {total_reviews_to_analyze_overall} reseñas en total")
      
      # procesar cada región secuencialmente con tracking
      for i, current_region_name_spanish in enumerate(regions_to_process_names_spanish):
        # verificar si se debe detener antes de cada región
        if stop_event.is_set():
          log.info("Análisis detenido por solicitud del usuario")
          break
          
        current_region_index = i
        
        # crear callback específico para esta región
        def region_progress_callback(progress, status):
          update_overall_progress(progress, status)
        
        # llamar función de análisis para región actual
        success, num_processed_in_call = await run_analysis_for_one_region(
          data_handler, 
          analyzer, 
          current_region_name_spanish,
          progress_callback=region_progress_callback,
          stop_event=stop_event
        )
                  
        # acumular reseñas procesadas si fue exitoso
        if success:
          processed_reviews_count_overall += num_processed_in_call
                  
        # manejar errores sin detener proceso completo
        if not success:
          if stop_event.is_set():
            break
          st.error(f"Error analizando '{current_region_name_spanish}' Continuando...")
      
      # completar barra de progreso al finalizar
      progress_bar.progress(1.0)
      
      # mostrar mensaje final según estado de completitud
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
      # cancelar tarea de verificación de detención
      check_task.cancel()
      try:
        await check_task
      except asyncio.CancelledError:
        pass
    
  except Exception as e:
    st.error(f"Error crítico en análisis: {str(e)}")
    log.error(f"Error crítico en analyze_reviews_ui: {e}")
  finally:
    # cleanup crítico de estado al finalizar proceso
    log.info("Sesión de análisis finalizada")
    st.session_state.analysis_active = False
    st.session_state.should_stop_analysis = False
    
    # forzar recarga de datos después del análisis
    try:
      data_handler.reload_data()
    except Exception as e:
      log.warning(f"Error recargando datos después del análisis: {e}")
          
    # mostrar estadísticas actualizadas post-análisis
    regions_to_show = []
    if selected_region_ui == "Todas las regiones":
      regions_to_show = data_handler.get_regions_with_data()
    else:
      regions_to_show = [selected_region_ui]
    
    display_current_stats(data_handler, regions_to_show)