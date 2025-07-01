from typing import List, Tuple
import streamlit as st 
import asyncio
from src.core.analyzer import load_analyzer
from loguru import logger as log
import pandas as pd
from datetime import datetime, timezone

# Idiomas disponibles en el sistema con sus nombres en español
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

# ================================================================
# OBTENER TIEMPO RELATIVO
# ================================================================

def get_relative_time(iso_date_string: str) -> str:
  # CONVIERTE FECHAS ISO A TEXTO LEGIBLE DE TIEMPO TRANSCURRIDO
  if not iso_date_string or iso_date_string == "Nunca":
    return "Nunca"
  
  try:
    # Normalizar formato de fecha ISO
    if iso_date_string.endswith('Z'):
      iso_date_string = iso_date_string[:-1] + '+00:00'
    
    # Parsear fecha con fallback para diferentes formatos
    try:
      past_date = datetime.fromisoformat(iso_date_string)
    except ValueError:
      past_date = datetime.fromisoformat(iso_date_string.replace('Z', ''))
      past_date = past_date.replace(tzinfo=timezone.utc)
    
    # Garantizar que tenga timezone
    if past_date.tzinfo is None:
      past_date = past_date.replace(tzinfo=timezone.utc)
    
    now = datetime.now(timezone.utc)
    diff = now - past_date
    total_seconds = int(diff.total_seconds())
    
    # Convertir segundos a texto legible
    if total_seconds < 0:
      return "En el futuro"
    elif total_seconds < 60:
      return f"Hace {total_seconds} segundos"
    elif total_seconds < 3600:
      minutes = total_seconds // 60
      return f"Hace {minutes} minuto{'s' if minutes != 1 else ''}"
    elif total_seconds < 86400:
      hours = total_seconds // 3600
      return f"Hace {hours} hora{'s' if hours != 1 else ''}"
    elif total_seconds < 2592000:  # 30 días
      days = total_seconds // 86400
      return f"Hace {days} día{'s' if days != 1 else ''}"
    elif total_seconds < 31536000:  # 365 días
      months = total_seconds // 2592000
      return f"Hace {months} mes{'es' if months != 1 else ''}"
    else:
      years = total_seconds // 31536000
      return f"Hace {years} año{'s' if years != 1 else ''}"
      
  except Exception as e:
    log.warning(f"Error parseando fecha: {e}")
    return "Fecha inválida"

# ================================================================
# OBTENER IDIOMAS DISPONIBLES
# ================================================================

def get_available_languages_from_data(data_handler) -> List[str]:
  # EXTRAE DINÁMICAMENTE LOS IDIOMAS DISPONIBLES EN LOS DATOS
  languages_found = set()
  languages_found.add("all")  # Opción para mostrar todos los idiomas
  
  # Recorrer regiones y atracciones para encontrar idiomas
  for region in data_handler.data.get("regions", []):
    for attraction in region.get("attractions", []):
      for lang_code in attraction.get("languages", {}).keys():
        languages_found.add(lang_code)
  
  # Filtrar solo idiomas conocidos y ordenar
  available_languages = ["all"]
  for lang_code in sorted(languages_found):
    if lang_code != "all" and lang_code in AVAILABLE_LANGUAGES:
      available_languages.append(lang_code)
  
  return available_languages

# ================================================================
# RENDERIZAR PÁGINA PRINCIPAL
# ================================================================

def render(data_handler):
  # RENDERIZA LA INTERFAZ PRINCIPAL DEL ANÁLISIS DE SENTIMIENTOS
  st.header("Análisis de Sentimientos")
  
  # Estado del análisis en curso
  analysis_active = st.session_state.get('analysis_active', False)
  
  st.markdown("---")

  # Validar que el gestor de datos esté disponible
  if data_handler is None:
    st.error("Error crítico: Gestor de datos no disponible")
    log.error("render análisis: data_handler es None")
    return

  # Recargar datos actuales del archivo
  try:
    data_handler.reload_data()
  except Exception as e:
    log.warning(f"Error recargando datos: {e}")

  # Obtener regiones disponibles para análisis
  regions_data = data_handler.data.get("regions", []) 
  region_names_for_ui = [r.get("region_name") for r in regions_data if r.get("region_name")]
  
  if not region_names_for_ui:
    st.warning("No hay regiones con datos disponibles para analizar")
    return

  # Selector de región (deshabilitado durante análisis)
  selected_region_ui = st.selectbox(
    "Selecciona una región para analizar:",
    options=["Todas las regiones"] + region_names_for_ui,
    disabled=analysis_active
  )

  # Verificar que el analizador de sentimientos esté listo
  analyzer_instance = load_analyzer()
  if not analyzer_instance or not analyzer_instance.nlp:
    st.warning("Modelo de análisis de sentimientos no está listo")
    st.caption("Verifica la instalación del modelo multilingual si el problema persiste")
    return

  # Opción para mostrar estadísticas actuales
  show_stats = st.checkbox(
    "Mostrar estadísticas actuales de análisis", 
    disabled=analysis_active
  )
  
  if show_stats:
    regions_to_stat = region_names_for_ui if selected_region_ui == "Todas las regiones" else [selected_region_ui]
    display_current_stats(data_handler, regions_to_stat)

  # Inicializar estados de sesión si no existen
  if 'analysis_active' not in st.session_state:
    st.session_state.analysis_active = False
  if 'should_stop_analysis' not in st.session_state:
    st.session_state.should_stop_analysis = False

  # Botones de control del análisis
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

  # Mostrar estado actual del análisis
  if analysis_active:
    if st.session_state.should_stop_analysis:
      st.warning("Deteniendo análisis... Por favor espera")
    else:
      current_region = st.session_state.get('current_analysis_region', selected_region_ui)
      st.info(f"Análisis activo para: **{current_region}**")
      
      # Guardar región actual para seguimiento
      st.session_state.current_analysis_region = selected_region_ui

  # Ejecutar análisis si está activo
  if analysis_active:
    st.markdown("### Progreso del Análisis")
    
    # Ejecutar función asíncrona de análisis
    asyncio.run(analyze_reviews_ui(data_handler, analyzer_instance, selected_region_ui))
    
    # Actualizar UI cuando termine el análisis
    if not st.session_state.analysis_active:
      st.rerun()

# ================================================================
# MOSTRAR ESTADÍSTICAS ACTUALES
# ================================================================

def display_current_stats(data_handler, region_names_to_show: List[str]):
  # MUESTRA ESTADÍSTICAS DETALLADAS DE RESEÑAS ANALIZADAS Y PENDIENTES
  
  # Obtener idiomas disponibles en los datos
  available_languages = get_available_languages_from_data(data_handler)
  
  # Configurar selector de idioma si hay múltiples opciones
  if not available_languages or len(available_languages) <= 1:
    display_language = "all"
    st.markdown("#### Estadísticas Generales")
    st.info("Mostrando estadísticas de todos los idiomas (no hay idiomas específicos disponibles)")
  else:
    st.markdown("#### Estadísticas por Idioma")
    
    # Crear opciones legibles para el selector
    language_options = []
    for lang_code in available_languages:
      if lang_code in AVAILABLE_LANGUAGES:
        language_options.append(AVAILABLE_LANGUAGES[lang_code])
      else:
        language_options.append(lang_code.title())
    
    # Determinar contexto para clave única
    analysis_active = st.session_state.get('analysis_active', False)
    context_suffix = "during_analysis" if analysis_active else "manual_view"
    
    selected_language_display = st.selectbox(
      "Seleccionar idioma:",
      options=language_options,
      key=f"stats_language_display_selector_{context_suffix}"
    )
    
    # Convertir nombre legible de vuelta a código de idioma
    for lang_code, lang_display in AVAILABLE_LANGUAGES.items():
      if lang_display == selected_language_display:
        display_language = lang_code
        break

  # Recargar datos antes de calcular estadísticas
  try:
    data_handler.reload_data()
  except Exception as e:
    log.warning(f"Error recargando datos para estadísticas: {e}")
  
  # Procesar estadísticas por región
  stats = []
  all_regions_data = data_handler.data.get("regions", [])

  for region_data_item in all_regions_data:
    current_region_name_spanish = region_data_item.get("region_name")
    if not current_region_name_spanish or current_region_name_spanish not in region_names_to_show:
      continue
    
    analyzed_count = 0
    not_analyzed_count = 0
    language_breakdown = {}
    
    # Procesar cada atracción en la región
    for attraction_item in region_data_item.get("attractions", []):
      if display_language == "all":
        # Procesar estructura antigua (compatibilidad)
        old_reviews = attraction_item.get("reviews", [])
        for review_item in old_reviews:
          if review_item.get("sentiment"):
            analyzed_count += 1
          else:
            not_analyzed_count += 1
        
        # Procesar estructura multilenguaje
        languages_data = attraction_item.get("languages", {})
        for lang, lang_data in languages_data.items():
          for review_item in lang_data.get("reviews", []):
            lang_key = f"{lang}_analyzed" if review_item.get("sentiment") else f"{lang}_pending"
            language_breakdown[lang_key] = language_breakdown.get(lang_key, 0) + 1
            
            if review_item.get("sentiment"):
              analyzed_count += 1
            else:
              not_analyzed_count += 1
      else:
        # Procesar solo un idioma específico
        language_data = attraction_item.get("languages", {}).get(display_language, {})
        for review_item in language_data.get("reviews", []):
          if review_item.get("sentiment"):
            analyzed_count += 1
          else:
            not_analyzed_count += 1
    
    # Obtener fecha del último análisis
    last_analyzed_date = region_data_item.get("last_analyzed_date", "Nunca")
    relative_time = get_relative_time(last_analyzed_date)
    
    # Generar información de desglose por idiomas
    idiomas_info = ""
    if display_language == "all" and language_breakdown:
      # Agrupar estadísticas por idioma
      lang_summary = {}
      for key, count in language_breakdown.items():
        lang = key.replace("_analyzed", "").replace("_pending", "")
        if lang not in lang_summary:
          lang_summary[lang] = {"analyzed": 0, "pending": 0}
        
        if "_analyzed" in key:
          lang_summary[lang]["analyzed"] = count
        else:
          lang_summary[lang]["pending"] = count
      
      # Crear texto de resumen por idioma
      idiomas_list = []
      for lang, counts in sorted(lang_summary.items()):
        total = counts["analyzed"] + counts["pending"]
        coverage = (counts["analyzed"] / total * 100) if total > 0 else 0
        idiomas_list.append(f"{lang}: {coverage:.0f}% ({counts['analyzed']}/{total})")
      
      idiomas_info = " | ".join(idiomas_list[:3])
      if len(lang_summary) > 3:
        idiomas_info += f" (+{len(lang_summary)-3} más)"
    
    # Agregar estadísticas de la región
    stats.append({
      "Región": current_region_name_spanish,
      "Analizadas": analyzed_count,
      "Pendientes": not_analyzed_count,
      "Total": analyzed_count + not_analyzed_count,
      "Cobertura": f"{(analyzed_count / (analyzed_count + not_analyzed_count) * 100):.1f}%" if (analyzed_count + not_analyzed_count) > 0 else "0%",
      "Último análisis": relative_time,
      "Idiomas": idiomas_info if display_language == "all" else f"{analyzed_count}/{analyzed_count + not_analyzed_count} en {display_language}"
    })
    
  # Mostrar tabla de estadísticas si hay datos
  if stats:
    df = pd.DataFrame(stats)
    
    # Configuración de columnas para la tabla
    column_config = {
      "Región": st.column_config.TextColumn("Región", width="medium"),
      "Analizadas": st.column_config.NumberColumn("Analizadas", width="small"),
      "Pendientes": st.column_config.NumberColumn("Pendientes", width="small"),
      "Total": st.column_config.NumberColumn("Total", width="small"),
      "Cobertura": st.column_config.TextColumn("Cobertura", width="small"),
      "Último análisis": st.column_config.TextColumn("Último análisis", width="medium"),
    }
    
    # Configurar columna de idiomas según el contexto
    if display_language == "all":
      column_config["Idiomas"] = st.column_config.TextColumn(
        "Desglose por Idioma", 
        width="large",
        help="Cobertura de análisis por idioma"
      )
    else:
      column_config["Idiomas"] = st.column_config.TextColumn(
        f"Progreso {display_language}", 
        width="medium",
        help=f"Progreso de análisis en {display_language}"
      )
    
    st.dataframe(
      df,
      use_container_width=True,
      hide_index=True,
      column_config=column_config
    )
    
    # Métricas resumen en columnas
    col1, col2, col3, col4 = st.columns(4)
    total_analyzed = sum(item["Analizadas"] for item in stats)
    total_pending = sum(item["Pendientes"] for item in stats)
    total_reviews = total_analyzed + total_pending
    overall_coverage = (total_analyzed / total_reviews * 100) if total_reviews > 0 else 0
    
    col1.metric("Total Reseñas", f"{total_reviews:,}")
    col2.metric("Analizadas", f"{total_analyzed:,}")
    col3.metric("Pendientes", f"{total_pending:,}")
    col4.metric("Cobertura Global", f"{overall_coverage:.1f}%")
    
    # Barra de progreso visual
    if total_reviews > 0:
      st.progress(overall_coverage / 100)
      if display_language == "all":
        st.caption(f"Progreso general: {overall_coverage:.1f}% de todas las reseñas analizadas")
      else:
        st.caption(f"Progreso en {display_language}: {overall_coverage:.1f}% de reseñas analizadas")
    
  else:
    st.info("No hay datos de análisis disponibles para las regiones seleccionadas")

# ================================================================
# EJECUTAR ANÁLISIS DE UNA REGIÓN
# ================================================================

async def run_analysis_for_one_region(data_handler, analyzer, region_name_spanish: str, progress_callback=None, stop_event=None) -> Tuple[bool, int]:
  # PROCESA TODAS LAS RESEÑAS PENDIENTES DE UNA REGIÓN ESPECÍFICA
  reviews_processed_count = 0
  try:
    # Verificar si se solicitó detener el análisis
    if stop_event and stop_event.is_set():
      log.info(f"Análisis detenido antes de procesar región '{region_name_spanish}'")
      return False, 0
    
    # Obtener datos de la región
    region_data_to_analyze = data_handler.get_region_data(region_name_spanish)

    if not region_data_to_analyze:
      log.error(f"Región '{region_name_spanish}' no encontrada")
      return False, 0

    # Contar reseñas pendientes de análisis
    pending_reviews_in_region = 0
    for attraction in region_data_to_analyze.get("attractions", []):
      # Compatibilidad con estructura antigua
      old_reviews = attraction.get("reviews", [])
      for review in old_reviews:
        if not review.get("sentiment"):
          pending_reviews_in_region += 1
      
      # Estructura multilenguaje
      languages_data = attraction.get("languages", {})
      for lang, lang_data in languages_data.items():
        for review in lang_data.get("reviews", []):
          if not review.get("sentiment"):
            pending_reviews_in_region += 1

    if pending_reviews_in_region == 0:
      log.info(f"Región '{region_name_spanish}' no tiene reseñas pendientes")
      return True, 0

    # Callback para actualizar progreso y verificar detención
    def ui_progress_callback(progress_value, status_text):
      if stop_event and stop_event.is_set():
        return False  # Señal para detener
      
      if progress_callback:
        progress_callback(progress_value, status_text)
      return True  # Continuar procesando

    # Ejecutar análisis de la región
    analyzed_region_data_dict = await analyzer.analyze_region_all_languages_ui(
      region_data_to_analyze, 
      progress_callback=ui_progress_callback
    )

    # Verificar si se detuvo durante el procesamiento
    if stop_event and stop_event.is_set():
      log.info(f"Análisis detenido durante procesamiento de región '{region_name_spanish}'")
      return False, 0

    if not isinstance(analyzed_region_data_dict, dict) or "attractions" not in analyzed_region_data_dict:
      log.error(f"Resultado de análisis para '{region_name_spanish}' no es dict válido")
      return False, 0
    
    # Actualizar fecha de último análisis
    current_time = datetime.now(timezone.utc).isoformat()
    data_handler.update_region_analysis_date(region_name_spanish, current_time)

    # Guardar datos actualizados
    data_handler.update_region_attractions(region_name_spanish, analyzed_region_data_dict["attractions"])
    await data_handler.save_data()
    
    reviews_processed_count = pending_reviews_in_region
    log.info(f"Región '{region_name_spanish}' analizada: {reviews_processed_count} reseñas")
    return True, reviews_processed_count

  except Exception as e:
    log.error(f"Error analizando '{region_name_spanish}': {e}")
    return False, reviews_processed_count

# ================================================================
# MANEJAR ANÁLISIS CON INTERFAZ
# ================================================================

async def analyze_reviews_ui(data_handler, analyzer, selected_region_ui: str):
  # COORDINA EL PROCESO COMPLETO DE ANÁLISIS CON FEEDBACK VISUAL
  
  # Evento para coordinar la detención del análisis
  stop_event = asyncio.Event()
  
  try:
    # Determinar regiones a procesar
    regions_to_process_names_spanish = []
    if selected_region_ui == "Todas las regiones":
      regions_to_process_names_spanish = data_handler.get_regions_with_data()
    else:
      regions_to_process_names_spanish = [selected_region_ui]

    if not regions_to_process_names_spanish:
      st.warning("No hay regiones seleccionadas o válidas para analizar")
      return

    # Tarea para verificar señales de detención
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
          # Estructura antigua
          old_reviews = attraction_iter.get("reviews", [])
          for review_iter in old_reviews:
            if not review_iter.get("sentiment"):
              total_reviews_to_analyze_overall += 1
          
          # Estructura multilenguaje
          languages_data = attraction_iter.get("languages", {})
          for lang, lang_data in languages_data.items():
            for review_iter in lang_data.get("reviews", []):
              if not review_iter.get("sentiment"):
                total_reviews_to_analyze_overall += 1
        
    if total_reviews_to_analyze_overall == 0:
      st.info("No hay reseñas pendientes de análisis en la selección")
      display_current_stats(data_handler, regions_to_process_names_spanish)
      return

    # Elementos de progreso en la UI
    progress_bar = st.progress(0.0)
    status_text = st.empty()
    
    # Variables para seguimiento del progreso
    processed_reviews_count_overall = 0
    current_region_index = 0
    num_total_regions_to_process = len(regions_to_process_names_spanish)
    
    # Función para actualizar el progreso general
    def update_overall_progress(region_progress, region_status):
      nonlocal processed_reviews_count_overall, current_region_index
      
      # Calcular progreso basado en regiones completadas más progreso actual
      region_weight = 1.0 / num_total_regions_to_process
      overall_progress = (current_region_index * region_weight) + (region_progress * region_weight)
      
      # Actualizar elementos de UI
      progress_bar.progress(min(1.0, overall_progress))
      
      region_name = regions_to_process_names_spanish[current_region_index] if current_region_index < len(regions_to_process_names_spanish) else "Completado"
      status_text.text(
        f"Región {current_region_index + 1}/{num_total_regions_to_process}: {region_name}\n"
        f"{region_status}\n"
        f"Progreso general: {overall_progress:.1%}\n"
        f"Reseñas pendientes (todos los idiomas): {total_reviews_to_analyze_overall}"
      )
    
    try:
      status_text.text(f"Preparando análisis... {total_reviews_to_analyze_overall} reseñas en total (todos los idiomas)")
      
      # Procesar cada región
      for i, current_region_name_spanish in enumerate(regions_to_process_names_spanish):
        # Verificar señal de detención
        if stop_event.is_set():
          log.info("Análisis detenido por solicitud del usuario")
          break
          
        current_region_index = i
        
        # Callback específico para esta región
        def region_progress_callback(progress, status):
          update_overall_progress(progress, status)
        
        # Procesar región actual
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
      
      # Completar barra de progreso
      progress_bar.progress(1.0)
      
      # Mensaje final según el resultado
      if stop_event.is_set():
        final_message = (
          f"Análisis DETENIDO por el usuario\n"
          f"Regiones procesadas: {current_region_index}/{num_total_regions_to_process}\n"
          f"Total reseñas procesadas (todos los idiomas): {processed_reviews_count_overall}"
        )
        st.warning(final_message)
      else:
        final_message = f"Análisis completado - Total reseñas procesadas (todos los idiomas): {processed_reviews_count_overall}"
        if processed_reviews_count_overall == total_reviews_to_analyze_overall:
          st.success(final_message)
        else:
          st.warning(final_message + f" (Esperadas: {total_reviews_to_analyze_overall})")
                              
    except Exception as e:
      st.error(f"Error inesperado durante el proceso de análisis: {str(e)}")
      log.error(f"Error en analyze_reviews_ui: {e}")
    finally:
      # Limpiar tarea de verificación
      check_task.cancel()
      try:
        await check_task
      except asyncio.CancelledError:
        pass
    
  except Exception as e:
    st.error(f"Error crítico en análisis: {str(e)}")
    log.error(f"Error crítico en analyze_reviews_ui: {e}")
  finally:
    # Resetear estados de sesión
    log.info("Sesión de análisis finalizada")
    st.session_state.analysis_active = False
    st.session_state.should_stop_analysis = False
    
    # Recargar datos actualizados
    try:
      data_handler.reload_data()
    except Exception as e:
      log.warning(f"Error recargando datos después del análisis: {e}")
          
    # Mostrar estadísticas finales
    regions_to_show = []
    if selected_region_ui == "Todas las regiones":
      regions_to_show = data_handler.get_regions_with_data()
    else:
      regions_to_show = [selected_region_ui]
    
    display_current_stats(data_handler, regions_to_show)
