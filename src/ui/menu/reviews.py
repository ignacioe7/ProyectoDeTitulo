from loguru import logger as log
import streamlit as st
import asyncio
import time
import pandas as pd
from datetime import datetime, timezone
from src.core.scraper import ReviewScraper
from src.utils.constants import CONSOLIDATED_DATA_PATH

# ===============================================================
# RENDERIZAR PÁGINA PRINCIPAL
# ===============================================================

def render(data_handler):
  # RENDERIZA LA PÁGINA DE SCRAPING DE RESEÑAS
  # Maneja la interfaz principal para extraer reseñas multilenguaje
  # Incluye controles, progreso y tabla de estado de regiones
  st.header("Scraping de Reseñas")
  
  # Verificar estado de scraping
  scraping_active = st.session_state.get('scraping_active', False)
  
  st.markdown("---")

  # Usar solo regiones que ya tienen datos scrapeados
  if not hasattr(data_handler, 'data') or not data_handler.data or not data_handler.data.get("regions"):
    st.warning("No se encontraron regiones con atracciones scrapeadas")
    st.info("Primero debes scrapear atracciones en la sección 'Atracciones'")
    return

  # Obtener solo regiones que ya tienen datos en consolidated_data.json
  scraped_regions = data_handler.data.get("regions", [])
  if not scraped_regions:
    st.warning("No hay regiones con atracciones para scrapear reseñas")
    st.info("Ve a la sección 'Atracciones' para scrapear algunas primero")
    return

  # Extraer nombres de regiones que tienen atracciones
  region_names_ui = []
  regions_with_attractions = {}
  
  for region in scraped_regions:
    region_name = region.get("region_name")
    attractions = region.get("attractions", [])
    
    if region_name and attractions:
      region_names_ui.append(region_name)
      regions_with_attractions[region_name] = len(attractions)

  if not region_names_ui:
    st.warning("Las regiones scrapeadas no tienen atracciones válidas")
    return

  region_names_ui.sort()

  # Widgets de UI - deshabilitar si hay scraping activo
  selected_region_name_ui = st.selectbox(
    "Selecciona una Región (solo con atracciones scrapeadas):",
    options=[""] + region_names_ui,
    format_func=lambda x: "Selecciona una opción..." if x == "" else f"{x} ({regions_with_attractions.get(x, 0)} atracciones)",
    key="reviews_region_selectbox",
    disabled=scraping_active
  )

  # Mostrar info de la región seleccionada
  if selected_region_name_ui:
    attraction_count = regions_with_attractions.get(selected_region_name_ui, 0)
    st.info(f"Región seleccionada: **{selected_region_name_ui}** con **{attraction_count}** atracciones")

  # Control de concurrencia
  st.markdown("### Configuración de Scraping")
  
  col1, col2 = st.columns(2)
  
  with col1:
    max_concurrency = st.slider(
      "Concurrencia máxima:",
      min_value=1,
      max_value=3,
      value=st.session_state.get('max_concurrency', 1),
      help="Número de atracciones a procesar simultáneamente",
      disabled=scraping_active
    )
  
  with col2:
    target_language = st.selectbox(
      "Idioma objetivo:",
      options=["english", "spanish", "portuguese", "french", "german", "italian", "dutch", "japanese", "chinese"],
      format_func=lambda x: {
        "english": "Inglés",
        "spanish": "Español", 
        "portuguese": "Portugués",
        "french": "Francés",
        "german": "Alemán",
        "italian": "Italiano",
        "dutch": "Holandés",
        "japanese": "Japonés",
        "chinese": "Chino"
      }.get(x, x),
      index=0,
      help="Idioma de las reseñas a scrapear",
      disabled=scraping_active,
      key="target_language_select"
    )

  # Estado de la aplicación en session_state
  if 'scraping_active' not in st.session_state:
    st.session_state.scraping_active = False
  if 'should_stop' not in st.session_state:
    st.session_state.should_stop = False

  ui_status_placeholder = st.empty()

  # Botones de control
  st.markdown("### Control de Scraping")
  col1, col2 = st.columns(2)
  
  with col1:
      if st.button("Iniciar", disabled=scraping_active, key="start_button"):
        if selected_region_name_ui:
          st.session_state.scraping_active = True
          st.session_state.should_stop = False
          st.session_state.max_concurrency = max_concurrency
          # Guardar el idioma objetivo sin conflicto con el widget
          st.session_state.scraping_target_language = target_language
          log.info(f"Iniciando scraping para {selected_region_name_ui} en idioma {target_language}")
          st.rerun()
        else:
          ui_status_placeholder.warning("Selecciona una región válida")
  
  # Mostrar estado actual solo si está activo
  if scraping_active:
    if st.session_state.should_stop:
      st.warning("Deteniendo scraping... Por favor espera")
    else:
      current_concurrency = st.session_state.get('max_concurrency', 1)
      current_region = st.session_state.get('current_scraping_region', selected_region_name_ui)
      current_attractions = regions_with_attractions.get(current_region, 0)
      st.info(f"Scraping activo para: **{current_region}** ({current_attractions} atracciones, Concurrencia: {current_concurrency})")
      
      # Guardar región actual en session_state
      st.session_state.current_scraping_region = selected_region_name_ui
  
  # Mostrar tabla con estado de regiones scrapeadas al final
  _render_scraped_regions_table(data_handler, scraped_regions)
  
  # Elementos de progreso (siempre visibles cuando está activo)
  if scraping_active:
    st.markdown("### Progreso del Scraping")
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # Ejecutar la función de scraping
    run_review_scraping_session(
      data_handler, 
      selected_region_name_ui, 
      ui_status_placeholder,
      progress_bar,
      status_text
    )
    
    # Después de que termine el scraping forzar rerun para actualizar UI
    if not st.session_state.scraping_active:
      time.sleep(0.5)
      st.rerun()

# ===============================================================
# OBTENER TIEMPO TRANSCURRIDO
# ===============================================================

def _get_time_ago(date_string):
  # CONVIERTE UNA FECHA A FORMATO 'HACE X TIEMPO'
  # Parsea diferentes formatos de fecha y calcula tiempo transcurrido
  # Retorna texto legible en español del tiempo relativo
  if not date_string or date_string == "-":
    return "Nunca"
  
  try:
    # Intentar parsear la fecha
    if 'T' in date_string:
      # Formato ISO con timezone
      date_obj = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
    else:
      # Formato simple YYYY-MM-DD HH:MM
      date_obj = datetime.strptime(date_string, "%Y-%m-%d %H:%M")
      # Asumir UTC si no hay timezone
      date_obj = date_obj.replace(tzinfo=timezone.utc)
    
    # Calcular diferencia con tiempo actual
    now = datetime.now(timezone.utc)
    diff = now - date_obj
    
    # Convertir a segundos totales
    total_seconds = int(diff.total_seconds())
    
    # Calcular tiempo transcurrido
    if total_seconds < 60:
      return "Hace unos segundos"
    elif total_seconds < 3600:  # Menos de 1 hora
      minutes = total_seconds // 60
      return f"Hace {minutes} minuto{'s' if minutes != 1 else ''}"
    elif total_seconds < 86400:  # Menos de 1 día
      hours = total_seconds // 3600
      return f"Hace {hours} hora{'s' if hours != 1 else ''}"
    elif total_seconds < 604800:  # Menos de 1 semana
      days = total_seconds // 86400
      return f"Hace {days} día{'s' if days != 1 else ''}"
    elif total_seconds < 2629746:  # Menos de 1 mes
      weeks = total_seconds // 604800
      return f"Hace {weeks} semana{'s' if weeks != 1 else ''}"
    elif total_seconds < 31556952:  # Menos de 1 año
      months = total_seconds // 2629746
      return f"Hace {months} mes{'es' if months != 1 else ''}"
    else:
      years = total_seconds // 31556952
      return f"Hace {years} año{'s' if years != 1 else ''}"
      
  except Exception as e:
    log.warning(f"Error parseando fecha '{date_string}': {e}")
    return "Fecha inválida"

# ===============================================================
# RENDERIZAR TABLA DE REGIONES SCRAPEADAS
# ===============================================================

def _render_scraped_regions_table(data_handler, scraped_regions):
  # MUESTRA TABLA CON REGIONES QUE TIENEN ATRACCIONES SCRAPEADAS
  # Soporte completo para estadísticas multilenguaje
  # Incluye métricas y progreso visual del scraping de reseñas
  st.markdown("---")
  st.subheader("Estado de Regiones para Scraping de Reseñas")
  
  # Selector de idioma para visualización de stats
  display_language = st.selectbox(
    "Ver estadísticas para idioma:",
    options=["all", "english", "spanish", "portuguese", "french", "german", "italian", "dutch", "japanese", "chinese"],
    format_func=lambda x: {
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
    }.get(x, x),
    index=0,
    help="Idioma para mostrar estadísticas de reseñas",
    key="stats_language_select"
  )
  
  if st.button("Actualizar Tabla de Estado de Regiones"):
    log.info("Botón 'Actualizar Tabla de Estado de Regiones' presionado.")
    if data_handler:
      data_handler.reload_data()
      log.info("DataHandler ha recargado sus datos. Solicitando re-renderizado de la UI.")
      st.rerun()
    else:
      log.error("DataHandler no disponible en _render_scraped_regions_table para recargar.")
      st.warning("No se pudo actualizar la tabla: DataHandler no encontrado.")
  
  table_data = []
  
  for region in scraped_regions:
    region_name = region.get("region_name", "Sin nombre")
    attractions = region.get("attractions", [])
    attraction_count = len(attractions)
    
    # Obtener fecha de scraping de atracciones
    last_attractions_scrape = region.get("last_attractions_scrape_date", "")
    scraping_date_relative = _get_time_ago(last_attractions_scrape)
    
    # Calcular estadísticas multilenguaje
    total_reviews = 0
    attractions_with_reviews = 0
    language_breakdown = {}
    
    for attraction in attractions:
      # Soporte para estructura multilenguaje
      if display_language == "all":
        # Mostrar total de todas las reseñas únicas
        languages_data = attraction.get("languages", {})
        for lang, lang_data in languages_data.items():
          lang_reviews = len(lang_data.get("reviews", []))
          if lang_reviews > 0:
            language_breakdown[lang] = language_breakdown.get(lang, 0) + lang_reviews
            total_reviews += lang_reviews
        
        if any(len(lang_data.get("reviews", [])) > 0 for lang_data in languages_data.values()):
          attractions_with_reviews += 1
      else:
        # Mostrar solo para idioma específico
        language_data = attraction.get("languages", {}).get(display_language, {})
        lang_reviews = len(language_data.get("reviews", []))
        if lang_reviews > 0:
          total_reviews += lang_reviews
          attractions_with_reviews += 1
    
    # Determinar estado de reseñas
    if total_reviews > 0:
      if display_language == "all":
        estado_reseñas = f"Con reseñas ({len(language_breakdown)} idiomas)"
      else:
        estado_reseñas = f"Con reseñas en {display_language}"
      progreso_reseñas = f"{attractions_with_reviews}/{attraction_count}"
    else:
      if display_language == "all":
        estado_reseñas = "Sin reseñas"
      else:
        estado_reseñas = f"Sin reseñas en {display_language}"
      progreso_reseñas = f"0/{attraction_count}"
    
    # Agregar desglose de idiomas
    idiomas_info = ""
    if display_language == "all" and language_breakdown:
      idiomas_list = [f"{lang}: {count}" for lang, count in sorted(language_breakdown.items())]
      idiomas_info = " | ".join(idiomas_list[:3])  # Mostrar solo primeros 3
      if len(language_breakdown) > 3:
        idiomas_info += f" (+{len(language_breakdown)-3} más)"
    
    table_data.append({
      "Región": region_name,
      "Atracciones": attraction_count,
      "Scrapeado": scraping_date_relative,
      "Estado": estado_reseñas,
      "Progreso": progreso_reseñas,
      "Reseñas": total_reviews,
      "Idiomas": idiomas_info if display_language == "all" else display_language 
    })
  
  if table_data:
    df = pd.DataFrame(table_data)
    
    # Configuración de columnas actualizada
    column_config = {
      "Región": st.column_config.TextColumn("Región", width="medium"),
      "Atracciones": st.column_config.NumberColumn("Atracciones", width="small"),
      "Scrapeado": st.column_config.TextColumn("Scrapeado", width="medium"),
      "Estado": st.column_config.TextColumn("Estado", width="medium"),
      "Progreso": st.column_config.TextColumn("Progreso", width="small"),
      "Reseñas": st.column_config.NumberColumn("Reseñas", width="small"),
    }
    
    if display_language == "all":
      column_config["Idiomas"] = st.column_config.TextColumn(
        "Desglose", 
        width="large",
        help="Desglose de reseñas por idioma"
      )
    else:
      column_config["Idiomas"] = st.column_config.TextColumn(
        "Idioma", 
        width="small",
        help="Idioma seleccionado"
      )
    
    st.dataframe(df, use_container_width=True, hide_index=True, column_config=column_config)
    
    # Métricas resumen con info de idiomas
    col1, col2, col3, col4 = st.columns(4)
    total_regions = len(scraped_regions)
    total_attractions = sum(len(r.get("attractions", [])) for r in scraped_regions)
    total_reviews = sum(item["Reseñas"] for item in table_data)
    
    col1.metric("Regiones", total_regions)
    col2.metric("Atracciones", total_attractions)
    col3.metric("Reseñas", f"{total_reviews:,}")
    
    if display_language == "all":
      # Contar idiomas únicos
      all_languages = set()
      for region in scraped_regions:
        for attraction in region.get("attractions", []):
          all_languages.update(attraction.get("languages", {}).keys())
      col4.metric("Idiomas", len(all_languages))
    else:
      # Mostrar % de cobertura para idioma específico
      attractions_with_lang = sum(1 for item in table_data if item["Reseñas"] > 0)
      coverage = (attractions_with_lang / total_attractions * 100) if total_attractions > 0 else 0
      col4.metric(f"Cobertura {display_language}", f"{coverage:.1f}%")
    
    # Progreso visual
    regions_with_reviews = len([item for item in table_data if item["Reseñas"] > 0])
    if total_regions > 0:
      reviews_progress = (regions_with_reviews / total_regions) * 100
      st.progress(reviews_progress / 100)
      if display_language == "all":
        st.caption(f"Progreso global: {reviews_progress:.1f}% de regiones tienen reseñas")
      else:
        st.caption(f"Progreso en {display_language}: {reviews_progress:.1f}% de regiones")
  else:
    st.info("No hay datos de regiones para mostrar")

# ===============================================================
# EJECUTAR SESIÓN DE SCRAPING
# ===============================================================

def run_review_scraping_session(data_handler, selected_region_name_ui, ui_status_placeholder, progress_bar, status_text):
  # MANEJA SESIÓN DE SCRAPING USANDO ASYNCIO
  # Coordinación completa con soporte multilenguaje
  # Actualiza progreso y guarda datos durante el proceso
  async def async_scraping():
    try:
      stop_event = asyncio.Event()
      
      region_consolidated_data = data_handler.get_region_data(selected_region_name_ui)
      
      if not region_consolidated_data or not region_consolidated_data.get("attractions"):
        ui_status_placeholder.warning(f"No se encontraron atracciones para '{selected_region_name_ui}'")
        return
      
      attractions_data_for_region = region_consolidated_data.get("attractions", [])
      total_attractions = len(attractions_data_for_region)
      
      if total_attractions == 0:
        ui_status_placeholder.warning(f"No hay atracciones en '{selected_region_name_ui}' para scrapear")
        return
      
      # Usar la key correcta del session state
      target_language = st.session_state.get('scraping_target_language', 'english')
      max_concurrency = st.session_state.get('max_concurrency', 1)
      max_retries = st.session_state.get('max_retries', 3)
      
      log.info(f"Scraping {total_attractions} atracciones en {selected_region_name_ui} para idioma: {target_language}")
      
      # Pasar idioma objetivo al scraper
      scraper = ReviewScraper(
        max_retries=max_retries,
        max_concurrency=max_concurrency,
        json_output_filepath=str(CONSOLIDATED_DATA_PATH),
        stop_event=stop_event,
        inter_attraction_base_delay=2.0,
        target_language=target_language
      )
      
      async with scraper:
        total_reviews_session = 0
        current_attraction_index = 0
        
        def attraction_update_callback(attraction_index, attraction_name, newly_scraped_count, status):
          nonlocal total_reviews_session, current_attraction_index
          
          current_attraction_index = attraction_index + 1
          total_reviews_session += newly_scraped_count
          
          progress_value = current_attraction_index / total_attractions
          progress_bar.progress(progress_value)
          
          # Mensajes de estado más específicos para idiomas
          if f"no_{target_language}_reviews" in status:
            status_icon = "○"
            status_msg = f"Sin reseñas en {target_language}"
          elif "incorrect_language_view" in status:
            status_icon = "⚠"
            status_msg = f"Vista de idioma incorrecta"
          elif "up_to_date" in status:
            status_icon = "✓"
            status_msg = f"Ya actualizada en {target_language}"
          elif newly_scraped_count > 0:
            status_icon = "✓"
            status_msg = f"Completada ({newly_scraped_count} reseñas en {target_language})"
          elif "stopped" in status:
            status_icon = "■"
            status_msg = "Detenida"
          else:
            status_icon = "⚠"
            status_msg = f"Sin nuevas reseñas en {target_language}"
          
          # UI con información de idioma
          status_text.text(
            f"Progreso: {current_attraction_index}/{total_attractions} atracciones\n"
            f"Procesando: {attraction_name}\n"
            f"Estado: {status_icon} {status_msg}\n"
            f"Idioma objetivo: {target_language.title()}\n"
            f"Concurrencia activa: {max_concurrency}\n"
            f"Total reseñas en {target_language}: {total_reviews_session}"
          )
        
        # Verificación de detención
        async def check_stop_signal():
          while not stop_event.is_set():
            if st.session_state.get('should_stop', False):
              log.info("Detectada señal de detención desde UI")
              stop_event.set()
              break
            await asyncio.sleep(0.1)
        
        check_task = asyncio.create_task(check_stop_signal())
        
        try:
          log.info(f"Iniciando scraping con {len(attractions_data_for_region)} atracciones para {target_language}")
          
          # Pasar idioma objetivo
          results = await scraper.scrape_multiple_attractions(
            attractions_data_for_region, 
            selected_region_name_ui,
            target_language=target_language,
            attraction_callback=attraction_update_callback,
            stop_event=stop_event
          )
          
          # Procesamiento de resultados
          total_successfully_processed = 0
          total_reviews_collected = 0
          
          for result in results:
            if result and isinstance(result, dict):
              newly_scraped = len(result.get("newly_scraped_reviews", []))
              total_reviews_collected += newly_scraped
              if newly_scraped > 0 or "completed" in result.get("scrape_status", ""):
                total_successfully_processed += 1
          
          # Mensajes finales con información de idioma
          if st.session_state.get('should_stop', False) or stop_event.is_set():
            final_message = (
              f"Scraping DETENIDO por el usuario\n"
              f"Atracciones procesadas: {total_successfully_processed}/{total_attractions}\n"
              f"Reseñas en {target_language}: {total_reviews_collected}"
            )
            ui_status_placeholder.warning(final_message)
          else:
            final_message = (
              f"Scraping completado para {target_language}\n"
              f"Atracciones procesadas: {total_successfully_processed}/{total_attractions}\n"
              f"Reseñas en {target_language}: {total_reviews_collected}"
            )
            ui_status_placeholder.success(final_message)
          
          log.info(final_message.replace('\n', ' '))
          
        finally:
          check_task.cancel()
          try:
            await check_task
          except asyncio.CancelledError:
            pass
        
    except Exception as e:
      error_msg = f"Error durante scraping de reseñas: {str(e)}"
      log.error(error_msg)
      ui_status_placeholder.error(error_msg)
      
    finally:
      log.info("Sesión de scraping finalizada")
      st.session_state.scraping_active = False
      st.session_state.should_stop = False
      # Limpiar el idioma objetivo cuando termine
      if 'scraping_target_language' in st.session_state:
        del st.session_state.scraping_target_language
  
  asyncio.run(async_scraping())