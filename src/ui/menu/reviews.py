# MÓDULO DE INTERFAZ PARA SCRAPING DE RESEÑAS DE TRIPADVISOR
# Maneja la UI de scraping de reseñas, control de estado y visualización de progreso
# Implementa sistema asíncrono con callbacks para actualizaciones en tiempo real

from loguru import logger as log
import streamlit as st
import asyncio
import time
import pandas as pd
from datetime import datetime, timezone
from src.core.scraper import ReviewScraper
from src.utils.constants import CONSOLIDATED_DATA_PATH

# ====================================================================================================================
#                                             RENDERIZAR PÁGINA PRINCIPAL
# ====================================================================================================================

def render(data_handler):
  # RENDERIZA INTERFAZ PRINCIPAL PARA SCRAPING DE RESEÑAS
  # Valida datos disponibles, maneja estados de scraping y controla navegación
  # Muestra configuración, progreso y tabla de estado de regiones
  st.header("Scraping de Reseñas")
  
  # verificar estado de scraping activo
  scraping_active = st.session_state.get('scraping_active', False)
  
  st.markdown("---")

  # validar que existan regiones con atracciones scrapeadas
  if not hasattr(data_handler, 'data') or not data_handler.data or not data_handler.data.get("regions"):
    st.warning("No se encontraron regiones con atracciones scrapeadas")
    st.info("Primero debes scrapear atracciones en la sección 'Atracciones'")
    return

  # obtener regiones con datos válidos desde archivo consolidado
  scraped_regions = data_handler.data.get("regions", [])
  if not scraped_regions:
    st.warning("No hay regiones con atracciones para scrapear reseñas")
    st.info("Ve a la sección 'Atracciones' para scrapear algunas primero")
    return

  # extraer nombres de regiones que contienen atracciones válidas
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

  # widgets de configuración con deshabilitación durante scraping
  col1 , col2 = st.columns(2)
  with col1:
    selected_region_name_ui = st.selectbox(
      "Selecciona una Región (solo con atracciones scrapeadas):",
      options=[""] + region_names_ui,
      format_func=lambda x: "Selecciona una opción..." if x == "" else f"{x} ({regions_with_attractions.get(x, 0)} atracciones)",
      key="reviews_region_selectbox",
      disabled=scraping_active
    )
    # mostrar información detallada de región seleccionada
    if selected_region_name_ui:
      attraction_count = regions_with_attractions.get(selected_region_name_ui, 0)
      st.info(f"Región seleccionada: **{selected_region_name_ui}** con **{attraction_count}** atracciones")

  with col1:
    # control de concurrencia para scraping
    st.markdown("### Configuración de Scraping")
    max_concurrency = st.slider(
      "Concurrencia máxima:",
      min_value=1,
      max_value=3,
      value=st.session_state.get('max_concurrency', 2),
      help="Número de atracciones a procesar simultáneamente",
      disabled=scraping_active
    )

  # inicialización de estados de sesión para control de proceso
  if 'scraping_active' not in st.session_state:
    st.session_state.scraping_active = False
  if 'should_stop' not in st.session_state:
    st.session_state.should_stop = False

  ui_status_placeholder = st.empty()

  # botón de inicio con validación de selección
  with col1:
    if st.button("Iniciar", disabled=scraping_active, key="start_button", use_container_width=True):
      if selected_region_name_ui:
        st.session_state.scraping_active = True
        st.session_state.should_stop = False
        st.session_state.max_concurrency = max_concurrency
        log.info(f"Iniciando scraping para {selected_region_name_ui}")
        st.rerun()
      else:
        ui_status_placeholder.warning("Selecciona una región válida")
  
  # sección de progreso visible solo durante scraping activo
  if scraping_active:
    st.markdown("---")
    st.markdown("### Progreso del Scraping")
    
    # mostrar estado actual del proceso
    if st.session_state.should_stop:
      st.warning("Deteniendo scraping... Por favor espera")
    else:
      current_concurrency = st.session_state.get('max_concurrency', 1)
      current_region = st.session_state.get('current_scraping_region', selected_region_name_ui)
      current_attractions = regions_with_attractions.get(current_region, 0)
      st.info(f"Scraping activo para: **{current_region}** ({current_attractions} atracciones, Concurrencia: {current_concurrency})")
      
      # persistir región actual en session state
      st.session_state.current_scraping_region = selected_region_name_ui
    
    # elementos de interfaz para mostrar progreso
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # ejecutar función principal de scraping
    run_review_scraping_session(
      data_handler, 
      selected_region_name_ui, 
      ui_status_placeholder,
      progress_bar,
      status_text
    )
    
    # forzar actualización de UI después de completar scraping
    if not st.session_state.scraping_active:
      time.sleep(0.5)
      st.rerun()
  
  # mostrar tabla resumen con estado de todas las regiones
  _render_scraped_regions_table(data_handler, scraped_regions)

# ====================================================================================================================
#                                           OBTENER TIEMPO TRANSCURRIDO
# ====================================================================================================================

def _get_time_ago(date_string):
  # CONVIERTE FECHA A FORMATO RELATIVO LEGIBLE PARA HUMANOS
  # Parsea múltiples formatos de fecha y calcula tiempo transcurrido
  # Retorna string descriptivo del tiempo relativo o mensaje de error
  if not date_string or date_string == "-":
    return "Nunca"
  
  try:
    # detectar formato de fecha y parsear apropiadamente
    if 'T' in date_string:
      # formato ISO con información de timezone
      date_obj = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
    else:
      # formato simple sin timezone
      date_obj = datetime.strptime(date_string, "%Y-%m-%d %H:%M")
      # asumir UTC si no hay timezone
      date_obj = date_obj.replace(tzinfo=timezone.utc)
    
    # calcular diferencia con momento actual
    now = datetime.now(timezone.utc)
    diff = now - date_obj
    
    # convertir a segundos para cálculo de unidades
    total_seconds = int(diff.total_seconds())
    
    # determinar unidad temporal apropiada
    if total_seconds < 60:
      return "Hace unos segundos"
    elif total_seconds < 3600:  # menos de 1 hora
      minutes = total_seconds // 60
      return f"Hace {minutes} minuto{'s' if minutes != 1 else ''}"
    elif total_seconds < 86400:  # menos de 1 día
      hours = total_seconds // 3600
      return f"Hace {hours} hora{'s' if hours != 1 else ''}"
    elif total_seconds < 604800:  # menos de 1 semana
      days = total_seconds // 86400
      return f"Hace {days} día{'s' if days != 1 else ''}"
    elif total_seconds < 2629746:  # menos de 1 mes
      weeks = total_seconds // 604800
      return f"Hace {weeks} semana{'s' if weeks != 1 else ''}"
    elif total_seconds < 31556952:  # menos de 1 año
      months = total_seconds // 2629746
      return f"Hace {months} mes{'es' if months != 1 else ''}"
    else:
      years = total_seconds // 31556952
      return f"Hace {years} año{'s' if years != 1 else ''}"
      
  except Exception as e:
    log.warning(f"Error parseando fecha '{date_string}': {e}")
    return "Fecha inválida"

# ====================================================================================================================
#                                        RENDERIZAR TABLA DE REGIONES SCRAPEADAS
# ====================================================================================================================

def _render_scraped_regions_table(data_handler, scraped_regions):
  # MUESTRA TABLA RESUMEN CON ESTADO DE REGIONES Y RESEÑAS SCRAPEADAS
  # Presenta estadísticas detalladas por región y métricas globales
  # Incluye funcionalidad de recarga de datos y progreso visual
  st.markdown("---")
  st.subheader("Estado de Regiones para Scraping de Reseñas")
  
  # botón para refrescar datos desde archivo JSON
  if st.button("Actualizar Tabla de Estado de Regiones"):
    log.info("Botón 'Actualizar Tabla de Estado de Regiones' presionado.")
    if data_handler:
      data_handler.reload_data()  # recargar datos desde archivo
      log.info("DataHandler ha recargado sus datos. Solicitando re-renderizado de la UI.")
      st.rerun()  # actualizar interfaz con nuevos datos
    else:
      log.error("DataHandler no disponible en _render_scraped_regions_table para recargar.")
      st.warning("No se pudo actualizar la tabla: DataHandler no encontrado.")
  
  table_data = []
  
  # procesar cada región para construir datos de tabla
  for region in scraped_regions:
    region_name = region.get("region_name", "Sin nombre")
    attractions = region.get("attractions", [])
    attraction_count = len(attractions)
    
    # obtener fecha de último scraping de atracciones
    last_attractions_scrape = region.get("last_attractions_scrape_date", "")
    scraping_date_relative = _get_time_ago(last_attractions_scrape)
    
    # calcular estadísticas de reseñas por región
    total_reviews = 0
    attractions_with_reviews = 0
    
    for attraction in attractions:
      reviews = attraction.get("reviews", [])
      if reviews:
        total_reviews += len(reviews)
        attractions_with_reviews += 1
    
    # determinar estado visual basado en reseñas disponibles
    if total_reviews > 0:
      estado_reseñas = "Con reseñas"
      progreso_reseñas = f"{attractions_with_reviews}/{attraction_count}"
    else:
      estado_reseñas = "Sin reseñas"
      progreso_reseñas = f"0/{attraction_count}"
    
    # agregar fila de datos a tabla
    table_data.append({
      "Región": region_name,
      "Atracciones": attraction_count,
      "Scrapeado": scraping_date_relative,
      "Estado": estado_reseñas,
      "Progreso": progreso_reseñas,
      "Reseñas": total_reviews
    })
  
  if table_data:
    # crear dataframe con configuración de columnas personalizada
    df = pd.DataFrame(table_data)
    st.dataframe(
      df,
      use_container_width=True,
      hide_index=True,
      column_config={
        "Región": st.column_config.TextColumn(
          "Región", 
          width="auto",
          help="Nombre de la región turística"
        ),
        "Atracciones": st.column_config.NumberColumn(
          "Atracciones", 
          width="auto",
          help="Número de atracciones scrapeadas"
        ),
        "Scrapeado": st.column_config.TextColumn(
          "Scrapeado", 
          width="auto",
          help="Tiempo transcurrido desde el scraping de atracciones"
        ),
        "Estado": st.column_config.TextColumn(
          "Estado", 
          width="auto",
          help="Estado del scraping de reseñas"
        ),
        "Progreso": st.column_config.TextColumn(
          "Progreso", 
          width="auto",
          help="Atracciones con reseñas vs total"
        ),
        "Reseñas": st.column_config.NumberColumn(
          "Reseñas",
          width="auto",
          format="%d",
          help="Total de reseñas scrapeadas"
        )
      }
    )
    
    # métricas resumen en columnas
    col1, col2, col3 = st.columns(3)
    total_regions = len(scraped_regions)
    total_attractions = sum(len(r.get("attractions", [])) for r in scraped_regions)
    total_reviews = sum(item["Reseñas"] for item in table_data)
    
    col1.metric("Regiones", total_regions)
    col2.metric("Atracciones", total_attractions)
    col3.metric("Reseñas", f"{total_reviews:,}")
    
    # barra de progreso visual para completitud de reseñas
    regions_with_reviews = len([item for item in table_data if item["Reseñas"] > 0])
    if total_regions > 0:
      reviews_progress = (regions_with_reviews / total_regions) * 100
      st.progress(reviews_progress / 100)
      st.caption(f"Progreso de reseñas: {reviews_progress:.1f}% de regiones tienen reseñas scrapeadas")
  else:
    st.info("No hay datos de regiones para mostrar")

# ====================================================================================================================
#                                       EJECUTAR SESIÓN DE SCRAPING DE RESEÑAS
# ====================================================================================================================

def run_review_scraping_session(data_handler, selected_region_name_ui, ui_status_placeholder, progress_bar, status_text):
  # MANEJA SESIÓN COMPLETA DE SCRAPING USANDO ASYNCIO Y CALLBACKS
  # Configura scraper, monitorea progreso y actualiza interfaz en tiempo real
  # Controla detención de usuario y cleanup de estados al finalizar
  
  async def async_scraping():
    try:
      stop_event = asyncio.Event()
      
      # obtener datos consolidados de región seleccionada
      region_consolidated_data = data_handler.get_region_data(selected_region_name_ui)
      
      # validar disponibilidad de atracciones en región
      if not region_consolidated_data or not region_consolidated_data.get("attractions"):
        ui_status_placeholder.warning(f"No se encontraron atracciones para '{selected_region_name_ui}'")
        # resetear estado inmediatamente y salir
        st.session_state.scraping_active = False
        st.session_state.should_stop = False
        return
      
      attractions_data_for_region = region_consolidated_data.get("attractions", [])
      total_attractions = len(attractions_data_for_region)
      
      # verificar que hay atracciones válidas para procesar
      if total_attractions == 0:
        ui_status_placeholder.warning(f"No hay atracciones en '{selected_region_name_ui}' para scrapear")
        # resetear estado inmediatamente y salir
        st.session_state.scraping_active = False
        st.session_state.should_stop = False
        return
      
      # obtener configuración del usuario desde session state
      max_concurrency = st.session_state.get('max_concurrency', 1)
      max_retries = st.session_state.get('max_retries', 3)
      
      log.info(f"Scraping {total_attractions} atracciones en {selected_region_name_ui}")
      
      # configurar scraper con parámetros personalizados
      scraper = ReviewScraper(
        max_retries=max_retries,
        max_concurrency=max_concurrency,
        json_output_filepath=str(CONSOLIDATED_DATA_PATH),
        stop_event=stop_event,
        inter_attraction_base_delay=2.0
      )
      
      async with scraper:
        # variables para tracking de progreso durante scraping
        total_reviews_session = 0
        current_attraction_index = 0
        
        # callback ejecutado por cada atracción procesada
        def attraction_update_callback(attraction_index, attraction_name, newly_scraped_count, status):
          nonlocal total_reviews_session, current_attraction_index
          
          current_attraction_index = attraction_index + 1
          total_reviews_session += newly_scraped_count
          
          # actualizar barra de progreso
          progress_value = current_attraction_index / total_attractions
          progress_bar.progress(progress_value)
          
          # determinar icono y mensaje según estado
          if "no_english_reviews" in status:
            status_icon = "○"
            status_msg = "Sin reseñas en inglés"
          elif "up_to_date" in status:
            status_icon = "✓"
            status_msg = "Ya actualizada"
          elif newly_scraped_count > 0:
            status_icon = "✓"
            status_msg = f"Completada ({newly_scraped_count} reseñas)"
          elif "stopped" in status:
            status_icon = "■"
            status_msg = "Detenida"
          else:
            status_icon = "⚠"
            status_msg = "Sin nuevas reseñas"
          
          # actualizar texto de estado en UI
          status_text.text(
            f"Progreso: {current_attraction_index}/{total_attractions} atracciones\n"
            f"Procesando: {attraction_name}\n"
            f"Estado: {status_icon} {status_msg}\n"
            f"Concurrencia activa: {max_concurrency}\n"
            f"Total reseñas recopiladas: {total_reviews_session}"
          )
        
        # función para monitorear señal de detención en bucle
        async def check_stop_signal():
          while not stop_event.is_set():
            if st.session_state.get('should_stop', False):
              log.info("Detectada señal de detención desde UI")
              stop_event.set()
              break
            await asyncio.sleep(0.1)
        
        # ejecutar monitoreo de detención en paralelo
        check_task = asyncio.create_task(check_stop_signal())
        
        try:
          log.info(f"Iniciando scraping con {len(attractions_data_for_region)} atracciones")
          
          # ejecutar scraping múltiple con callback de progreso
          results = await scraper.scrape_multiple_attractions(
            attractions_data_for_region, 
            selected_region_name_ui,
            attraction_update_callback,
            stop_event
          )
          
          # procesar resultados para generar estadísticas finales
          total_successfully_processed = 0
          total_reviews_collected = 0
          
          for result in results:
            if result and isinstance(result, dict):
              newly_scraped = len(result.get("newly_scraped_reviews", []))
              total_reviews_collected += newly_scraped
              if newly_scraped > 0 or "completed" in result.get("scrape_status", ""):
                total_successfully_processed += 1
          
          # mostrar mensaje final según estado de completitud
          if st.session_state.get('should_stop', False) or stop_event.is_set():
            final_message = (
              f"Scraping DETENIDO por el usuario\n"
              f"Atracciones procesadas: {total_successfully_processed}/{total_attractions}\n"
              f"Total reseñas recopiladas: {total_reviews_collected}"
            )
            ui_status_placeholder.warning(final_message)
          else:
            final_message = (
              f"Scraping completado exitosamente\n"
              f"Atracciones procesadas: {total_successfully_processed}/{total_attractions}\n"
              f"Total reseñas recopiladas: {total_reviews_collected}"
            )
            ui_status_placeholder.success(final_message)
          
          log.info(final_message.replace('\n', ' '))
          
        finally:
          # cancelar tarea de monitoreo al completar
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
      # cleanup crítico de estados al finalizar proceso
      log.info("Sesión de scraping finalizada - reseteando estado")
      st.session_state.scraping_active = False
      st.session_state.should_stop = False
      # recargar datos para reflejar cambios en UI
      if hasattr(data_handler, 'reload_data'):
        data_handler.reload_data()
      log.info("Estado reseteado completamente")
  
  # ejecutar función asíncrona principal
  asyncio.run(async_scraping())