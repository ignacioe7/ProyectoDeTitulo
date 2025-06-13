# MÓDULO DE INTERFAZ PARA SCRAPING DE ATRACCIONES DE TRIPADVISOR
# Maneja scraping asíncrono de múltiples páginas con control de estado y progreso
# Implementa sistema de inicio/parada y visualización de estado por región

import streamlit as st
import asyncio
import pandas as pd
from datetime import datetime, timezone
from src.core.scraper import AttractionScraper
from loguru import logger as log

# ====================================================================================================================
#                                            RENDERIZAR PÁGINA PRINCIPAL
# ====================================================================================================================

def render(data_handler):
  # RENDERIZA INTERFAZ PRINCIPAL PARA SCRAPING DE ATRACCIONES
  # Coordina inicialización de estado, controles de scraping y tabla de progreso
  # Maneja flujo completo desde selección de región hasta visualización de resultados
  st.header("Scraping de Atracciones")
  st.markdown("---")
  
  # inicializar estado de sesión para control de procesos activos
  if 'scraping' not in st.session_state:
    st.session_state.scraping = {
      'activo': False,
      'detener': False,
      'region': None,
      'atracciones': [],
      'pagina_actual': 1
    }

  # verificar y obtener configuraciones de regiones disponibles
  region_configs = _get_region_configs(data_handler)
  if not region_configs:
    st.error("No hay regiones configuradas para scraping")
    st.info("Asegúrate de que regions.json existe y tiene datos válidos")
    return

  # renderizar controles de inicio y configuración
  _render_scraping_controls(data_handler, region_configs)
  
  # ejecutar proceso de scraping si está activo en session state
  if st.session_state.scraping['activo']:
    _handle_active_scraping(data_handler, region_configs)
  
  # mostrar tabla resumen con estado actual de todas las regiones
  _render_regions_table(data_handler, region_configs)

# ====================================================================================================================
#                                        OBTENER CONFIGURACIONES DE REGIONES
# ====================================================================================================================

def _get_region_configs(data_handler):
  # OBTIENE CONFIGURACIONES DE REGIONES DESDE MÚLTIPLES FUENTES
  # Prioriza datos de configuración sobre datos ya scrapeados
  # Retorna diccionario con nombres de región y URLs o None si no hay datos
  
  # priorizar regions_data desde configuración JSON
  if hasattr(data_handler, 'regions_data') and data_handler.regions_data:
    return data_handler.regions_data
  
  # fallback usando regiones ya scrapeadas para permitir re-scraping
  elif hasattr(data_handler, 'data') and data_handler.data and data_handler.data.get("regions"):
    region_configs = {}
    for region in data_handler.data.get("regions", []):
      region_name = region.get("region_name", "Región Desconocida")
      region_configs[region_name] = {"url": ""}  # placeholder sin URL válida
    return region_configs
  
  return None

# ====================================================================================================================
#                                           RENDERIZAR CONTROLES DE SCRAPING
# ====================================================================================================================

def _render_scraping_controls(data_handler, region_configs):
  # RENDERIZA CONTROLES DE SELECCIÓN Y BOTONES DE INICIO/PARADA
  # Proporciona dropdown para regiones y validación de URLs configuradas
  # Maneja habilitación/deshabilitación de controles según estado activo
  st.subheader("Iniciar Scraping")
  
  available_regions = sorted(list(region_configs.keys()))
  
  col1, col2 = st.columns(2)
  
  with col1:
    # selector de región con validación de disponibilidad
    selected_region = st.selectbox(
      "Selecciona una región para scrapear:",
      options=[""] + available_regions,
      format_func=lambda x: "Selecciona una región..." if x == "" else x,
      disabled=st.session_state.scraping['activo'],
      key="region_selector"
    )
    
    # mostrar estado de configuración de URL para región seleccionada
    if selected_region:
      region_config = region_configs.get(selected_region, {})
      url = region_config.get('url', '')
      if url:
        st.success(f"URL configurada para {selected_region}")
      else:
        st.warning(f"No hay URL configurada para {selected_region}")

  # botones de control con validación de estados
  col1, col2, col3, col4 = st.columns(4)
  with col1:
    # botón de inicio con múltiples validaciones de estado
    start_disabled = (
      st.session_state.scraping['activo'] or 
      not selected_region or 
      not region_configs.get(selected_region, {}).get('url')
    )
    
    if st.button("Iniciar", disabled=start_disabled, use_container_width=True):
      _start_scraping(selected_region)
      
  with col2:
    # botón de detener solo disponible durante scraping activo
    if st.button("Detener", disabled=not st.session_state.scraping['activo'], use_container_width=True):
      st.session_state.scraping['detener'] = True
      st.warning("Deteniendo scraping...")

# ====================================================================================================================
#                                              INICIAR PROCESO DE SCRAPING
# ====================================================================================================================

def _start_scraping(region_name: str):
  # INICIA PROCESO DE SCRAPING ACTUALIZANDO ESTADO DE SESIÓN
  # Configura variables de control y reinicia contadores de progreso
  # Registra inicio en logs y fuerza actualización de interfaz
  st.session_state.scraping.update({
    'activo': True,
    'detener': False,
    'region': region_name,
    'atracciones': [],
    'pagina_actual': 1
  })
  log.info(f"Iniciando scraping para {region_name}")
  st.rerun()

# ====================================================================================================================
#                                           MANEJAR SCRAPING ACTIVO
# ====================================================================================================================

def _handle_active_scraping(data_handler, region_configs):
  # COORDINA EJECUCIÓN DE SCRAPING ACTIVO CON MANEJO DE ERRORES
  # Valida región seleccionada y ejecuta proceso asíncrono
  # Maneja cleanup de estado y actualización de UI al finalizar
  region_name = st.session_state.scraping.get('region')
  
  # validación crítica de región especificada
  if not region_name:
    st.error("Error: Región no especificada")
    st.session_state.scraping['activo'] = False
    st.rerun()
    return

  # mostrar sección de progreso durante scraping activo
  st.markdown("---")
  st.subheader(f"Scraping en Progreso: {region_name}")
  
  # crear contenedores para actualización dinámica de progreso
  progress_container = st.container()
  status_container = st.container()
  
  try:
    # ejecutar scraping con contenedores para feedback visual
    result = _run_scraping_sync(
      data_handler, 
      region_configs, 
      region_name,
      progress_container,
      status_container
    )
    
    # mostrar resultado final según éxito o fallo
    if result:
      st.success("Scraping completado exitosamente")
    else:
      st.warning("Scraping detenido o falló")
      
  except Exception as e:
    # manejo de errores críticos con logging detallado
    st.error(f"Error crítico: {str(e)}")
    log.error(f"Error en scraping: {e}")
  finally:
    # cleanup obligatorio de estado para permitir nuevos procesos
    st.session_state.scraping.update({
      'activo': False, 
      'detener': False
    })
    # recargar datos para reflejar cambios en la tabla de progreso
    data_handler.reload_data()
    st.rerun()

# ====================================================================================================================
#                                         EJECUTAR SCRAPING SINCRONIZADO
# ====================================================================================================================

def _run_scraping_sync(data_handler, region_configs, region_name, progress_container, status_container):
  # EJECUTA SCRAPING ASÍNCRONO USANDO ASYNCIO CON PROGRESO EN TIEMPO REAL
  # Coordina múltiples páginas, actualizaciones de UI y persistencia de datos
  # Retorna True si completa exitosamente, False en caso de error o detención
  
  async def scraping_coroutine():
    # obtener configuración específica de región seleccionada
    region_config = region_configs.get(region_name, {})
    url_region = region_config.get('url')
    
    # validación crítica de URL antes de iniciar scraping
    if not url_region:
      status_container.error(f"No se encontró URL para {region_name}")
      return False
    
    log.info(f"Iniciando scraping para {region_name}")
    
    try:
      async with AttractionScraper() as scraper:
        current_url = url_region
        page_count = 0
        total_attractions = 0
        
        # crear elementos de UI actualizables para progreso dinámico
        progress_bar = progress_container.progress(0)
        status_placeholder = status_container.empty()  # placeholder reutilizable
        
        # bucle principal de scraping página por página
        while current_url and not st.session_state.scraping['detener']:
          page_count += 1
          
          # actualizar estado usando placeholder reutilizable para evitar acumulación
          status_placeholder.markdown(f"""
          **Progreso:**
          - Página actual: {page_count}
          - Atracciones encontradas: {total_attractions}
          - URL actual: {current_url[:80]}...
          """)
          
          # scrapear página actual y procesar datos obtenidos
          page_data = await scraper.scrape_page(current_url)
          if page_data:
            st.session_state.scraping['atracciones'].extend(page_data)
            total_attractions += len(page_data)
            
            # persistir progreso inmediatamente para evitar pérdida de datos
            await data_handler.save_attractions(
              region_name, 
              st.session_state.scraping['atracciones']
            )
          
          # actualizar barra de progreso con máximo del 90% hasta completar
          progress_bar.progress(min(page_count * 0.1, 0.9))
          
          # obtener HTML de página actual para extraer siguiente URL
          html = await scraper.get_page_html(current_url)
          if not html:
            break
            
          # extraer URL de siguiente página y validar que sea diferente
          next_url = await scraper.get_next_page_url(html)
          if not next_url or next_url == current_url:
            break
            
          # actualizar variables para siguiente iteración
          current_url = next_url
          st.session_state.scraping['pagina_actual'] = page_count
          
          # pausa inteligente entre páginas para evitar detección anti-bot
          await asyncio.sleep(1.5)
        
        # completar barra de progreso al 100% al finalizar
        progress_bar.progress(1.0)
        
        # guardar datos finales y recargar para actualizar tabla
        if st.session_state.scraping['atracciones']:
          await data_handler.save_attractions(
            region_name, 
            st.session_state.scraping['atracciones']
          )
        
        # mostrar resumen final usando mismo placeholder para consistencia
        status_placeholder.success(f"""
        **Scraping Completado:**
        - Páginas procesadas: {page_count}
        - Total atracciones: {total_attractions}
        - Región: {region_name}
        """)
        
        return True
        
    except Exception as e:
      # manejo de errores con logging y feedback visual
      log.error(f"Error en scraping asíncrono: {e}")
      status_placeholder.error(f"Error durante scraping: {str(e)}")
      return False
  
  # ejecutar asíncrono usando asyncio.run para compatibilidad con Streamlit
  return asyncio.run(scraping_coroutine())

# ====================================================================================================================
#                                          OBTENER TIEMPO TRANSCURRIDO
# ====================================================================================================================

def _get_time_ago(date_string):
  # CONVIERTE FECHA A FORMATO RELATIVO LEGIBLE PARA HUMANOS
  # Maneja múltiples formatos de fecha y calcula tiempo transcurrido
  # Retorna string descriptivo del tiempo relativo o mensaje de error
  if not date_string or date_string == "-":
    return "Nunca"
  
  try:
    # detectar y parsear diferentes formatos de fecha
    if 'T' in date_string:
      # formato ISO con información de timezone
      date_obj = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
    else:
      # formato simple sin timezone
      date_obj = datetime.strptime(date_string, "%Y-%m-%d %H:%M")
      # asumir UTC si no hay timezone especificado
      date_obj = date_obj.replace(tzinfo=timezone.utc)
    
    # calcular diferencia con momento actual en UTC
    now = datetime.now(timezone.utc)
    diff = now - date_obj
    
    # convertir a segundos para cálculo de unidades apropiadas
    total_seconds = int(diff.total_seconds())
    
    # determinar unidad temporal más apropiada para mostrar
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
    # manejo de errores en parsing con logging para debugging
    log.warning(f"Error parseando fecha '{date_string}': {e}")
    return "Fecha inválida"

# ====================================================================================================================
#                                            RENDERIZAR TABLA DE REGIONES
# ====================================================================================================================

def _render_regions_table(data_handler, region_configs):
  # MUESTRA TABLA RESUMEN CON ESTADO DE TODAS LAS REGIONES CONFIGURADAS
  # Combina datos de configuración con datos scrapeados para vista completa
  # Incluye métricas agregadas y progreso visual del avance general
  st.markdown("---")
  st.subheader("Estado de Regiones")
  
  # preparar datos combinando configuración con datos scrapeados
  table_data = []
  scraped_regions_data = _get_scraped_regions_data(data_handler)
  
  # procesar cada región configurada para construir fila de tabla
  for region_name in sorted(region_configs.keys()):
    scraped_info = scraped_regions_data.get(region_name, {})
    last_scrape_raw = scraped_info.get("last_scrape_date", "")
    
    # determinar estado y conteo basado en datos disponibles
    if scraped_info:
      estado = "Scrapeada"
      attractions_count = scraped_info.get("attractions_count", 0)
    else:
      estado = "Pendiente"
      attractions_count = 0
    
    # convertir fecha absoluta a tiempo relativo legible
    tiempo_relativo = _get_time_ago(last_scrape_raw)
    
    # agregar fila completa con todos los datos procesados
    table_data.append({
      "Región": region_name,
      "Estado": estado,
      "Última Scrapeada": tiempo_relativo,
      "Atracciones": attractions_count
    })
  
  # renderizar tabla y métricas si hay datos disponibles
  if table_data:
    df = pd.DataFrame(table_data)
    col1, col2 = st.columns(2)
    
    with col1:
      # tabla principal con configuración de columnas personalizada
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
          "Estado": st.column_config.TextColumn(
            "Estado", 
            width="auto",
            help="Estado del scraping de atracciones"
          ),
          "Última Scrapeada": st.column_config.TextColumn(
            "Última Scrapeada", 
            width="auto",
            help="Tiempo transcurrido desde el último scraping"
          ),
          "Atracciones": st.column_config.NumberColumn(
            "Atracciones",
            width="auto",
            format="%d",
            help="Número total de atracciones encontradas"
          )
        }
      )
      
    with col2:
      # métricas resumen calculadas desde datos de tabla
      col1, col2, col3 = st.columns(3)
      total_regions = len(region_configs)
      scraped_count = len([r for r in scraped_regions_data.values() if r])
      total_attractions = sum(item.get("attractions_count", 0) for item in scraped_regions_data.values())
      
      col1.metric("Total Regiones", total_regions)
      col2.metric("Scrapeadas", f"{scraped_count}/{total_regions}")
      col3.metric("Total Atracciones", f"{total_attractions:,}")
      
      # mostrar progreso visual con porcentaje de completitud
      if total_regions > 0:
        progress_percentage = (scraped_count / total_regions) * 100
        st.progress(progress_percentage / 100)
        st.caption(f"Progreso general: {progress_percentage:.1f}% completado")
  else:
    st.info("No hay datos de regiones para mostrar")

# ====================================================================================================================
#                                        OBTENER DATOS DE REGIONES SCRAPEADAS
# ====================================================================================================================

def _get_scraped_regions_data(data_handler):
  # EXTRAE DATOS DE REGIONES YA SCRAPEADAS DESDE DATA HANDLER
  # Procesa estructura de datos para obtener fechas y conteos de atracciones
  # Retorna diccionario con información resumida por región
  scraped_data = {}
  
  # validar que data handler contenga datos válidos
  if not (hasattr(data_handler, 'data') and data_handler.data):
    return scraped_data
  
  # procesar cada región en datos consolidados
  for region in data_handler.data.get("regions", []):
    region_name = region.get("region_name")
    if region_name:
      attractions = region.get("attractions", [])
      last_scrape = region.get("last_attractions_scrape_date", "")
      
      # construir información resumida de región
      scraped_data[region_name] = {
        "last_scrape_date": last_scrape,
        "attractions_count": len(attractions)
      }
  
  return scraped_data