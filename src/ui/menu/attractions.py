import streamlit as st
import asyncio
import pandas as pd
from datetime import datetime, timezone
from src.core.scraper import AttractionScraper
from loguru import logger as log

def render(data_handler):
  """Renderiza la página de scraping de atracciones"""
  st.header("Scraping de Atracciones")
  st.markdown("---")
  
  # Inicializar estado de sesión
  if 'scraping' not in st.session_state:
    st.session_state.scraping = {
      'activo': False,
      'detener': False,
      'region': None,
      'atracciones': [],
      'pagina_actual': 1
    }

  # Verificar fuentes de datos disponibles
  region_configs = _get_region_configs(data_handler)
  if not region_configs:
    st.error("No hay regiones configuradas para scraping")
    st.info("Asegúrate de que regions.json existe y tiene datos válidos")
    return

  # Controles de scraping
  _render_scraping_controls(data_handler, region_configs)
  
  # Ejecutar scraping si está activo
  if st.session_state.scraping['activo']:
    _handle_active_scraping(data_handler, region_configs)
  
  # Mostrar tabla de estado de regiones
  _render_regions_table(data_handler, region_configs)

def _get_region_configs(data_handler):
  """Obtiene configuraciones de regiones disponibles"""
  # Priorizar regions_data (configuración)
  if hasattr(data_handler, 'regions_data') and data_handler.regions_data:
    return data_handler.regions_data
  
  # Fallback: usar regiones ya scrapeadas
  elif hasattr(data_handler, 'data') and data_handler.data and data_handler.data.get("regions"):
    region_configs = {}
    for region in data_handler.data.get("regions", []):
      region_name = region.get("region_name", "Región Desconocida")
      region_configs[region_name] = {"url": ""}  # Placeholder
    return region_configs
  
  return None

def _render_scraping_controls(data_handler, region_configs):
  """Renderiza controles de scraping"""
  st.subheader("Iniciar Scraping")
  
  available_regions = sorted(list(region_configs.keys()))
  
  col1, col2 = st.columns([3, 1])
  
  with col1:
    selected_region = st.selectbox(
      "Selecciona una región para scrapear:",
      options=[""] + available_regions,
      format_func=lambda x: "Selecciona una región..." if x == "" else x,
      disabled=st.session_state.scraping['activo'],
      key="region_selector"
    )
    
    if selected_region:
      region_config = region_configs.get(selected_region, {})
      url = region_config.get('url', '')
      if url:
        st.success(f"URL configurada para {selected_region}")
      else:
        st.warning(f"No hay URL configurada para {selected_region}")
  
  with col2:
    # Botón de inicio
    start_disabled = (
      st.session_state.scraping['activo'] or 
      not selected_region or 
      not region_configs.get(selected_region, {}).get('url')
    )
    
    if st.button("Iniciar", disabled=start_disabled, use_container_width=True):
      _start_scraping(selected_region)
    
    # Botón de detener
    if st.button("Detener", disabled=not st.session_state.scraping['activo'], use_container_width=True):
      st.session_state.scraping['detener'] = True
      st.warning("Deteniendo scraping...")

def _start_scraping(region_name: str):
  """Inicia el proceso de scraping"""
  st.session_state.scraping.update({
    'activo': True,
    'detener': False,
    'region': region_name,
    'atracciones': [],
    'pagina_actual': 1
  })
  log.info(f"Iniciando scraping para {region_name}")
  st.rerun()

def _handle_active_scraping(data_handler, region_configs):
  """Maneja el scraping activo"""
  region_name = st.session_state.scraping.get('region')
  
  if not region_name:
    st.error("Error: Región no especificada")
    st.session_state.scraping['activo'] = False
    st.rerun()
    return

  # Mostrar estado actual
  st.markdown("---")
  st.subheader(f"Scraping en Progreso: {region_name}")
  
  # Contenedores para progreso
  progress_container = st.container()
  status_container = st.container()
  
  try:
    # Ejecutar scraping
    result = _run_scraping_sync(
      data_handler, 
      region_configs, 
      region_name,
      progress_container,
      status_container
    )
    
    if result:
      st.success("Scraping completado exitosamente")
    else:
      st.warning("Scraping detenido o falló")
      
  except Exception as e:
    st.error(f"Error crítico: {str(e)}")
    log.error(f"Error en scraping: {e}")
  finally:
    # Limpiar estado
    st.session_state.scraping.update({
      'activo': False, 
      'detener': False
    })
    st.rerun()

def _run_scraping_sync(data_handler, region_configs, region_name, progress_container, status_container):
  """Ejecuta el scraping de forma síncrona"""
  
  async def scraping_coroutine():
    # Obtener configuración
    region_config = region_configs.get(region_name, {})
    url_region = region_config.get('url')
    
    if not url_region:
      status_container.error(f"No se encontró URL para {region_name}")
      return False
    
    log.info(f"Iniciando scraping para {region_name}")
    
    try:
      async with AttractionScraper() as scraper:
        current_url = url_region
        page_count = 0
        total_attractions = 0
        
        # Barra de progreso
        progress_bar = progress_container.progress(0)
        
        while current_url and not st.session_state.scraping['detener']:
          page_count += 1
          
          # Actualizar estado
          with status_container.container():
            st.markdown(f"""
            **Progreso:**
            - Página actual: {page_count}
            - Atracciones encontradas: {total_attractions}
            - URL actual: {current_url[:80]}...
            """)
          
          # Scrapear página
          page_data = await scraper.scrape_page(current_url)
          if page_data:
            st.session_state.scraping['atracciones'].extend(page_data)
            total_attractions += len(page_data)
            
            # Guardar progreso
            await data_handler.save_attractions(
              region_name, 
              st.session_state.scraping['atracciones']
            )
          
          # Actualizar progreso (máximo 90% hasta completar)
          progress_bar.progress(min(page_count * 0.1, 0.9))
          
          # Obtener siguiente página
          html = await scraper.get_page_html(current_url)
          if not html:
            break
            
          next_url = await scraper.get_next_page_url(html)
          if not next_url or next_url == current_url:
            break
            
          current_url = next_url
          st.session_state.scraping['pagina_actual'] = page_count
          
          # Pausa entre páginas
          await asyncio.sleep(1.5)
        
        # Completar progreso
        progress_bar.progress(1.0)
        
        # Mostrar resumen final
        with status_container.container():
          st.success(f"""
          **Scraping Completado:**
          - Páginas procesadas: {page_count}
          - Total atracciones: {total_attractions}
          - Región: {region_name}
          """)
        
        return True
        
    except Exception as e:
      log.error(f"Error en scraping asíncrono: {e}")
      status_container.error(f"Error durante scraping: {str(e)}")
      return False
  
  # Ejecutar la corrutina
  return asyncio.run(scraping_coroutine())

def _get_time_ago(date_string):
  """Convierte una fecha a formato 'hace X tiempo'"""
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

def _render_regions_table(data_handler, region_configs):
  """Muestra tabla con estado de las regiones"""
  st.markdown("---")
  st.subheader("Estado de Regiones")
  
  # Preparar datos para la tabla
  table_data = []
  scraped_regions_data = _get_scraped_regions_data(data_handler)
  
  for region_name in sorted(region_configs.keys()):
    scraped_info = scraped_regions_data.get(region_name, {})
    last_scrape_raw = scraped_info.get("last_scrape_date", "")
    
    # Determinar estado
    if scraped_info:
      estado = "Scrapeada"
      attractions_count = scraped_info.get("attractions_count", 0)
    else:
      estado = "Pendiente"
      attractions_count = 0
    
    # Convertir fecha a tiempo relativo
    tiempo_relativo = _get_time_ago(last_scrape_raw)
    
    table_data.append({
      "Región": region_name,
      "Estado": estado,
      "Última Scrapeada": tiempo_relativo,
      "Atracciones": attractions_count
    })
  
  # Mostrar tabla
  if table_data:
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
    
    # Métricas resumen
    col1, col2, col3 = st.columns(3)
    total_regions = len(region_configs)
    scraped_count = len([r for r in scraped_regions_data.values() if r])
    total_attractions = sum(item.get("attractions_count", 0) for item in scraped_regions_data.values())
    
    col1.metric("Total Regiones", total_regions)
    col2.metric("Scrapeadas", f"{scraped_count}/{total_regions}")
    col3.metric("Total Atracciones", f"{total_attractions:,}")
    
    # Mostrar progreso visual
    if total_regions > 0:
      progress_percentage = (scraped_count / total_regions) * 100
      st.progress(progress_percentage / 100)
      st.caption(f"Progreso general: {progress_percentage:.1f}% completado")
  else:
    st.info("No hay datos de regiones para mostrar")

def _get_scraped_regions_data(data_handler):
  """Obtiene datos de regiones ya scrapeadas"""
  scraped_data = {}
  
  if not (hasattr(data_handler, 'data') and data_handler.data):
    return scraped_data
  
  for region in data_handler.data.get("regions", []):
    region_name = region.get("region_name")
    if region_name:
      attractions = region.get("attractions", [])
      last_scrape = region.get("last_attractions_scrape_date", "")
      
      scraped_data[region_name] = {
        "last_scrape_date": last_scrape,
        "attractions_count": len(attractions)
      }
  
  return scraped_data