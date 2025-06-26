import sys
import os
import streamlit as st

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.utils import setup_logging  # config del logger
setup_logging()  # inicializar logging

from streamlit_option_menu import option_menu  # menu de navegación
from src.ui.menu import analyzer, attractions, filters, home, results, reviews  # módulos de las páginas
from loguru import logger as log 
from src.core.data_handler import DataHandler  # gestor de datos principal

# config de página
st.set_page_config(
  page_title="Análisis de Sentimientos TripAdvisor",
  page_icon="📊",
  layout="wide",
  initial_sidebar_state="expanded"
)

# ===============================================================
# OBTENER GESTOR DE DATOS
# ===============================================================

@st.cache_resource  # cachear para tener una única instancia
def get_data_handler():
  # CARGA INSTANCIA ÚNICA DE DATAHANDLER
  # Maneja datos consolidados y operaciones de scraping
  # Se cachea para evitar recargas innecesarias
  try:
    handler = DataHandler()
    log.info("DataHandler cargado")
    return handler
  except Exception as e:
    log.error(f"Fallo DataHandler: {e}")
    st.error(f"Error crítico DataHandler: {e}")
    return None

# ===============================================================
# OBTENER INFO DE PROCESOS ACTIVOS
# ===============================================================

def get_active_process_info():
  # OBTIENE INFORMACIÓN SOBRE PROCESOS ACTIVOS
  # Determina qué operación está ejecutándose actualmente
  # Retorna tipo, nombre y índice de página activa
  scraping_active = st.session_state.get('scraping_active', False)
  attractions_scraping_active = st.session_state.get('attractions_scraping_active', False)
  analysis_active = st.session_state.get('analysis_active', False)
  
  if scraping_active:
    return "scraping_reviews", "Scraping de Reseñas", 2
  elif attractions_scraping_active:
    return "scraping_attractions", "Scraping de Atracciones", 1
  elif analysis_active:
    return "analysis", "Análisis de Sentimientos", 3
  else:
    return None, None, 0

# ===============================================================
# INYECTAR CSS DE BLOQUEO
# ===============================================================

def inject_blocking_css():
  # INYECTA CSS QUE BLOQUEA CLICKS DEL MENÚ
  # Previene navegación durante procesos activos
  # Deshabilita elementos no activos visualmente
  st.markdown("""
  <style>
  /* Bloquear todos los elementos del menú excepto el activo */
  .blocked-menu .nav-link:not(.active) {
    pointer-events: none !important;
    opacity: 0.8 !important;
    cursor: not-allowed !important;
    background-color: #dc2d22 !important;
    color: #999 !important;
  }
  
  /* Mantener el elemento activo normal */
  .blocked-menu .nav-link.active {
    pointer-events: auto !important;
    opacity: 1 !important;
    cursor: pointer !important;
  }
  
  /* Overlay invisible para capturar clicks */
  .blocked-menu::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    z-index: 10;
    pointer-events: none;
  }
  
  /* Bloquear interacción con elementos específicos */
  .blocked-menu .nav-link:not(.active) * {
    pointer-events: none !important;
  }
  </style>
  """, unsafe_allow_html=True)

# ===============================================================
# MOSTRAR ADVERTENCIA ANTI-BOT
# ===============================================================

def show_anti_bot_warning():
  # MUESTRA ADVERTENCIA SOBRE SISTEMAS ANTI-BOT
  # Informa sobre detección de TripAdvisor y mejores prácticas
  # Ayuda a usuarios a evitar bloqueos durante scraping
  st.sidebar.markdown("---")
  st.sidebar.warning(
    "⚠️ **Importante:** Si el scraping se completa muy rápido "
    "(menos de 30 segundos), es posible que TripAdvisor haya detectado "
    "actividad automatizada y esté bloqueando las peticiones."
  )
  st.sidebar.info(
    "💡 **Recomendación:** Usa configuraciones de concurrencia bajas "
    "(1-2) y evita hacer scraping frecuente en períodos cortos."
  )

# Obtener instancia de DataHandler
data_handler = get_data_handler()

# Verificar si DataHandler se cargó correctamente
if data_handler is None:
  st.sidebar.error("Error: DataHandler no disponible")
  st.stop()

# Inicializar estados si no existen
if 'scraping_active' not in st.session_state:
  st.session_state.scraping_active = False
if 'attractions_scraping_active' not in st.session_state:
  st.session_state.attractions_scraping_active = False
if 'analysis_active' not in st.session_state:
  st.session_state.analysis_active = False

# Obtener info del proceso activo
process_type, process_name, active_index = get_active_process_info()
any_process_active = process_type is not None

# Inyectar CSS de bloqueo
inject_blocking_css()

# Menú principal en la barra lateral
with st.sidebar:
  # Mostrar estado si hay proceso activo
  if any_process_active:
    st.error(f"**{process_name.upper()} ACTIVO**")
    st.warning("Navegación bloqueada")
    st.markdown("---")
  
  # Aplicar clase de bloqueo condicionalmente
  menu_class = "blocked-menu" if any_process_active else ""
  
  if any_process_active:
    st.markdown(f'<div class="{menu_class}">', unsafe_allow_html=True)
  
  # Menú con key dinámica para forzar re-render
  menu_key = f"menu_{process_type}_{active_index}" if any_process_active else "menu_normal"
  
  selected = option_menu(
    menu_title="Menú Principal",
    options=[
      "Inicio", 
      "Scraping de Atracciones", 
      "Scraping de Reseñas", 
      "Análisis de Sentimientos", 
      "Resultados y Visualización",
      "Filtros y descargas"
    ],
    icons=["house", "geo-alt-fill", "card-list", "graph-up-arrow", "clipboard-data", "filter"],
    menu_icon="list-ul",
    default_index=active_index if any_process_active else 0,
    orientation="vertical",
    key=menu_key
  )
  
  if any_process_active:
    st.markdown('</div>', unsafe_allow_html=True)
    st.info("Solo la página activa es accesible")
  
  # Mostrar advertencia anti-bot para páginas de scraping
  if selected in ["Scraping de Atracciones", "Scraping de Reseñas"]:
    show_anti_bot_warning()

# Forzar selección si hay proceso activo
if any_process_active:
  # Mapeo de índices a páginas
  index_to_page = {
    0: "Inicio",
    1: "Scraping de Atracciones", 
    2: "Scraping de Reseñas",
    3: "Análisis de Sentimientos",
    4: "Resultados y Visualización",
    5: "Filtros y descargas"
  }
  
  # Obtener página activa permitida
  allowed_page = index_to_page[active_index]
  
  # Si el usuario logra cambiar forzar regreso
  if selected != allowed_page:
    st.error(f"**Acceso denegado durante {process_name.lower()}**")
    selected = allowed_page

# Renderizar la página seleccionada
if selected == "Inicio":
  if any_process_active:
    st.error(f"**Inicio no disponible durante {process_name.lower()}**")
    st.info("Detén el proceso activo para acceder")
  else:
    home.render()

elif selected == "Scraping de Atracciones":
  if any_process_active and process_type != "scraping_attractions":
    st.error(f"**Scraping de Atracciones no disponible durante {process_name.lower()}**")
    st.info("Detén el proceso activo para acceder")
  elif data_handler:
    attractions.render(data_handler)
  else:
    st.error("DataHandler no disponible")

elif selected == "Scraping de Reseñas":
  if any_process_active and process_type != "scraping_reviews":
    st.error(f"**Scraping de Reseñas no disponible durante {process_name.lower()}**")
    st.info("Detén el proceso activo para acceder")
  elif data_handler:
    reviews.render(data_handler) 
  else:
    st.error("DataHandler no disponible")

elif selected == "Análisis de Sentimientos":
  if any_process_active and process_type != "analysis":
    st.error(f"**Análisis no disponible durante {process_name.lower()}**")
    st.info("Detén el proceso activo para acceder")
  elif data_handler:
    analyzer.render(data_handler)
  else:
    st.error("DataHandler no disponible")

elif selected == "Resultados y Visualización":
  if any_process_active:
    st.error(f"**Resultados no disponibles durante {process_name.lower()}**")
    st.info("Detén el proceso activo para acceder")
  elif data_handler:
    results.render(data_handler)
  else:
    st.error("DataHandler no disponible")
  
elif selected == "Filtros y descargas":
  if any_process_active:
    st.error(f"**Filtros y descargas no disponibles durante {process_name.lower()}**")
    st.info("Detén el proceso activo para acceder")
  elif data_handler:
    filters.render(data_handler)
  else:
    st.error("DataHandler no disponible")

# Log mínimo
log.debug(f"Página: {selected} | Proceso: {process_name or 'ninguno'}")