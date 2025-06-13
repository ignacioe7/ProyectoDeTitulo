# APLICACIÓN PRINCIPAL DE STREAMLIT PARA ANÁLISIS DE SENTIMIENTOS TRIPADVISOR
# Archivo principal que configura la interfaz web y maneja la navegación entre módulos
# Implementa sistema de bloqueo de navegación durante procesos activos

import sys
import os
import streamlit as st

# configuración del path para importaciones relativas
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.utils import setup_logging  # config del logger
setup_logging()  # inicializar logging

from streamlit_option_menu import option_menu  # menu de navegación
from src.ui.menu import analyzer, attractions, filters, home, results, reviews  # módulos de las páginas
from loguru import logger as log 
from src.core.data_handler import DataHandler  # gestor de datos principal

# configuración inicial de la página web con layout amplio y sidebar expandido
st.set_page_config(
  page_title="Análisis de Sentimientos TripAdvisor",
  page_icon="📊",
  layout="wide",
  initial_sidebar_state="expanded"
)

# ====================================================================================================================
#                                             OBTENER GESTOR DE DATOS
# ====================================================================================================================

@st.cache_resource  # cachear para tener una única instancia
def get_data_handler():
  # OBTIENE INSTANCIA ÚNICA DE DATAHANDLER CON MANEJO DE ERRORES
  # Carga el gestor principal de datos con cache para evitar múltiples instancias
  # Retorna handler válido o None en caso de error crítico
  try:
    handler = DataHandler()
    log.info("DataHandler cargado exitosamente")
    return handler
  except Exception as e:
    log.error(f"Error crítico al cargar DataHandler: {e}")
    st.error(f"Error crítico DataHandler: {e}")
    return None

# ====================================================================================================================
#                                         OBTENER INFORMACIÓN DE PROCESOS ACTIVOS
# ====================================================================================================================

def get_active_process_info():
  # DETERMINA QUÉ PROCESO ESTÁ ACTUALMENTE EN EJECUCIÓN
  # Verifica estados de session para identificar procesos activos de scraping o análisis
  # Retorna tipo de proceso, nombre descriptivo y índice del menú correspondiente
  scraping_active = st.session_state.get('scraping_active', False)
  attractions_scraping_active = st.session_state.get('attractions_scraping_active', False)
  analysis_active = st.session_state.get('analysis_active', False)
  
  # mapeo de estados a información de proceso
  if scraping_active:
    return "scraping_reviews", "Scraping de Reseñas", 2
  elif attractions_scraping_active:
    return "scraping_attractions", "Scraping de Atracciones", 1
  elif analysis_active:
    return "analysis", "Análisis de Sentimientos", 3
  else:
    return None, None, 0

# ====================================================================================================================
#                                            INYECTAR ESTILOS CSS DE BLOQUEO
# ====================================================================================================================

def inject_blocking_css():
  # INYECTA ESTILOS CSS PARA BLOQUEAR NAVEGACIÓN DURANTE PROCESOS ACTIVOS
  # Aplica estilos que deshabilitan elementos del menú excepto el activo
  # Utiliza pointer-events y overlay visual para prevenir interacción del usuario
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

# ====================================================================================================================
#                                            MOSTRAR ADVERTENCIAS ANTI-BOT
# ====================================================================================================================

def show_anti_bot_warning():
  # MUESTRA ADVERTENCIAS SOBRE SISTEMAS ANTI-BOT DE TRIPADVISOR
  # Informa sobre detección automatizada y mejores prácticas para evitar bloqueos
  # Recomienda configuraciones de concurrencia seguras para scraping
  st.sidebar.markdown("---")
  st.sidebar.warning(
    "Importante: Si el scraping se completa muy rápido "
    "(menos de 30 segundos), es posible que TripAdvisor haya detectado "
    "actividad automatizada y esté bloqueando las peticiones."
  )
  st.sidebar.info(
    "Recomendación: Usa configuraciones de concurrencia bajas "
    "(1-2) y evita hacer scraping frecuente en períodos cortos."
  )

# ====================================================================================================================
#                                           INICIALIZACIÓN PRINCIPAL
# ====================================================================================================================

# obtener instancia única de DataHandler con manejo de errores
data_handler = get_data_handler()

# verificación crítica de disponibilidad del gestor de datos
if data_handler is None:
  st.sidebar.error("Error: DataHandler no disponible")
  st.stop()

# inicialización de estados de sesión para control de procesos activos
if 'scraping_active' not in st.session_state:
  st.session_state.scraping_active = False
if 'attractions_scraping_active' not in st.session_state:
  st.session_state.attractions_scraping_active = False
if 'analysis_active' not in st.session_state:
  st.session_state.analysis_active = False

# obtención de información sobre procesos activos actuales
process_type, process_name, active_index = get_active_process_info()
any_process_active = process_type is not None

# aplicación de estilos CSS de bloqueo si hay procesos activos
inject_blocking_css()

# ====================================================================================================================
#                                       CONSTRUCCIÓN DEL MENÚ LATERAL
# ====================================================================================================================

# CONSTRUCCIÓN DEL MENÚ LATERAL CON SISTEMA DE BLOQUEO
with st.sidebar:
  # mostrar indicadores visuales de estado activo
  if any_process_active:
    st.error(f"**{process_name.upper()} ACTIVO**")
    st.warning("Navegación bloqueada")
    st.markdown("---")
  
  # aplicar clases CSS de bloqueo condicionalmente
  menu_class = "blocked-menu" if any_process_active else ""
  
  if any_process_active:
    st.markdown(f'<div class="{menu_class}">', unsafe_allow_html=True)
  
  # generar key dinámica para mantener estado del menú
  menu_key = f"menu_{process_type}_{active_index}" if any_process_active else "menu_normal"
  
  # construir menú principal con opciones y iconos
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
  
  # cerrar contenedor de bloqueo y mostrar info
  if any_process_active:
    st.markdown('</div>', unsafe_allow_html=True)
    st.info("Solo la página activa es accesible")
  
  # mostrar advertencias específicas para páginas de scraping
  if selected in ["Scraping de Atracciones", "Scraping de Reseñas"]:
    show_anti_bot_warning()

# ====================================================================================================================
#                                      SISTEMA DE CONTROL DE NAVEGACIÓN
# ====================================================================================================================

# SISTEMA DE CONTROL DE NAVEGACIÓN FORZADA
if any_process_active:
  # mapeo de índices numéricos a nombres de páginas
  index_to_page = {
    0: "Inicio",
    1: "Scraping de Atracciones", 
    2: "Scraping de Reseñas",
    3: "Análisis de Sentimientos",
    4: "Resultados y Visualización",
    5: "Filtros y descargas"
  }
  
  # determinar página permitida según proceso activo
  allowed_page = index_to_page[active_index]
  
  # forzar retorno a página permitida si el usuario intenta cambiar
  if selected != allowed_page:
    st.error(f"**Acceso denegado durante {process_name.lower()}**")
    selected = allowed_page

# ====================================================================================================================
#                                     RENDERIZADO CONDICIONAL DE PÁGINAS
# ====================================================================================================================

# RENDERIZADO CONDICIONAL DE PÁGINAS CON CONTROL DE ACCESO
if selected == "Inicio":
  # renderizar página de inicio con bloqueo durante procesos activos
  if any_process_active:
    st.error(f"**Inicio no disponible durante {process_name.lower()}**")
    st.info("Detén el proceso activo para acceder")
  else:
    home.render()

elif selected == "Scraping de Atracciones":
  # renderizar módulo de scraping de atracciones con validación de estado
  if any_process_active and process_type != "scraping_attractions":
    st.error(f"**Scraping de Atracciones no disponible durante {process_name.lower()}**")
    st.info("Detén el proceso activo para acceder")
  elif data_handler:
    attractions.render(data_handler)
  else:
    st.error("DataHandler no disponible")

elif selected == "Scraping de Reseñas":
  # renderizar módulo de scraping de reseñas con validación de estado
  if any_process_active and process_type != "scraping_reviews":
    st.error(f"**Scraping de Reseñas no disponible durante {process_name.lower()}**")
    st.info("Detén el proceso activo para acceder")
  elif data_handler:
    reviews.render(data_handler) 
  else:
    st.error("DataHandler no disponible")

elif selected == "Análisis de Sentimientos":
  # renderizar módulo de análisis con validación de estado
  if any_process_active and process_type != "analysis":
    st.error(f"**Análisis no disponible durante {process_name.lower()}**")
    st.info("Detén el proceso activo para acceder")
  elif data_handler:
    analyzer.render(data_handler)
  else:
    st.error("DataHandler no disponible")

elif selected == "Resultados y Visualización":
  # renderizar módulo de resultados bloqueado durante procesos activos
  if any_process_active:
    st.error(f"**Resultados no disponibles durante {process_name.lower()}**")
    st.info("Detén el proceso activo para acceder")
  elif data_handler:
    results.render(data_handler)
  else:
    st.error("DataHandler no disponible")
  
elif selected == "Filtros y descargas":
  # renderizar módulo de filtros bloqueado durante procesos activos
  if any_process_active:
    st.error(f"**Filtros y descargas no disponibles durante {process_name.lower()}**")
    st.info("Detén el proceso activo para acceder")
  elif data_handler:
    filters.render(data_handler)
  else:
    st.error("DataHandler no disponible")

# registro básico de navegación para depuración y monitoreo
log.debug(f"Página activa: {selected} | Proceso en ejecución: {process_name or 'ninguno'}")