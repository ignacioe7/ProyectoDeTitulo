# filepath: c:\Users\doria\Documents\GitHub\proyecto_cientifico\src\ui\streamlit_app.py
import sys
import os
import streamlit as st


# Añade el directorio raíz al path para importar módulos de src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.utils.logger import setup_logging
setup_logging()

from streamlit_option_menu import option_menu
from src.ui.menu import inicio, scraping_atracciones, scraping_resenas, analisis_sentimientos, resultados
from loguru import logger as log 
from src.core.data_handler import DataHandler

@st.cache_resource
def get_data_handler():
    log.info("Intentando cargar/recuperar instancia de DataHandler...")
    try:
        handler = DataHandler()
        log.success("Instancia de DataHandler cargada/recuperada.")
        return handler
    except Exception as e:
        log.critical(f"No se pudo inicializar DataHandler: {e}", exc_info=True)
        st.error(f"Error crítico al inicializar DataHandler: {e}. La aplicación puede no funcionar correctamente.")
        return None

log.info("Iniciando aplicación Streamlit...")

# Obtiene la instancia única de DataHandler (o None si falló)
data_handler = get_data_handler()

# Mensaje de advertencia si el analizador dentro del handler no cargó
if data_handler is None:
     st.sidebar.error("❌ Error crítico: DataHandler no disponible.")
     st.stop()


st.title("Proyecto de Scraping de TripAdvisor")

# Menú principal
with st.sidebar:
    selected = option_menu(
        menu_title="Menú",
        options=["Inicio", "Scraping de Atracciones", "Scraping de Reseñas", "Análisis de Sentimientos", "Resultados"],
        icons=["house", "search", "list-task", "bar-chart", "bar-chart"],
        menu_icon="cast",
        default_index=0,
        orientation="vertical",
    )

# Renderizar la opción seleccionada
if selected == "Inicio":
    inicio.render()
elif selected == "Scraping de Atracciones":
    if data_handler:
         scraping_atracciones.render(data_handler)
    else:
         st.error("DataHandler no disponible para Scraping de Atracciones.")
elif selected == "Scraping de Reseñas":
    if data_handler:
         scraping_resenas.render(data_handler)
    else:
         st.error("DataHandler no disponible para Scraping de Reseñas.")
elif selected == "Análisis de Sentimientos":
    if data_handler:
         analisis_sentimientos.render(data_handler)
    else:
         st.error("DataHandler no disponible para Análisis de Sentimientos.")
elif selected == "Resultados":
     if data_handler:
         resultados.render(data_handler)
     else:
         st.error("DataHandler no disponible para Resultados.")

log.debug(f"Opción seleccionada: {selected}") 