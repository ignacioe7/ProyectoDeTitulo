import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import streamlit as st
from streamlit_option_menu import option_menu
from src.ui.menu import inicio, scraping_atracciones, scraping_resenas, analisis_sentimientos, resultados


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
    scraping_atracciones.render()
elif selected == "Scraping de Reseñas":
    scraping_resenas.render()
elif selected == "Análisis de Sentimientos":
    analisis_sentimientos.render()
elif selected == "Resultados":
    resultados.render()

