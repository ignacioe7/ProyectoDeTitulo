import streamlit as st
import asyncio
from src.core.data_handler import DataHandler

data_handler = DataHandler()

def render():
    st.header("Análisis de Sentimientos")
    regions = data_handler.load_regions()
    region_names = [region["nombre"] for region in regions]
    selected_region = st.selectbox("Selecciona una región", region_names)

    if st.button("Iniciar Análisis"):
        st.write(f"Iniciando análisis de sentimientos para la región: {selected_region}")
        success = asyncio.run(data_handler.analyze_and_update_excel(selected_region))
        if success:
            st.success(f"Análisis completado para {selected_region}.")
        else:
            st.error(f"Error durante el análisis para {selected_region}.")