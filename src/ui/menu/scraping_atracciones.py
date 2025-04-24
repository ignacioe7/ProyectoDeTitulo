import streamlit as st
import asyncio
from src.core.scraper import AttractionScraper
from src.core.data_handler import DataHandler

data_handler = DataHandler()

def render():
    st.header("Scraping de Atracciones")
    regions = data_handler.load_regions()
    region_names = [region["nombre"] for region in regions]
    selected_region = st.selectbox("Selecciona una región", region_names)

    if st.button("Iniciar Scraping"):
        region = next(r for r in regions if r["nombre"] == selected_region)
        st.write(f"Iniciando scraping para la región: {selected_region}")
        async def scrape_attractions():
            async with AttractionScraper() as scraper:
                attractions = await scraper.get_all_attractions(region["url"])
                await data_handler.save_attractions(region["nombre"], attractions)
                st.success(f"Scraping completado para {selected_region}.")
        asyncio.run(scrape_attractions())