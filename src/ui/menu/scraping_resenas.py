import streamlit as st
import asyncio
from src.core.scraper import ReviewScraper
from src.core.data_handler import DataHandler

data_handler = DataHandler()

def render():
    st.header("Scraping de Reseñas")
    regions = data_handler.load_regions()
    region_names = [region["nombre"] for region in regions]
    selected_region = st.selectbox("Selecciona una región", region_names)

    max_pages = st.number_input("Máximo de páginas a extraer (0 para todas)", min_value=0, step=1)

    if st.button("Iniciar Scraping"):
        region = next(r for r in regions if r["nombre"] == selected_region)
        st.write(f"Iniciando scraping de reseñas para la región: {selected_region}")
        async def scrape_reviews():
            attractions = data_handler.load_attractions(region["nombre"])
            async with ReviewScraper() as scraper:
                results = await scraper.scrape_multiple_attractions(attractions)
                region_data = {
                    "region": region["nombre"],
                    "attractions": results,
                    "scrape_date": "2023-10-01"
                }
                await data_handler.export_reviews(region_data, format="excel")
                st.success(f"Scraping de reseñas completado para {selected_region}.")
        asyncio.run(scrape_reviews())