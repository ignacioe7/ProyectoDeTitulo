import streamlit as st # Para la interfaz
import asyncio # Para cosas asíncronas
from src.core.scraper import ReviewScraper # El scraper de reseñas


def render(data_handler):
  st.header("Scraping de Reseñas") # Título
  regions = data_handler.load_regions() # Cargamos regiones
  region_names = [region["nombre"] for region in regions] # Nombres para el selector
  selected_region = st.selectbox("Selecciona una región", region_names) # El selector

  # Cuántas páginas como máximo
  max_pages = st.number_input("Máximo de páginas a extraer (0 para todas)", min_value=0, step=1)

  if st.button("Iniciar Scraping"): # Si se pulsa el botón
    # Buscamos la región elegida
    region = next(r for r in regions if r["nombre"] == selected_region)
    st.write(f"Iniciando scraping de reseñas para la región: {selected_region}") # Mensaje de inicio

    # Función asíncrona para el scraping
    async def scrape_reviews():
      attractions = data_handler.load_attractions(region["nombre"]) # Cargamos atracciones de la región
      async with ReviewScraper() as scraper: # Usamos el scraper
        # Sacamos las reseñas, ojo con el max_pages que no se usa aquí
        results = await scraper.scrape_multiple_attractions(attractions)
        # Preparamos los datos
        region_data = {
          "region": region["nombre"],
          "attractions": results,
          "scrape_date": "2023-10-01" # Fecha fija, cuidado
        }
        # Exportamos a Excel
        await data_handler.export_reviews(region_data, format="excel")
        st.success(f"Scraping de reseñas completado para {selected_region}") # Mensaje de éxito
    # Ejecutamos la función asíncrona
    asyncio.run(scrape_reviews())