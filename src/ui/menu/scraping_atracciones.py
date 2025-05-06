import streamlit as st
import asyncio
from src.core.scraper import AttractionScraper
from loguru import logger as log # Importa logger

def render(data_handler):
  st.header("Scraping de Atracciones")

  if data_handler is None:
      st.error("Error: DataHandler no disponible.")
      return

  regions = data_handler.load_regions()
  if not regions:
      st.warning("No se encontraron regiones.")
      return

  region_names = [region["nombre"] for region in regions]
  selected_region = st.selectbox("Selecciona una región para scrapear", region_names)

  if st.button("Iniciar Scraping de Atracciones"):
    region = next((r for r in regions if r["nombre"] == selected_region), None) # Busca la región de forma segura

    if region is None:
        st.error(f"No se encontró la configuración para la región: {selected_region}")
        return

    log.info(f"Iniciando scraping de atracciones para: {selected_region} ({region['url']})")

    with st.spinner(f"Buscando atracciones en {selected_region}... Esto puede tardar unos minutos."):
      try:
        async def scrape_attractions():
          async with AttractionScraper() as scraper:
            log.debug(f"Scraper de atracciones inicializado para {selected_region}")
            attractions = await scraper.get_all_attractions(region["url"])
            log.info(f"Se encontraron {len(attractions)} atracciones para {selected_region}")
            if attractions: # Solo guardar si se encontraron atracciones
                await data_handler.save_attractions(region["nombre"], attractions)
            else:
                log.warning(f"No se encontraron atracciones para guardar en {selected_region}")
            return len(attractions) # Devolver el número de atracciones encontradas

        num_attractions = asyncio.run(scrape_attractions())

        if num_attractions > 0:
            st.success(f"Scraping completado. Se guardaron {num_attractions} atracciones para {selected_region}.")
            log.success(f"Scraping de atracciones completado para {selected_region}. Guardadas {num_attractions} atracciones.")
        else:
            st.warning(f"Scraping completado, pero no se encontraron atracciones para {selected_region}.")
            log.warning(f"Scraping de atracciones completado para {selected_region}, pero no se encontraron atracciones.")

      except Exception as e:
        st.error(f"Ocurrió un error durante el scraping de atracciones: {e}")
        log.error(f"Error durante scraping de atracciones para {selected_region}: {e}", exc_info=True)