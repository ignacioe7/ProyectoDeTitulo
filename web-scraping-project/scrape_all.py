from datetime import datetime
import asyncio
from typing import Dict, List, Optional
import httpx
from loguru import logger as log

# Importar módulos propios
from constants import HEADERS
from places import AttractionScraper
from parserThingsToDo import parse_things_to_do_page
from exporters import save_to_json, save_to_excel


async def scrape_attraction_reviews(client: httpx.AsyncClient, url: str, max_pages: Optional[int] = None) -> Dict:
  """Extrae todas las reseñas de un lugar con paginación"""
  reviews_data = []
  current_page = 1
  base_url = url.split('?')[0]
  
  # Establecer límite de páginas (infinito si es None)
  max_pages_limit = max_pages if max_pages is not None else float('inf')

  while current_page <= max_pages_limit:
    # Generar URL con offset apropiado
    if current_page == 1:
      page_url = base_url  # Primera página sin offset
    else:
      offset = (current_page - 1) * 10
      page_url = page_url = base_url.replace("-Reviews-", f"-Reviews-or{offset}-")
      if "-Reviews-" not in base_url:
        page_url = base_url.replace("Review-", f"Review-or{offset}-")
    
    # Obtener datos de esta página
    try:
      page_data = await parse_things_to_do_page(await client.get(page_url), client)
      
      # Agregar reseñas a la lista principal
      reviews_data.extend(page_data["reviews"])
      
      # Terminar si es la última página o no tiene más reseñas
      if page_data.get("is_last_page", False) or len(page_data["reviews"]) == 0:
        log.info(f"Completada extracción de {page_data['place_name']}: {len(reviews_data)} reseñas totales")
        break
      
      # Avanzar a la siguiente página
      current_page += 1
      await asyncio.sleep(2)  # Pausa entre páginas
    except Exception as e:
      log.error(f"Error procesando página {current_page}: {str(e)}")
      break
  
  # Verificar si pudimos extraer datos
  if not reviews_data or 'page_data' not in locals():
    log.warning(f"No se pudieron extraer datos de {url}")
    return None
    
  return {
    "place_name": page_data["place_name"],
    "attraction": page_data["attraction"],
    "score": page_data["score"],
    "url": url,
    "total_reviews": page_data["total_reviews"],
    "available_reviews": page_data["available_reviews"],
    "reviews_collected": len(reviews_data),
    "review_counts": page_data["review_counts"],
    "reviews": reviews_data
  }

async def process_attractions(urls: List[str], max_pages: Optional[int] = None) -> Dict:
  """Procesa una lista de lugares y recolecta sus reseñas"""
  # Inicializar estructura de datos
  valparaiso_data = {
    "city": "Valparaíso",
    "total_attractions": 0,
    "attractions": [],
    "scrape_date": datetime.now().isoformat(),
  }
  
  # Procesar cada atracción
  async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as client:
    for i, url in enumerate(urls, 1):
      log.info(f"\n{'='*80}")
      log.info(f"Procesando atracción {i}/{len(urls)}")
      log.info(f"URL: {url}")
      
      # Extraer reseñas para esta atracción
      attraction_data = await scrape_attraction_reviews(client, url, max_pages)
      
      if attraction_data:
        # Añadir a la estructura principal
        valparaiso_data["attractions"].append(attraction_data)
        valparaiso_data["total_attractions"] = len(valparaiso_data["attractions"])
        
        # Guardar progreso después de cada atracción
        await save_to_json(valparaiso_data, 'valparaiso_attractions.json')
        await save_to_excel(valparaiso_data)
        
        log.info(f"Datos guardados para: {attraction_data['place_name']}")
  
  # Devolver todos los datos recopilados
  return valparaiso_data

async def main():
  """Función principal de ejecución"""
  log.info("Analizador de Sentimiento para Atracciones Turísticas")
  log.info("1. Extraer atracciones y reseñas")
  log.info("2. Cargar URLs desde JSON y extraer reseñas")
  
  
  try:
    # Obtener opción del usuario
    choice = input("\nIngrese su opción (1, 2): ")
    
    if choice == "1":
      # Flujo completo: extraer atracciones y luego sus reseñas
      log.info("Iniciando extracción completa")
      async with AttractionScraper() as scraper:
        attraction_urls = await scraper.get_all_attractions()
        log.info(f"Encontradas {len(attraction_urls)} atracciones para procesar")
      
    elif choice == "2":
      # Extraer solo las reseñas de URLs guardadas
      from places import load_urls_from_json
      log.info("Cargando URLs desde attractions_urls.json")
      attraction_urls = await load_urls_from_json()
      if not attraction_urls:
        log.error("No se encontraron URLs en el archivo JSON")
        return 1
      log.info(f"Cargadas {len(attraction_urls)} URLs desde archivo")
    
    else:
      log.error("Opción inválida")
      return 1
    
    # Si llegamos aquí, estamos en el flujo 1 o 2 (extracción)
    
    # Obtener máximo de páginas del usuario
    max_pages_input = input("\nIngrese número máximo de páginas a extraer (o Enter para todas): ")
    max_review_pages = int(max_pages_input) if max_pages_input.strip() else None
    
    # Procesar atracciones
    attractions_data = await process_attractions(attraction_urls, max_review_pages)
    
  except Exception as e:
    log.error(f"Error en la ejecución: {e}")
    import traceback
    log.error(traceback.format_exc())
    return 1
  
  log.info("Proceso completado exitosamente")
  return 0

if __name__ == "__main__":
  # Configurar logging
  log.remove()
  log.add("scraper.log", rotation="1 day")
  log.add(lambda msg: print(msg), colorize=True, format="<cyan>{message}</cyan>")
  
  # Ejecutar el script
  exit_code = asyncio.run(main())
  exit(exit_code)