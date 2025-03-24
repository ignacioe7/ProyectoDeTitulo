import json
import asyncio
from datetime import datetime
from typing import Dict, List, Optional
import httpx
from loguru import logger as log

# Importar módulos propios
from constants import HEADERS
from places import AttractionScraper
from parserThingsToDo import count_attraction_metrics, extract_reviews_from_all_pages, scrape_attraction_with_metrics
from exporters import save_to_excel, load_json


async def scrape_attraction_reviews(client: httpx.AsyncClient, url: str, attraction_metadata: Dict, max_pages: Optional[int] = None) -> Dict:
    """
    Extrae todas las reseñas de un lugar con paginación usando metadatos conocidos.
    """
    reviews_data = []
    base_url = url.split('?')[0]

    # Usar metadatos conocidos
    place_name = attraction_metadata["place_name"]
    total_reviews = attraction_metadata["reviews_count"]
    place_type = attraction_metadata["place_type"]
    rating = attraction_metadata["rating"]

    # Obtener métricas actualizadas
    updated_metrics = await count_attraction_metrics(client, url, total_reviews)
    attraction_metadata.update(updated_metrics)

    # Calcular páginas a procesar
    total_pages = attraction_metadata.get("total_pages", 1)
    max_pages_limit = min(total_pages, max_pages) if max_pages is not None else total_pages

    log.info(f"Procesando {place_name}: {updated_metrics['reviews_count']} reseñas en {max_pages_limit} páginas")

    # Extraer reseñas de todas las páginas
    reviews_data = await extract_reviews_from_all_pages(client, base_url, attraction_metadata, max_pages_limit)

    return {
        **attraction_metadata,
        "url": base_url,
        "reviews": reviews_data
    }


async def smart_sleep(page: int):
    """
    Realiza pausas inteligentes para evitar ser bloqueado.
    """
    if page % 50 == 0:
        pause_time = 45
    elif page % 10 == 0:
        pause_time = 15
    else:
        pause_time = 2 + (page // 100)
    log.info(f"Pausa de {pause_time} segundos...")
    await asyncio.sleep(pause_time)


async def process_attractions_with_reviews(attractions: List[Dict], max_pages: Optional[int] = None) -> Dict:
    """
    Procesa las atracciones que tienen reseñas (reviews_count > 0) y recopila sus reseñas.
    Guarda los datos de cada atracción individualmente.
    """
    valparaiso_data = {
        "city": "Valparaíso",
        "total_attractions": len(attractions),
        "attractions": [],
        "attractions_without_reviews": [],
        "scrape_date": datetime.now().isoformat(),
    }

    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as client:
        for i, attraction in enumerate(attractions, 1):
            url = attraction["url"]
            basic_metadata = {
                "place_name": attraction.get("place_name", "Desconocido"),
                "place_type": attraction.get("place_type", "Desconocido"),
                "rating": attraction.get("rating", 0.0),
                "reviews_count": attraction.get("reviews_count", 0)
            }

            log.info(f"\n{'='*80}")
            log.info(f"Procesando atracción con reseñas [{i}/{len(attractions)}]: {basic_metadata['place_name']}")

            try:
                # Llamar a la función que recopila las reseñas
                attraction_data = await scrape_attraction_reviews(client, url, basic_metadata, max_pages)

                if attraction_data:
                    # Añadir a la estructura principal
                    valparaiso_data["attractions"].append(attraction_data)

                    # Guardar los datos de la atracción individualmente
                    await save_to_excel(valparaiso_data, 'valparaiso_reviews.xlsx')
                    log.info(f"Datos guardados para: {attraction_data['place_name']}")

                    # Tomar un descanso de 15 segundos antes de continuar
                    await asyncio.sleep(15)
            except Exception as e:
                log.error(f"Error procesando {basic_metadata['place_name']}: {e}")
                log.info("Continuando con la siguiente atracción...")
                await asyncio.sleep(15)

    return valparaiso_data


async def process_attractions_without_reviews(attractions: List[Dict]) -> Dict:
    """
    Procesa las atracciones que no tienen reseñas (reviews_count == 0).
    """
    valparaiso_data = {
        "city": "Valparaíso",
        "total_attractions": len(attractions),
        "attractions": [],
        "attractions_without_reviews": [],
        "scrape_date": datetime.now().isoformat(),
    }

    for attraction in attractions:
        empty_attraction = {
            "place_name": attraction.get("place_name", "Desconocido"),
            "place_type": attraction.get("place_type", "Desconocido"),
            "score": attraction.get("rating", 0.0),
            "url": attraction.get("url", ""),
            "total_reviews": 0,
            "reviews": []
        }
        valparaiso_data["attractions"].append(empty_attraction)
        log.info(f"Agregada atracción sin reseñas: {empty_attraction['place_name']}")

    return valparaiso_data


async def main():
    """
    Función principal de ejecución.
    """
    log.info("Analizador de Sentimiento para Atracciones Turísticas")
    log.info("1. Extraer atracciones y reseñas")
    log.info("2. Cargar URLs desde JSON y extraer reseñas")

    try:
        # Obtener opción del usuario
        choice = input("\nIngrese su opción (1, 2): ")

        if choice == "1":
            # Opción 1: Extraer todas las atracciones y luego sus reseñas
            log.info("Iniciando extracción completa")
            async with AttractionScraper() as scraper:
                attractions = await scraper.get_all_attractions()
                attraction_urls = [attraction["url"] for attraction in attractions]
                log.info(f"Encontradas {len(attraction_urls)} atracciones para procesar")

                # Separar atracciones con y sin reseñas
                attractions_with_reviews = [attraction for attraction in attractions if attraction.get("reviews_count", 0) > 0]
                attractions_without_reviews = [attraction for attraction in attractions if attraction.get("reviews_count", 0) == 0]

                # Procesar atracciones con reseñas
                valparaiso_data = await process_attractions_with_reviews(attractions_with_reviews)

                # Procesar atracciones sin reseñas
                valparaiso_data_without_reviews = await process_attractions_without_reviews(attractions_without_reviews)

                # Combinar los datos de ambas listas
                valparaiso_data["attractions"].extend(valparaiso_data_without_reviews["attractions"])

                # Guardar el archivo final con todas las atracciones
                await save_to_excel(valparaiso_data, 'valparaiso_reviews.xlsx')

                log.info("Proceso de extracción completado")

        elif choice == "2":
            # Opción 2: Cargar URLs desde JSON y extraer reseñas
            log.info("Iniciando opción 2: Cargar URLs desde JSON y extraer reseñas")
            try:
                # Cargar datos completos desde JSON
                attractions_data = await load_json('attractions_data.json')
                attractions = attractions_data["attractions"]

                log.info(f"Cargados datos de {len(attractions)} atracciones desde archivo")

                # Separar atracciones con y sin reseñas
                attractions_with_reviews = [attraction for attraction in attractions if attraction.get("reviews_count", 0) > 0]
                attractions_without_reviews = [attraction for attraction in attractions if attraction.get("reviews_count", 0) == 0]

                log.info(f"Encontradas {len(attractions_with_reviews)} atracciones con reseñas y {len(attractions_without_reviews)} sin reseñas")

                # Obtener máximo de páginas del usuario
                max_pages_input = input("\nIngrese número máximo de páginas a extraer (o Enter para todas): ")
                max_review_pages = int(max_pages_input) if max_pages_input.strip() else None

                # Procesar atracciones con reseñas
                valparaiso_data = await process_attractions_with_reviews(attractions_with_reviews, max_review_pages)

                # Procesar atracciones sin reseñas
                valparaiso_data_without_reviews = await process_attractions_without_reviews(attractions_without_reviews)

                # Combinar los datos de ambas listas
                valparaiso_data["attractions"].extend(valparaiso_data_without_reviews["attractions"])

                # Guardar el archivo final con todas las atracciones
                await save_to_excel(valparaiso_data, 'valparaiso_reviews.xlsx')

                log.info("Proceso de extracción completado")

            except FileNotFoundError:
                log.error("No se encontró el archivo attractions_data.json")
                return 1
            except json.JSONDecodeError:
                log.error("El archivo attractions_data.json tiene un formato inválido")
                return 1
            except KeyError:
                log.error("El archivo attractions_data.json no tiene la estructura esperada")
                return 1

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