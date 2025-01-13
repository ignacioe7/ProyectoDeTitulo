import asyncio
import json
from src.scraping import scrape_things_to_do

async def run():
    things_to_do_data = await scrape_things_to_do(
        url="https://www.tripadvisor.com/Attraction_Review-g294306-d314672-Reviews-La_Sebastiana-Valparaiso_Valparaiso_Region.html",
        max_review_pages = None # TODAS LAS RESEÑAS
    )
    
    # Guardar datos en un archivo JSON
    with open('things_to_do_data.json', 'w', encoding='utf-8') as f:
        json.dump(things_to_do_data, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    asyncio.run(run())