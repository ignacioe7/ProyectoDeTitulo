import asyncio
import math
from typing import Optional, Dict, List
from httpx import AsyncClient
from .parserThingsToDo import parse_things_to_do_page

async def scrape_things_to_do(url: str, max_review_pages: Optional[int] = None) -> Dict:
    """Scrape things to do data and reviews"""
    async with AsyncClient(
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        follow_redirects=True
    ) as client:
        try:
            # Tomar datos de la primera página
            first_page = await client.get(url)
            assert first_page.status_code != 403, "request is blocked"
            things_to_do_data = parse_things_to_do_page(first_page)

            # Calcular número de páginas de reseñas
            total_reviews = int(things_to_do_data["total_reviews"].split()[0].replace(",", ""))
            reviews_per_page = 10
            total_pages = math.ceil(total_reviews / reviews_per_page)
            
            if max_review_pages:
                total_pages = min(total_pages, max_review_pages)

            # Generar URLs de páginas adicionales
            review_urls = [
                url.replace("-Reviews-", f"-Reviews-or{reviews_per_page * i}-")
                for i in range(1, total_pages)
            ]

            # Scrapear páginas adicionales
            tasks = [client.get(url) for url in review_urls]
            responses = await asyncio.gather(*tasks)

            # Combinar datos de todas las páginas
            for response in responses:
                if response.status_code == 200:
                    page_data = parse_things_to_do_page(response)
                    things_to_do_data["reviews"].extend(page_data["reviews"])

            return things_to_do_data

        except Exception as e:
            print(f"Error scraping data: {e}")
            return None