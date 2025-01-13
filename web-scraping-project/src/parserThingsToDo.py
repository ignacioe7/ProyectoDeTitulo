import json
from typing import Dict
from httpx import Response
from parsel import Selector
from loguru import logger

def parse_things_to_do_page(result: Response) -> Dict:
    """Parse things to do data from the page"""
    selector = Selector(result.text)
    place_name = selector.css("h1.biGQs._P.fiohW.eIegw::text").get()
    attraction = selector.css("span.eojVo::text").get()
    score = selector.css("div[data-automation='reviewBubbleScore']::text").get()
    total_reviews = selector.css("span[data-automation='reviewCount']::text").get()
    
    review_counts = {
        "excellent": selector.xpath("//div[@class='jxnKb'][1]//div[@class='biGQs _P fiohW biKBZ osNWb']/text()").get(),
        "very_good": selector.xpath("//div[@class='jxnKb'][2]//div[@class='biGQs _P fiohW biKBZ osNWb']/text()").get(),
        "average": selector.xpath("//div[@class='jxnKb'][3]//div[@class='biGQs _P fiohW biKBZ osNWb']/text()").get(),
        "poor": selector.xpath("//div[@class='jxnKb'][4]//div[@class='biGQs _P fiohW biKBZ osNWb']/text()").get(),
        "terrible": selector.xpath("//div[@class='jxnKb'][5]//div[@class='biGQs _P fiohW biKBZ osNWb']/text()").get(),
    }


    reviews = []
    review_section = selector.xpath("//div[@class='LbPSX']")
    review_cards = review_section.xpath(".//div[@class='_c' and @data-automation='reviewCard']")
    
    for card in review_cards:
        # Extraer nombre de usuario, ubicación y total de contribuciones
        user_info_block = card.xpath(".//div[@class='QIHsu Zb']")
        name = user_info_block.xpath(".//span[@class='biGQs _P fiohW fOtGX']/a/text()").get() or "NO NAME"
        info_spans = user_info_block.xpath(".//div[@class='vYLts']//div[@class='biGQs _P pZUbB osNWb']/span/text()").getall()
        if len(info_spans) == 1:
            location = "NO LOCATION"
            contributions = info_spans[0]
        elif len(info_spans) == 2:
            location = info_spans[0]
            contributions = info_spans[1]
        else:
            location = "NO LOCATION"
            contributions = "NO INFORMATION"
        
        # Extraer calificación de la reseña
        rating = card.xpath(".//svg[contains(@class, 'UctUV')]//title/text()").get()
        if rating:
            rating = float(rating.split()[0])
        else:
            rating = 0.0

        title = card.xpath(".//div[contains(@class, 'biGQs')]//span[@class='yCeTE']/text()").get() or "NO TITLE"

        # Extraer información de fecha de visita y tipo de compañía
        visit_info = card.xpath(".//div[@class='RpeCd']/text()").get() or "NO DATE"
        if "•" in visit_info:
            visit_date, companion_type = visit_info.split("•")
            visit_date = visit_date.strip()
            companion_type = companion_type.strip()
        else:
            visit_date = visit_info.strip()
            companion_type = "NO INFORMATION"

        # Extraer texto de la reseña
        review_text = card.xpath(".//span[@class='JguWG']//span[@class='yCeTE']/text()").get() or "NO REVIEW TEXT"

        # Extraer fecha de la reseña
        written_date = card.xpath(".//div[contains(@class, 'ncFvv')]/text()").get() or "NO WRITTEN DATE"
        if written_date.startswith("Written "):
            written_date = written_date.replace("Written ", "")

        reviews.append({
            "username": name,
            "location": location,
            "contributions": contributions,
            "rating": rating,
            "title": title,
            "visit_date": visit_date,
            "companion_type": companion_type,
            "review_text": review_text,
            "written_date": written_date
        })

    return {
        "place_name": place_name,
        "attraction": attraction,
        "score": score,
        "total_reviews": total_reviews,
        "review_counts": review_counts,
        "reviews": reviews
    }