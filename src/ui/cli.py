from datetime import datetime
from typing import List, Dict, Optional
from loguru import logger as log
import pandas as pd
from ..core.scraper import AttractionScraper, ReviewScraper
from ..core.data_handler import DataHandler
from ..core.analyzer import SentimentAnalyzer
from ..models.attraction import Attraction

class ScrapeAllCLI:
    """Interfaz de línea de comandos con capacidad para evolucionar a GUI"""
    
    def __init__(self):
        self.data_handler = DataHandler()
        self.regions = self._load_regions()
        self.current_region = None
    
    async def run(self):
        """Punto de entrada principal"""
        while True:
            choice = self._show_main_menu()
            
            if choice == "1":
                await self.handle_attraction_scraping()
            elif choice == "2":
                await self.handle_review_scraping()
            elif choice == "3":
                await self.handle_sentiment_analysis()
            elif choice.lower() == "q":
                break
            else:
                log.error("Opción inválida")
    
    async def handle_attraction_scraping(self):
        """Menú para scraping de atracciones"""
        region = self._select_region()
        async with AttractionScraper() as scraper:
            attractions = await scraper.get_all_attractions(region['url'])
            await self.data_handler.save_attractions(region['nombre'], attractions)
    
    async def handle_review_scraping(self):
        """Versión corregida del scraping de reseñas"""
        region = self._select_region()
        max_pages = self._get_max_pages_input()
    
        try:
            # Cargar atracciones
            attractions = self.data_handler.load_attractions(region['nombre'])
            if not attractions:
                log.error(f"No se encontraron atracciones para {region['nombre']}. ¿Ejecutó primero la opción 1?")
                return
    
            # Filtrar atracciones con reseñas
            attractions_with_reviews = [
                attr for attr in attractions 
                if isinstance(attr, Attraction) and getattr(attr, 'reviews_count', 0) > 0
            ]
    
            if not attractions_with_reviews:
                log.warning("No hay atracciones con reseñas para extraer")
                return
    
            # Procesar las atracciones
            async with ReviewScraper() as scraper:
                results = await scraper.scrape_multiple_attractions(attractions_with_reviews)
    
                # Verificar duplicados antes de exportar
                for result in results:
                    if 'reviews' in result:
                        unique_reviews = []
                        seen_hashes = set()
                        for review in result['reviews']:
                            review_hash = hash((
                                review.get('username', ''),
                                review.get('title', ''),
                                review.get('written_date', ''),
                                str(review.get('rating', 0))
                            ))
                            if review_hash not in seen_hashes:
                                seen_hashes.add(review_hash)
                                unique_reviews.append(review)
                        result['reviews'] = unique_reviews
    
                # Preparar datos para exportación
                region_data = {
                    "region": region['nombre'],
                    "attractions": [],
                    "scrape_date": datetime.now().isoformat()
                }
    
                for attr, result in zip(attractions_with_reviews, results):
                    attraction_data = {
                        **attr.__dict__,
                        **result,
                        "total_reviews": getattr(attr, 'reviews_count', 0),
                        "english_reviews": result.get('english_reviews', 0)
                    }
                    region_data['attractions'].append(attraction_data)
    
                await self.data_handler.export_reviews(region_data, format="excel")

        except Exception as e:
            log.error(f"Error en el scraping: {str(e)}")
            raise
    
    async def handle_sentiment_analysis(self):
        """Versión mejorada con feedback más claro al usuario"""
        region = self._select_region()

        log.info(f"\nIniciando análisis para: {region['nombre']}")

        try:
            # Verificar pre-requisitos
            if not hasattr(self.data_handler, 'analyzer') or self.data_handler.analyzer is None:
                log.error("\n❌ Error: El analizador no está configurado correctamente")
                log.info("🔧 Por favor instale los requerimientos:")
                log.info("pip install torch transformers")
                return False

            # Ejecutar análisis
            start_time = datetime.now()
            success = await self.data_handler.analyze_and_update_excel(region['nombre'])
            elapsed = (datetime.now() - start_time).total_seconds()

            if success:
                file_path = self.data_handler.get_reviews_filepath(region['nombre'])
                log.success(f"\n✅ Análisis completado en {elapsed:.2f} segundos")
                log.info(f"📊 Archivo actualizado: {file_path}")

            else:
                log.error("\n❌ No se pudo completar el análisis")

            return success

        except Exception as e:
            log.error(f"\n⚠️ Error inesperado: {str(e)}")
            return False
    
    # Métodos auxiliares (fáciles de migrar a GUI después)
    def _show_main_menu(self) -> str:
        """Muestra el menú principal y captura la selección"""
        log.info("\n=== MENÚ PRINCIPAL ===")
        log.info("1. Extraer atracciones de una región")
        log.info("2. Extraer reseñas de atracciones")
        log.info("3. Analizar sentimiento de reseñas")
        log.info("Q. Salir")
        return input("\nSeleccione una opción: ").strip()
    
    def _select_region(self) -> Dict:
        """Permite seleccionar una región de la lista"""
        log.info("\nRegiones disponibles:")
        for i, region in enumerate(self.regions, 1):
            log.info(f"{i}. {region['nombre']}")
        
        while True:
            try:
                choice = int(input("\nSeleccione región: ")) - 1
                if 0 <= choice < len(self.regions):
                    self.current_region = self.regions[choice]
                    return self.current_region
                log.error("Número fuera de rango")
            except ValueError:
                log.error("Ingrese un número válido")
    
    def _get_max_pages_input(self) -> Optional[int]:
        """Captura el límite de páginas para scraping"""
        input_str = input("\nLímite de páginas a extraer (Enter para todas): ").strip()
        return int(input_str) if input_str else None
    
    def _load_regions(self) -> List[Dict]:
        """Carga las regiones desde el archivo JSON"""
        return self.data_handler.load_regions()
    
# Preparación para futura GUI
class ScrapeAllGUI(ScrapeAllCLI):
    """Versión gráfica que hereda la misma lógica de negocio"""
    def __init__(self):
        super().__init__()
        # Aquí iría la inicialización de componentes GUI
    
    def _show_main_menu(self) -> str:
        # Sobreescribir métodos para usar GUI
        pass