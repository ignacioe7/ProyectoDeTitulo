import json
import re
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from loguru import logger as log

from .constants import PathConfig

class DataExporter:
    """Maneja la exportación de datos a diferentes formatos"""
    
    def __init__(self):
        self.paths = PathConfig()
        self._ensure_dirs()
    
    def _ensure_dirs(self):
        """Crea los directorios necesarios"""
        Path(self.paths.ATTRACTIONS_DIR).mkdir(parents=True, exist_ok=True)
        Path(self.paths.OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    async def save_to_json(self, data: Dict, filename: str = None) -> Path:
        """Guarda datos en formato JSON"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = filename or f"attractions_{timestamp}.json"
        filepath = Path(self.paths.ATTRACTIONS_DIR) / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            log.success(f"Datos JSON guardados en {filepath}")
            return filepath
        except Exception as e:
            log.error(f"Error guardando JSON: {e}")
            raise

    async def save_to_excel(self, region_data: Dict, filename: str = None) -> Path:
        """Exporta datos a Excel usando nombres consistentes"""
        region_name = region_data.get('region', 'unknown')

        def sanitize_region_name(name: str) -> str:
            """Función local para sanitizar nombres"""
            name = re.sub(r'[XIV]+-?\s*', '', name, flags=re.IGNORECASE)
            name = re.sub(r'[^\w\s]', '', name.lower())
            name = re.sub(r'[\s-]+', '_', name).strip('_')
            name = (name.replace('á', 'a').replace('é', 'e').replace('í', 'i')
                   .replace('ó', 'o').replace('ú', 'u').replace('ñ', 'n'))
            return name

        sanitized_name = sanitize_region_name(region_name)
        filename = filename or f"{sanitized_name}_reviews.xlsx"
        filepath = Path(self.paths.OUTPUT_DIR) / filename

        try:
            with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
                # Hoja de resumen
                summary_df = self._create_summary_df(region_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)

                # Hoja de reseñas
                reviews_df = self._create_reviews_df(region_data)
                reviews_df.to_excel(writer, sheet_name='Reviews', index=False)

                # Ajustar columnas
                self._adjust_column_widths(writer, [summary_df, reviews_df])

            log.success(f"Archivo Excel creado: {filepath}")
            return filepath
        except Exception as e:
            log.error(f"Error generando Excel: {e}")
            raise

    def _create_summary_df(self, data: Dict) -> pd.DataFrame:
        """Crea DataFrame para la hoja de resumen con los nuevos campos"""
        rows = []

        for attraction in data['attractions']:
            # Extraer datos relevantes
            rows.append({
                'Attraction Name': attraction.get('place_name'),
                'Type': attraction.get('place_type'),
                'Rating': attraction.get('rating', 0.0),
                'Total Reviews': attraction.get('total_reviews', 0),
                'Total English Reviews': attraction.get('english_reviews', 0),
                'URL': attraction.get('url', '')
            })

        return pd.DataFrame(rows)

    def _create_reviews_df(self, data: Dict) -> pd.DataFrame:
        """Crea DataFrame para la hoja de reseñas eliminando duplicados"""
        reviews = []
        seen_hashes = set()
        
        for attraction in data['attractions']:
            for review in attraction.get('reviews', []):
                # Crear hash único para cada reseña
                review_hash = hash((
                    review.get('username', ''),
                    review.get('title', ''),
                    review.get('written_date', ''),
                    str(review.get('rating', 0))
                ))
                
                if review_hash not in seen_hashes:
                    seen_hashes.add(review_hash)
                    reviews.append({
                        'Attraction': attraction.get('place_name'),
                        'Username': review.get('username'),
                        'Rating': review.get('rating'),
                        'Location': review.get('location'),
                        'Contributions': review.get('contributions'),
                        'Visit Date': review.get('visit_date'),
                        'Written Date': review.get('written_date'),
                        'Companion Type': review.get('companion_type'),
                        'Title': review.get('title'),
                        'Review Text': review.get('review_text'),
                    })
        
        df = pd.DataFrame(reviews)
        
        # Eliminar posibles duplicados restantes por si acaso
        return df.drop_duplicates(
            subset=['Username', 'Title', 'Written Date', 'Rating'],
            keep='first'
        )

    def _adjust_column_widths(self, writer, dfs: list):
        """Ajusta automáticamente el ancho de columnas"""
        for sheet_name, df in zip(writer.sheets.keys(), dfs):
            worksheet = writer.sheets[sheet_name]
            for idx, col in enumerate(df.columns):
                max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(idx, idx, min(max_len, 50))