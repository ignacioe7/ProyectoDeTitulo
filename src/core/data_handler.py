import json
import aiofiles
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from loguru import logger as log
import re
from ..models.attraction import Attraction
from ..utils.constants import PathConfig
from ..utils.exporters import DataExporter

class DataHandler:
    """Maneja la carga y exportación de datos relacionados con atracciones y reseñas"""
    def __init__(self):
        self.paths = PathConfig()
        self.exporter = self._get_exporter()
        self._ensure_dirs()
        self.analyzer = self._initialize_analyzer()
    
    def _ensure_dirs(self):
        """Garantiza que existen todos los directorios necesarios"""
        Path(self.paths.ATTRACTIONS_DIR).mkdir(parents=True, exist_ok=True)
        Path(self.paths.REGIONS_DIR).mkdir(parents=True, exist_ok=True)
        Path(self.paths.OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    # ---- Operaciones con Regiones ----
    def load_regions(self) -> List[Dict]:
        """Carga el listado de regiones desde el archivo JSON"""
        regions_file = Path(self.paths.REGIONS_DIR) / "regions.json"
        try:
            with open(regions_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            log.error(f"Archivo de regiones no encontrado: {regions_file}")
            return []
    
    # ---- Operaciones con Atracciones ----
    async def save_attractions(self, region_name: str, attractions: List[Dict]) -> Path:
        """Guarda atracciones en formato JSON (versión asíncrona)"""
        try:
            filename = self.get_attraction_filepath(region_name)
            data = {
                "region": region_name,
                "attractions": attractions,
                "scrape_date": datetime.now().isoformat()
            }

            # Usar aiofiles para escritura asíncrona
            async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, indent=2, ensure_ascii=False))

            log.success(f"Atracciones guardadas en {filename}")
            return filename

        except Exception as e:
            log.error(f"Error guardando atracciones: {e}")
            raise
    
    def load_attractions(self, region_name: str) -> List[Attraction]:
        """Carga atracciones y las convierte a objetos Attraction"""
        filename = self.get_attraction_filepath(region_name)
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return [Attraction(**attr) for attr in data.get('attractions', [])]
        except FileNotFoundError:
            log.error(f"Archivo no encontrado: {filename}")
            return []
        except json.JSONDecodeError:
            log.error(f"Error leyendo archivo: {filename}")
            return []
    
    # ---- Operaciones con Reviews ----
    async def export_reviews(self, data: Dict, format: str = "excel") -> Path:
        """Versión que incluye análisis de sentimiento automático"""
        if not isinstance(data, dict) or 'region' not in data:
            raise ValueError("Data debe ser un diccionario con clave 'region'")

        if format == "excel":
            return await self.exporter.save_to_excel(data)
        elif format == "json":
            return await self.exporter.save_to_json(data)
        raise ValueError(f"Formato no soportado: {format}")
    
    # ---- Análisis de Sentimientos ----
    def _initialize_analyzer(self):
        """Inicializa el analizador con manejo de errores"""
        try:
            from .analyzer import SentimentAnalyzer
            analyzer = SentimentAnalyzer()
            if analyzer.nlp is None:
                log.warning("El analizador se inicializó pero no funciona correctamente")
            return analyzer
        except Exception as e:
            log.error(f"No se pudo inicializar el analizador: {e}")
            return None
        
    async def analyze_and_update_excel(self, region_name: str) -> bool:
        """Análisis de sentimientos con manejo mejorado de nombres de archivo"""
        try:
            if not hasattr(self, 'analyzer') or self.analyzer is None:
                raise RuntimeError("Analizador de sentimientos no disponible")
    
            # Obtener el nombre normalizado
            sanitized_name = self._sanitize_region_name(region_name)
            filename = self.get_reviews_filepath(sanitized_name)
            
            # Si no existe, buscar archivos similares
            if not filename.exists():
                filename = self._find_matching_file(region_name)
                if not filename:
                    raise FileNotFoundError(f"No se encontró archivo de reseñas para {region_name}")
    
            log.info(f"Procesando archivo: {filename}")

            # Leer el Excel
            with pd.ExcelFile(filename) as xls:
                summary_df = pd.read_excel(xls, sheet_name='Summary')
                reviews_df = pd.read_excel(xls, sheet_name='Reviews')

            # Verificar columnas requeridas
            required_cols = {'Attraction', 'Title', 'Review Text'}
            if not required_cols.issubset(reviews_df.columns):
                missing = required_cols - set(reviews_df.columns)
                raise ValueError(f"Faltan columnas requeridas: {missing}")

            # Aplicar análisis de sentimiento (solo una vez)
            reviews_df[['Sentiment', 'Sentiment Score']] = reviews_df.apply(
                lambda row: pd.Series(self.analyzer.analyze_review(
                    row.get('Title'), 
                    row.get('Review Text')
                )),
                axis=1
            )

            # Calcular estadísticas
            stats = self._calculate_sentiment_stats(reviews_df)
            summary_df = self._update_summary_stats(summary_df, stats)

            # Guardar los cambios
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                reviews_df.to_excel(writer, sheet_name='Reviews', index=False)

                # Ajustar anchos de columna automáticamente
                self._adjust_column_widths(writer, summary_df, reviews_df)

            log.success(f"Análisis completado exitosamente: {filename}")
            return True

        except Exception as e:
            log.error(f"Error en analyze_and_update_excel: {str(e)}")
            return False

    def _calculate_sentiment_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcula estadísticas de sentimiento por atracción"""
        stats = df.groupby(['Attraction', 'Sentiment']).size().unstack(fill_value=0)

        # Asegurar todas las categorías
        for sentiment in ['POSITIVE', 'NEGATIVE']:
            if sentiment not in stats.columns:
                stats[sentiment] = 0

        stats['Total'] = stats.sum(axis=1)

        # Cambiar los nombres para usar el símbolo %
        stats['POSITIVE %'] = (stats['POSITIVE'] / stats['Total'] * 100).round(1)
        stats['NEGATIVE %'] = (stats['NEGATIVE'] / stats['Total'] * 100).round(1)
        
        return stats

    def _update_summary_stats(self, summary_df: pd.DataFrame, stats: pd.DataFrame) -> pd.DataFrame:
        """Actualiza el summary con las estadísticas de sentimiento"""
        stats_to_merge = stats[['POSITIVE %', 'NEGATIVE %']].reset_index()
        
        return summary_df.merge(
            stats_to_merge,
            left_on='Attraction Name',
            right_on='Attraction',
            how='left'
        ).drop(columns=['Attraction'])

    def _adjust_column_widths(self, writer, summary_df, reviews_df):
        """Ajusta automáticamente los anchos de columna"""
        for sheet_name, df in [('Summary', summary_df), ('Reviews', reviews_df)]:
            worksheet = writer.sheets[sheet_name]
            
            for idx, col in enumerate(df.columns):
                # Calcular ancho máximo basado en contenido
                max_len = max(
                    df[col].astype(str).map(len).max(),
                    len(str(col))
                )
                
                # Limitar el ancho máximo a 50 caracteres
                worksheet.column_dimensions[chr(65 + idx)].width = min(max_len + 2, 50)
    
    # ---- Métodos de ayuda ----
    def _sanitize_name(self, name: str) -> str:
        """Limpia nombres para usarlos en archivos"""
        return name.lower().replace(" ", "_").replace("-", "_")
    
    def _sanitize_region_name(self, region_name: str) -> str:
        """Normaliza el nombre de la región para nombres de archivo"""
        # Primero eliminar números romanos y guiones
        name = re.sub(r'[XIV]+-?\s*', '', region_name, flags=re.IGNORECASE)
        # Eliminar caracteres especiales y normalizar espacios
        name = re.sub(r'[^\w\s-]', '', name.lower()) 
        # Reemplazar espacios por underscores
        name = re.sub(r'\s+', '_', name).strip('_')
        # Eliminar acentos y caracteres especiales
        name = (name.replace('á', 'a').replace('é', 'e').replace('í', 'i')
                .replace('ó', 'o').replace('ú', 'u').replace('ñ', 'n'))
        return name

    def get_reviews_filepath(self, region_name: str, format: str = "excel") -> Path:
        """Genera la ruta del archivo de reseñas usando el mismo formato siempre"""
        sanitized_name = self._sanitize_region_name(region_name)
        ext = "xlsx" if format == "excel" else "json"
        return Path(self.paths.OUTPUT_DIR) / f"{sanitized_name}_reviews.{ext}"

    def _find_matching_file(self, base_name: str) -> Optional[Path]:
        """Busca archivos que coincidan con el nombre base"""
        output_dir = Path(self.paths.OUTPUT_DIR)
        if not output_dir.exists():
            return None

        target_name = self._sanitize_region_name(base_name)

        # Buscar archivos que coincidan exactamente
        exact_match = output_dir / f"{target_name}_reviews.xlsx"
        if exact_match.exists():
            return exact_match

        # Si no hay coincidencia exacta, buscar aproximada
        for file in output_dir.glob('*_reviews.xlsx'):
            file_stem = self._sanitize_region_name(file.stem.replace('_reviews', ''))
            if file_stem == target_name:
                return file

        return None

    def get_attraction_filepath(self, region_name: str) -> Path:
        """Genera la ruta esperada para archivos de atracciones"""
        return Path(self.paths.ATTRACTIONS_DIR) / f"{self._sanitize_name(region_name)}_attractions.json"
    
    def get_reviews_filepath(self, region_name: str, format: str = "excel") -> Path:
        """Genera la ruta esperada para archivos de reseñas"""
        ext = "xlsx" if format == "excel" else "json"
        return Path(self.paths.OUTPUT_DIR) / f"{self._sanitize_name(region_name)}_reviews.{ext}"
    
    def _get_exporter(self):
        from ..utils.exporters import DataExporter
        return DataExporter()
