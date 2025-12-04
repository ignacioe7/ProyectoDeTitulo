import json
import os
from datetime import datetime
from typing import Dict, List, Any
import re

def convert_old_format_to_multilingual(input_file_path: str, output_file_path: str, default_language: str = "english") -> bool:
    """
    Convierte un archivo JSON con formato antiguo (reseñas directas) al nuevo formato multilenguaje.
    
    Args:
        input_file_path: Ruta del archivo JSON original
        output_file_path: Ruta donde guardar el archivo convertido
        default_language: Idioma por defecto para las reseñas existentes
    
    Returns:
        bool: True si la conversión fue exitosa
    """
    
    try:
        # Cargar archivo original
        with open(input_file_path, 'r', encoding='utf-8') as f:
            original_data = json.load(f)
        
        print(f"📂 Cargando archivo: {input_file_path}")
        
        # Validar estructura básica
        if "regions" not in original_data:
            print("❌ Error: El archivo no contiene la clave 'regions'")
            return False
        
        # Crear nueva estructura
        converted_data = {
            "regions": []
        }
        
        total_reviews_converted = 0
        total_attractions_converted = 0
        attractions_with_reviews = 0
        attractions_without_reviews = 0
        
        # Procesar cada región
        for region in original_data["regions"]:
            region_name = region.get("region_name", "Región Desconocida")
            print(f"🌍 Procesando región: {region_name}")
            
            converted_region = {
                "region_name": region_name,
                "attractions": [],
                "last_attractions_scrape_date": region.get("last_attractions_scrape_date"),
                "last_analyzed_date": datetime.now().isoformat()
            }
            
            # Procesar cada atracción
            for attraction in region.get("attractions", []):
                attraction_name = attraction.get("attraction_name", "Atracción Desconocida")
                print(f"  🏛️ Procesando atracción: {attraction_name}")
                
                # SIEMPRE contar la atracción, independientemente de si tiene reseñas
                total_attractions_converted += 1
                
                # Obtener reseñas del formato antiguo
                old_reviews = attraction.get("reviews", [])
                reviews_count = len(old_reviews)
                
                # Crear estructura multilenguaje
                languages_structure = {}
                
                if old_reviews:
                    # Contar reseñas y atracciones con reseñas
                    total_reviews_converted += reviews_count
                    attractions_with_reviews += 1
                    
                    # Procesar y limpiar todas las reseñas como inglés
                    cleaned_reviews = []
                    for review in old_reviews:
                        cleaned_review = _clean_and_standardize_review(review)
                        cleaned_reviews.append(cleaned_review)
                    
                    # Agregar al idioma especificado
                    languages_structure[default_language] = {
                        "reviews": cleaned_reviews,
                        "reviews_count": len(cleaned_reviews),
                        "stored_reviews": len(cleaned_reviews),
                        "skipped_duplicates": [],
                        "previously_scraped": True,
                        "last_scrape_date": datetime.now().isoformat()
                    }
                    print(f"    🗣️ {default_language}: {len(cleaned_reviews)} reseñas")
                else:
                    # Atracción sin reseñas - crear estructura vacía pero mantener la atracción
                    attractions_without_reviews += 1
                    languages_structure[default_language] = {
                        "reviews": [],
                        "reviews_count": 0,
                        "stored_reviews": 0,
                        "skipped_duplicates": [],
                        "previously_scraped": False,
                        "last_scrape_date": None
                    }
                    print(f"    🗣️ {default_language}: 0 reseñas (atracción sin reseñas)")
                
                # Crear nueva estructura de atracción (siempre se incluye)
                converted_attraction = {
                    "position": attraction.get("position"),
                    "attraction_name": attraction_name,
                    "url": attraction.get("url"),
                    "rating": attraction.get("rating"),
                    "reviews_count": attraction.get("reviews_count"),
                    "place_type": attraction.get("place_type"),
                    "languages": languages_structure,
                    "scraped_reviews_count": 0,  # Se resetea porque ahora están en languages
                    "last_analyzed_date": datetime.now().isoformat() if old_reviews else None
                }
                
                # Limpiar campos None
                converted_attraction = {k: v for k, v in converted_attraction.items() if v is not None}
                
                converted_region["attractions"].append(converted_attraction)
            
            converted_data["regions"].append(converted_region)
        
        # Guardar archivo convertido
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(converted_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Conversión completada exitosamente!")
        print(f"📊 Estadísticas:")
        print(f"   - Regiones procesadas: {len(converted_data['regions'])}")
        print(f"   - Atracciones TOTALES convertidas: {total_attractions_converted}")
        print(f"   - Atracciones CON reseñas: {attractions_with_reviews}")
        print(f"   - Atracciones SIN reseñas: {attractions_without_reviews}")
        print(f"   - Reseñas convertidas: {total_reviews_converted}")
        print(f"   - Idioma usado: {default_language}")
        print(f"📁 Archivo guardado en: {output_file_path}")
        
        return True
        
    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo {input_file_path}")
        return False
    except json.JSONDecodeError as e:
        print(f"❌ Error: El archivo JSON no es válido - {e}")
        return False
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return False

def diagnose_original_file(file_path: str):
    """
    Diagnostica el archivo original para verificar cuántas atracciones realmente tiene.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print("🔍 DIAGNÓSTICO DEL ARCHIVO ORIGINAL:")
        print("=" * 50)
        
        total_attractions = 0
        attractions_with_reviews = 0
        attractions_without_reviews = 0
        total_reviews = 0
        
        for region in data.get("regions", []):
            region_name = region.get("region_name", "Desconocida")
            attractions = region.get("attractions", [])
            region_attraction_count = len(attractions)
            total_attractions += region_attraction_count
            
            region_reviews = 0
            region_with_reviews = 0
            region_without_reviews = 0
            
            for attraction in attractions:
                reviews = attraction.get("reviews", [])
                review_count = len(reviews)
                region_reviews += review_count
                
                if review_count > 0:
                    region_with_reviews += 1
                    attractions_with_reviews += 1
                else:
                    region_without_reviews += 1
                    attractions_without_reviews += 1
            
            total_reviews += region_reviews
            
            print(f"🌍 {region_name}:")
            print(f"   - Atracciones: {region_attraction_count}")
            print(f"   - Con reseñas: {region_with_reviews}")
            print(f"   - Sin reseñas: {region_without_reviews}")
            print(f"   - Total reseñas: {region_reviews}")
        
        print("=" * 50)
        print(f"📊 TOTALES:")
        print(f"   - Regiones: {len(data.get('regions', []))}")
        print(f"   - Atracciones TOTALES: {total_attractions}")
        print(f"   - Atracciones con reseñas: {attractions_with_reviews}")
        print(f"   - Atracciones sin reseñas: {attractions_without_reviews}")
        print(f"   - Reseñas TOTALES: {total_reviews}")
        
        if total_attractions != 6057:
            print(f"⚠️  ADVERTENCIA: Se esperaban 6057 atracciones, pero se encontraron {total_attractions}")
        else:
            print("✅ El conteo coincide con lo esperado (6057 atracciones)")
            
        return total_attractions, attractions_with_reviews, attractions_without_reviews, total_reviews
        
    except Exception as e:
        print(f"❌ Error diagnosticando archivo: {e}")
        return 0, 0, 0, 0

def _clean_and_standardize_review(review: Dict) -> Dict:
    """
    Limpia y estandariza los datos de una reseña con campos normalizados.
    """
    cleaned_review = {}
    
    # Campos estándar que se copian directamente
    direct_copy_fields = [
        "review_id", "username", "rating", "title", "review_text",
        "location", "sentiment", "sentiment_score", "analyzed_at"
    ]
    
    for field in direct_copy_fields:
        if field in review:
            cleaned_review[field] = review[field]
    
    # Estandarizar contributions como entero
    if "contributions" in review:
        try:
            cleaned_review["contributions"] = int(review["contributions"])
        except (ValueError, TypeError):
            cleaned_review["contributions"] = 0
    
    # Estandarizar companion_type
    cleaned_review["companion_type"] = _standardize_companion_type(review.get("companion_type"))
    
    # Estandarizar written_date
    cleaned_review["written_date"] = _standardize_date_field(review.get("written_date"))
    
    # Estandarizar visit_date
    cleaned_review["visit_date"] = _standardize_date_field(review.get("visit_date"))
    
    # Asegurar que campos críticos existan
    if "review_id" not in cleaned_review or not cleaned_review["review_id"]:
        cleaned_review["review_id"] = f"converted_{abs(hash(str(review)))}"
    
    # Estandarizar rating como float
    if "rating" in cleaned_review and cleaned_review["rating"] is not None:
        try:
            cleaned_review["rating"] = float(cleaned_review["rating"])
        except (ValueError, TypeError):
            cleaned_review["rating"] = None
    
    # Estandarizar sentiment_score como float
    if "sentiment_score" in cleaned_review and cleaned_review["sentiment_score"] is not None:
        try:
            cleaned_review["sentiment_score"] = float(cleaned_review["sentiment_score"])
        except (ValueError, TypeError):
            cleaned_review["sentiment_score"] = None
    
    return cleaned_review

def _standardize_date_field(date_value) -> str:
    """
    Estandariza campos de fecha a formato ISO (YYYY-MM-DD) cuando sea posible.
    """
    if not date_value:
        return None
    
    # Convertir a string si no lo es
    date_str = str(date_value).strip()
    
    # Si ya está en formato ISO, devolverlo
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str
    
    # Mapeo de meses en inglés
    month_mapping = {
        'january': '01', 'jan': '01',
        'february': '02', 'feb': '02',
        'march': '03', 'mar': '03',
        'april': '04', 'apr': '04',
        'may': '05',
        'june': '06', 'jun': '06',
        'july': '07', 'jul': '07',
        'august': '08', 'aug': '08',
        'september': '09', 'sep': '09', 'sept': '09',
        'october': '10', 'oct': '10',
        'november': '11', 'nov': '11',
        'december': '12', 'dec': '12'
    }
    
    # Intentar parsear diferentes formatos comunes
    date_lower = date_str.lower()
    
    # Formato: "May 16, 2025", "March 2, 2025"
    month_day_year_pattern = r'(\w+)\s+(\d{1,2}),?\s+(\d{4})'
    match = re.match(month_day_year_pattern, date_lower)
    if match:
        month_name, day, year = match.groups()
        if month_name in month_mapping:
            return f"{year}-{month_mapping[month_name]}-{day.zfill(2)}"
    
    # Formato: "May 2025", "June 2024" (solo mes y año)
    month_year_pattern = r'^(\w+)\s+(\d{4})$'
    match = re.match(month_year_pattern, date_lower)
    if match:
        month_name, year = match.groups()
        if month_name in month_mapping:
            return f"{year}-{month_mapping[month_name]}-01"
    
    # Formato: "2024-05-16", "2025/06/15"
    iso_like_pattern = r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})'
    match = re.match(iso_like_pattern, date_str)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    
    # Formato: "16/05/2024", "15-06-2025"
    day_month_year_pattern = r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})'
    match = re.match(day_month_year_pattern, date_str)
    if match:
        day, month, year = match.groups()
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    
    # Formato: "2/3/2025", "15/6/2024" (MM/DD/YYYY o DD/MM/YYYY)
    short_date_pattern = r'(\d{1,2})/(\d{1,2})/(\d{4})'
    match = re.match(short_date_pattern, date_str)
    if match:
        first, second, year = match.groups()
        # Asumir MM/DD/YYYY si el primer número es <= 12
        if int(first) <= 12:
            month, day = first, second
        else:
            # Si el primer número > 12, asumir DD/MM/YYYY
            day, month = first, second
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    
    # Si no puede parsearse, devolver None
    print(f"⚠️ No se pudo convertir fecha: '{date_value}'")
    return None

def _standardize_companion_type(companion_type: str) -> str:
    """
    Estandariza el campo companion_type a valores consistentes.
    """
    if not companion_type or not isinstance(companion_type, str):
        return "Unknown"
    
    companion_lower = companion_type.lower().strip()
    
    # Mapeo de variaciones a valores estándar
    companion_mapping = {
        # Couples variations
        "couples": "Couples",
        "couple": "Couples",
        "romantic": "Couples",
        "partner": "Couples",
        "spouse": "Couples",
        
        # Family variations
        "family": "Family",
        "families": "Family",
        "family with young kids": "Family",
        "family with teens": "Family",
        "family with children": "Family",
        "kids": "Family",
        "children": "Family",
        
        # Friends variations
        "friends": "Friends",
        "friend": "Friends",
        "group of friends": "Friends",
        "with friends": "Friends",
        
        # Solo variations
        "solo": "Solo",
        "alone": "Solo",
        "single": "Solo",
        "individual": "Solo",
        "by myself": "Solo",
        
        # Business variations
        "business": "Business",
        "work": "Business",
        "business trip": "Business",
        "corporate": "Business"
    }
    
    # Buscar coincidencia exacta
    if companion_lower in companion_mapping:
        return companion_mapping[companion_lower]
    
    # Buscar coincidencia parcial
    for key, value in companion_mapping.items():
        if key in companion_lower:
            return value
    
    # Si no encuentra coincidencia, capitalizar el original
    return companion_type.title()

def batch_convert_files(input_directory: str, output_directory: str, default_language: str = "english"):
    """
    Convierte múltiples archivos JSON de un directorio.
    """
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    
    converted_files = []
    
    for filename in os.listdir(input_directory):
        if filename.endswith('.json') and not filename.startswith('multilingual_'):
            input_path = os.path.join(input_directory, filename)
            output_filename = f"multilingual_{filename}"
            output_path = os.path.join(output_directory, output_filename)
            
            print(f"\n🔄 Convirtiendo: {filename}")
            
            if convert_old_format_to_multilingual(input_path, output_path, default_language):
                converted_files.append(output_filename)
    
    print(f"\n🎉 Conversión por lotes completada!")
    print(f"✅ Archivos convertidos: {len(converted_files)}")
    for file in converted_files:
        print(f"   - {file}")

# ================================================================
# FUNCIÓN PRINCIPAL PARA USAR EL SCRIPT
# ================================================================

def main():
    """Función principal para ejecutar la conversión."""
    print("🔧 Conversor de formato antiguo a multilenguaje")
    print("=" * 50)
    
    # Configuración de rutas (ajusta según tus necesidades)
    input_file = "consolidated_data_english.json"  # Tu archivo actual
    output_file = "consolidated_data_multilingual.json"  # Archivo convertido
    default_language = "english"  # Idioma por defecto para reseñas existentes
    
    # Verificar si el archivo existe
    if not os.path.exists(input_file):
        print(f"❌ No se encontró el archivo: {input_file}")
        print("Por favor, ajusta la ruta en la variable 'input_file'")
        return
    
    # Primero diagnosticar el archivo original
    print("🔍 PASO 1: Diagnosticar archivo original")
    diagnose_original_file(input_file)
    
    print(f"\n🔄 PASO 2: Convertir archivo")
    # Ejecutar conversión
    success = convert_old_format_to_multilingual(
        input_file_path=input_file,
        output_file_path=output_file,
        default_language=default_language
    )
    
    if success:
        print(f"\n🔍 PASO 3: Verificar archivo convertido")
        diagnose_original_file(output_file)  # Usar la misma función para verificar
        
        print(f"\n🎯 Usa el archivo convertido: {output_file}")
        print("El nuevo archivo es compatible con el sistema multilenguaje.")
        print("\n📋 Estandarizaciones aplicadas:")
        print("   ✅ companion_type normalizado (Couples, Family, Friends, Solo, Business)")
        print("   ✅ written_date convertido a formato ISO (YYYY-MM-DD)")
        print("   ✅ visit_date convertido a formato ISO (YYYY-MM-DD)")
        print("   ✅ Estructura multilenguaje con solo inglés")
        print("   ✅ TODAS las atracciones incluidas (con y sin reseñas)")
    else:
        print("\n💥 La conversión falló. Revisa los errores anteriores.")

if __name__ == "__main__":
    main()