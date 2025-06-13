# MÓDULO DE INICIALIZACIÓN PARA UTILIDADES DEL SISTEMA
# Centraliza importación de componentes de utilidad para fácil acceso
# Proporciona punto de entrada único para funciones de red, logging y exportación

from .constants import BASE_URL, HEADERS, PathConfig, get_headers
from .exporters import DataExporter
from .logger import setup_logging
from .networking import smart_sleep

# lista de elementos públicos disponibles para importación externa
# define API pública del módulo utils
__all__ = [
  'BASE_URL',          # URL base para TripAdvisor
  'HEADERS',           # headers por defecto para requests
  'PathConfig',        # configuración de rutas del sistema
  'get_headers',       # función para generar headers dinámicos
  'DataExporter',      # clase para exportar datos a múltiples formatos
  'setup_logging',     # función para inicializar sistema de logs
  'smart_sleep'        # función de pausa inteligente anti-detección
]