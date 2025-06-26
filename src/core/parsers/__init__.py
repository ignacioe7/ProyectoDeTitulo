# ===============================================================
# MODULO DE PARSERS - EXPORTACIONES PRINCIPALES
# ===============================================================

# Importa las clases principales del parser de reseñas
from .review_parser import ReviewParser, ReviewParserConfig

# ===============================================================
# EXPORTACIONES PUBLICAS
# ===============================================================

# Define que clases estan disponibles cuando se importa el modulo
# Permite usar: from src.core.parsers import ReviewParser
__all__ = ['ReviewParser', 'ReviewParserConfig']