# MÓDULO DE PÁGINA DE INICIO PARA SISTEMA DE ANÁLISIS DE SENTIMIENTOS
# Presenta información general del proyecto, características principales y flujo de trabajo
# Proporciona navegación inicial y orientación sobre el uso del sistema

import streamlit as st

# ====================================================================================================================
#                                            RENDERIZAR PÁGINA DE INICIO
# ====================================================================================================================

def render():
  # RENDERIZA LA PÁGINA PRINCIPAL DE BIENVENIDA DEL SISTEMA
  # Muestra información general del proyecto, características y guía de uso
  # Proporciona contexto sobre flujo de trabajo y navegación entre módulos
  st.title("🌟 Análisis de Sentimientos TripAdvisor")
  st.markdown("---") 
  
  # introducción general del proyecto y propósito principal
  st.header("🎯 Bienvenido al Sistema")
  st.write("Sistema completo para extraer y analizar reseñas de atracciones turísticas")
  
  # sección de características principales organizadas en columnas
  st.subheader("🚀 Características Principales")
  
  col1, col2 = st.columns(2)
  
  with col1:
    # características de scraping y extracción de datos
    st.markdown("""
    **📍 Scraping de Atracciones:**
    - Extrae atracciones por región
    - Configurable por tipo de lugar
    - Manejo automático de errores
    
    **📝 Scraping de Reseñas:**
    - Reseñas en inglés únicamente
    - Control de concurrencia
    - Pausa inteligente anti-bloqueo
    """)
  
  with col2:
    # características de análisis y visualización de resultados
    st.markdown("""
    **🤖 Análisis de Sentimientos:**
    - IA para clasificar texto
    - Procesamiento por lotes
    - Métricas detalladas
    
    **📊 Visualización:**
    - Gráficos comparativos
    - Exportación a Excel
    - Filtros por región
    """)
  
  # explicación del flujo de trabajo secuencial del sistema
  st.subheader("🔄 Flujo de Trabajo")
  st.markdown("""
  1. **Scraping de Atracciones** → Obtener lista de lugares
  2. **Scraping de Reseñas** → Extraer comentarios de usuarios  
  3. **Análisis de Sentimientos** → Clasificar texto con IA
  4. **Resultados** → Visualizar y exportar datos
  """)
  
  # instrucciones de uso y advertencias importantes para el usuario
  st.markdown("---")
  st.info("💡 **Tip:** Usa el menú lateral para navegar entre módulos")
  st.warning("⚠️ **Importante:** Solo un proceso puede estar activo a la vez")