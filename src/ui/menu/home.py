import streamlit as st

def render():
  """renderiza la pagina de inicio"""
  st.title("🌟 Análisis de Sentimientos TripAdvisor")
  st.markdown("---") 
  
  # intro del proyecto
  st.header("🎯 Bienvenido al Sistema")
  st.write("Sistema completo para extraer y analizar reseñas de atracciones turísticas")
  
  # caracteristicas principales
  st.subheader("🚀 Características Principales")
  
  col1, col2 = st.columns(2)
  
  with col1:
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
  
  # flujo de trabajo
  st.subheader("🔄 Flujo de Trabajo")
  st.markdown("""
  1. **Scraping de Atracciones** → Obtener lista de lugares
  2. **Scraping de Reseñas** → Extraer comentarios de usuarios  
  3. **Análisis de Sentimientos** → Clasificar texto con IA
  4. **Resultados** → Visualizar y exportar datos
  """)
  
  # instrucciones
  st.markdown("---")
  st.info("💡 **Tip:** Usa el menú lateral para navegar entre módulos")
  st.warning("⚠️ **Importante:** Solo un proceso puede estar activo a la vez")