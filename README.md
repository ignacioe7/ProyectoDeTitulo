# Análisis de Sentimientos en Reseñas Turísticas

Un proyecto de investigación académica para analizar sentimientos en reseñas de atracciones turísticas usando machine learning y NLP.

## Características

- **Scraping inteligente** de datos de atracciones turísticas
- **Análisis de sentimientos** usando DistilBERT
- **Interfaz web moderna** con Streamlit
- **Exportación de datos** a Excel y JSON
- **Visualizaciones interactivas** de resultados
- **Procesamiento concurrente** para mejor rendimiento

## Tecnologías

- **Python 3.8+**
- **Streamlit** - Interfaz web
- **Transformers** - Análisis de sentimientos
- **PyTorch** - Machine Learning
- **Pandas** - Manipulación de datos
- **Plotly** - Visualizaciones

## Instalación

### 1. Clonar el repositorio
```bash
git clone https://github.com/user/nombre_proyecto.git
cd proyecto_cientifico
```

### 2. Crear entorno virtual
```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/macOS
python3 -m venv venv
source venv/bin/activate
```

### 3. Instalar dependencias
```bash
pip install -r requirements.txt
```

## Uso

### Ejecutar la aplicación
```bash
streamlit run src/ui/streamlit_app.py
python -m streamlit run src/ui/streamlit_app.py
```

### Funcionalidades principales

1. **Configurar regiones** - Define las áreas geográficas a analizar
2. **Extraer atracciones** - Obtiene listado de lugares turísticos
3. **Scrapear reseñas** - Recolecta comentarios de usuarios
4. **Analizar sentimientos** - Clasifica opiniones como positivas/negativas
5. **Visualizar resultados** - Genera gráficos y estadísticas
6. **Exportar datos** - Guarda resultados en diferentes formatos

## Casos de uso

- **Investigación académica** sobre turismo y opinión pública
- **Análisis de mercado** para destinos turísticos
- **Estudios de satisfacción** de visitantes
- **Benchmarking** de atracciones similares

## Consideraciones legales y éticas

**IMPORTANTE**: Este proyecto es únicamente para fines educativos e investigación académica.

### Responsabilidades del usuario

Los usuarios son completamente responsables de:

1. **Revisar y cumplir** los Términos de Servicio de TripAdvisor
2. **Usar el código de manera ética** y dentro del marco legal
3. **Limitar la frecuencia** de solicitudes para no sobrecargar servidores
4. **Respetar robots.txt** y políticas de scraping del sitio web
5. **No usar con fines comerciales** sin autorización explícita

### Limitaciones

- TripAdvisor **prohíbe explícitamente** la extracción automatizada de datos
- Este código **NO debe usarse** para fines comerciales no autorizados
- Los autores **NO se responsabilizan** del uso indebido o consecuencias legales
- El software se proporciona **"tal cual"** sin garantías de ningún tipo

### Uso recomendado

- **Solo para investigación académica** con dataset pequeño
- **Implementar delays** apropiados entre solicitudes
- **Considerar APIs oficiales** cuando estén disponibles
- **Citar apropiadamente** el uso de datos en publicaciones

##  Licencia

Este proyecto está bajo licencia MIT para uso educativo. Ver `LICENSE` para más detalles.

## Autores

- Dorien Canales
- Ignacio Villalobos

## Agradecimientos

- Comunidad de Hugging Face por los modelos de NLP
- Streamlit por la plataforma de desarrollo
- Contribuidores de las librerías open source utilizadas

---
