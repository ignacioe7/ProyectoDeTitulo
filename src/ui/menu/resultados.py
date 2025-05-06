import streamlit as st
import pandas as pd
from pathlib import Path

def render(data_handler):
  st.header("Resultados Almacenados")
  st.write("Selecciona una región para ver el resumen del análisis de sentimientos de sus atracciones.")

  available_regions = []
  # Intenta obtener las regiones desde data_handler
  # Prioridad 1: data_handler.regions_data (dict cuyas claves son nombres de región)
  if hasattr(data_handler, 'regions_data') and isinstance(data_handler.regions_data, dict) and data_handler.regions_data:
      available_regions = list(data_handler.regions_data.keys())
  # Prioridad 2: data_handler.regions (lista de nombres de región)
  elif hasattr(data_handler, 'regions') and isinstance(data_handler.regions, list) and data_handler.regions:
      available_regions = data_handler.regions
  
  if not available_regions:
      st.info(
          "No se pudo determinar la lista de regiones con resultados disponibles. "
          "Asegúrate de que las regiones hayan sido cargadas y los análisis completados."
      )
      st.caption("Sugerencia: `data_handler` debería tener un atributo como `regions_data` (dict) o `regions` (list) con los nombres de las regiones.")
      return

  available_regions.sort() # Ordenar para el selectbox

  selected_region_name = st.selectbox(
      "Selecciona una Región:",
      options=[""] + available_regions, # Opción vacía para estado inicial
      format_func=lambda x: "Selecciona una opción..." if x == "" else x,
      key="resultados_region_selectbox" # Clave única para el widget
  )

  if selected_region_name and selected_region_name != "":
    st.subheader(f"Resultados para: {selected_region_name}")
    
    # Encontrar el archivo Excel usando el método de data_handler
    excel_file_path = data_handler._find_matching_file(selected_region_name)

    if excel_file_path and excel_file_path.exists():
      try:
        # Cargar la hoja 'Summary' del archivo Excel
        df_summary = pd.read_excel(excel_file_path, sheet_name='Summary')
        st.markdown("#### Resumen de Atracciones")
        
        # --- Sección de Filtros ---
        # Usar columnas para organizar los filtros
        filter_col1, filter_col2 = st.columns(2)

        with filter_col1:
            search_attraction = st.text_input(
                "Buscar por nombre de atracción:", 
                key=f"search_summary_{selected_region_name.replace(' ', '_')}" # Clave única
            )
        
        # Filtro para la columna 'POSITIVE %'
        selected_positive_range = None
        if 'POSITIVE %' in df_summary.columns and not df_summary['POSITIVE %'].empty:
            # Asegurarse que los valores sean numéricos antes de min/max
            numeric_positive = pd.to_numeric(df_summary['POSITIVE %'], errors='coerce').dropna()
            if not numeric_positive.empty:
                min_val = float(numeric_positive.min())
                max_val = float(numeric_positive.max())
                if min_val > max_val: min_val = max_val # Evitar error si min > max
                
                with filter_col2:
                    selected_positive_range = st.slider(
                        "Filtrar por % Positivo:",
                        min_value=min_val,
                        max_value=max_val,
                        value=(min_val, max_val), # Rango por defecto es todo
                        key=f"slider_positive_{selected_region_name.replace(' ', '_')}" # Clave única
                    )
            else:
                 with filter_col2:
                    st.caption("No hay valores numéricos válidos en 'POSITIVE %' para filtrar.")

        elif 'POSITIVE %' not in df_summary.columns:
             with filter_col2:
                st.caption("La columna 'POSITIVE %' no está disponible para filtrar.")
        
        # Aplicar los filtros al DataFrame
        filtered_df = df_summary.copy() # Trabajar sobre una copia

        if search_attraction:
          filtered_df = filtered_df[
              filtered_df['Attraction Name'].astype(str).str.contains(search_attraction, case=False, na=False)
          ]
        
        if selected_positive_range and 'POSITIVE %' in filtered_df.columns:
            # Asegurar que la columna sea numérica para la comparación
            filtered_df['POSITIVE %'] = pd.to_numeric(filtered_df['POSITIVE %'], errors='coerce')
            filtered_df = filtered_df.dropna(subset=['POSITIVE %']) # Quitar filas donde la conversión falló
            filtered_df = filtered_df[
                (filtered_df['POSITIVE %'] >= selected_positive_range[0]) &
                (filtered_df['POSITIVE %'] <= selected_positive_range[1])
            ]
        
        # --- Visualización de la Tabla ---
        num_results_to_show = 10
        
        if filtered_df.empty:
            st.info("No hay atracciones que coincidan con los filtros aplicados.")
        else:
            st.write(f"Mostrando los primeros {min(num_results_to_show, len(filtered_df))} de {len(filtered_df)} atracciones filtradas:")
            # Usar st.dataframe para mejor visualización interactiva
            st.dataframe(filtered_df.head(num_results_to_show)) 

            if len(filtered_df) > num_results_to_show:
                # Usar st.expander para no ocupar tanto espacio por defecto
                with st.expander(f"Mostrar todas las {len(filtered_df)} atracciones filtradas"):
                    st.dataframe(filtered_df)
        
        # Opcional: Mostrar la tabla de Reviews también, dentro de un expander
        with st.expander("Mostrar tabla de Reseñas detalladas?"):
          try:
              df_reviews = pd.read_excel(excel_file_path, sheet_name='Reviews')
              st.markdown("#### Detalle de Reseñas")
              # Aquí podrías añadir filtros específicos para la tabla de reseñas si quieres
              st.dataframe(df_reviews.head(num_results_to_show)) # Mostrar solo las primeras 10 por defecto
              if len(df_reviews) > num_results_to_show:
                   if st.button(f"Mostrar todas las {len(df_reviews)} reseñas", key=f"show_all_reviews_{selected_region_name.replace(' ', '_')}"):
                        st.dataframe(df_reviews)
          except Exception as e_rev:
              st.warning(f"No se pudo cargar la hoja 'Reviews': {e_rev}")

      except FileNotFoundError:
        st.error(f"El archivo Excel para '{selected_region_name}' no fue encontrado en la ruta esperada. Verifica que el archivo exista.")
      except ValueError as ve: 
        st.error(f"Error al leer el archivo Excel para '{selected_region_name}': {ve}. Asegúrate que la hoja 'Summary' existe y el archivo es válido.")
      except Exception as e:
        st.error(f"Ocurrió un error inesperado al procesar el archivo de '{selected_region_name}': {e}")
        st.exception(e) # Muestra el traceback para más detalles en modo debug
        
    elif selected_region_name: # Si se seleccionó una región pero _find_matching_file no devolvió una ruta válida
        st.warning(f"No se encontró el archivo de resultados para '{selected_region_name}'. Verifica que haya sido procesado y que el nombre del archivo sea el esperado.")
  
  elif selected_region_name == "" and len(available_regions) > 0 :
      st.info("Por favor, selecciona una región del menú desplegable para ver sus resultados.")