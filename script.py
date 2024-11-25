# # TRABAJANDO: LOGICA NUEVO ACTUAIZDA 

# import pdfplumber
# import pandas as pd
# import re
# import os
# import json
# from tqdm import tqdm
# import glob
# import numpy as np

# # Configuración de carpetas y columnas requeridas
# input_folder = "upload"
# output_folder = "output"
# combined_excel_path = "output/resultado_concatenado.xlsx"
# json_path = "indicadores/indicadores.json"
# required_columns = ['RUT', 'Apellido Paterno, Materno, Nombres', 'Remuneración', 'Cod.', 'Fecha Inicio', 'Fecha Término', 'AFP', 'Días', 'Rem_Días', 'Periodo', 'Porcentaje', 'Pensión', 'Comisión', 'Total_AFP', 'Análisis', 'Tipo_Renta']

# header_patterns = [
#     r"^RUT$", r"Apellido Paterno,? Materno,? Nombres", r"Remuneración", r"Fecha Inicio", r"Fecha Término", r"Cod."
# ]

# specific_headers = [
#     "Identificación del Trabajador", "Fondo de Pensiones", "Seguro Cesantía", "Movimiento de Personal"
# ]

# def is_header_row(row):
#     return any(re.search(pattern, str(cell), re.IGNORECASE) for cell in row for pattern in header_patterns) or \
#            any(header in str(cell) for cell in row for header in specific_headers)

# def extract_data_from_pdf(pdf_path):
#     data = []
#     afp_name = ""
#     try:
#         with pdfplumber.open(pdf_path) as pdf:
#             first_page_text = pdf.pages[0].extract_text()
#             match = re.search(r"AFP\s+(\w+)", first_page_text)
#             if match:
#                 afp_name = match.group(1)
            
#             for page in tqdm(pdf.pages, desc=f"Procesando páginas de {pdf_path}", unit="página"):
#                 table = page.extract_table()
#                 if table:
#                     for row in table:
#                         if row and len(row) >= len(required_columns) - 9 and not is_header_row(row):
#                             data.append([row[0], row[1], re.sub(r'[^0-9,]', '', row[2]).replace(',', '.'), row[12], row[13], row[14], afp_name])
#     except Exception as e:
#         print(f"Error al procesar {pdf_path}: {e}")
#     return data

# def transform_data_to_dataframe(data):
#     df = pd.DataFrame(data, columns=required_columns[:-9])
#     df['Remuneración'] = pd.to_numeric(df['Remuneración'].str.replace(',', '.'), errors='coerce')
#     df = df[~df['RUT'].str.contains("Identificación del Trabajador|Fondo de Pensiones|Seguro Cesantía|Movimiento de Personal|Totales Generales", case=False, na=False)]
    
#     df['Fecha Inicio'] = pd.to_datetime(df['Fecha Inicio'], errors='coerce', dayfirst=True)
#     df['Fecha Término'] = pd.to_datetime(df['Fecha Término'], errors='coerce', dayfirst=True)
    
#     df['Días'] = (df['Fecha Término'] - df['Fecha Inicio']).dt.days + 1
#     df['Periodo'] = df['Fecha Inicio'].dt.strftime('%Y%m').fillna('')
    
#     # Load JSON with indicators
#     try:
#         with open(json_path, 'r', encoding='utf-8') as f:
#             indicadores = json.load(f)
#     except Exception as e:
#         print(f"Error al cargar el archivo JSON: {e}")
#         indicadores = {}
    
#     # Calculate 'Porcentaje' column
#     porcentaje_list = []
#     for index, row in df.iterrows():
#         periodo = row['Periodo']
#         afp = row['AFP']
#         porcentaje = None
#         if periodo in indicadores and 'afp' in indicadores[periodo] and 'tasa_afp_dependientes' in indicadores[periodo]['afp']:
#             afp_list = indicadores[periodo]['afp']['afp']
#             tasa_list = indicadores[periodo]['afp']['tasa_afp_dependientes']
#             if afp in afp_list:
#                 idx = afp_list.index(afp)
#                 tasa_str = tasa_list[idx].replace('%', '').replace(',', '.')
#                 tasa = float(tasa_str) - 10.0
#                 porcentaje = round(tasa, 2)
#         porcentaje_list.append(porcentaje)
    
#     df['Porcentaje'] = porcentaje_list
    
#     # Calculate 'Tipo_Renta' column
#     tipo_renta_list = []
#     for index, row in df.iterrows():
#         periodo = row['Periodo']
#         remuneracion = row['Remuneración']
#         tipo_renta = "Inferior a Renta Tope"
#         if periodo in indicadores and 'rentas_topes_imponibles' in indicadores[periodo]:
#             renta_tope_str = indicadores[periodo]['rentas_topes_imponibles']['valor'][0].replace('$', '').replace('.', '').replace(',', '.')
#             renta_tope = float(renta_tope_str)
#             if remuneracion >= renta_tope:
#                 tipo_renta = "Renta Tope"
#         tipo_renta_list.append(tipo_renta)
    
#     df['Tipo_Renta'] = tipo_renta_list
    
#     # Update rows with 'Remuneración' = 0 using the highest value for the same RUT
#     for rut in df['RUT'].unique():
#         max_remuneracion = df.loc[(df['RUT'] == rut) & (df['Remuneración'] > 0), 'Remuneración'].max()
#         if pd.notna(max_remuneracion):
#             df.loc[(df['RUT'] == rut) & (df['Remuneración'] == 0), 'Remuneración'] = max_remuneracion
    
    

#     # Inicializar 'Rem_Días' como float
#     df['Rem_Días'] = 0.0

# # Condición: Días <= 11
#     cond1 = df['Días'] <= 11

# # Subcondición 1a: Tipo_Renta == 'Renta Tope'
#     cond1a = cond1 & (df['Tipo_Renta'] == 'Renta Tope')
#     df.loc[cond1a, 'Rem_Días'] = (df.loc[cond1a, 'Remuneración'] / 30) * df.loc[cond1a, 'Días']

# # Subcondición 1b: Tipo_Renta != 'Renta Tope'
#     cond1b = cond1 & (df['Tipo_Renta'] != 'Renta Tope')

# # Subcondición 1b1: Días <= 3
#     cond1b1 = cond1b & (df['Días'] <= 3)
#     df.loc[cond1b1, 'Rem_Días'] = (df.loc[cond1b1, 'Remuneración'] / 30) * df.loc[cond1b1, 'Días']

# # Subcondición 1b2: Días > 3
#     cond1b2 = cond1b & (df['Días'] > 3)
#     df.loc[cond1b2, 'Rem_Días'] = (df.loc[cond1b2, 'Remuneración'] / 30) * 3

# # Condición: Días > 11
#     cond2 = df['Días'] > 11

# # Subcondición 2a: Días es 29, 30 o 31
#     cond2a = cond2 & df['Días'].isin([29, 30, 31])
#     df.loc[cond2a, 'Rem_Días'] = (df.loc[cond2a, 'Remuneración'] / 30) * df.loc[cond2a, 'Días']

# # Subcondición 2b: Días no es 29, 30 o 31
#     cond2b = cond2 & ~df['Días'].isin([29, 30, 31])
#     df.loc[cond2b, 'Rem_Días'] = 0.0

# # Finalizar el cálculo de 'Rem_Días'
#     df['Rem_Días'] = df['Rem_Días'].fillna(0).round(0).astype(int)

    
#     # Calculate 'Pensión' column
#     df['Pensión'] = (df['Rem_Días'] * 0.10).fillna(0).round(0).astype(int)
    
#     # Calculate 'Comisión' column
#     df['Comisión'] = ((df['Porcentaje'] / 100) * df['Rem_Días']).fillna(0).round(0).astype(int)
    
#     # Calculate 'Total_AFP' column
#     df['Total_AFP'] = (df['Pensión'] + df['Comisión']).fillna(0).astype(int)
    
#     # Calculate 'Análisis_Individual' column for each individual license
#     df['Análisis_Individual'] = df['Días'].apply(lambda x: 'CORRESPONDE' if pd.notna(x) and x <= 10 else 'NO CORRESPONDE')
    
#     # Calculate continuous licenses and analysis of the charge
#     df.sort_values(by=['RUT', 'Fecha Inicio'], inplace=True)
#     df.reset_index(drop=True, inplace=True)
    
#     grupos_continuos = []
#     grupo_id = 0
#     dias_continuos_dict = {}
    
#     for index in range(len(df)):
#         if index == 0 or df.loc[index, 'RUT'] != df.loc[index - 1, 'RUT']:
#             # New RUT or first row
#             grupo_id += 1
#             dias_continuos_dict[grupo_id] = df.loc[index, 'Días']
#         else:
#             # Check if the license is continuous with the previous one
#             fecha_termino_anterior = df.loc[index - 1, 'Fecha Término']
#             fecha_inicio_actual = df.loc[index, 'Fecha Inicio']
#             if fecha_termino_anterior + pd.Timedelta(days=1) == fecha_inicio_actual:
#                 dias_continuos_dict[grupo_id] += df.loc[index, 'Días']
#             else:
#                 grupo_id += 1
#                 dias_continuos_dict[grupo_id] = df.loc[index, 'Días']
#         grupos_continuos.append(grupo_id)
    
#     df['Grupo_Continuo'] = grupos_continuos
    
#     # Update 'Análisis_Grupo' column based on continuous license rules
#     analisis_grupo_list = []
#     for index, row in df.iterrows():
#         grupo = row['Grupo_Continuo']
#         total_dias = dias_continuos_dict[grupo]
#         if total_dias < 11:
#             analisis_grupo = 'CORRESPONDE'
#         elif 11 <= total_dias <= 29:
#             analisis_grupo = 'NO CORRESPONDE'
#         else:
#             analisis_grupo = 'CORRESPONDE'
#         analisis_grupo_list.append(analisis_grupo)
    
#     df['Análisis_Grupo'] = analisis_grupo_list
    
#     # Generate a summary column combining individual and group analysis
#     df['Resumen'] = df.apply(lambda row: f"Individual: {row['Análisis_Individual']}, Grupo: {row['Análisis_Grupo']}", axis=1)
    
#     df['Fecha Inicio'] = df['Fecha Inicio'].dt.strftime('%d-%m-%Y').fillna('')
#     df['Fecha Término'] = df['Fecha Término'].dt.strftime('%d-%m-%Y').fillna('')
    
#     return df

# def main():
#     if not os.path.exists(output_folder):
#         os.makedirs(output_folder)
    
#     for filename in tqdm(os.listdir(input_folder), desc="Procesando archivos PDF", unit="archivo"):
#         if filename.endswith(".pdf"):
#             pdf_path = os.path.join(input_folder, filename)
#             extracted_data = extract_data_from_pdf(pdf_path)
#             if extracted_data:
#                 df = transform_data_to_dataframe(extracted_data)
#                 output_path = os.path.join(output_folder, f"{os.path.splitext(filename)[0]}.xlsx")
#                 df.to_excel(output_path, index=False)
    
#     excel_files = glob.glob(os.path.join(output_folder, "*.xlsx"))
#     if excel_files:
#         dataframes = [pd.read_excel(file) for file in excel_files]
#         combined_df = pd.concat(dataframes, ignore_index=True)
#         combined_df.to_excel(combined_excel_path, index=False)
#         print(f"Datos extraídos y guardados en {combined_excel_path}")
#     else:
#         print("No se encontraron archivos Excel para consolidar.")

# if __name__ == "__main__":
#     main()


# NUEVO: TRABAJANDO: FIXME: ProVida nombre mal escrito se crea diccionario 

# TRABAJANDO: LOGICA NUEVO ACTUAIZDA 

# import pdfplumber
# import pandas as pd
# import re
# import os
# import json
# from tqdm import tqdm
# import glob
# import numpy as np

# # Configuración de carpetas y columnas requeridas
# input_folder = "upload"
# output_folder = "output"
# combined_excel_path = "output/resultado_concatenado.xlsx"
# json_path = "indicadores/indicadores.json"
# required_columns = ['RUT', 'Apellido Paterno, Materno, Nombres', 'Remuneración', 'Cod.', 'Fecha Inicio', 'Fecha Término', 'AFP', 'Días', 'Rem_Días', 'Periodo', 'Porcentaje', 'Pensión', 'Comisión', 'Total_AFP', 'Análisis', 'Tipo_Renta']

# header_patterns = [
#     r"^RUT$", r"Apellido Paterno,? Materno,? Nombres", r"Remuneración", r"Fecha Inicio", r"Fecha Término", r"Cod."
# ]

# specific_headers = [
#     "Identificación del Trabajador", "Fondo de Pensiones", "Seguro Cesantía", "Movimiento de Personal"
# ]

# def is_header_row(row):
#     return any(re.search(pattern, str(cell), re.IGNORECASE) for cell in row for pattern in header_patterns) or \
#            any(header in str(cell) for cell in row for header in specific_headers)

# def extract_data_from_pdf(pdf_path):
#     data = []
#     afp_name = ""
#     try:
#         with pdfplumber.open(pdf_path) as pdf:
#             first_page_text = pdf.pages[0].extract_text()
#             match = re.search(r"AFP\s+(\w+)", first_page_text)
#             if match:
#                 afp_name = match.group(1)
            
#             for page in tqdm(pdf.pages, desc=f"Procesando páginas de {pdf_path}", unit="página"):
#                 table = page.extract_table()
#                 if table:
#                     for row in table:
#                         if row and len(row) >= len(required_columns) - 9 and not is_header_row(row):
#                             data.append([row[0], row[1], re.sub(r'[^0-9,]', '', row[2]).replace(',', '.'), row[12], row[13], row[14], afp_name])
#     except Exception as e:
#         print(f"Error al procesar {pdf_path}: {e}")
#     return data

# def transform_data_to_dataframe(data):
#     df = pd.DataFrame(data, columns=required_columns[:-9])
#     df['Remuneración'] = pd.to_numeric(df['Remuneración'].str.replace(',', '.'), errors='coerce')
#     df = df[~df['RUT'].str.contains("Identificación del Trabajador|Fondo de Pensiones|Seguro Cesantía|Movimiento de Personal|Totales Generales", case=False, na=False)]
    
#     df['Fecha Inicio'] = pd.to_datetime(df['Fecha Inicio'], errors='coerce', dayfirst=True)
#     df['Fecha Término'] = pd.to_datetime(df['Fecha Término'], errors='coerce', dayfirst=True)
    
#     df['Días'] = (df['Fecha Término'] - df['Fecha Inicio']).dt.days + 1
#     df['Periodo'] = df['Fecha Inicio'].dt.strftime('%Y%m').fillna('')
    
#     # Cargar JSON con indicadores
#     try:
#         with open(json_path, 'r', encoding='utf-8') as f:
#             indicadores = json.load(f)
#     except Exception as e:
#         print(f"Error al cargar el archivo JSON: {e}")
#         indicadores = {}
    
#     # Diccionario de mapeo de nombres de AFPs
#     afp_mapping = {
#         "provida": "Provida",
#         "proviva": "Provida",
#         "pro vida": "Provida",
#         "capital": "Capital",
#         "cuprum": "Cuprum",
#         "habitat": "Habitat",
#         "planvital": "PlanVital",
#         "plan vital": "PlanVital",
#         "modelo": "Modelo",
#         "uno": "Uno",
#         # Añade más mapeos si hay otras variantes
#     }
    
#     def normalize_afp_name(afp_name, mapping_dict):
#         """
#         Normaliza y mapea el nombre de la AFP a su forma estándar.

#         Args:
#             afp_name (str): Nombre de la AFP extraído del DataFrame.
#             mapping_dict (dict): Diccionario de mapeo de AFPs.

#         Returns:
#             str: Nombre estándar de la AFP si se encuentra en el mapeo, de lo contrario, nombre original.
#         """
#         afp_clean = afp_name.strip().lower()
#         return mapping_dict.get(afp_clean, afp_name)
    
#     # Cálculo de la columna 'Porcentaje' con normalización y manejo de errores
#     porcentaje_list = []
#     for index, row in df.iterrows():
#         periodo = row['Periodo']
#         afp = row['AFP']
#         porcentaje = None
#         if periodo in indicadores and 'afp' in indicadores[periodo] and 'tasa_afp_dependientes' in indicadores[periodo]['afp']:
#             afp_list = indicadores[periodo]['afp']['afp']
#             tasa_list = indicadores[periodo]['afp']['tasa_afp_dependientes']
            
#             # Normalizar y mapear el nombre de la AFP
#             afp_normalized = normalize_afp_name(afp, afp_mapping)
            
#             afp_list_clean = [a.strip().lower() for a in afp_list]
            
#             # Intentar encontrar el índice de la AFP mapeada
#             if afp_normalized.lower() in afp_list_clean:
#                 idx = afp_list_clean.index(afp_normalized.lower())
#                 tasa_str = tasa_list[idx].replace('%', '').replace(',', '.')
#                 try:
#                     tasa = float(tasa_str) - 10.0
#                     porcentaje = round(tasa, 2)
#                 except ValueError:
#                     print(f"Error al convertir la tasa para AFP '{afp}' (mapeada como '{afp_normalized}') en el período '{periodo}'. Tasa: '{tasa_str}'")
#                     porcentaje = 0.0  # Tasa por defecto
#             else:
#                 print(f"AFP '{afp}' (mapeada como '{afp_normalized}') no encontrada en el período '{periodo}'. Asignando tasa por defecto.")
#                 porcentaje = 0.0  # Tasa por defecto
#         else:
#             print(f"Período '{periodo}' o campos 'afp'/'tasa_afp_dependientes' no encontrados en el JSON. Asignando tasa por defecto.")
#             porcentaje = 0.0  # Tasa por defecto
#         porcentaje_list.append(porcentaje)
    
#     df['Porcentaje'] = porcentaje_list
    
#     # Cálculo de la columna 'Tipo_Renta'
#     tipo_renta_list = []
#     for index, row in df.iterrows():
#         periodo = row['Periodo']
#         remuneracion = row['Remuneración']
#         tipo_renta = "Inferior a Renta Tope"
#         if periodo in indicadores and 'rentas_topes_imponibles' in indicadores[periodo]:
#             renta_tope_str = indicadores[periodo]['rentas_topes_imponibles']['valor'][0].replace('$', '').replace('.', '').replace(',', '.')
#             try:
#                 renta_tope = float(renta_tope_str)
#                 if remuneracion >= renta_tope:
#                     tipo_renta = "Renta Tope"
#             except ValueError:
#                 print(f"Error al convertir el tope imponible en el período '{periodo}'. Tope: '{renta_tope_str}'")
#         tipo_renta_list.append(tipo_renta)
    
#     df['Tipo_Renta'] = tipo_renta_list
    
#     # Actualizar filas con 'Remuneración' = 0 usando el valor más alto para el mismo RUT
#     for rut in df['RUT'].unique():
#         max_remuneracion = df.loc[(df['RUT'] == rut) & (df['Remuneración'] > 0), 'Remuneración'].max()
#         if pd.notna(max_remuneracion):
#             df.loc[(df['RUT'] == rut) & (df['Remuneración'] == 0), 'Remuneración'] = max_remuneracion
    
    
#     # Inicializar 'Rem_Días' como float
#     df['Rem_Días'] = 0.0

#     # Condición: Días <= 11
#     cond1 = df['Días'] <= 11

#     # Subcondición 1a: Tipo_Renta == 'Renta Tope'
#     cond1a = cond1 & (df['Tipo_Renta'] == 'Renta Tope')
#     df.loc[cond1a, 'Rem_Días'] = (df.loc[cond1a, 'Remuneración'] / 30) * df.loc[cond1a, 'Días']

#     # Subcondición 1b: Tipo_Renta != 'Renta Tope'
#     cond1b = cond1 & (df['Tipo_Renta'] != 'Renta Tope')

#     # Subcondición 1b1: Días <= 3
#     cond1b1 = cond1b & (df['Días'] <= 3)
#     df.loc[cond1b1, 'Rem_Días'] = (df.loc[cond1b1, 'Remuneración'] / 30) * df.loc[cond1b1, 'Días']

#     # Subcondición 1b2: Días > 3
#     cond1b2 = cond1b & (df['Días'] > 3)
#     df.loc[cond1b2, 'Rem_Días'] = (df.loc[cond1b2, 'Remuneración'] / 30) * 3

#     # Condición: Días > 11
#     cond2 = df['Días'] > 11

#     # Subcondición 2a: Días es 29, 30 o 31
#     cond2a = cond2 & df['Días'].isin([29, 30, 31])
#     df.loc[cond2a, 'Rem_Días'] = (df.loc[cond2a, 'Remuneración'] / 30) * df.loc[cond2a, 'Días']

#     # Subcondición 2b: Días no es 29, 30 o 31
#     cond2b = cond2 & ~df['Días'].isin([29, 30, 31])
#     df.loc[cond2b, 'Rem_Días'] = 0.0

#     # Finalizar el cálculo de 'Rem_Días'
#     df['Rem_Días'] = df['Rem_Días'].fillna(0).round(0).astype(int)
    
#     # Calcular columna 'Pensión'
#     df['Pensión'] = (df['Rem_Días'] * 0.10).fillna(0).round(0).astype(int)
    
#     # Calcular columna 'Comisión'
#     df['Comisión'] = ((df['Porcentaje'] / 100) * df['Rem_Días']).fillna(0).round(0).astype(int)
    
#     # Calcular columna 'Total_AFP'
#     df['Total_AFP'] = (df['Pensión'] + df['Comisión']).fillna(0).astype(int)
    
#     # Calcular columna 'Análisis_Individual'
#     df['Análisis_Individual'] = df['Días'].apply(lambda x: 'CORRESPONDE' if pd.notna(x) and x <= 10 else 'NO CORRESPONDE')
    
#     # Calcular licencias continuas y análisis del grupo
#     df.sort_values(by=['RUT', 'Fecha Inicio'], inplace=True)
#     df.reset_index(drop=True, inplace=True)
    
#     grupos_continuos = []
#     grupo_id = 0
#     dias_continuos_dict = {}
    
#     for index in range(len(df)):
#         if index == 0 or df.loc[index, 'RUT'] != df.loc[index - 1, 'RUT']:
#             # Nuevo RUT o primera fila
#             grupo_id += 1
#             dias_continuos_dict[grupo_id] = df.loc[index, 'Días']
#         else:
#             # Verificar si la licencia es continua con la anterior
#             fecha_termino_anterior = df.loc[index - 1, 'Fecha Término']
#             fecha_inicio_actual = df.loc[index, 'Fecha Inicio']
#             if pd.notna(fecha_termino_anterior) and pd.notna(fecha_inicio_actual):
#                 if fecha_termino_anterior + pd.Timedelta(days=1) == fecha_inicio_actual:
#                     dias_continuos_dict[grupo_id] += df.loc[index, 'Días']
#                 else:
#                     grupo_id += 1
#                     dias_continuos_dict[grupo_id] = df.loc[index, 'Días']
#             else:
#                 grupo_id += 1
#                 dias_continuos_dict[grupo_id] = df.loc[index, 'Días']
#         grupos_continuos.append(grupo_id)
    
#     df['Grupo_Continuo'] = grupos_continuos
    
#     # Actualizar columna 'Análisis_Grupo' basado en reglas de licencias continuas
#     analisis_grupo_list = []
#     for index, row in df.iterrows():
#         grupo = row['Grupo_Continuo']
#         total_dias = dias_continuos_dict[grupo]
#         if total_dias < 11:
#             analisis_grupo = 'CORRESPONDE'
#         elif 11 <= total_dias <= 29:
#             analisis_grupo = 'NO CORRESPONDE'
#         else:
#             analisis_grupo = 'CORRESPONDE'
#         analisis_grupo_list.append(analisis_grupo)
    
#     df['Análisis_Grupo'] = analisis_grupo_list
    
#     # Generar columna resumen combinando análisis individual y de grupo
#     df['Resumen'] = df.apply(lambda row: f"Individual: {row['Análisis_Individual']}, Grupo: {row['Análisis_Grupo']}", axis=1)
    
#     df['Fecha Inicio'] = df['Fecha Inicio'].dt.strftime('%d-%m-%Y').fillna('')
#     df['Fecha Término'] = df['Fecha Término'].dt.strftime('%d-%m-%Y').fillna('')
    
#     return df

# def main():
#     if not os.path.exists(output_folder):
#         os.makedirs(output_folder)
    
#     for filename in tqdm(os.listdir(input_folder), desc="Procesando archivos PDF", unit="archivo"):
#         if filename.endswith(".pdf"):
#             pdf_path = os.path.join(input_folder, filename)
#             extracted_data = extract_data_from_pdf(pdf_path)
#             if extracted_data:
#                 df = transform_data_to_dataframe(extracted_data)
#                 output_path = os.path.join(output_folder, f"{os.path.splitext(filename)[0]}.xlsx")
#                 df.to_excel(output_path, index=False)
    
#     excel_files = glob.glob(os.path.join(output_folder, "*.xlsx"))
#     if excel_files:
#         dataframes = [pd.read_excel(file) for file in excel_files]
#         combined_df = pd.concat(dataframes, ignore_index=True)
#         combined_df.to_excel(combined_excel_path, index=False)
#         print(f"Datos extraídos y guardados en {combined_excel_path}")
#     else:
#         print("No se encontraron archivos Excel para consolidar.")

# if __name__ == "__main__":
#     main()


# NUEVO: lunes 25 noviembre 2024

# TRABAJANDO: LOGICA NUEVO ACTUAIZDA 

import pdfplumber
import pandas as pd
import re
import os
import json
from tqdm import tqdm
import glob
import numpy as np
import logging

# Configuración básica del logging
logging.basicConfig(
    filename='procesamiento.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Configuración de carpetas y columnas requeridas
input_folder = "upload"
output_folder = "output"
combined_excel_path = "output/resultado_concatenado.xlsx"
json_path = "indicadores/indicadores.json"
required_columns = ['RUT', 'Apellido Paterno, Materno, Nombres', 'Remuneración', 'Cod.', 'Fecha Inicio', 'Fecha Término', 'AFP', 'Días', 'Rem_Días', 'Periodo', 'Porcentaje', 'Pensión', 'Comisión', 'Total_AFP', 'Análisis', 'Tipo_Renta']

header_patterns = [
    r"^RUT$", r"Apellido Paterno,? Materno,? Nombres", r"Remuneración", r"Fecha Inicio", r"Fecha Término", r"Cod."
]

specific_headers = [
    "Identificación del Trabajador", "Fondo de Pensiones", "Seguro Cesantía", "Movimiento de Personal"
]

def is_header_row(row):
    return any(re.search(pattern, str(cell), re.IGNORECASE) for cell in row for pattern in header_patterns) or \
           any(header in str(cell) for cell in row for header in specific_headers)

def extract_data_from_pdf(pdf_path):
    data = []
    afp_name = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            first_page_text = pdf.pages[0].extract_text()
            match = re.search(r"AFP\s+(\w+)", first_page_text, re.IGNORECASE)
            if match:
                afp_name = match.group(1)
                logging.info(f"AFP encontrada en {pdf_path}: {afp_name}")
            else:
                logging.warning(f"No se encontró el nombre de la AFP en {pdf_path}. Asignando 'Desconocida'.")
                afp_name = "Desconocida"
            
            for page in tqdm(pdf.pages, desc=f"Procesando páginas de {pdf_path}", unit="página"):
                table = page.extract_table()
                if table:
                    for row in table:
                        if row and len(row) >= 15 and not is_header_row(row):
                            try:
                                # Extracción de datos con verificación de longitud de fila
                                rut = row[0]
                                nombres = row[1]
                                remuneracion = re.sub(r'[^0-9,]', '', row[2]).replace(',', '.')
                                cod = row[12]
                                fecha_inicio = row[13]
                                fecha_termino = row[14]
                                # Agregar fila extraída
                                data.append([rut, nombres, remuneracion, cod, fecha_inicio, fecha_termino, afp_name])
                            except IndexError:
                                logging.error(f"Fila con longitud insuficiente en {pdf_path}: {row}")
    except Exception as e:
        logging.error(f"Error al procesar {pdf_path}: {e}")
    return data

def transform_data_to_dataframe(data):
    df = pd.DataFrame(data, columns=required_columns[:-9])
    # Convertir 'Remuneración' a numérico
    df['Remuneración'] = pd.to_numeric(df['Remuneración'].str.replace(',', '.'), errors='coerce')
    # Eliminar filas con RUTs de encabezado u otras filas no deseadas
    df = df[~df['RUT'].str.contains("Identificación del Trabajador|Fondo de Pensiones|Seguro Cesantía|Movimiento de Personal|Totales Generales", case=False, na=False)]
    
    # Convertir fechas a datetime
    df['Fecha Inicio'] = pd.to_datetime(df['Fecha Inicio'], errors='coerce', dayfirst=True)
    df['Fecha Término'] = pd.to_datetime(df['Fecha Término'], errors='coerce', dayfirst=True)
    
    # Calcular 'Días' y 'Periodo'
    df['Días'] = (df['Fecha Término'] - df['Fecha Inicio']).dt.days + 1
    df['Periodo'] = df['Fecha Inicio'].dt.strftime('%Y%m').fillna('')
    
    # Cargar JSON con indicadores
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            indicadores = json.load(f)
        logging.info(f"JSON de indicadores cargado correctamente desde {json_path}.")
    except Exception as e:
        logging.error(f"Error al cargar el archivo JSON: {e}")
        indicadores = {}
    
    # Diccionario de mapeo de nombres de AFPs
    afp_mapping = {
        "provida": "Provida",
        "proviva": "Provida",
        "pro vida": "Provida",
        "capital": "Capital",
        "cuprum": "Cuprum",
        "habitat": "Habitat",
        "planvital": "PlanVital",
        "plan vital": "PlanVital",
        "modelo": "Modelo",
        "uno": "Uno",
        # Añade más mapeos si hay otras variantes
    }
    
    def normalize_afp_name(afp_name, mapping_dict):
        """
        Normaliza y mapea el nombre de la AFP a su forma estándar.

        Args:
            afp_name (str): Nombre de la AFP extraído del DataFrame.
            mapping_dict (dict): Diccionario de mapeo de AFPs.

        Returns:
            str: Nombre estándar de la AFP si se encuentra en el mapeo, de lo contrario, nombre original.
        """
        afp_clean = afp_name.strip().lower()
        return mapping_dict.get(afp_clean, afp_name)
    
    # Cálculo de la columna 'Porcentaje' con normalización y manejo de errores
    porcentaje_list = []
    for index, row in df.iterrows():
        periodo = row['Periodo']
        afp = row['AFP']
        porcentaje = None
        if periodo and periodo in indicadores and 'afp' in indicadores[periodo] and 'tasa_afp_dependientes' in indicadores[periodo]['afp']:
            afp_list = indicadores[periodo]['afp']['afp']
            tasa_list = indicadores[periodo]['afp']['tasa_afp_dependientes']
            
            # Normalizar y mapear el nombre de la AFP
            afp_normalized = normalize_afp_name(afp, afp_mapping)
            
            afp_list_clean = [a.strip().lower() for a in afp_list]
            
            # Intentar encontrar el índice de la AFP mapeada
            if afp_normalized.lower() in afp_list_clean:
                idx = afp_list_clean.index(afp_normalized.lower())
                tasa_str = tasa_list[idx].replace('%', '').replace(',', '.').strip()
                try:
                    tasa = float(tasa_str) - 10.0
                    porcentaje = round(tasa, 2)
                except ValueError:
                    logging.error(f"Error al convertir la tasa para AFP '{afp}' (mapeada como '{afp_normalized}') en el período '{periodo}'. Tasa: '{tasa_str}'")
                    porcentaje = 0.0  # Tasa por defecto
            else:
                logging.warning(f"AFP '{afp}' (mapeada como '{afp_normalized}') no encontrada en el período '{periodo}'. Asignando tasa por defecto.")
                porcentaje = 0.0  # Tasa por defecto
        else:
            logging.warning(f"Período '{periodo}' o campos 'afp'/'tasa_afp_dependientes' no encontrados en el JSON. Asignando tasa por defecto.")
            porcentaje = 0.0  # Tasa por defecto
        porcentaje_list.append(porcentaje)
    
    df['Porcentaje'] = porcentaje_list
    
    # Cálculo de la columna 'Tipo_Renta'
    tipo_renta_list = []
    for index, row in df.iterrows():
        periodo = row['Periodo']
        remuneracion = row['Remuneración']
        tipo_renta = "Inferior a Renta Tope"
        if periodo and periodo in indicadores and 'rentas_topes_imponibles' in indicadores[periodo]:
            renta_tope_str = indicadores[periodo]['rentas_topes_imponibles']['valor'][0].replace('$', '').replace('.', '').replace(',', '.').strip()
            try:
                renta_tope = float(renta_tope_str)
                if remuneracion >= renta_tope:
                    tipo_renta = "Renta Tope"
            except ValueError:
                logging.error(f"Error al convertir el tope imponible en el período '{periodo}'. Tope: '{renta_tope_str}'")
        tipo_renta_list.append(tipo_renta)
    
    df['Tipo_Renta'] = tipo_renta_list
    
    # Actualizar filas con 'Remuneración' = 0 usando el valor más alto para el mismo RUT
    for rut in df['RUT'].unique():
        max_remuneracion = df.loc[(df['RUT'] == rut) & (df['Remuneración'] > 0), 'Remuneración'].max()
        if pd.notna(max_remuneracion):
            df.loc[(df['RUT'] == rut) & (df['Remuneración'] == 0), 'Remuneración'] = max_remuneracion
    
    # Inicializar 'Rem_Días' como float
    df['Rem_Días'] = 0.0

    # Condición: Días <= 11
    cond1 = df['Días'] <= 11

    # Subcondición 1a: Tipo_Renta == 'Renta Tope'
    cond1a = cond1 & (df['Tipo_Renta'] == 'Renta Tope')
    df.loc[cond1a, 'Rem_Días'] = (df.loc[cond1a, 'Remuneración'] / 30) * df.loc[cond1a, 'Días']

    # Subcondición 1b: Tipo_Renta != 'Renta Tope'
    cond1b = cond1 & (df['Tipo_Renta'] != 'Renta Tope')

    # Subcondición 1b1: Días <= 3
    cond1b1 = cond1b & (df['Días'] <= 3)
    df.loc[cond1b1, 'Rem_Días'] = (df.loc[cond1b1, 'Remuneración'] / 30) * df.loc[cond1b1, 'Días']

    # Subcondición 1b2: Días > 3
    cond1b2 = cond1b & (df['Días'] > 3)
    df.loc[cond1b2, 'Rem_Días'] = (df.loc[cond1b2, 'Remuneración'] / 30) * 3

    # Condición: Días > 11
    cond2 = df['Días'] > 11

    # Subcondición 2a: Días es 29, 30 o 31
    cond2a = cond2 & df['Días'].isin([29, 30, 31])
    df.loc[cond2a, 'Rem_Días'] = (df.loc[cond2a, 'Remuneración'] / 30) * df.loc[cond2a, 'Días']

    # Subcondición 2b: Días no es 29, 30 o 31
    cond2b = cond2 & ~df['Días'].isin([29, 30, 31])
    df.loc[cond2b, 'Rem_Días'] = 0.0

    # Finalizar el cálculo de 'Rem_Días'
    df['Rem_Días'] = df['Rem_Días'].fillna(0).round(0).astype(int)
    
    # Calcular columna 'Pensión'
    df['Pensión'] = (df['Rem_Días'] * 0.10).fillna(0).round(0).astype(int)
    
    # Calcular columna 'Comisión'
    df['Comisión'] = ((df['Porcentaje'] / 100) * df['Rem_Días']).fillna(0).round(0).astype(int)
    
    # Calcular columna 'Total_AFP'
    df['Total_AFP'] = (df['Pensión'] + df['Comisión']).fillna(0).astype(int)
    
    # Calcular columna 'Análisis_Individual'
    df['Análisis_Individual'] = df['Días'].apply(lambda x: 'CORRESPONDE' if pd.notna(x) and x <= 10 else 'NO CORRESPONDE')
    
    # Calcular licencias continuas y análisis del grupo
    df.sort_values(by=['RUT', 'Fecha Inicio'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    grupos_continuos = []
    grupo_id = 0
    dias_continuos_dict = {}
    
    for index in range(len(df)):
        if index == 0 or df.loc[index, 'RUT'] != df.loc[index - 1, 'RUT']:
            # Nuevo RUT o primera fila
            grupo_id += 1
            dias_continuos_dict[grupo_id] = df.loc[index, 'Días']
        else:
            # Verificar si la licencia es continua con la anterior
            fecha_termino_anterior = df.loc[index - 1, 'Fecha Término']
            fecha_inicio_actual = df.loc[index, 'Fecha Inicio']
            if pd.notna(fecha_termino_anterior) and pd.notna(fecha_inicio_actual):
                if fecha_termino_anterior + pd.Timedelta(days=1) == fecha_inicio_actual:
                    dias_continuos_dict[grupo_id] += df.loc[index, 'Días']
                else:
                    grupo_id += 1
                    dias_continuos_dict[grupo_id] = df.loc[index, 'Días']
            else:
                grupo_id += 1
                dias_continuos_dict[grupo_id] = df.loc[index, 'Días']
        grupos_continuos.append(grupo_id)
    
    df['Grupo_Continuo'] = grupos_continuos
    
    # Actualizar columna 'Análisis_Grupo' basado en reglas de licencias continuas
    analisis_grupo_list = []
    for index, row in df.iterrows():
        grupo = row['Grupo_Continuo']
        total_dias = dias_continuos_dict[grupo]
        if total_dias < 11:
            analisis_grupo = 'CORRESPONDE'
        elif 11 <= total_dias <= 29:
            analisis_grupo = 'NO CORRESPONDE'
        else:
            analisis_grupo = 'CORRESPONDE'
        analisis_grupo_list.append(analisis_grupo)
    
    df['Análisis_Grupo'] = analisis_grupo_list
    
    # Generar columna resumen combinando análisis individual y de grupo
    df['Resumen'] = df.apply(lambda row: f"Individual: {row['Análisis_Individual']}, Grupo: {row['Análisis_Grupo']}", axis=1)
    
    # Formatear fechas
    df['Fecha Inicio'] = df['Fecha Inicio'].dt.strftime('%d-%m-%Y').fillna('')
    df['Fecha Término'] = df['Fecha Término'].dt.strftime('%d-%m-%Y').fillna('')
    
    return df

def main():
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    for filename in tqdm(os.listdir(input_folder), desc="Procesando archivos PDF", unit="archivo"):
        if filename.endswith(".pdf"):
            pdf_path = os.path.join(input_folder, filename)
            extracted_data = extract_data_from_pdf(pdf_path)
            if extracted_data:
                df = transform_data_to_dataframe(extracted_data)
                output_path = os.path.join(output_folder, f"{os.path.splitext(filename)[0]}.xlsx")
                df.to_excel(output_path, index=False)
                logging.info(f"Archivo Excel creado: {output_path}")
            else:
                logging.warning(f"No se extrajo ningún dato de {pdf_path}.")
    
    excel_files = glob.glob(os.path.join(output_folder, "*.xlsx"))
    if excel_files:
        try:
            dataframes = [pd.read_excel(file) for file in excel_files]
            combined_df = pd.concat(dataframes, ignore_index=True)
            combined_df.to_excel(combined_excel_path, index=False)
            logging.info(f"Datos extraídos y guardados en {combined_excel_path}")
        except Exception as e:
            logging.error(f"Error al concatenar archivos Excel: {e}")
    else:
        logging.warning("No se encontraron archivos Excel para consolidar.")

if __name__ == "__main__":
    main()
