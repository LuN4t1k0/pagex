
# # # TRABAJANDO: v8 campos calculados

# import pdfplumber
# import pandas as pd
# import re
# import os
# import json
# from tqdm import tqdm
# import glob

# # Configuración de carpetas y columnas requeridas
# input_folder = "upload"
# output_folder = "output"
# combined_excel_path = "output/resultado_concatenado.xlsx"
# json_path = "indicadores/indicadores.json"
# required_columns = ['RUT', 'Apellido Paterno, Materno, Nombres', 'Remuneración', 'Cod.', 'Fecha Inicio', 'Fecha Término', 'AFP', 'Días', 'Rem_Días', 'Periodo', 'Porcentaje', 'Pensión', 'Comisión', 'Total_AFP', 'Análisis']

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
#                         if row and len(row) >= len(required_columns) - 8 and not is_header_row(row):
#                             data.append([row[0], row[1], re.sub(r'[^0-9,]', '', row[2]).replace(',', '.'), row[12], row[13], row[14], afp_name])
#     except Exception as e:
#         print(f"Error al procesar {pdf_path}: {e}")
#     return data

# def transform_data_to_dataframe(data):
#     df = pd.DataFrame(data, columns=required_columns[:-8])
#     df['Remuneración'] = pd.to_numeric(df['Remuneración'].str.replace(',', '.'), errors='coerce')
#     df = df[~df['RUT'].str.contains("Identificación del Trabajador|Fondo de Pensiones|Seguro Cesantía|Movimiento de Personal|Totales Generales", case=False, na=False)]
    
#     df['Fecha Inicio'] = pd.to_datetime(df['Fecha Inicio'], errors='coerce', dayfirst=True)
#     df['Fecha Término'] = pd.to_datetime(df['Fecha Término'], errors='coerce', dayfirst=True)
    
#     df['Días'] = (df['Fecha Término'] - df['Fecha Inicio']).dt.days + 1
#     df['Rem_Días'] = ((df['Remuneración'] / 30) * df['Días']).fillna(0).round(0).astype(int)
#     df['Periodo'] = df['Fecha Inicio'].dt.strftime('%Y%m').fillna('')
#     df['Pensión'] = (df['Rem_Días'] * 0.10).fillna(0).round(0).astype(int)
    
#     # Cargar el JSON con los indicadores
#     try:
#         with open(json_path, 'r', encoding='utf-8') as f:
#             indicadores = json.load(f)
#     except Exception as e:
#         print(f"Error al cargar el archivo JSON: {e}")
#         indicadores = {}
    
#     # Calcular la columna 'Porcentaje'
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
    
#     # Calcular la columna 'Comisión'
#     df['Comisión'] = ((df['Porcentaje'] / 100) * df['Rem_Días']).fillna(0).round(0).astype(int)
    
#     # Calcular la columna 'Total_AFP'
#     df['Total_AFP'] = (df['Pensión'] + df['Comisión']).fillna(0).astype(int)
    
#     # Calcular la columna 'Análisis'
#     df['Análisis'] = df['Días'].apply(lambda x: 'CORRESPONDE' if pd.notna(x) and x <= 10 else 'NO CORRESPONDE')
    
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



# TRABAJANDO: IDENTIFICAR RENTA TOPE
# import pdfplumber
# import pandas as pd
# import re
# import os
# import json
# from tqdm import tqdm
# import glob

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
#     df['Rem_Días'] = ((df['Remuneración'] / 30) * df['Días']).fillna(0).round(0).astype(int)
#     df['Periodo'] = df['Fecha Inicio'].dt.strftime('%Y%m').fillna('')
#     df['Pensión'] = (df['Rem_Días'] * 0.10).fillna(0).round(0).astype(int)
    
#     # Cargar el JSON con los indicadores
#     try:
#         with open(json_path, 'r', encoding='utf-8') as f:
#             indicadores = json.load(f)
#     except Exception as e:
#         print(f"Error al cargar el archivo JSON: {e}")
#         indicadores = {}
    
#     # Calcular la columna 'Porcentaje'
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
    
#     # Calcular la columna 'Comisión'
#     df['Comisión'] = ((df['Porcentaje'] / 100) * df['Rem_Días']).fillna(0).round(0).astype(int)
    
#     # Calcular la columna 'Total_AFP'
#     df['Total_AFP'] = (df['Pensión'] + df['Comisión']).fillna(0).astype(int)
    
#     # Calcular la columna 'Análisis'
#     df['Análisis'] = df['Días'].apply(lambda x: 'CORRESPONDE' if pd.notna(x) and x <= 10 else 'NO CORRESPONDE')
    
#     # Calcular la columna 'Tipo_Renta'
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


# # TRABAJANDO: REVISAR: muy complejo rellenar segun rut 

# import pdfplumber
# import pandas as pd
# import re
# import os
# import json
# from tqdm import tqdm
# import glob

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
#     df['Rem_Días'] = ((df['Remuneración'] / 30) * df['Días']).fillna(0).round(0).astype(int)
#     df['Periodo'] = df['Fecha Inicio'].dt.strftime('%Y%m').fillna('')
#     df['Pensión'] = (df['Rem_Días'] * 0.10).fillna(0).round(0).astype(int)
    
#     # Cargar el JSON con los indicadores
#     try:
#         with open(json_path, 'r', encoding='utf-8') as f:
#             indicadores = json.load(f)
#     except Exception as e:
#         print(f"Error al cargar el archivo JSON: {e}")
#         indicadores = {}
    
#     # Calcular la columna 'Porcentaje'
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
    
#     # Calcular la columna 'Comisión'
#     df['Comisión'] = ((df['Porcentaje'] / 100) * df['Rem_Días']).fillna(0).round(0).astype(int)
    
#     # Calcular la columna 'Total_AFP'
#     df['Total_AFP'] = (df['Pensión'] + df['Comisión']).fillna(0).astype(int)
    
#     # Calcular la columna 'Análisis'
#     df['Análisis'] = df['Días'].apply(lambda x: 'CORRESPONDE' if pd.notna(x) and x <= 10 else 'NO CORRESPONDE')
    
#     # Calcular la columna 'Tipo_Renta'
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
    
#     # Actualizar las filas con 'Remuneración' = 0 usando el valor más alto del mismo RUT
#     for rut in df['RUT'].unique():
#         max_remuneracion = df.loc[(df['RUT'] == rut) & (df['Remuneración'] > 0), 'Remuneración'].max()
#         if pd.notna(max_remuneracion):
#             df.loc[(df['RUT'] == rut) & (df['Remuneración'] == 0), 'Remuneración'] = max_remuneracion
    
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


# TRABAJANDO: REVISAR: COMPLEJO calculo continuidad de fechas

# import pdfplumber
# import pandas as pd
# import re
# import os
# import json
# from tqdm import tqdm
# import glob

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
#     df['Rem_Días'] = ((df['Remuneración'] / 30) * df['Días']).fillna(0).round(0).astype(int)
#     df['Periodo'] = df['Fecha Inicio'].dt.strftime('%Y%m').fillna('')
#     df['Pensión'] = (df['Rem_Días'] * 0.10).fillna(0).round(0).astype(int)
    
#     # Cargar el JSON con los indicadores
#     try:
#         with open(json_path, 'r', encoding='utf-8') as f:
#             indicadores = json.load(f)
#     except Exception as e:
#         print(f"Error al cargar el archivo JSON: {e}")
#         indicadores = {}
    
#     # Calcular la columna 'Porcentaje'
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
    
#     # Calcular la columna 'Comisión'
#     df['Comisión'] = ((df['Porcentaje'] / 100) * df['Rem_Días']).fillna(0).round(0).astype(int)
    
#     # Calcular la columna 'Total_AFP'
#     df['Total_AFP'] = (df['Pensión'] + df['Comisión']).fillna(0).astype(int)
    
#     # Calcular la columna 'Análisis' para cada licencia individual
#     df['Análisis_Individual'] = df['Días'].apply(lambda x: 'CORRESPONDE' if pd.notna(x) and x <= 10 else 'NO CORRESPONDE')
    
#     # Calcular la columna 'Tipo_Renta'
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
    
#     # Actualizar las filas con 'Remuneración' = 0 usando el valor más alto del mismo RUT
#     for rut in df['RUT'].unique():
#         max_remuneracion = df.loc[(df['RUT'] == rut) & (df['Remuneración'] > 0), 'Remuneración'].max()
#         if pd.notna(max_remuneracion):
#             df.loc[(df['RUT'] == rut) & (df['Remuneración'] == 0), 'Remuneración'] = max_remuneracion
    
#     # Calcular las licencias continuas y el análisis del cobro
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
#             if fecha_termino_anterior + pd.Timedelta(days=1) == fecha_inicio_actual:
#                 dias_continuos_dict[grupo_id] += df.loc[index, 'Días']
#             else:
#                 grupo_id += 1
#                 dias_continuos_dict[grupo_id] = df.loc[index, 'Días']
#         grupos_continuos.append(grupo_id)
    
#     df['Grupo_Continuo'] = grupos_continuos
    
#     # Actualizar la columna 'Análisis_Grupo' basado en las reglas de licencias continuas
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
    
#     # Generar una columna de resumen que combine el análisis individual y grupal
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


# REVISAR:
import pdfplumber
import pandas as pd
import re
import os
import json
from tqdm import tqdm
import glob

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
            match = re.search(r"AFP\s+(\w+)", first_page_text)
            if match:
                afp_name = match.group(1)
            
            for page in tqdm(pdf.pages, desc=f"Procesando páginas de {pdf_path}", unit="página"):
                table = page.extract_table()
                if table:
                    for row in table:
                        if row and len(row) >= len(required_columns) - 9 and not is_header_row(row):
                            data.append([row[0], row[1], re.sub(r'[^0-9,]', '', row[2]).replace(',', '.'), row[12], row[13], row[14], afp_name])
    except Exception as e:
        print(f"Error al procesar {pdf_path}: {e}")
    return data

def transform_data_to_dataframe(data):
    df = pd.DataFrame(data, columns=required_columns[:-9])
    df['Remuneración'] = pd.to_numeric(df['Remuneración'].str.replace(',', '.'), errors='coerce')
    df = df[~df['RUT'].str.contains("Identificación del Trabajador|Fondo de Pensiones|Seguro Cesantía|Movimiento de Personal|Totales Generales", case=False, na=False)]
    
    df['Fecha Inicio'] = pd.to_datetime(df['Fecha Inicio'], errors='coerce', dayfirst=True)
    df['Fecha Término'] = pd.to_datetime(df['Fecha Término'], errors='coerce', dayfirst=True)
    
    df['Días'] = (df['Fecha Término'] - df['Fecha Inicio']).dt.days + 1
    df['Rem_Días'] = ((df['Remuneración'] / 30) * df['Días']).fillna(0).round(0).astype(int)
    df['Periodo'] = df['Fecha Inicio'].dt.strftime('%Y%m').fillna('')
    df['Pensión'] = (df['Rem_Días'] * 0.10).fillna(0).round(0).astype(int)
    
    # Cargar el JSON con los indicadores
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            indicadores = json.load(f)
    except Exception as e:
        print(f"Error al cargar el archivo JSON: {e}")
        indicadores = {}
    
    # Calcular la columna 'Porcentaje'
    porcentaje_list = []
    for index, row in df.iterrows():
        periodo = row['Periodo']
        afp = row['AFP']
        porcentaje = None
        if periodo in indicadores and 'afp' in indicadores[periodo] and 'tasa_afp_dependientes' in indicadores[periodo]['afp']:
            afp_list = indicadores[periodo]['afp']['afp']
            tasa_list = indicadores[periodo]['afp']['tasa_afp_dependientes']
            if afp in afp_list:
                idx = afp_list.index(afp)
                tasa_str = tasa_list[idx].replace('%', '').replace(',', '.')
                tasa = float(tasa_str) - 10.0
                porcentaje = round(tasa, 2)
        porcentaje_list.append(porcentaje)
    
    df['Porcentaje'] = porcentaje_list
    
    # Calcular la columna 'Comisión'
    df['Comisión'] = ((df['Porcentaje'] / 100) * df['Rem_Días']).fillna(0).round(0).astype(int)
    
    # Calcular la columna 'Total_AFP'
    df['Total_AFP'] = (df['Pensión'] + df['Comisión']).fillna(0).astype(int)
    
    # Calcular la columna 'Análisis' para cada licencia individual
    df['Análisis_Individual'] = df['Días'].apply(lambda x: 'CORRESPONDE' if pd.notna(x) and x <= 10 else 'NO CORRESPONDE')
    
    # Calcular la columna 'Tipo_Renta'
    tipo_renta_list = []
    for index, row in df.iterrows():
        periodo = row['Periodo']
        remuneracion = row['Remuneración']
        tipo_renta = "Inferior a Renta Tope"
        if periodo in indicadores and 'rentas_topes_imponibles' in indicadores[periodo]:
            renta_tope_str = indicadores[periodo]['rentas_topes_imponibles']['valor'][0].replace('$', '').replace('.', '').replace(',', '.')
            renta_tope = float(renta_tope_str)
            if remuneracion >= renta_tope:
                tipo_renta = "Renta Tope"
        tipo_renta_list.append(tipo_renta)
    
    df['Tipo_Renta'] = tipo_renta_list
    
    # Actualizar las filas con 'Remuneración' = 0 usando el valor más alto del mismo RUT
    for rut in df['RUT'].unique():
        max_remuneracion = df.loc[(df['RUT'] == rut) & (df['Remuneración'] > 0), 'Remuneración'].max()
        if pd.notna(max_remuneracion):
            df.loc[(df['RUT'] == rut) & (df['Remuneración'] == 0), 'Remuneración'] = max_remuneracion
    
    # Calcular las licencias continuas y el análisis del cobro
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
            if fecha_termino_anterior + pd.Timedelta(days=1) == fecha_inicio_actual:
                dias_continuos_dict[grupo_id] += df.loc[index, 'Días']
            else:
                grupo_id += 1
                dias_continuos_dict[grupo_id] = df.loc[index, 'Días']
        grupos_continuos.append(grupo_id)
    
    df['Grupo_Continuo'] = grupos_continuos
    
    # Actualizar la columna 'Análisis_Grupo' basado en las reglas de licencias continuas
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
    
    # Generar una columna de resumen que combine el análisis individual y grupal
    df['Resumen'] = df.apply(lambda row: f"Individual: {row['Análisis_Individual']}, Grupo: {row['Análisis_Grupo']}", axis=1)
    
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
    
    excel_files = glob.glob(os.path.join(output_folder, "*.xlsx"))
    if excel_files:
        dataframes = [pd.read_excel(file) for file in excel_files]
        combined_df = pd.concat(dataframes, ignore_index=True)
        combined_df.to_excel(combined_excel_path, index=False)
        print(f"Datos extraídos y guardados en {combined_excel_path}")
    else:
        print("No se encontraron archivos Excel para consolidar.")

if __name__ == "__main__":
    main()

