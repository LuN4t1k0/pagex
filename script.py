
# # TRABAJANDO: v9 campos calculados 
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
# required_columns = ['RUT', 'Apellido Paterno, Materno, Nombres', 'Remuneración', 'Cod.', 'Fecha Inicio', 'Fecha Término', 'AFP', 'Días', 'Rem_Días', 'Periodo', 'Porcentaje', 'Pensión', 'Comisión', 'Total_AFP']

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
#                         if row and len(row) >= len(required_columns) - 7 and not is_header_row(row):
#                             data.append([row[0], row[1], re.sub(r'[^0-9,]', '', row[2]).replace(',', '.'), row[12], row[13], row[14], afp_name])
#     except Exception as e:
#         print(f"Error al procesar {pdf_path}: {e}")
#     return data

# def transform_data_to_dataframe(data):
#     df = pd.DataFrame(data, columns=required_columns[:-7])
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


# # TRABAJANDO: v8 campos calculados

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
required_columns = ['RUT', 'Apellido Paterno, Materno, Nombres', 'Remuneración', 'Cod.', 'Fecha Inicio', 'Fecha Término', 'AFP', 'Días', 'Rem_Días', 'Periodo', 'Porcentaje', 'Pensión', 'Comisión', 'Total_AFP', 'Análisis']

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
                        if row and len(row) >= len(required_columns) - 8 and not is_header_row(row):
                            data.append([row[0], row[1], re.sub(r'[^0-9,]', '', row[2]).replace(',', '.'), row[12], row[13], row[14], afp_name])
    except Exception as e:
        print(f"Error al procesar {pdf_path}: {e}")
    return data

def transform_data_to_dataframe(data):
    df = pd.DataFrame(data, columns=required_columns[:-8])
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
    
    # Calcular la columna 'Análisis'
    df['Análisis'] = df['Días'].apply(lambda x: 'CORRESPONDE' if pd.notna(x) and x <= 10 else 'NO CORRESPONDE')
    
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
