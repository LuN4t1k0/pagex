# import pdfplumber
# import pandas as pd
# import re
# import os
# from tqdm import tqdm

# # Carpeta con archivos PDF y archivo Excel de salida
# input_folder = "upload"
# output_excel_path = "resultado_concatenado.xlsx"

# # Columnas requeridas según tu indicación
# required_columns = ['RUT', 'Apellido Paterno, Materno, Nombres', 'Remuneración Imponible',
#                     'Cotización Obligatoria', 'SIS', 'Cotización Voluntaria (APVI)', 'N° Contrato APVI',
#                     'Deposito Convenido', 'Dep. en Cta. Ahorro', 'Remuneración Imponible',
#                     'Cotización Afiliado', 'Cotización Empleador', 'Cod.', 'Fecha Inicio', 'Fecha Término', 'AFP1']

# def is_header_row(row):
#     # Verificar si la fila contiene valores que podrían indicar un encabezado
#     header_patterns = [
#         r"^RUT$", r"Apellido Paterno, Materno, Nombres", r"Remuneración Imponible", r"Cotización Obligatoria", r"SIS",
#         r"Cotización Voluntaria.*", r"N° Contrato APVI", r"Deposito Convenido", r"Dep. en Cta.*", r"Cotización Afiliado",
#         r"Cotización Empleador", r"Fecha Inicio", r"Fecha Término"
#     ]
#     specific_headers = [
#         "Identificación del Trabajador", "Fondo de Pensiones", "Seguro Cesantía", "Movimiento de Personal"
#     ]
#     # Si cualquier celda en la fila coincide con patrones de encabezado o contiene palabras específicas de encabezado
#     return any(re.search(pattern, str(cell), re.IGNORECASE) for cell in row for pattern in header_patterns) or \
#            any(header in str(cell) for cell in row for header in specific_headers)

# def extract_data_from_pdf(pdf_path):
#     data = []
#     afp_name = ""
#     with pdfplumber.open(pdf_path) as pdf:
#         # Extraer el nombre de la AFP desde la primera página
#         first_page_text = pdf.pages[0].extract_text()
#         match = re.search(r"AFP\s+(\w+)", first_page_text)
#         if match:
#             afp_name = match.group(1)
        
#         # Iterar sobre las páginas del PDF
#         for page in tqdm(pdf.pages[1:], desc=f"Procesando páginas de {pdf_path}", unit="página"):
#             table = page.extract_table()
#             if table:
#                 for row in table:  # Iterar sobre todas las filas de la tabla
#                     # Filtrar filas que tengan datos relevantes y no sean encabezados
#                     if row and len(row) >= len(required_columns) - 1 and not is_header_row(row):
#                         row.append(afp_name)  # Agregar el nombre de la AFP a la fila
#                         data.append(row[:len(required_columns)])  # Solo tomar las columnas requeridas
#     return data

# def transform_data_to_dataframe(data):
#     # Crear un DataFrame con los datos extraídos
#     df = pd.DataFrame(data, columns=required_columns)
#     # Eliminar filas específicas de encabezados manualmente
#     df = df[~df['RUT'].str.contains("Identificación del Trabajador|Fondo de Pensiones|Seguro Cesantía|Movimiento de Personal|Totales Generales", case=False, na=False)]
#     return df

# def main():
#     all_data = []
#     # Iterar sobre todos los archivos PDF en la carpeta de entrada
#     for filename in tqdm(os.listdir(input_folder), desc="Procesando archivos PDF", unit="archivo"):
#         if filename.endswith(".pdf"):
#             pdf_path = os.path.join(input_folder, filename)
#             # Extraer datos del PDF actual
#             extracted_data = extract_data_from_pdf(pdf_path)
#             all_data.extend(extracted_data)
    
#     # Transformar los datos a un DataFrame
#     df = transform_data_to_dataframe(all_data)
    
#     # Guardar los datos en un archivo Excel
#     df.to_excel(output_excel_path, index=False)
#     print(f"Datos extraídos y guardados en {output_excel_path}")

# if __name__ == "__main__":
#     main()


# REVISAR: v2
import pdfplumber
import pandas as pd
import re
import os
from tqdm import tqdm
import glob

# Carpeta con archivos PDF y archivo Excel de salida
input_folder = "upload"
output_folder = "output"
combined_excel_path = "output/resultado_concatenado.xlsx"

# Columnas requeridas según tu indicación
required_columns = ['RUT', 'Apellido Paterno, Materno, Nombres', 'Remuneración Imponible',
                    'Cotización Obligatoria', 'SIS', 'Cotización Voluntaria (APVI)', 'N° Contrato APVI',
                    'Deposito Convenido', 'Dep. en Cta. Ahorro', 'Remuneración Imponible',
                    'Cotización Afiliado', 'Cotización Empleador', 'Cod.', 'Fecha Inicio', 'Fecha Término', 'AFP', 'Días', 'AFP2', 'Periodo', 'Análisis']

def is_header_row(row):
    # Verificar si la fila contiene valores que podrían indicar un encabezado
    header_patterns = [
        r"^RUT$", r"Apellido Paterno, Materno, Nombres", r"Remuneración Imponible", r"Cotización Obligatoria", r"SIS",
        r"Cotización Voluntaria.*", r"N° Contrato APVI", r"Deposito Convenido", r"Dep. en Cta.*", r"Cotización Afiliado",
        r"Cotización Empleador", r"Fecha Inicio", r"Fecha Término"
    ]
    specific_headers = [
        "Identificación del Trabajador", "Fondo de Pensiones", "Seguro Cesantía", "Movimiento de Personal"
    ]
    # Si cualquier celda en la fila coincide con patrones de encabezado o contiene palabras específicas de encabezado
    return any(re.search(pattern, str(cell), re.IGNORECASE) for cell in row for pattern in header_patterns) or \
           any(header in str(cell) for cell in row for header in specific_headers)

def extract_data_from_pdf(pdf_path):
    data = []
    afp_name = ""
    with pdfplumber.open(pdf_path) as pdf:
        # Extraer el nombre de la AFP desde la primera página
        first_page_text = pdf.pages[0].extract_text()
        match = re.search(r"AFP\s+(\w+)", first_page_text)
        if match:
            afp_name = match.group(1)
        
        # Iterar sobre las páginas del PDF
        for page in tqdm(pdf.pages, desc=f"Procesando páginas de {pdf_path}", unit="página"):
            table = page.extract_table()
            if table:
                for row in table:  # Iterar sobre todas las filas de la tabla
                    # Filtrar filas que tengan datos relevantes y no sean encabezados
                    if row and len(row) >= len(required_columns) - 5 and not is_header_row(row):
                        row.append(afp_name)  # Agregar el nombre de la AFP a la fila
                        row.append(None)  # Placeholder para la columna 'Días'
                        row.append(None)  # Placeholder para la columna 'AFP2'
                        row.append(None)  # Placeholder para la columna 'Periodo'
                        row.append(None)  # Placeholder para la columna 'Análisis'
                        data.append(row[:len(required_columns)])  # Solo tomar las columnas requeridas
    return data

def transform_data_to_dataframe(data):
    # Crear un DataFrame con los datos extraídos
    df = pd.DataFrame(data, columns=required_columns)
    # Eliminar filas específicas de encabezados manualmente
    df = df[~df['RUT'].str.contains("Identificación del Trabajador|Fondo de Pensiones|Seguro Cesantía|Movimiento de Personal|Totales Generales", case=False, na=False)]
    
    # Calcular la diferencia en días entre Fecha Inicio y Fecha Término, considerando el día de inicio
    df['Fecha Inicio'] = pd.to_datetime(df['Fecha Inicio'], errors='coerce', dayfirst=True)
    df['Fecha Término'] = pd.to_datetime(df['Fecha Término'], errors='coerce', dayfirst=True)
    df['Días'] = (df['Fecha Término'] - df['Fecha Inicio']).dt.days + 1
    
    # Crear la columna 'Periodo' con el formato mes-año
    df['Periodo'] = df['Fecha Inicio'].dt.strftime('%m-%Y').fillna('')
    
    # Crear la columna 'Análisis' basada en la cantidad de días
    df['Análisis'] = df['Días'].apply(lambda x: 'CORRESPONDE' if pd.notna(x) and x <= 10 else 'NO CORRESPONDE')
    
    # Asegurar que las fechas mantengan su formato original al guardar en Excel
    df['Fecha Inicio'] = df['Fecha Inicio'].dt.strftime('%d-%m-%Y').fillna('')
    df['Fecha Término'] = df['Fecha Término'].dt.strftime('%d-%m-%Y').fillna('')
    
    return df

def main():
    # Crear la carpeta de salida si no existe
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Iterar sobre todos los archivos PDF en la carpeta de entrada
    for filename in tqdm(os.listdir(input_folder), desc="Procesando archivos PDF", unit="archivo"):
        if filename.endswith(".pdf"):
            pdf_path = os.path.join(input_folder, filename)
            # Extraer datos del PDF actual
            extracted_data = extract_data_from_pdf(pdf_path)
            df = transform_data_to_dataframe(extracted_data)
            
            # Guardar los datos en un archivo Excel individual
            output_path = os.path.join(output_folder, f"{os.path.splitext(filename)[0]}.xlsx")
            df.to_excel(output_path, index=False)
    
    # Consolidar todos los archivos Excel generados en un solo archivo
    excel_files = glob.glob(os.path.join(output_folder, "*.xlsx"))
    dataframes = []
    for file in excel_files:
        df = pd.read_excel(file)
        dataframes.append(df)
    
    combined_df = pd.concat(dataframes, ignore_index=True)
    combined_df.to_excel(combined_excel_path, index=False)
    print(f"Datos extraídos y guardados en {combined_excel_path}")

if __name__ == "__main__":
    main()
