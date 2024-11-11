
# # TRABAJANDO:

# import pdfplumber
# import pandas as pd
# import re

# # Archivo PDF de entrada y archivo Excel de salida
# pdf_path = "03.2023-11.pdf"
# output_excel_path = "resultado.xlsx"

# # Columnas requeridas según tu indicación
# required_columns = ['RUT', 'Apellido Paterno, Materno, Nombres', 'Remuneración Imponible',
#                     'Cotización Obligatoria', 'SIS', 'Cotización Voluntaria (APVI)', 'N° Contrato APVI',
#                     'Deposito Convenido', 'Dep. en Cta. Ahorro', 'Remuneración Imponible',
#                     'Cotización Afiliado', 'Cotización Empleador', 'Cod.', 'Fecha Inicio', 'Fecha Término']

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
#     with pdfplumber.open(pdf_path) as pdf:
#         # Iterar sobre las páginas del PDF
#         for page in pdf.pages[1:]:  # Desde la segunda página en adelante
#             table = page.extract_table()
#             if table:
#                 for row in table:  # Iterar sobre todas las filas de la tabla
#                     # Filtrar filas que tengan datos relevantes y no sean encabezados
#                     if row and len(row) >= len(required_columns) and not is_header_row(row):
#                         data.append(row[:len(required_columns)])  # Solo tomar las columnas requeridas
#     return data

# def transform_data_to_dataframe(data):
#     # Crear un DataFrame con los datos extraídos
#     df = pd.DataFrame(data, columns=required_columns)
#     # Eliminar filas que contengan valores específicos no deseados
#     df = df[~df.apply(lambda row: row.astype(str).str.contains("Identificación del Trabajador|Fondo de Pensiones|Seguro Cesantía|Movimiento de Personal|TOTALES GENERALES", case=False).any(), axis=1)]
#     return df

# def save_to_excel(df, output_path):
#     # Guardar el DataFrame en un archivo Excel
#     df.to_excel(output_path, index=False)

# def main():
#     # Extraer datos del PDF
#     extracted_data = extract_data_from_pdf(pdf_path)
    
#     # Transformar los datos a un DataFrame
#     df = transform_data_to_dataframe(extracted_data)
    
#     # Guardar los datos en un archivo Excel
#     save_to_excel(df, output_excel_path)
#     print(f"Datos extraídos y guardados en {output_excel_path}")

# if __name__ == "__main__":
#     main()


# TRABAJANDO:

import pdfplumber
import pandas as pd
import re

# Archivo PDF de entrada y archivo Excel de salida
pdf_path = "03.2023-11.pdf"
output_excel_path = "resultado.xlsx"

# Columnas requeridas según tu indicación
required_columns = ['RUT', 'Apellido Paterno, Materno, Nombres', 'Remuneración Imponible',
                    'Cotización Obligatoria', 'SIS', 'Cotización Voluntaria (APVI)', 'N° Contrato APVI',
                    'Deposito Convenido', 'Dep. en Cta. Ahorro', 'Remuneración Imponible',
                    'Cotización Afiliado', 'Cotización Empleador', 'Cod.', 'Fecha Inicio', 'Fecha Término']

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
    with pdfplumber.open(pdf_path) as pdf:
        # Iterar sobre las páginas del PDF
        for page in pdf.pages[1:]:  # Desde la segunda página en adelante
            table = page.extract_table()
            if table:
                for row in table:  # Iterar sobre todas las filas de la tabla
                    # Filtrar filas que tengan datos relevantes y no sean encabezados
                    if row and len(row) >= len(required_columns) and not is_header_row(row):
                        data.append(row[:len(required_columns)])  # Solo tomar las columnas requeridas
    return data

def transform_data_to_dataframe(data):
    # Crear un DataFrame con los datos extraídos
    df = pd.DataFrame(data, columns=required_columns)
    # Eliminar filas específicas de encabezados manualmente
    df = df[~df['RUT'].str.contains("Identificación del Trabajador|Fondo de Pensiones|Seguro Cesantía|Movimiento de Personal|Totales Generales", case=False, na=False)]
    return df

def save_to_excel(df, output_path):
    # Guardar el DataFrame en un archivo Excel
    df.to_excel(output_path, index=False)

def main():
    # Extraer datos del PDF
    extracted_data = extract_data_from_pdf(pdf_path)
    
    # Transformar los datos a un DataFrame
    df = transform_data_to_dataframe(extracted_data)
    
    # Guardar los datos en un archivo Excel
    save_to_excel(df, output_excel_path)
    print(f"Datos extraídos y guardados en {output_excel_path}")

if __name__ == "__main__":
    main()
