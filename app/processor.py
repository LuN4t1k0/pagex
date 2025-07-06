# # app/processor.py

# import os
# import pandas as pd
# import shutil
# from datetime import datetime
# from tempfile import TemporaryDirectory, NamedTemporaryFile
# from script import extract_data_from_pdf, transform_data_to_dataframe

# from dotenv import load_dotenv
# load_dotenv()
# import os


# SUMMARY_FILENAME = "resumen_corresponde.xlsx"


# def procesar_archivos_desde_entrada(files, indicadores_path: str = "indicadores/indicadores.json") -> tuple[str, str]:
#     timestamp = datetime.now().strftime("%Y%m%d_%H%M")

#     with TemporaryDirectory() as temp_output:
#         pdf_dir = os.path.join(temp_output, "pdfs")
#         os.makedirs(pdf_dir, exist_ok=True)

#         result_dir = os.path.join(temp_output, "result")
#         os.makedirs(result_dir, exist_ok=True)

#         all_data = []

#         for file in files:
#             filename = file.filename
#             temp_pdf_path = os.path.join(pdf_dir, filename)
#             with open(temp_pdf_path, "wb") as f:
#                 shutil.copyfileobj(file.file, f)

#             extracted_data = extract_data_from_pdf(temp_pdf_path)
#             if extracted_data:
#                 df = transform_data_to_dataframe(extracted_data)
#                 all_data.append(df)

#         if not all_data:
#             raise ValueError("No se extrajo ningún dato de los archivos proporcionados.")

#         combined_df = pd.concat(all_data, ignore_index=True)
#         resumen_df = combined_df[
#             (combined_df['Cod.'] == 3) &
#             (combined_df['Análisis_Individual'] == 'CORRESPONDE') &
#             (combined_df['Análisis_Grupo'] == 'CORRESPONDE') &
#             (combined_df['Remuneración'] > 0)
#         ]

#         resumen_path = os.path.join(result_dir, SUMMARY_FILENAME)
#         resumen_df.to_excel(resumen_path, index=False)

#         for afp, group in resumen_df.groupby("AFP"):
#             safe_afp = afp.replace(" ", "_")
#             afp_path = os.path.join(result_dir, f"AFP_{safe_afp}.xlsx")
#             group.to_excel(afp_path, index=False)

#         # Crear archivo ZIP dentro del entorno temporal
#         temp_zip = NamedTemporaryFile(delete=False, suffix=f".zip")
#         shutil.make_archive(base_name=temp_zip.name.replace(".zip", ""), format='zip', root_dir=result_dir)

#         download_name = f"pagex_procesado_{timestamp}.zip"
#         return temp_zip.name, download_name


# app/processor.py

import os
import pandas as pd
import shutil
from datetime import datetime
from tempfile import TemporaryDirectory, NamedTemporaryFile
from dotenv import load_dotenv
from script import extract_data_from_pdf, transform_data_to_dataframe

load_dotenv()

SUMMARY_FILENAME = "resumen_corresponde.xlsx"


def procesar_archivos_desde_entrada(files, indicadores_path: str = None) -> tuple[str, str]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    # Si no se proporciona indicadores_path, usar el de la variable de entorno
    if not indicadores_path:
        indicadores_path = os.getenv("INDICADORES_PATH", "indicadores/indicadores.json")

    with TemporaryDirectory() as temp_output:
        pdf_dir = os.path.join(temp_output, "pdfs")
        os.makedirs(pdf_dir, exist_ok=True)

        result_dir = os.path.join(temp_output, "result")
        os.makedirs(result_dir, exist_ok=True)

        all_data = []

        for file in files:
            filename = file.filename
            temp_pdf_path = os.path.join(pdf_dir, filename)
            with open(temp_pdf_path, "wb") as f:
                shutil.copyfileobj(file.file, f)

            extracted_data = extract_data_from_pdf(temp_pdf_path)
            if extracted_data:
                df = transform_data_to_dataframe(extracted_data)
                all_data.append(df)

        if not all_data:
            raise ValueError("No se extrajo ningún dato de los archivos proporcionados.")

        combined_df = pd.concat(all_data, ignore_index=True)
        resumen_df = combined_df[
            (combined_df['Cod.'] == 3) &
            (combined_df['Análisis_Individual'] == 'CORRESPONDE') &
            (combined_df['Análisis_Grupo'] == 'CORRESPONDE') &
            (combined_df['Remuneración'] > 0)
        ]

        resumen_path = os.path.join(result_dir, SUMMARY_FILENAME)
        resumen_df.to_excel(resumen_path, index=False)

        for afp, group in resumen_df.groupby("AFP"):
            safe_afp = afp.replace(" ", "_")
            afp_path = os.path.join(result_dir, f"AFP_{safe_afp}.xlsx")
            group.to_excel(afp_path, index=False)

        # Crear archivo ZIP dentro del entorno temporal
        temp_zip = NamedTemporaryFile(delete=False, suffix=f".zip")
        shutil.make_archive(base_name=temp_zip.name.replace(".zip", ""), format='zip', root_dir=result_dir)

        download_name = f"pagex_procesado_{timestamp}.zip"
        return temp_zip.name, download_name
