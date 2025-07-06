# # app/processor.py

# import os
# import pandas as pd
# import shutil
# from datetime import datetime
# from script import extract_data_from_pdf, transform_data_to_dataframe  # Puedes separar si lo deseas

# OUTPUT_DIR = "output"
# UPLOAD_DIR = "upload"
# SUMMARY_FILENAME = "resumen_corresponde.xlsx"


# def procesar_archivos_desde_entrada(files, indicadores_path: str = "indicadores/indicadores.json") -> str:
#     if not os.path.exists(OUTPUT_DIR):
#         os.makedirs(OUTPUT_DIR)

#     timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
#     temp_output = os.path.join(OUTPUT_DIR, f"procesamiento_{timestamp}")
#     os.makedirs(temp_output, exist_ok=True)

#     all_data = []

#     for file in files:
#         filename = file.filename
#         temp_pdf_path = os.path.join(temp_output, filename)
#         with open(temp_pdf_path, "wb") as f:
#             shutil.copyfileobj(file.file, f)

#         extracted_data = extract_data_from_pdf(temp_pdf_path)
#         if extracted_data:
#             df = transform_data_to_dataframe(extracted_data)
#             all_data.append(df)

#     if not all_data:
#         raise ValueError("No se extrajo ningún dato de los archivos proporcionados.")

#     combined_df = pd.concat(all_data, ignore_index=True)
#     resumen_df = combined_df[
#         (combined_df['Cod.'] == 3) &
#         (combined_df['Análisis_Individual'] == 'CORRESPONDE') &
#         (combined_df['Análisis_Grupo'] == 'CORRESPONDE') &
#         (combined_df['Remuneración'] > 0)
#     ]

#     resumen_path = os.path.join(temp_output, SUMMARY_FILENAME)
#     resumen_df.to_excel(resumen_path, index=False)

#     # Crear un archivo por AFP
#     for afp, group in resumen_df.groupby("AFP"):
#         safe_afp = afp.replace(" ", "_")
#         afp_path = os.path.join(temp_output, f"AFP_{safe_afp}.xlsx")
#         group.to_excel(afp_path, index=False)

#     # Empaquetar el resultado en un .zip
#     zip_path = f"{temp_output}.zip"
#     shutil.make_archive(temp_output, 'zip', temp_output)

#     return zip_path

# REVISAR:
# app/main.py

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import os
import pandas as pd
import shutil
from datetime import datetime
from tempfile import TemporaryDirectory
from script import extract_data_from_pdf, transform_data_to_dataframe

app = FastAPI(title="Microservicio de Procesamiento de AFP")

# Habilitar CORS si lo consumirás desde otro dominio/frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SUMMARY_FILENAME = "resumen_corresponde.xlsx"


def procesar_archivos_desde_entrada(files, indicadores_path: str = "indicadores/indicadores.json") -> str:
    with TemporaryDirectory(dir="output") as temp_output:
        pdf_dir = os.path.join(temp_output, "pdfs")
        os.makedirs(pdf_dir, exist_ok=True)

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

        resumen_path = os.path.join(temp_output, SUMMARY_FILENAME)
        resumen_df.to_excel(resumen_path, index=False)

        for afp, group in resumen_df.groupby("AFP"):
            safe_afp = afp.replace(" ", "_")
            afp_path = os.path.join(temp_output, f"AFP_{safe_afp}.xlsx")
            group.to_excel(afp_path, index=False)

        # Crear ZIP temporal
        zip_base = temp_output.rstrip(os.sep)
        zip_path = f"{zip_base}.zip"
        shutil.make_archive(base_name=zip_base, format='zip', root_dir=temp_output)

        return zip_path


@app.post("/procesar")
def procesar_archivos(archivos: List[UploadFile] = File(...)):
    try:
        zip_path = procesar_archivos_desde_entrada(archivos)
        return FileResponse(zip_path, media_type="application/zip", filename=os.path.basename(zip_path))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
