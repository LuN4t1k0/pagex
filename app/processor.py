
# import os
# import json
# import shutil
# import pandas as pd
# from datetime import datetime
# from tempfile import TemporaryDirectory, NamedTemporaryFile
# from dotenv import load_dotenv

# from script import extrae_tablas, procesa_dataframe   # funciones del core

# load_dotenv()

# SUMMARY_FILENAME = "resumen_corresponde.xlsx"


# def procesar_archivos_desde_entrada(
#     files,
#     indicadores_path: str | None = None,
# ) -> tuple[str, str]:
#     """
#     • Sube una lista de UploadFile (FastAPI) con PDF-AFP.
#     • Devuelve ruta temporal al .zip y nombre de descarga.
#     """
#     timestamp = datetime.now().strftime("%Y%m%d_%H%M")

#     if not indicadores_path:
#         indicadores_path = os.getenv(
#             "INDICADORES_PATH", "indicadores/indicadores.json"
#         )

#     with open(indicadores_path, encoding="utf-8") as f:
#         indicadores = json.load(f)

#     # ------------------- carpetas temporales -------------------
#     with TemporaryDirectory() as temp_output:
#         pdf_dir    = os.path.join(temp_output, "pdfs")
#         result_dir = os.path.join(temp_output, "result")
#         os.makedirs(pdf_dir, exist_ok=True)
#         os.makedirs(result_dir, exist_ok=True)

#         all_data: list[pd.DataFrame] = []
#         detalles_global: dict[tuple, list] = {}   # ← ERA []   ahora es {}

#         # ------------------- procesar cada PDF -------------------
#         for file in files:
#             filename = file.filename
#             temp_pdf_path = os.path.join(pdf_dir, filename)

#             # guarda el PDF subido
#             with open(temp_pdf_path, "wb") as f_out:
#                 shutil.copyfileobj(file.file, f_out)

#             # extrae tablas
#             extracted_data = extrae_tablas(temp_pdf_path)
#             if not extracted_data:
#                 continue  # PDF vacío o sin tablas

#             df_res, det_pdf = procesa_dataframe(extracted_data, indicadores)

#             # ---- SOLO agregar DataFrames con filas (evita dtypes mixtos) ----
#             if not df_res.empty:
#                 all_data.append(df_res)

#             # ---- fusionar detalles para el .txt (si lo usas) ---------------
#             for k, v in det_pdf.items():
#                 detalles_global.setdefault(k, []).extend(v)

#         # --------------------------------------------------------------------
#         if not all_data:
#             raise ValueError(
#                 "No se extrajo ningún dato de los archivos proporcionados."
#             )

#         combined_df = pd.concat(all_data, ignore_index=True)

#         # aseguramos tipos numéricos seguros
#         combined_df["Remuneración"] = pd.to_numeric(
#             combined_df["Remuneración"], errors="coerce"
#         )
#         combined_df["Cod."] = pd.to_numeric(
#             combined_df["Cod."], errors="coerce"
#         )

#         # ------------------- resumen final -------------------
#         resumen_df = combined_df[
#             (combined_df["Cod."] == 3) & (combined_df["Remuneración"] > 0)
#         ].fillna(0)

#         resumen_path = os.path.join(result_dir, SUMMARY_FILENAME)
#         resumen_df.to_excel(resumen_path, index=False)

#         # -------------- división por AFP ---------------------
#         for afp, grp in resumen_df.groupby("AFP"):
#             safe = afp.replace(" ", "_")
#             grp.to_excel(
#                 os.path.join(result_dir, f"AFP_{safe}.xlsx"), index=False
#             )

#         # ------------------- empaquetar ZIP -------------------
#         temp_zip = NamedTemporaryFile(delete=False, suffix=".zip")
#         shutil.make_archive(
#             base_name=temp_zip.name[:-4],  # quita ".zip"
#             format="zip",
#             root_dir=result_dir,
#         )

#         download_name = f"pagex_procesado_{timestamp}.zip"
#         return temp_zip.name, download_name


# NUEVO:
import os
import json
import shutil
import pandas as pd
from datetime import datetime
from tempfile import TemporaryDirectory, NamedTemporaryFile
from dotenv import load_dotenv

from script import extrae_tablas, procesa_dataframe   # funciones del core

load_dotenv()

SUMMARY_FILENAME = "resumen_corresponde.xlsx"


def procesar_archivos_desde_entrada(
    files,
    indicadores_path: str | None = None,
) -> tuple[str, str]:
    """
    • Sube una lista de UploadFile (FastAPI) con PDF-AFP.
    • Devuelve ruta temporal al .zip y nombre de descarga.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    if not indicadores_path:
        indicadores_path = os.getenv(
            "INDICADORES_PATH", "indicadores/indicadores.json"
        )

    with open(indicadores_path, encoding="utf-8") as f:
        indicadores = json.load(f)

    with TemporaryDirectory() as temp_output:
        pdf_dir = os.path.join(temp_output, "pdfs")
        result_dir = os.path.join(temp_output, "result")
        os.makedirs(pdf_dir, exist_ok=True)
        os.makedirs(result_dir, exist_ok=True)

        all_data: list[pd.DataFrame] = []
        detalles_global: dict[tuple, list] = {}

        for file in files:
            filename = file.filename
            temp_pdf_path = os.path.join(pdf_dir, filename)

            with open(temp_pdf_path, "wb") as f_out:
                shutil.copyfileobj(file.file, f_out)

            extracted_data = extrae_tablas(temp_pdf_path)
            if not extracted_data:
                print(f"[INFO] No se extrajeron datos de: {filename}")
                continue

            df_res, det_pdf = procesa_dataframe(extracted_data, indicadores)

            if df_res.empty:
                print(f"[INFO] No hay registros válidos en: {filename}")
                continue

            all_data.append(df_res)

            for k, v in det_pdf.items():
                detalles_global.setdefault(k, []).extend(v)

        if not all_data:
            raise ValueError("Todos los archivos fueron inválidos o vacíos.")

        combined_df = pd.concat(all_data, ignore_index=True)

        # Forzar columnas esperadas
        expected_cols = [
            "RUT", "Nombre completo", "Remuneración", "Cod.", "Periodo", "Fecha Inicio",
            "Fecha Término", "AFP", "Días", "Días_pagados", "Tipo_Renta", "Rem_Días",
            "Pensión", "Comisión", "Total_AFP"
        ]
        for col in expected_cols:
            if col not in combined_df.columns:
                combined_df[col] = pd.NA

        # Asegurar dtypes numéricos
        numeric_cols = [
            "Remuneración", "Cod.", "Días", "Días_pagados",
            "Rem_Días", "Pensión", "Comisión", "Total_AFP"
        ]
        for col in numeric_cols:
            combined_df[col] = pd.to_numeric(combined_df[col], errors="coerce")

        # ------------------- resumen final -------------------
        resumen_df = combined_df[
            (combined_df["Cod."] == 3) & (combined_df["Remuneración"] > 0)
        ].fillna(0)

        if resumen_df.empty:
            raise ValueError("No hay registros que cumplan los criterios de resumen.")

        resumen_path = os.path.join(result_dir, SUMMARY_FILENAME)
        resumen_df.to_excel(resumen_path, index=False)

        # -------------- división por AFP ---------------------
        for afp, grp in resumen_df.groupby("AFP"):
            safe = afp.replace(" ", "_")
            grp.to_excel(
                os.path.join(result_dir, f"AFP_{safe}.xlsx"), index=False
            )

        # ------------------- empaquetar ZIP -------------------
        temp_zip = NamedTemporaryFile(delete=False, suffix=".zip")
        shutil.make_archive(
            base_name=temp_zip.name[:-4],  # quita ".zip"
            format="zip",
            root_dir=result_dir,
        )

        download_name = f"pagex_procesado_{timestamp}.zip"
        return temp_zip.name, download_name
