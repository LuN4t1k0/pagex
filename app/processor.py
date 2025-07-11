
# import os
# import json
# import shutil
# import pandas as pd
# from datetime import datetime
# from tempfile import TemporaryDirectory, NamedTemporaryFile
# from dotenv import load_dotenv

# from script import extrae_tablas, procesa_dataframe   # <─ tu módulo de reglas

# # ------------------- CONFIG -------------------
# load_dotenv()
# SUMMARY_FILENAME = "resumen_corresponde.xlsx"
# DEBUG        = os.getenv("DEBUG", "false").lower() == "true"
# LOG_TO_FILE  = os.getenv("LOG_TO_FILE", "false").lower() == "true"

# NUMERIC_COLS = [
#     "Remuneración", "Cod.", "Días", "Días_pagados",
#     "Rem_Días", "Pensión", "Comisión", "Total_AFP",
# ]

# log_file_lines: list[str] = []

# def log_debug(msg: str) -> None:
#     if DEBUG:
#         print(msg)
#     if LOG_TO_FILE:
#         log_file_lines.append(msg)

# def _force_numeric(df: pd.DataFrame) -> pd.DataFrame:
#     """Convierte NUMERIC_COLS a float64 (o Int64 cuando aplica) de forma segura."""
#     for col in NUMERIC_COLS:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors="coerce")
#     return df.astype({c: "float64" for c in NUMERIC_COLS if c in df.columns})

# # ------------------- FUNCIÓN PRINCIPAL -------------------
# def procesar_archivos_desde_entrada(
#     files,
#     indicadores_path: str | None = None,
# ) -> tuple[str, str]:

#     timestamp = datetime.now().strftime("%Y%m%d_%H%M")

#     if not indicadores_path:
#         indicadores_path = os.getenv(
#             "INDICADORES_PATH",
#             "indicadores/indicadores.json",
#         )

#     with open(indicadores_path, encoding="utf-8") as f:
#         indicadores = json.load(f)

#     with TemporaryDirectory() as temp_output:
#         pdf_dir    = os.path.join(temp_output, "pdfs")
#         result_dir = os.path.join(temp_output, "result")
#         os.makedirs(pdf_dir, exist_ok=True)
#         os.makedirs(result_dir, exist_ok=True)

#         dataframes: list[pd.DataFrame] = []
#         detalles_global: dict[tuple, list] = {}

#         for file in files:
#             filename      = file.filename
#             temp_pdf_path = os.path.join(pdf_dir, filename)

#             with open(temp_pdf_path, "wb") as f_out:
#                 shutil.copyfileobj(file.file, f_out)

#             log_debug(f"▶ Procesando {filename}")
#             raw_rows = extrae_tablas(temp_pdf_path)

#             if not raw_rows:
#                 log_debug("  ⚠️  No se detectaron filas de datos.")
#                 continue

#             df_res, det_pdf = procesa_dataframe(raw_rows, indicadores)
#             if df_res.empty:
#                 log_debug("  ⚠️  DataFrame vacío tras aplicar reglas.")
#                 continue

#             # *** Normaliza tipos ANTES de concatenar ***
#             df_res = _force_numeric(df_res)

#             dataframes.append(df_res)
#             for k, v in det_pdf.items():
#                 detalles_global.setdefault(k, []).extend(v)

#             log_debug(f"  ✅ Filas válidas: {len(df_res)}")

#         if not dataframes:
#             raise ValueError("Todos los archivos fueron inválidos o vacíos.")

#         # ------------ CONCATENAR ------------
#         combined_df = pd.concat(dataframes, ignore_index=True, sort=False)

#         # *** Vuelta a normalizar por si el concat degradó dtypes ***
#         combined_df = _force_numeric(combined_df)

#         log_debug("=== dtypes después de concat ===")
#         log_debug(combined_df.dtypes.to_string())

#         # ------------ FILTRO RESUMEN ------------
#         resumen_df = combined_df[
#             (combined_df["Cod."] == 3) & (combined_df["Remuneración"] > 0)
#         ].fillna(0)

#         if resumen_df.empty:
#             raise ValueError("No hay registros que cumplan los criterios de resumen.")

#         resumen_path = os.path.join(result_dir, SUMMARY_FILENAME)
#         resumen_df.to_excel(resumen_path, index=False)

#         # ------------ ARCHIVOS POR AFP ------------
#         for afp, grp in resumen_df.groupby("AFP", dropna=False):
#             safe = (afp or "Sin_AFP").replace(" ", "_")
#             grp.to_excel(os.path.join(result_dir, f"AFP_{safe}.xlsx"), index=False)

#         # ------------ LOG (opcional) ------------
#         if LOG_TO_FILE and log_file_lines:
#             log_path = os.path.join(result_dir, "debug.log")
#             with open(log_path, "w", encoding="utf-8") as f:
#                 f.write("\n".join(log_file_lines))
#             log_debug(f"📝 Log guardado en: {log_path}")

#         # ------------ ZIP FINAL ------------
#         temp_zip = NamedTemporaryFile(delete=False, suffix=".zip")
#         shutil.make_archive(temp_zip.name[:-4], "zip", result_dir)

#         download_name = f"pagex_procesado_{timestamp}.zip"
#         log_debug(f"🎉 ZIP generado: {download_name}")

#         return temp_zip.name, download_name


"""
processor.py
Convierte los PDF (UploadFile) en un .zip con:
    • pagos_corresponde.xlsx
    • analisis_licencias_codigo3.xlsx
Retorna (ruta_zip, nombre_descarga).
"""

import os
import json
import shutil
from datetime import datetime
from tempfile import TemporaryDirectory, NamedTemporaryFile
from typing import List

import pandas as pd
from dotenv import load_dotenv
from fastapi import UploadFile

# 👇 importa tu core
from script import extrae_licencias, procesa_licencias   # <── NUEVO

load_dotenv()

SUMMARY_NAME   = "pagos_corresponde.xlsx"
ANALISIS_NAME  = "analisis_licencias_codigo3.xlsx"
NUMERIC_COLS = [
    "Remuneracion", "Cod.", "dias_licencia", "dias_pagados",
    "monto_rem_dias", "aporte_pension", "comision_afp", "total_aporte_afp",
]


def _force_numeric(df: pd.DataFrame) -> pd.DataFrame:
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def procesar_archivos_desde_entrada(
    files: List[UploadFile],
    indicadores_path: str | None = None,
) -> tuple[str, str]:
    """Core que usa las funciones del script principal."""

    ts = datetime.now().strftime("%Y%m%d_%H%M")

    if not indicadores_path:
        indicadores_path = os.getenv(
            "INDICADORES_PATH",
            "indicadores/indicadores.json",
        )

    with open(indicadores_path, encoding="utf-8") as f:
        indicadores = json.load(f)

    with TemporaryDirectory() as tmpdir:
        pdf_dir    = os.path.join(tmpdir, "pdfs")
        result_dir = os.path.join(tmpdir, "result")
        os.makedirs(pdf_dir), os.makedirs(result_dir)

        lic_global = []          # ← pasa TODAS las líneas (códigos varios)
        for f in files:
            dst = os.path.join(pdf_dir, f.filename)
            with open(dst, "wb") as out:
                shutil.copyfileobj(f.file, out)

            lic_global.extend(extrae_licencias(dst))

        if not lic_global:
            raise ValueError("No se extrajeron datos de los PDFs.")

        # --- reglas de negocio ---
        pagos_df, lic_df = procesa_licencias(lic_global, indicadores)

        # --- normaliza numéricos ---
        pagos_df = _force_numeric(pagos_df)

        # --- Excel #1 (solo Cod. 3 aprobados) ---
        pagos_corresponde = pagos_df[
            (pagos_df["Cod."] == 3) & (pagos_df["estado"] == "aprobado")
        ].copy()

        resumen_path = os.path.join(result_dir, SUMMARY_NAME)
        pagos_corresponde.to_excel(resumen_path, index=False)

        # --- Excel #2 (análisis completo) ---
        analisis_path = os.path.join(result_dir, ANALISIS_NAME)
        lic_df.to_excel(analisis_path, index=False)

        # --- empaquetar ZIP ---
        zip_tmp = NamedTemporaryFile(delete=False, suffix=".zip")
        shutil.make_archive(zip_tmp.name[:-4], "zip", result_dir)
        download_name = f"pagex_procesado_{ts}.zip"

        return zip_tmp.name, download_name
