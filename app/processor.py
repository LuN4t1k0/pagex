

# """
# processor.py
# Convierte los PDF (UploadFile) en un .zip con:
#     • pagos_corresponde.xlsx
#     • analisis_licencias_codigo3.xlsx
# Retorna (ruta_zip, nombre_descarga).
# """

# import os
# import json
# import shutil
# from datetime import datetime
# from tempfile import TemporaryDirectory, NamedTemporaryFile
# from typing import List

# import pandas as pd
# from dotenv import load_dotenv
# from fastapi import UploadFile

# # 👇 importa tu core
# from script import extrae_licencias, procesa_licencias   # <── NUEVO

# load_dotenv()

# SUMMARY_NAME   = "pagos_corresponde.xlsx"
# ANALISIS_NAME  = "analisis_licencias_codigo3.xlsx"
# NUMERIC_COLS = [
#     "Remuneracion", "Cod.", "dias_licencia", "dias_pagados",
#     "monto_rem_dias", "aporte_pension", "comision_afp", "total_aporte_afp",
# ]


# def _force_numeric(df: pd.DataFrame) -> pd.DataFrame:
#     for col in NUMERIC_COLS:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors="coerce")
#     return df


# def procesar_archivos_desde_entrada(
#     files: List[UploadFile],
#     indicadores_path: str | None = None,
# ) -> tuple[str, str]:
#     """Core que usa las funciones del script principal."""

#     ts = datetime.now().strftime("%Y%m%d_%H%M")

#     if not indicadores_path:
#         indicadores_path = os.getenv(
#             "INDICADORES_PATH",
#             "indicadores/indicadores.json",
#         )

#     with open(indicadores_path, encoding="utf-8") as f:
#         indicadores = json.load(f)

#     with TemporaryDirectory() as tmpdir:
#         pdf_dir    = os.path.join(tmpdir, "pdfs")
#         result_dir = os.path.join(tmpdir, "result")
#         os.makedirs(pdf_dir), os.makedirs(result_dir)

#         lic_global = []          # ← pasa TODAS las líneas (códigos varios)
#         for f in files:
#             dst = os.path.join(pdf_dir, f.filename)
#             with open(dst, "wb") as out:
#                 shutil.copyfileobj(f.file, out)

#             lic_global.extend(extrae_licencias(dst))

#         if not lic_global:
#             raise ValueError("No se extrajeron datos de los PDFs.")

#         # --- reglas de negocio ---
#         pagos_df, lic_df = procesa_licencias(lic_global, indicadores)

#         # --- normaliza numéricos ---
#         pagos_df = _force_numeric(pagos_df)

#         # --- Excel #1 (solo Cod. 3 aprobados) ---
#         pagos_corresponde = pagos_df[
#             (pagos_df["Cod."] == 3) & (pagos_df["estado"] == "aprobado")
#         ].copy()

#         resumen_path = os.path.join(result_dir, SUMMARY_NAME)
#         pagos_corresponde.to_excel(resumen_path, index=False)

#         # --- Excel #2 (análisis completo) ---
#         analisis_path = os.path.join(result_dir, ANALISIS_NAME)
#         lic_df.to_excel(analisis_path, index=False)

#         # --- empaquetar ZIP ---
#         zip_tmp = NamedTemporaryFile(delete=False, suffix=".zip")
#         shutil.make_archive(zip_tmp.name[:-4], "zip", result_dir)
#         download_name = f"pagex_procesado_{ts}.zip"

#         return zip_tmp.name, download_name


# NUEVO:

# import os
# import gc
# import json
# import shutil
# import psutil
# from datetime import datetime
# from tempfile import TemporaryDirectory
# from typing import List

# import pandas as pd
# from dotenv import load_dotenv
# from fastapi import UploadFile

# from script import extrae_licencias, procesa_licencias

# load_dotenv()

# MAX_CHUNK_MB = 5  # 🔥 conservador por comportamiento en Railway
# MAX_CHUNK_BYTES = MAX_CHUNK_MB * 1024 * 1024

# SUMMARY_NAME = "pagos_corresponde.xlsx"
# ANALISIS_NAME = "analisis_licencias_codigo3.xlsx"
# NUMERIC_COLS = [
#     "Remuneracion", "Cod.", "dias_licencia", "dias_pagados",
#     "monto_rem_dias", "aporte_pension", "comision_afp", "total_aporte_afp",
# ]

# def _force_numeric(df: pd.DataFrame) -> pd.DataFrame:
#     for col in NUMERIC_COLS:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors="coerce")
#     return df

# def chunk_files_by_size(files: List[UploadFile], max_bytes: int) -> List[List[UploadFile]]:
#     chunks = []
#     current_chunk = []
#     current_size = 0

#     for file in files:
#         file.file.seek(0, os.SEEK_END)
#         size = file.file.tell()
#         file.file.seek(0)

#         if current_size + size > max_bytes and current_chunk:
#             chunks.append(current_chunk)
#             current_chunk = [file]
#             current_size = size
#         else:
#             current_chunk.append(file)
#             current_size += size

#     if current_chunk:
#         chunks.append(current_chunk)

#     return chunks

# def get_mem_usage_mb() -> float:
#     return psutil.Process(os.getpid()).memory_info().rss / 1024**2

# def procesar_archivos_desde_entrada(
#     files: List[UploadFile],
#     indicadores_path: str | None = None,
# ) -> tuple[str, str]:

#     ts = datetime.now().strftime("%Y%m%d_%H%M")

#     if not indicadores_path:
#         indicadores_path = os.getenv("INDICADORES_PATH", "indicadores/indicadores.json")

#     with open(indicadores_path, encoding="utf-8") as f:
#         indicadores = json.load(f)

#     with TemporaryDirectory() as tmpdir:
#         pdf_dir = os.path.join(tmpdir, "pdfs")
#         result_dir = os.path.join(tmpdir, "result")
#         os.makedirs(pdf_dir), os.makedirs(result_dir)

#         licencias_totales = []
#         chunks = chunk_files_by_size(files, MAX_CHUNK_BYTES)

#         print(f"📦 Total de archivos: {len(files)}")
#         print(f"🔀 Divididos en {len(chunks)} chunks de máximo {MAX_CHUNK_MB} MB")

#         for idx, chunk in enumerate(chunks):
#             chunk_size_mb = 0
#             print(f"\n🚀 Procesando chunk {idx + 1}/{len(chunks)} ({len(chunk)} archivos)")

#             for f in chunk:
#                 dst = os.path.join(pdf_dir, f.filename)
#                 with open(dst, "wb") as out:
#                     shutil.copyfileobj(f.file, out)

#                 f.file.seek(0, os.SEEK_END)
#                 size_mb = f.file.tell() / 1024 / 1024
#                 f.file.seek(0)
#                 chunk_size_mb += size_mb

#                 licencias = extrae_licencias(dst)
#                 licencias_totales.extend(licencias)

#             print(f"📁 Chunk {idx + 1} → peso total: {chunk_size_mb:.2f} MB")
#             print(f"🧠 Memoria usada tras chunk: {get_mem_usage_mb():.2f} MB")

#             # Forzar limpieza de memoria
#             gc.collect()

#         if not licencias_totales:
#             raise ValueError("No se extrajeron datos de los PDFs.")

#         print("\n📊 Procesando DataFrames...")
#         pagos_df, lic_df = procesa_licencias(licencias_totales, indicadores)
#         pagos_df = _force_numeric(pagos_df)

#         summary_path = os.path.join(result_dir, SUMMARY_NAME)
#         analisis_path = os.path.join(result_dir, ANALISIS_NAME)

#         pagos_df.to_excel(summary_path, index=False)
#         lic_df.to_excel(analisis_path, index=False)

#         print("✅ Archivos Excel generados con éxito")
#         print(f"📄 Resumen: {SUMMARY_NAME}")
#         print(f"📄 Detalle: {ANALISIS_NAME}")

#         return summary_path, analisis_path


# REVISAR:
import os
import gc
import json
import shutil
import psutil
from datetime import datetime
from tempfile import TemporaryDirectory
from typing import List, Generator

import pandas as pd
from fastapi import UploadFile
from dotenv import load_dotenv

from script import extrae_licencias, procesa_licencias

load_dotenv()

# Configuración
MAX_CHUNK_MB = 5
MAX_CHUNK_BYTES = MAX_CHUNK_MB * 1024 * 1024
MAX_FILES_PER_CHUNK = 30

SUMMARY_NAME = "pagos_corresponde.xlsx"
ANALISIS_NAME = "analisis_licencias_codigo3.xlsx"
NUMERIC_COLS = [
    "Remuneracion", "Cod.", "dias_licencia", "dias_pagados",
    "monto_rem_dias", "aporte_pension", "comision_afp", "total_aporte_afp",
]


def _force_numeric(df: pd.DataFrame) -> pd.DataFrame:
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def get_mem_usage_mb() -> float:
    return psutil.Process(os.getpid()).memory_info().rss / 1024**2


def guardar_archivos_temporales(files: List[UploadFile], destino: str) -> List[str]:
    """
    Guarda todos los UploadFile en el disco (en `destino`) y retorna la lista de rutas guardadas.
    """
    rutas = []
    for f in files:
        ruta = os.path.join(destino, f.filename)
        with open(ruta, "wb") as out_file:
            shutil.copyfileobj(f.file, out_file)
        rutas.append(ruta)
    return rutas


def chunk_file_paths(file_paths: List[str]) -> List[List[str]]:
    """
    Divide una lista de paths de archivos en chunks según peso total y cantidad máxima por chunk.
    """
    chunks = []
    current_chunk = []
    current_size = 0

    for path in file_paths:
        size = os.path.getsize(path)

        if (current_size + size > MAX_CHUNK_BYTES or len(current_chunk) >= MAX_FILES_PER_CHUNK) and current_chunk:
            chunks.append(current_chunk)
            current_chunk = [path]
            current_size = size
        else:
            current_chunk.append(path)
            current_size += size

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def procesar_archivos(files: List[UploadFile], indicadores_path: str | None = None) -> Generator[str, None, None]:
    ts = datetime.now().strftime("%Y%m%d_%H%M")

    if not indicadores_path:
        indicadores_path = os.getenv("INDICADORES_PATH", "indicadores/indicadores.json")

    with open(indicadores_path, encoding="utf-8") as f:
        indicadores = json.load(f)

    with TemporaryDirectory() as tmpdir:
        pdf_dir = os.path.join(tmpdir, "pdfs")
        result_dir = os.path.join(tmpdir, "result")
        os.makedirs(pdf_dir), os.makedirs(result_dir)

        # Guardar archivos en disco
        yield f"data: 📦 Total de archivos: {len(files)}\n\n"
        file_paths = guardar_archivos_temporales(files, pdf_dir)

        # Chunkear archivos en disco
        chunks = chunk_file_paths(file_paths)
        yield f"data: 🔀 Divididos en {len(chunks)} chunks de máximo {MAX_CHUNK_MB}MB o {MAX_FILES_PER_CHUNK} archivos\n\n"

        licencias_totales = []

        for idx, chunk in enumerate(chunks):
            yield f"data: 🚀 Procesando chunk {idx+1}/{len(chunks)} ({len(chunk)} archivos)\n\n"
            chunk_size_mb = 0

            for i, path in enumerate(chunk):
                size_mb = os.path.getsize(path) / 1024 / 1024
                chunk_size_mb += size_mb

                yield f"data:   📄 [{i+1}/{len(chunk)}] {os.path.basename(path)} ({size_mb:.2f} MB)\n\n"

                licencias = extrae_licencias(path)
                licencias_totales.extend(licencias)

            yield f"data: 📁 Chunk {idx+1} → peso: {chunk_size_mb:.2f} MB\n\n"
            yield f"data: 🧠 RAM usada: {get_mem_usage_mb():.2f} MB\n\n"
            gc.collect()

        if not licencias_totales:
            yield "data: ❌ No se extrajeron datos de ningún PDF\n\n"
            return

        yield "data: 📊 Aplicando reglas y generando archivos Excel...\n\n"
        pagos_df, lic_df = procesa_licencias(licencias_totales, indicadores)
        pagos_df = _force_numeric(pagos_df)

        summary_path = os.path.join(result_dir, SUMMARY_NAME)
        analisis_path = os.path.join(result_dir, ANALISIS_NAME)

        pagos_df.to_excel(summary_path, index=False)
        lic_df.to_excel(analisis_path, index=False)

        yield f"data: ✅ Archivos generados\n\n"
        yield f"data: 📄 Resumen: {SUMMARY_NAME}\n\n"
        yield f"data: 📄 Detalle : {ANALISIS_NAME}\n\n"
        yield "data: 🔚 Proceso finalizado\n\n"
