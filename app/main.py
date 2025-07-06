
# from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
# from fastapi.responses import FileResponse
# from fastapi.middleware.cors import CORSMiddleware
# from typing import List
# import os

# from dotenv import load_dotenv
# load_dotenv()
# import os

# from .processor import procesar_archivos_desde_entrada

# app = FastAPI(title="Microservicio de Procesamiento de AFP")

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# @app.post("/procesar")
# def procesar_archivos(background_tasks: BackgroundTasks, archivos: List[UploadFile] = File(...)):
#     try:
#         zip_path, download_name = procesar_archivos_desde_entrada(archivos)
#         background_tasks.add_task(os.remove, zip_path)
#         return FileResponse(zip_path, media_type="application/zip", filename=download_name)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# # # para correr el servidor:

# # # uvicorn app.main:app --reload
# # # uvicorn app.main:app --reload --host

# app/main.py

from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import os
from dotenv import load_dotenv

from .processor import procesar_archivos_desde_entrada

load_dotenv()

app = FastAPI(title=os.getenv("APP_NAME", "Microservicio de Procesamiento de AFP"))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Puedes restringir esto en producción
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/procesar")
def procesar_archivos(background_tasks: BackgroundTasks, archivos: List[UploadFile] = File(...)):
    try:
        zip_path, download_name = procesar_archivos_desde_entrada(archivos)
        background_tasks.add_task(os.remove, zip_path)
        return FileResponse(zip_path, media_type="application/zip", filename=download_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Ejecutar con:
# uvicorn app.main:app --reload
# uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
