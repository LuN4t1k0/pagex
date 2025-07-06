# app/main.py

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import os

from .processor import procesar_archivos_desde_entrada

app = FastAPI(title="Microservicio de Procesamiento de AFP")

# Habilitar CORS si lo consumirás desde otro dominio/frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/procesar")
def procesar_archivos(archivos: List[UploadFile] = File(...)):
    try:
        zip_path = procesar_archivos_desde_entrada(archivos)
        return FileResponse(zip_path, media_type="application/zip", filename=os.path.basename(zip_path))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
