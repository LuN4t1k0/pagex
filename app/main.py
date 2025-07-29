
# # Ejecutar con:
# # uvicorn app.main:app --reload
# # uvicorn app.main:app --reload --host 0.0.0.0 --port 8000


# from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
# from fastapi.responses import FileResponse
# from fastapi.middleware.cors import CORSMiddleware
# from typing import List
# import os
# from dotenv import load_dotenv

# from .processor import procesar_archivos_desde_entrada   # <── tu nuevo processor

# load_dotenv()

# app = FastAPI(title=os.getenv("APP_NAME", "Microservicio Pagex"))

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],          # ajústalo en producción
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# @app.post("/procesar")
# async def procesar_archivos(
#     background_tasks: BackgroundTasks,
#     archivos: List[UploadFile] = File(...),
# ):
#     try:
#         zip_path, download_name = procesar_archivos_desde_entrada(archivos)
#         background_tasks.add_task(os.remove, zip_path)
#         return FileResponse(zip_path, media_type="application/zip", filename=download_name)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# NUEVO:
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import List
import os
from dotenv import load_dotenv

from .processor import procesar_archivos

load_dotenv()

app = FastAPI(title=os.getenv("APP_NAME", "Microservicio Pagex"))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Ajustar en producción
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/procesar")
async def procesar_archivos_endpoint(
    archivos: List[UploadFile] = File(...),
):
    try:
        def event_stream():
            for mensaje in procesar_archivos(archivos):
                yield mensaje  # ya incluye "data: ...\n\n"

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
