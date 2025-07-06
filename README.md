
# Microservicio de Procesamiento de Pagex (FastAPI)

Este proyecto es un microservicio desarrollado con **FastAPI** que permite subir múltiples archivos PDF de licencias o cotizaciones, procesarlos, generar un resumen y devolver un archivo `.zip` con:

- Un resumen general (`resumen_corresponde.xlsx`)
- Un Excel separado por cada AFP detectada (`AFP_Modelo.xlsx`, etc.)

Los archivos son procesados temporalmente y **eliminados automáticamente** después de la descarga.

---

## 🚀 ¿Cómo usar en desarrollo local?

1. **Clonar el proyecto**:
```bash
git clone https://github.com/tu-usuario/tu-repo.git
cd tu-repo
```

2. **Crear entorno virtual**:
```bash
python3 -m venv venv
source venv/bin/activate  # o venv\Scripts\activate en Windows
```

3. **Instalar dependencias**:
```bash
pip install -r requirements.txt
```

4. **Crear archivo `.env` (opcional)**:
```env
APP_NAME=Microservicio Pagex
INDICADORES_PATH=indicadores/indicadores.json
```

5. **Ejecutar el servidor**:
```bash
uvicorn app.main:app --reload
```

Accede a Swagger UI en: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

---

## 🌐 Despliegue en Railway

1. **Sube tu proyecto a GitHub**

2. **Entra a Railway**: [https://railway.app](https://railway.app)

3. **Crea nuevo proyecto > Deploy from GitHub**

4. **Agrega estas variables en la sección "Variables"**:

| Clave               | Valor                                 |
|---------------------|----------------------------------------|
| `APP_NAME`          | Microservicio de Procesamiento de Pagex |
| `INDICADORES_PATH`  | indicadores/indicadores.json           |

5. **Railway detectará `requirements.txt` y `Procfile` automáticamente**

6. Una vez desplegado, accede a:
```
https://<tu-app>.up.railway.app/docs
```

---

## 📦 Estructura esperada

```
├── app/
│   ├── main.py
│   ├── processor.py
│   └── script.py
├── indicadores/
│   └── indicadores.json
├── requirements.txt
├── Procfile
├── .env           # local, no se sube
├── .gitignore
```

---

## 📬 Endpoint disponible

### `POST /procesar`
- Entrada: Archivos PDF (uno o varios)
- Salida: ZIP con resumen y archivos por AFP

Puedes probarlo directamente desde `/docs`

---

## ✅ Consideraciones

- Los archivos subidos son procesados y eliminados automáticamente.
- El nombre del ZIP devuelto es semántico (ej: `pagex_procesado_20250705_2042.zip`)
- No se guarda ningún dato sensible en el servidor.

---

## ✨ Pendiente (futuras mejoras)

- Soporte para múltiples tipos de reportes
- Protección por token/API Key
- Exportación a S3 o correo

---

¡Listo para producción con FastAPI y Railway! ⚡
