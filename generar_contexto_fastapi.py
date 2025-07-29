import os
import re
from pathlib import Path

# ========== CONFIGURACIÓN ==========
EXTENSIONES_VALIDAS = [
    '.py', '.json', '.yml', '.yaml', '.env', '.md', '.txt'
]

EXCLUIR_ARCHIVOS = ['poetry.lock', 'Pipfile.lock']
EXCLUIR_CARPETAS = [
    '.git', '.venv', '__pycache__', 'logs', 'migrations',
    '.vscode', '.idea', 'dist', 'build', '.export'
]

# Ruta raíz del proyecto
directorio_raiz = os.getcwd()
carpeta_export = os.path.join(directorio_raiz, '.export')
ruta_salida = os.path.join(carpeta_export, 'contexto_ia_fastapi.txt')

# Crear carpeta si no existe
os.makedirs(carpeta_export, exist_ok=True)

# ========== FUNCIONES ==========
def es_archivo_valido(ruta):
    if any(parte in EXCLUIR_CARPETAS for parte in Path(ruta).parts):
        return False
    if os.path.basename(ruta) in EXCLUIR_ARCHIVOS:
        return False
    if not os.path.isfile(ruta):
        return False
    if not any(ruta.endswith(ext) for ext in EXTENSIONES_VALIDAS):
        return False
    return True


def obtener_ruta_relativa(ruta_absoluta):
    return os.path.relpath(ruta_absoluta, directorio_raiz)


def limpiar_comentarios_y_lineas(contenido, extension):
    # 1. Eliminar comentarios
    if extension == '.py':
        # Comentarios de bloque (""" o ''') y de línea (#)
        contenido = re.sub(r'"""[\s\S]*?"""', '', contenido)
        contenido = re.sub(r"'''[\s\S]*?'''", '', contenido)
        contenido = re.sub(r'^\s*#.*$', '', contenido, flags=re.MULTILINE)
    elif extension in ['.json', '.env']:
        contenido = re.sub(r'^\s*#.*$', '', contenido, flags=re.MULTILINE)
    elif extension in ['.yml', '.yaml']:
        contenido = re.sub(r'^\s*#.*$', '', contenido, flags=re.MULTILINE)

    # 2. Eliminar líneas vacías consecutivas
    lineas = contenido.splitlines()
    nuevas_lineas = []
    salto = False
    for linea in lineas:
        if linea.strip() == '':
            if not salto:
                nuevas_lineas.append('')
                salto = True
        else:
            nuevas_lineas.append(linea)
            salto = False

    return '\n'.join(nuevas_lineas).strip()

# ========== EJECUCIÓN ==========
with open(ruta_salida, 'w', encoding='utf-8') as salida:
    salida.write("# CONTEXTO DEL BACKEND PARA LLM\n")
    salida.write("# Proyecto basado en Python, FastAPI, SQLAlchemy, PostgreSQL\n\n")

    for carpeta_actual, subcarpetas, archivos in os.walk(directorio_raiz):
        subcarpetas[:] = [d for d in subcarpetas if d not in EXCLUIR_CARPETAS]
        for archivo in archivos:
            ruta_completa = os.path.join(carpeta_actual, archivo)
            if es_archivo_valido(ruta_completa):
                ruta_relativa = obtener_ruta_relativa(ruta_completa)
                extension = Path(ruta_completa).suffix
                salida.write(f"\n### File: {ruta_relativa}\n")
                salida.write("-" * 80 + "\n")
                try:
                    with open(ruta_completa, 'r', encoding='utf-8') as archivo_script:
                        contenido = archivo_script.read()
                        contenido_limpio = limpiar_comentarios_y_lineas(contenido, extension)
                        salida.write(contenido_limpio + "\n")
                except Exception as e:
                    salida.write(f"[Error al leer el archivo: {e}]\n")
                salida.write("-" * 80 + "\n")

print(f"✅ Contexto del backend generado en: {ruta_salida}")
