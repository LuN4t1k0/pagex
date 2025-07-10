# import os, re, json, calendar, logging
# from datetime import datetime

# import pdfplumber
# import pandas as pd
# from tqdm import tqdm

# # --------------------------------------------------
# # CONFIGURACIÓN GENERAL
# # --------------------------------------------------
# input_folder   = "upload"
# output_folder  = "output"
# summary_excel_path = os.path.join(output_folder, "resumen_corresponde.xlsx")
# json_path      = "indicadores/indicadores.json"

# logging.basicConfig(
#     filename="procesamiento.log",
#     filemode="a",
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     level=logging.INFO,
# )

# required_columns = [
#     "RUT", "Apellido Paterno, Materno, Nombres", "Remuneración", "Cod.",
#     "Fecha Inicio", "Fecha Término", "AFP",
# ]

# header_patterns = [
#     r"^RUT$", r"Apellido Paterno,? Materno,? Nombres", r"Remuneración",
#     r"Fecha Inicio", r"Fecha Término", r"Cod\.",
# ]
# specific_headers = [
#     "Identificación del Trabajador", "Fondo de Pensiones",
#     "Seguro Cesantía", "Movimiento de Personal",
# ]

# # --------------------------------------------------
# # UTILIDADES
# # --------------------------------------------------
# def is_header_row(row):
#     return any(
#         re.search(p, str(c), re.IGNORECASE)
#         for c in row for p in header_patterns
#     ) or any(h in str(c) for c in row for h in specific_headers)

# def ultimo_dia_mes(dt):
#     return calendar.monthrange(dt.year, dt.month)[1]

# def normaliza_afp(nombre):
#     mapa = {
#         "provida": "Provida", "proviva": "Provida", "pro vida": "Provida",
#         "capital": "Capital", "cuprum": "Cuprum", "habitat": "Habitat",
#         "planvital": "PlanVital", "plan vital": "PlanVital",
#         "modelo": "Modelo", "uno": "Uno",
#     }
#     return mapa.get(nombre.strip().lower(), nombre)

# # --------------------------------------------------
# # 1. EXTRACCIÓN DESDE PDF
# # --------------------------------------------------
# def extrae_tablas(pdf_path):
#     filas = []
#     afp_name = "Desconocida"
#     try:
#         with pdfplumber.open(pdf_path) as pdf:
#             txt0 = pdf.pages[0].extract_text()
#             m = re.search(r"AFP\s+(\w+)", txt0, re.IGNORECASE)
#             if m:
#                 afp_name = m.group(1)

#             for pg in pdf.pages:
#                 table = pg.extract_table()
#                 if not table:
#                     continue
#                 for row in table:
#                     if not (row and len(row) >= 15 and not is_header_row(row)):
#                         continue
#                     try:
#                         filas.append([
#                             row[0].strip(),  # RUT
#                             row[1].strip(),  # Nombre
#                             re.sub(r"[^\d,]", "", row[2]).replace(",", "."),  # Remun
#                             row[12].strip(),  # Cod.
#                             row[13].strip(), row[14].strip(),  # Fechas
#                             afp_name,
#                         ])
#                     except IndexError:
#                         logging.error(f"Fila malformada en {pdf_path}")
#     except Exception as e:
#         logging.error(f"Error en {pdf_path}: {e}")
#     return filas

# # --------------------------------------------------
# # 2. TRANSFORMACIÓN Y REGLAS
# # --------------------------------------------------
# def procesa_dataframe(raw_rows, indicadores):
#     # ---- carga básica
#     df = pd.DataFrame(raw_rows, columns=required_columns)

#     # *** Convertir a numérico de inmediato ***
#     df["Remuneración"] = pd.to_numeric(df["Remuneración"], errors="coerce")
#     df["Cod."]         = pd.to_numeric(df["Cod."], errors="coerce")

#     # Quitar filas que son cabeceras fantasma
#     df = df[
#         ~df["RUT"].str.contains("|".join(specific_headers), case=False, na=False)
#     ]


#     # ---- fechas originales
#     df["Fecha Inicio"] = pd.to_datetime(df["Fecha Inicio"], dayfirst=True, errors="coerce")
#     df["Fecha Término"] = pd.to_datetime(df["Fecha Término"], dayfirst=True, errors="coerce")
#     df["Dias_lic"] = (df["Fecha Término"] - df["Fecha Inicio"]).dt.days + 1

#     # ---- tipo de renta
#     df["Tipo_Renta"] = df.apply(
#         lambda r: "Renta Tope"
#         if pd.notna(r["Fecha Inicio"])
#         and (p := r["Fecha Inicio"].strftime("%Y%m")) in indicadores
#         and r["Remuneración"] >= float(
#             indicadores[p]["rentas_topes_imponibles"]["valor"][0]
#             .replace("$", "")
#             .replace(".", "")
#             .replace(",", ".")
#         )
#         else "Renta No Tope",
#         axis=1,
#     )

#     # -----------------------------------------------------------------
#     #  Estructura para informe: (rut, periodo) -> [dict(...)]
#     # -----------------------------------------------------------------
#     detalle_lic: dict[tuple, list] = {}

#     # ---- SEGMENTACIÓN por mes
#     segmentos = []
#     for _, r in df.iterrows():
#         if pd.isna(r["Fecha Inicio"]) or pd.isna(r["Fecha Término"]):
#             continue

#         rut, nom, ren, cod = (
#             r["RUT"],
#             r["Apellido Paterno, Materno, Nombres"],
#             r["Remuneración"],
#             r["Cod."],
#         )
#         afp = normaliza_afp(r["AFP"])

#         if r["Tipo_Renta"] == "Renta Tope":
#             # pagar todos los días – se divide por mes
#             cur, end = r["Fecha Inicio"], r["Fecha Término"]
#             while cur <= end:
#                 fin_mes = pd.Timestamp(cur.year, cur.month, ultimo_dia_mes(cur))
#                 bloc_fin = min(fin_mes, end)
#                 dias = (bloc_fin - cur).days + 1
#                 per = cur.strftime("%Y%m")

#                 segmentos.append([rut, nom, ren, cod, afp, per, dias, "Renta Tope"])
#                 detalle_lic.setdefault((rut, per), []).append(
#                     {"ini": cur.strftime("%d-%m-%Y"),
#                      "fin": bloc_fin.strftime("%d-%m-%Y"),
#                      "dias_orig": dias, "dias_pag": None, "motivo": ""}
#                 )
#                 cur = bloc_fin + pd.Timedelta(days=1)
#         else:  # Renta No Tope  (licencias < 10 días)
#             if r["Dias_lic"] >= 10:
#                 continue
#             start, end = r["Fecha Inicio"], r["Fecha Término"]

#             if start.month == end.month and start.year == end.year:
#                 dias = r["Dias_lic"]
#                 per = start.strftime("%Y%m")
#                 segmentos.append([rut, nom, ren, cod, afp, per, dias, "Renta No Tope"])
#                 detalle_lic.setdefault((rut, per), []).append(
#                     {"ini": start.strftime("%d-%m-%Y"),
#                      "fin": end.strftime("%d-%m-%Y"),
#                      "dias_orig": dias, "dias_pag": None, "motivo": ""}
#                 )
#             else:  # cruza mes → sólo bloque del mes origen
#                 dias_origen = ultimo_dia_mes(start) - start.day + 1
#                 per = start.strftime("%Y%m")
#                 segmentos.append([rut, nom, ren, cod, afp, per, dias_origen, "Renta No Tope"])
#                 detalle_lic.setdefault((rut, per), []).append(
#                     {"ini": start.strftime("%d-%m-%Y"),
#                      "fin": pd.Timestamp(start.year, start.month, ultimo_dia_mes(start)).strftime("%d-%m-%Y"),
#                      "dias_orig": dias_origen, "dias_pag": None, "motivo": ""}
#                 )

#     # ---------- DataFrame de segmentos ----------
#     seg_df = pd.DataFrame(
#         segmentos,
#         columns=["RUT", "Nombre", "Remuneración", "Cod.", "AFP", "Periodo",
#                  "Dias_segmento", "Tipo_Renta"],
#     )

#     # ---------- AGRUPACIÓN mínima (RUT+Periodo) ----------
#     agrup = (
#         seg_df.groupby(["RUT", "Periodo"], as_index=False)
#         .agg({
#             "Nombre": "first",
#             "Remuneración": "max",
#             "Cod.": "first",
#             "AFP": lambda x: x.mode()[0] if len(x) else "",
#             "Tipo_Renta": "first",
#             "Dias_segmento": "sum",
#         })
#         .rename(columns={"Dias_segmento": "Dias_mes"})
#     )

#     # ---------- Rem_Días (monto) ----------
#     agrup["Tasa_diaria"] = agrup["Remuneración"] / 30

#     def calc_monto(row):
#         if row["Tipo_Renta"] == "Renta Tope":
#             dias_pag = row["Dias_mes"]
#         else:
#             dias_pag = min(3, row["Dias_mes"]) if row["Dias_mes"] < 10 else 0
#         return round(dias_pag * row["Tasa_diaria"], 0)

#     agrup["Rem_Días"] = agrup.apply(calc_monto, axis=1)
#     agrup = agrup[agrup["Rem_Días"] > 0]

#     # ---------- REPARTO de días pagados ----------
#     for _, row in agrup.iterrows():
#         clave = (row["RUT"], row["Periodo"])
#         lic_list = detalle_lic.get(clave, [])
#         if not lic_list:
#             continue

#         if row["Tipo_Renta"] == "Renta Tope":
#             for lic in lic_list:
#                 lic["dias_pag"] = lic["dias_orig"]
#                 lic["motivo"] = "RT-1 (pago completo)"
#         else:
#             saldo = min(3, row["Dias_mes"]) if row["Dias_mes"] < 10 else 0
#             for lic in sorted(
#                 lic_list, key=lambda x: pd.to_datetime(x["ini"], dayfirst=True)
#             ):
#                 if saldo > 0:
#                     pagar = min(lic["dias_orig"], saldo)
#                     lic["dias_pag"] = pagar
#                     lic["motivo"] = "Pagado (RN-1/RN-2)"
#                     saldo -= pagar
#                 else:
#                     lic["dias_pag"] = 0
#                     lic["motivo"] = "RN-1/RN-2 → 0"

#     # ---------- Columnas monetarias ----------
#     with open(json_path, encoding="utf-8") as f:
#         ind = json.load(f)

#     def pct_afp(row):
#         per, afp = row["Periodo"], row["AFP"]
#         try:
#             idx = [a.lower() for a in ind[per]["afp"]["afp"]].index(afp.lower())
#             return float(
#                 ind[per]["afp"]["tasa_afp_dependientes"][idx]
#                 .replace("%", "")
#                 .replace(",", ".")
#             ) - 10.0
#         except Exception:
#             return 0.0

#     agrup["Porcentaje"] = agrup.apply(pct_afp, axis=1)
#     agrup["Pensión"] = (agrup["Rem_Días"] * 0.10).round()
#     agrup["Comisión"] = ((agrup["Porcentaje"] / 100) * agrup["Rem_Días"]).round()
#     agrup["Total_AFP"] = (agrup["Pensión"] + agrup["Comisión"]).astype(int)

#     # ---------- Fecha Inicio / Término reales ----------
#     def rango_fechas(key):
#         lic_lst = detalle_lic.get(key, [])
#         if not lic_lst:
#             return ("", "")
#         fechas_ini = [pd.to_datetime(l["ini"], dayfirst=True) for l in lic_lst]
#         fechas_fin = [pd.to_datetime(l["fin"], dayfirst=True) for l in lic_lst]
#         return (
#             min(fechas_ini).strftime("%d-%m-%Y"),
#             max(fechas_fin).strftime("%d-%m-%Y"),
#         )

#     if not agrup.empty:
#         fechas_df = agrup.apply(
#             lambda r: rango_fechas((r["RUT"], r["Periodo"])),
#             axis=1,
#             result_type="expand",
#         )
#         agrup["Fecha Inicio"] = fechas_df[0]
#         agrup["Fecha Término"] = fechas_df[1]
#     else:
#         agrup["Fecha Inicio"] = ""
#         agrup["Fecha Término"] = ""

#     # ---------- Días efectivamente pagados ----------
#     agrup["Días_pagados"] = (agrup["Rem_Días"] / agrup["Tasa_diaria"]).round().astype(int)

#     # ---------- Renombres y orden final ----------
#     agrup.rename(columns={"Nombre": "Nombre completo", "Dias_mes": "Días"}, inplace=True)

#     FINAL_COLS = [
#         "RUT", "Nombre completo", "Remuneración", "Cod.",
#         "Periodo", "Fecha Inicio", "Fecha Término", "AFP",
#         "Días", "Días_pagados", "Tipo_Renta", "Rem_Días",
#         "Pensión", "Comisión", "Total_AFP",
#     ]
#     agrup = agrup.reindex(columns=FINAL_COLS)

#     # ---------- Si queda vacío, devolver DF con dtypes correctos ----------
#     if agrup.empty:
#         empty = pd.DataFrame(columns=FINAL_COLS)
#         num_cols = [
#             "Remuneración", "Cod.", "Días", "Días_pagados",
#             "Rem_Días", "Pensión", "Comisión", "Total_AFP",
#         ]
#         empty[num_cols] = empty[num_cols].astype("float64")
#         return empty, detalle_lic

#     return agrup, detalle_lic

# # --------------------------------------------------
# # 3. FLUJO PRINCIPAL (ejecución en local)
# # --------------------------------------------------
# def main():
#     os.makedirs(output_folder, exist_ok=True)

#     with open(json_path, encoding="utf-8") as f:
#         indicadores = json.load(f)

#     dataframes: list[pd.DataFrame] = []
#     detalles_global: dict[tuple, list] = {}
#     pdfs = []

#     for file in os.listdir(input_folder):
#         if not file.lower().endswith(".pdf"):
#             continue
#         pdf_path = os.path.join(input_folder, file)
#         raws = extrae_tablas(pdf_path)
#         if not raws:
#             continue
#         df_res, det_pdf = procesa_dataframe(raws, indicadores)

#         if not df_res.empty:
#             dataframes.append(df_res)
#         for k, v in det_pdf.items():
#             detalles_global.setdefault(k, []).extend(v)
#         pdfs.append(file)

#     if not dataframes:
#         print("No se extrajo ningún dato.")
#         return

#     df = pd.concat(dataframes, ignore_index=True)

#     # ---------- RESUMEN ----------
#     resumen = df[(df["Cod."] == 3) & (df["Remuneración"] > 0)]
#     if resumen.empty:
#         print("Sin registros que cumplan criterio.")
#         return

#     resumen.to_excel(summary_excel_path, index=False)
#     print("Resumen guardado en:", summary_excel_path)

#     # ---------- Archivos por AFP ----------
#     for afp, g in resumen.groupby("AFP"):
#         g.to_excel(
#             os.path.join(output_folder, f"AFP_{afp.replace(' ', '_')}.xlsx"),
#             index=False,
#         )
#     print("Archivos por AFP generados.")

#     # ---------- TXT de justificación ----------
#     txt_path = os.path.join(
#         output_folder, f"justificacion_{datetime.now():%Y%m%d_%H%M}.txt"
#     )
#     with open(txt_path, "w", encoding="utf-8") as f:
#         f.write("Fecha de procesamiento: " + datetime.now().isoformat() + "\n")
#         f.write("PDFs procesados: " + ", ".join(pdfs) + "\n")
#         f.write(f"Registros con pago: {len(resumen)}\n\n")

#         f.write("REGLAS APLICADAS\n")
#         f.write("  RN-1  Licencia <10 días paga máx. 3 días.\n")
#         f.write("  RN-2  Suma mensual <10 días paga máx. 3 días en total.\n")
#         f.write("  RN-3  Si cruza mes (<10d) sólo paga el mes origen.\n")
#         f.write("  RT-1  Renta Tope paga todos los días.\n\n")

#         for _, row in resumen.iterrows():
#             clave = (row["RUT"], row["Periodo"])
#             f.write(f"{row['RUT']} – {row['Periodo']}\n")
#             f.write(f"  • Monto pagable: ${int(row['Rem_Días']):,}\n")
#             regla_mes = "RT-1 (Tope)" if row["Tipo_Renta"] == "Renta Tope" else "RN-1/RN-2"
#             f.write(f"    Regla mes: {regla_mes}\n")
#             f.write("    Licencias analizadas:\n")
#             for lic in detalles_global.get(clave, []):
#                 f.write(
#                     f"      - {lic['ini']} → {lic['fin']}  "
#                     f"(orig: {lic['dias_orig']} d, pagados: {lic['dias_pag']} d)  "
#                     f"{lic['motivo']}\n"
#                 )
#             f.write("\n")

#     print("Justificación generada:", txt_path)


# if __name__ == "__main__":
#     main()


#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Identificación y cálculo de pagos en exceso para licencias médicas (Cod. 3).

• Aplica reglas de renta tope, licencias <10 d, cadenas continuas, discontinuas,
  cruces de mes, etc.
• Genera dos archivos Excel:
    1) pagos_corresponde.xlsx   → resumen ejecutivo (RUT + período)
    2) analisis_licencias_codigo3.xlsx → detalle licencia a licencia
"""

import os
import re
import json
import calendar
import logging
from datetime import datetime, timedelta
from collections import defaultdict

import pdfplumber
import pandas as pd
from tqdm import tqdm

# --------------------------------------------------
# CONFIGURACIÓN GENERAL
# --------------------------------------------------
INPUT_FOLDER   = "upload"
OUTPUT_FOLDER  = "output"
RESUMEN_PATH   = os.path.join(OUTPUT_FOLDER, "pagos_corresponde.xlsx")
DETALLE_PATH   = os.path.join(OUTPUT_FOLDER, "analisis_licencias_codigo3.xlsx")
INDICADORES_JS = "indicadores/indicadores.json"

logging.basicConfig(
    filename="procesamiento.log",
    filemode="a",
    format="%(asctime)s − %(levelname)s − %(message)s",
    level=logging.INFO,
)

# --------------------------------------------------
# UTILIDADES
# --------------------------------------------------
def ultimo_dia_mes(dt: datetime) -> int:
    return calendar.monthrange(dt.year, dt.month)[1]

def normaliza_afp(nombre: str) -> str:
    mapa = {
        "provida": "Provida", "proviva": "Provida", "pro vida": "Provida",
        "capital": "Capital", "cuprum": "Cuprum", "habitat": "Habitat",
        "planvital": "PlanVital", "plan vital": "PlanVital",
        "modelo": "Modelo", "uno": "Uno",
    }
    return mapa.get(nombre.strip().lower(), nombre.strip())

def lee_indicadores() -> dict:
    with open(INDICADORES_JS, encoding="utf-8") as f:
        return json.load(f)

def tasa_dependientes(periodo: str, afp: str, indicadores: dict) -> float | None:
    try:
        idx = [a.lower() for a in indicadores[periodo]["afp"]["afp"]].index(afp.lower())
        return float(
            indicadores[periodo]["afp"]["tasa_afp_dependientes"][idx]
            .replace("%", "")
            .replace(",", ".")
        )
    except Exception:
        return None

def renta_tope(periodo: str, indicadores: dict) -> float | None:
    try:
        return float(
            indicadores[periodo]["rentas_topes_imponibles"]["valor"][0]
            .replace("$", "")
            .replace(".", "")
            .replace(",", ".")
        )
    except Exception:
        return None

# --------------------------------------------------
# 1. EXTRACCIÓN DE LICENCIAS DESDE PDF
# --------------------------------------------------
HEADER_PATTERNS = [
    r"^RUT$", r"Apellido Paterno,? Materno,? Nombres", r"Remuneración",
    r"Fecha Inicio", r"Fecha Término", r"Cod\.",
]
SPECIFIC_HEADERS = [
    "Identificación del Trabajador", "Fondo de Pensiones",
    "Seguro Cesantía", "Movimiento de Personal",
]

def is_header_row(row: list[str]) -> bool:
    return any(
        re.search(pat, str(cell), re.IGNORECASE)
        for cell in row for pat in HEADER_PATTERNS
    ) or any(h in str(cell) for cell in row for h in SPECIFIC_HEADERS)

def extrae_licencias(pdf_path: str) -> list[dict]:
    """
    Extrae TODAS las líneas (no sólo Cod. 3) a partir de la segunda página
    de un PDF Previred y devuelve una lista de dict con:

        rut, nombre, remun, cod, inicio, fin, afp, src

    • La AFP se detecta en la portada.
    • Las 3 últimas columnas de cada fila son: Cod., Fecha Inicio, Fecha Término.
    • Fechas aceptan «dd/mm/aaaa» o «dd-mm-aaaa».
    """
    licencias: list[dict] = []
    afp_name = "Desconocida"

    # --- helper para fechas -------------------------------------------------
    def parse_date(text: str) -> datetime:
        text = text.strip()
        for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        raise ValueError(f"Formato de fecha desconocido: {text}")

    try:
        with pdfplumber.open(pdf_path) as pdf:
            # 1) AFP en la portada (página 0)
            portada_txt = pdf.pages[0].extract_text() or ""
            m = re.search(r"AFP\s+(\w+)", portada_txt, re.IGNORECASE)
            if m:
                afp_name = m.group(1)

            # 2) Recorrer SOLO de la 2.ª página en adelante
            for pg in pdf.pages[1:]:
                table = pg.extract_table()
                if not table:
                    continue

                for row in table:
                    # a) descartar cabeceras
                    if not row or is_header_row(row):
                        continue
                    # b) al menos 6 columnas (formato AFP Capital)
                    if len(row) < 6:
                        continue

                    # --- columnas relevantes --------------------------------
                    rut    = row[0].strip()
                    nombre = row[1].strip()

                    # Remuneración: limpiar separador de miles y convertir a float
                    remun_txt = re.sub(r"[^\d,]", "", row[2]).replace(",", ".") or "0"
                    try:
                        remun = float(remun_txt)
                    except ValueError:
                        remun = 0.0

                    # Últimas tres celdas → Cod., Fecha Inicio, Fecha Término
                    cod_txt, fecha_ini_txt, fecha_fin_txt = row[-3], row[-2], row[-1]

                    cod_txt = re.sub(r"[^\d]", "", cod_txt or "")
                    cod = int(cod_txt) if cod_txt else 0  # 0 si viene vacío

                    try:
                        fecha_ini = parse_date(fecha_ini_txt)
                        fecha_fin = parse_date(fecha_fin_txt)

                        licencias.append(
                            {
                                "rut"   : rut,
                                "nombre": nombre,
                                "remun" : remun,
                                "cod"   : cod,
                                "inicio": fecha_ini,
                                "fin"   : fecha_fin,
                                "afp"   : normaliza_afp(afp_name),
                                "src"   : os.path.basename(pdf_path),
                            }
                        )
                    except Exception as e:
                        logging.error(f"Fila malformada en {pdf_path}: {e}")

    except Exception as e:
        logging.error(f"No se pudo abrir {pdf_path}: {e}")

    return licencias

# --------------------------------------------------
# 2. PROCESAMIENTO Y REGLAS
# --------------------------------------------------
def procesa_licencias(lic_raw: list[dict], indicadores: dict):
    """
    Aplica reglas y devuelve:
        pagos_df   → resumen por RUT+período
        lic_df     → detalle licencia a licencia
    """

    if not lic_raw:
        return pd.DataFrame(), pd.DataFrame()

    # ------------------------------------------------------------------
    # 2.1 Preparación DataFrame base (licencia a licencia)
    # ------------------------------------------------------------------
    lic_df = pd.DataFrame(lic_raw)
    lic_df["dias_licencia"] = (lic_df["fin"] - lic_df["inicio"]).dt.days + 1
    lic_df["periodo"] = lic_df["inicio"].dt.strftime("%Y%m")

    # Indicadores
    lic_df["tope_mes"] = lic_df.apply(
        lambda r: renta_tope(r["periodo"], indicadores), axis=1
    )
    lic_df["tasa_dep"] = lic_df.apply(
        lambda r: tasa_dependientes(r["periodo"], r["afp"], indicadores), axis=1
    )

    # Flags y saneo de indicadores
    lic_df["indicador_ok"] = lic_df[["tope_mes", "tasa_dep"]].notna().all(axis=1)
    lic_df["renta_tope"] = lic_df["remun"] >= lic_df["tope_mes"].fillna(10**15)

    # ------------------------------------------------------------------
    # 2.2 Identificar cadenas continuas
    # ------------------------------------------------------------------
    lic_df = lic_df.sort_values(["rut", "inicio"]).reset_index(drop=True)
    cadena_id = 0
    dias_cadena = 0
    prev_rut = None
    prev_fin = None

    cadena_ids = []
    dias_acum = []

    for idx, row in lic_df.iterrows():
        if (row["rut"] != prev_rut) or (prev_fin is None) or (
            row["inicio"] > prev_fin + timedelta(days=1)
        ):
            # nueva cadena
            cadena_id += 1
            dias_cadena = row["dias_licencia"]
        else:
            dias_cadena += row["dias_licencia"]

        cadena_ids.append(cadena_id)
        dias_acum.append(dias_cadena)

        prev_rut = row["rut"]
        prev_fin = row["fin"]

    lic_df["cadena_id"] = cadena_ids
    lic_df["dias_cadena"] = dias_acum

    # Debemos conocer la suma total de cada cadena
    suma_cadena = lic_df.groupby("cadena_id")["dias_licencia"].transform("sum")
    lic_df["suma_cadena"] = suma_cadena
    lic_df["tipo_cadena"] = lic_df.groupby("cadena_id")["inicio"].transform("count")
    lic_df["tipo_cadena"] = lic_df["tipo_cadena"].apply(
        lambda n: "continua" if n > 1 else "discontinua"
    )

    # ------------------------------------------------------------------
    # 2.3 Calcular días pagados y comentarios
    # ------------------------------------------------------------------
    lic_df["dias_pagados"] = 0
    lic_df["estado"] = "rechazado"
    lic_df["comentario"] = ""

    # Primero: casos con indicadores faltantes
    mask_no_indicador = ~lic_df["indicador_ok"]
    lic_df.loc[mask_no_indicador, "comentario"] = "AFP o periodo sin datos en indicadores"

    # Caso A: Renta Tope
    mask_tope = lic_df["indicador_ok"] & lic_df["renta_tope"]
    lic_df.loc[mask_tope, "dias_pagados"] = lic_df.loc[mask_tope, "dias_licencia"]
    lic_df.loc[mask_tope, "estado"] = "aprobado"
    lic_df.loc[mask_tope, "comentario"] = "Renta tope – se pagan todos los días"

    # Caso B / C / D para renta no tope
    def procesa_cadena(df_sub: pd.DataFrame):
        """Aplica reglas a una cadena continua o discontinua."""
        nonlocal indicadores
        if df_sub.empty:
            return df_sub

        # Si cualquier fila ya está aprobada (renta tope) la dejamos así
        if (df_sub["renta_tope"]).any():
            return df_sub

        continuo = (df_sub["tipo_cadena"].iloc[0] == "continua")
        suma = df_sub["suma_cadena"].iloc[0]

        if continuo:
            if suma >= 11:
                # No se paga
                df_sub.loc[:, "comentario"] = "Continuas ≥ 11 días – no paga"
                return df_sub
            else:
                # pagar 3 días en total (primeras licencias en orden)
                dias_restantes = 3
                for idx, r in df_sub.iterrows():
                    pagar = min(r["dias_licencia"], dias_restantes)
                    if pagar > 0:
                        df_sub.at[idx, "dias_pagados"] = pagar
                        df_sub.at[idx, "estado"] = "aprobado"
                    dias_restantes -= pagar
                    if dias_restantes == 0:
                        break
                df_sub.loc[df_sub["dias_pagados"] > 0, "comentario"] = (
                    "Continuas < 11 días – se pagan 3 en total"
                )
                df_sub.loc[df_sub["dias_pagados"] == 0, "comentario"] = (
                    "Continuas < 11 días – cupo 3 ya consumido"
                )
                return df_sub
        else:
            # Discontinuas: cada licencia individual <10 → paga 3; ≥10 → no paga
            for idx, r in df_sub.iterrows():
                if r["dias_licencia"] < 10:
                    df_sub.at[idx, "dias_pagados"] = min(3, r["dias_licencia"])
                    df_sub.at[idx, "estado"] = "aprobado"
                    df_sub.at[idx, "comentario"] = "Discontinua < 10 días – paga 3"
                else:
                    df_sub.at[idx, "comentario"] = "Discontinua ≥ 10 días – no paga"
            return df_sub

    lic_df = lic_df.groupby("cadena_id", group_keys=False).apply(procesa_cadena)

    # ------------------------------------------------------------------
    # 2.4 Cálculos monetarios
    # ------------------------------------------------------------------
    lic_df["tasa_diaria"] = lic_df["remun"] / 30
    lic_df["monto_rem_dias"]  = (lic_df["dias_pagados"] * lic_df["tasa_diaria"]).round()

    # Aporte pensión 10 %
    lic_df["aporte_pension"] = (lic_df["monto_rem_dias"] * 0.10).round()

    # Comisión AFP = monto_rem_dias * (tasa_dependientes - 10)%
    lic_df["pct_comision"] = lic_df["tasa_dep"].fillna(10) - 10
    lic_df["comision_afp"] = (
        lic_df["monto_rem_dias"] * (lic_df["pct_comision"] / 100)
    ).round()

    lic_df["total_aporte_afp"] = (lic_df["aporte_pension"] + lic_df["comision_afp"]).round()

    # ------------------------------------------------------------------
    # 2.5 Resumen por RUT + período (pagos_df)
    # ------------------------------------------------------------------
    pagos_cols_agg = {
        "nombre": "first",
        "remun": "max",
        "cod": "first",
        "afp": "first",
        "dias_licencia": "sum",
        "dias_pagados": "sum",
        "monto_rem_dias": "sum",
        "aporte_pension": "sum",
        "comision_afp": "sum",
        "total_aporte_afp": "sum",
    }
    pagos_df = (
        lic_df.groupby(["rut", "periodo"], as_index=False)
        .agg(pagos_cols_agg)
    )

    # Para estado / comentario: si al menos una licencia aprobada → aprobado,
    # si todas rechazadas → rechazado (tomamos primer comentario rechazado)
    def estado_grupo(grp):
        return "aprobado" if (grp["estado"] == "aprobado").any() else "rechazado"

    def comentario_grupo(grp):
        if (grp["estado"] == "aprobado").any():
            # CONCAT comentarios de aprobados (para transparencia)
            return "; ".join(grp.loc[grp["estado"] == "aprobado", "comentario"].unique())
        return grp["comentario"].iloc[0]

    pagos_df["estado"] = lic_df.groupby(["rut", "periodo"]).apply(estado_grupo).values
    pagos_df["comentario"] = lic_df.groupby(["rut", "periodo"]).apply(comentario_grupo).values

    # Fechas inicio / fin reales
    fechas_min = lic_df.groupby(["rut", "periodo"])["inicio"].min().dt.strftime("%d-%m-%Y")
    fechas_max = lic_df.groupby(["rut", "periodo"])["fin"].max().dt.strftime("%d-%m-%Y")
    pagos_df["fecha_inicio"]  = fechas_min.values
    pagos_df["fecha_termino"] = fechas_max.values

    # Orden y nombres finales
    pagos_df = pagos_df.rename(
        columns={
            "rut": "RUT",
            "nombre": "Nombre completo",
            "remun": "Remuneracion",
            "cod": "Cod.",
            "periodo": "Periodo",
            "afp": "AFP",
            "dias_licencia": "dias_licencia",
            "dias_pagados": "dias_pagados",
            "monto_rem_dias": "monto_rem_dias",
            "aporte_pension": "aporte_pension",
            "comision_afp": "comision_afp",
            "total_aporte_afp": "total_aporte_afp",
            "fecha_inicio": "Fecha Inicio",
            "fecha_termino": "Fecha Término",
        }
    )

    # Reordenar columnas
    col_order = [
        "RUT", "Nombre completo", "Remuneracion", "Cod.", "Periodo",
        "Fecha Inicio", "Fecha Término", "AFP",
        "dias_licencia", "dias_pagados",
        "monto_rem_dias", "aporte_pension", "comision_afp", "total_aporte_afp",
        "estado", "comentario",
    ]
    pagos_df = pagos_df[col_order]

    # ------------------------------------------------------------------
    # 2.6 Columnas finales en lic_df (detalle)
    # ------------------------------------------------------------------
    lic_df = lic_df.rename(
        columns={
            "rut": "RUT",
            "nombre": "Nombre completo",
            "remun": "Remuneracion",
            "cod": "Cod.",
            "afp": "AFP",
        }
    )
    detalle_cols = [
        "RUT", "Nombre completo", "Cod.", "inicio", "fin", "dias_licencia",
        "cadena_id", "tipo_cadena", "suma_cadena",
        "renta_tope", "dias_pagados",
        "monto_rem_dias", "aporte_pension", "comision_afp", "total_aporte_afp",
        "estado", "comentario", "src",
    ]
    lic_df = lic_df[detalle_cols]
    lic_df = lic_df.rename(
        columns={
            "inicio": "Fecha Inicio",
            "fin": "Fecha Término",
            "suma_cadena": "dias_cadena",
        }
    )

    return pagos_df, lic_df


# --------------------------------------------------
# 3. FLUJO PRINCIPAL
# --------------------------------------------------
def main():
    """
    Flujo principal:
        1. Lee PDFs y extrae TODAS las líneas (cód. varios)
        2. Aplica reglas y cálculos → pagos_df (resumen) + lic_df (detalle)
        3. Genera        pagos_corresponde.xlsx   (solo Cod. 3 aprobados)
                        analisis_licencias_codigo3.xlsx (todos Cod. 3)
        4. No genera TXT de justificación
    """
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    indicadores = lee_indicadores()

    licencias_totales: list[dict] = []
    pdfs_proc: list[str] = []

    print("Extrayendo licencias de PDF…")
    for file in tqdm(os.listdir(INPUT_FOLDER)):
        if not file.lower().endswith(".pdf"):
            continue
        pdf_path = os.path.join(INPUT_FOLDER, file)
        lic = extrae_licencias(pdf_path)
        if lic:
            licencias_totales.extend(lic)
            pdfs_proc.append(file)

    if not licencias_totales:
        print("No se extrajeron líneas de licencias en los PDF.")
        return

    # Procesar reglas
    pagos_df, lic_df = procesa_licencias(licencias_totales, indicadores)

    # ------------------------------------------------------------
    # Añadir nombre(s) de PDF origen a pagos_df para trazabilidad
    # ------------------------------------------------------------
    archivos_grp = (
        lic_df.groupby(["RUT", "periodo"])["src"]
        .apply(lambda s: ", ".join(sorted(set(s))))
        .reset_index(name="archivos_pdf")
    )
    pagos_df = (
        pagos_df.merge(
            archivos_grp,
            left_on=["RUT", "Periodo"],  # 'Periodo' ya viene renombrado en pagos_df
            right_on=["RUT", "periodo"],
        )
        .drop(columns=["periodo"])
    )

    # ------------------------------------------------------------
    # ①  Archivo de pagos solo Cod. 3 + aprobados
    # ------------------------------------------------------------
    pagos_corresponde = pagos_df[
        (pagos_df["Cod."] == 3) & (pagos_df["estado"] == "aprobado")
    ].copy()

    pagos_corresponde_path = os.path.join(OUTPUT_FOLDER, "pagos_corresponde.xlsx")
    pagos_corresponde.to_excel(pagos_corresponde_path, index=False)
    print(f"► pagos_corresponde.xlsx generado: {pagos_corresponde_path}")

    # ------------------------------------------------------------
    # ②  Archivo de análisis completo (Cod. 3 aprobados + rechazados)
    # ------------------------------------------------------------
    # Orden de columnas similar al resumen, más 'archivos_pdf'
    col_order = [
        "RUT", "Nombre completo", "Remuneracion", "Cod.", "Periodo",
        "Fecha Inicio", "Fecha Término", "AFP",
        "dias_licencia", "dias_pagados",
        "monto_rem_dias", "aporte_pension", "comision_afp", "total_aporte_afp",
        "estado", "comentario", "archivos_pdf",
    ]
    analisis_df = pagos_df[col_order]

    analisis_path = os.path.join(OUTPUT_FOLDER, "analisis_licencias_codigo3.xlsx")
    analisis_df.to_excel(analisis_path, index=False)
    print(f"► analisis_licencias_codigo3.xlsx generado: {analisis_path}")


if __name__ == "__main__":
    main()
