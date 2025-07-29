

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
from typing import List, Dict, Any

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


# ---------- regex pre-compilados ----------
RE_AFP       = re.compile(r"AFP\s+(\w+)", re.I)
RE_PERIOD     = re.compile(r"Per[ií]odo de Remuneraciones:\s*(\d{2})/(\d{4})")
RE_ONLY_DIGITS = re.compile(r"[^\d]")
RE_MONEY_CSL   = re.compile(r"[^\d,]")      # para limpiar remuneración

# ---------- columnas mínimas esperadas ----------
COL_RUT   = 0
COL_NAME  = 1
COL_REMUN = 2
# las tres últimas columnas: cod, fecha_ini, fecha_fin

# ---------- helpers ----------
def s(val) -> str:
    """Devuelve `str(val).strip()` manejando None."""
    return str(val or "").strip()

def parse_fecha(text: str) -> datetime | None:
    text = s(text)
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None

def clean_money(raw: str) -> float:
    txt = RE_MONEY_CSL.sub("", s(raw)).replace(",", ".") or "0"
    try:
        return float(txt)
    except ValueError:
        return 0.0

def is_valid_rut(rut: str) -> bool:
    """Valida formato básico NN.NNN.NNN-K (sin dígito verificador)."""
    rtn = RE_ONLY_DIGITS.sub("", rut)
    return 7 <= len(rtn) <= 9  # muy simple; ajusta si necesitas dv

# ---------- función principal ----------
def extrae_licencias(pdf_path: str) -> List[Dict[str, Any]]:
    licencias: list[dict] = []
    afp_name = "Desconocida"
    periodo_pdf = None                              # YYYYMM

    with pdfplumber.open(pdf_path) as pdf:
        portada_txt = pdf.pages[0].extract_text() or ""

        if (m := RE_AFP.search(portada_txt)):
            afp_name = m.group(1)

        if (m := RE_PERIOD.search(portada_txt)):
            periodo_pdf = f"{m.group(2)}{m.group(1)}"  # YYYYMM

        # ---------- recorrer desde la 2.ª página ----------
        for pg in pdf.pages[1:]:
            for row in (pg.extract_table() or []):
                if not row or len(row) < 6 or is_header_row(row):
                    continue

                rut = s(row[COL_RUT])
                if not rut or not is_valid_rut(rut):
                    continue

                nombre = s(row[COL_NAME])
                remun  = clean_money(row[COL_REMUN])

                cod_txt, ini_txt, fin_txt = map(s, row[-3:])
                cod = int(RE_ONLY_DIGITS.sub("", cod_txt) or 0)

                ini = parse_fecha(ini_txt)
                fin = parse_fecha(fin_txt) or ini

                # ---------- derivar período ----------
                if ini:
                    periodo = ini.strftime("%Y%m")
                elif fin:
                    periodo = fin.strftime("%Y%m")
                elif periodo_pdf:
                    ini = fin = datetime.strptime(periodo_pdf + "01", "%Y%m%d")
                    periodo = periodo_pdf
                else:
                    continue  # sin fechas ni encabezado → descarta

                licencias.append(
                    dict(
                        rut=rut,
                        nombre=nombre,
                        remun=remun,
                        cod=cod,
                        inicio=ini,
                        fin=fin,
                        periodo=periodo,
                        afp=normaliza_afp(afp_name),
                        src=os.path.basename(pdf_path),
                    )
                )

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

    # ---------------------------------------------------------------
    #  Rellenar remuneración de los códigos 3 con remuneración = 0
    #  usando la remuneración >0 del mismo RUT y mismo PDF
    # ---------------------------------------------------------------
    max_rem_por_rut_pdf = (
        lic_df.groupby(["rut", "src"])["remun"]
              .transform(lambda s: s[s > 0].max() if (s > 0).any() else 0)
    )

    mask_fill = (
        (lic_df["cod"] == 3) &
        (lic_df["remun"] == 0) &
        (max_rem_por_rut_pdf > 0)
    )

    if "comentario" not in lic_df.columns:
        lic_df["comentario"] = ""

    lic_df.loc[mask_fill, "remun"] = max_rem_por_rut_pdf[mask_fill]
    lic_df.loc[mask_fill, "comentario"] = lic_df.loc[mask_fill, "comentario"].mask(
        lic_df.loc[mask_fill, "comentario"] == "",
        "Remuneración copiada desde otra línea del mismo PDF"
    )


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
        • Extrae líneas de los PDF (todos los códigos)
        • Aplica reglas de pago → pagos_df (resumen) y lic_df (detalle Cod. 3)
        • Genera:
              1) pagos_corresponde.xlsx   (solo Cod. 3 aprobados)
              2) analisis_licencias_codigo3.xlsx (Cod. 3 completos + PDF origen)
        • No crea TXT de justificación
    """
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    indicadores = lee_indicadores()

    licencias_totales, pdfs_proc = [], []

    print("Extrayendo licencias de PDF…")
    for file in tqdm(os.listdir(INPUT_FOLDER)):
        if file.lower().endswith(".pdf"):
            pdf_path = os.path.join(INPUT_FOLDER, file)
            lic = extrae_licencias(pdf_path)
            if lic:
                licencias_totales.extend(lic)
                pdfs_proc.append(file)

    if not licencias_totales:
        print("No se extrajeron líneas de licencias en los PDF.")
        return

    # ---------- Procesar reglas ------------------------------------------
    pagos_df, lic_df = procesa_licencias(licencias_totales, indicadores)

    # ---------------------------------------------------------------------
    # Asegurar que lic_df conserva la columna 'periodo'
    # (puede haber sido eliminada al seleccionar columnas)
    # ---------------------------------------------------------------------
    if "periodo" not in lic_df.columns:
        # Recalcular a partir de la Fecha Inicio
        lic_df["periodo"] = pd.to_datetime(
            lic_df["Fecha Inicio"], dayfirst=True, errors="coerce"
        ).dt.strftime("%Y%m")

    # ---------------------------------------------------------------------
    # PDF origen → agregar a pagos_df para trazabilidad
    # ---------------------------------------------------------------------
    archivos_grp = (
        lic_df.groupby(["RUT", "periodo"])["src"]
        .apply(lambda s: ", ".join(sorted(set(s))))
        .reset_index(name="archivos_pdf")
    )

    pagos_df = (
        pagos_df.merge(
            archivos_grp,
            left_on=["RUT", "Periodo"],
            right_on=["RUT", "periodo"],
            how="left",
        )
        .drop(columns=["periodo"])
    )

    # ---------------------------------------------------------------------
    # ①  pagos_corresponde.xlsx  (Cod. 3 y aprobados)
    # ---------------------------------------------------------------------
    pagos_corresponde = pagos_df[
        (pagos_df["Cod."] == 3) & (pagos_df["estado"] == "aprobado")
    ].copy()

    pagos_corresponde_path = os.path.join(OUTPUT_FOLDER, "pagos_corresponde.xlsx")
    pagos_corresponde.to_excel(pagos_corresponde_path, index=False)
    print(f"► pagos_corresponde.xlsx generado: {pagos_corresponde_path}")

    # ---------------------------------------------------------------------
    # ②  analisis_licencias_codigo3.xlsx (todos Cod. 3)
    # ---------------------------------------------------------------------
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

    # al final de script.py  (o en __all__)
__all__ = ["extrae_licencias", "procesa_licencias"]
