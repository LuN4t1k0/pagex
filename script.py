import pdfplumber
import pandas as pd
import re, os, json, calendar, logging
from tqdm import tqdm

# --------------------------------------------------
# CONFIGURACIÓN GENERAL
# --------------------------------------------------
input_folder  = "upload"
output_folder = "output"
combined_excel_path = os.path.join(output_folder, "resultado_concatenado.xlsx")
summary_excel_path  = os.path.join(output_folder, "resumen_corresponde.xlsx")
json_path     = "indicadores/indicadores.json"

logging.basicConfig(
    filename="procesamiento.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

required_columns = [
    "RUT", "Apellido Paterno, Materno, Nombres", "Remuneración", "Cod.",
    "Fecha Inicio", "Fecha Término", "AFP",
]

header_patterns  = [
    r"^RUT$", r"Apellido Paterno,? Materno,? Nombres", r"Remuneración",
    r"Fecha Inicio", r"Fecha Término", r"Cod\.",
]
specific_headers = [
    "Identificación del Trabajador", "Fondo de Pensiones",
    "Seguro Cesantía", "Movimiento de Personal",
]

# --------------------------------------------------
# UTILIDADES
# --------------------------------------------------
def is_header_row(row):
    """Detecta filas de encabezado/sub-título en la tabla PDF."""
    return any(
        re.search(p, str(c), re.IGNORECASE)
        for c in row for p in header_patterns
    ) or any(h in str(c) for c in row for h in specific_headers)

def ultimo_dia_mes(dt):
    return calendar.monthrange(dt.year, dt.month)[1]

def normaliza_afp(nombre):
    mapa = {
        "provida":"Provida","proviva":"Provida","pro vida":"Provida",
        "capital":"Capital","cuprum":"Cuprum","habitat":"Habitat",
        "planvital":"PlanVital","plan vital":"PlanVital",
        "modelo":"Modelo","uno":"Uno"
    }
    return mapa.get(nombre.strip().lower(), nombre)

# --------------------------------------------------
# 1. EXTRACCIÓN DESDE PDF
# --------------------------------------------------
def extrae_tablas(pdf_path):
    filas = []
    afp_name = "Desconocida"
    try:
        with pdfplumber.open(pdf_path) as pdf:
            txt0 = pdf.pages[0].extract_text()
            m    = re.search(r"AFP\s+(\w+)", txt0, re.IGNORECASE)
            if m: afp_name = m.group(1)

            for pg in pdf.pages:
                table = pg.extract_table()
                if not table: continue
                for row in table:
                    if not (row and len(row) >= 15 and not is_header_row(row)):
                        continue
                    try:
                        filas.append([
                            row[0].strip(),               # RUT
                            row[1].strip(),               # Nombre
                            re.sub(r"[^\d,]","",row[2]).replace(",","."), # Remun
                            row[12].strip(),              # Cod.
                            row[13].strip(), row[14].strip(), # Fechas
                            afp_name,
                        ])
                    except IndexError:
                        logging.error(f"Fila malformada en {pdf_path}")
    except Exception as e:
        logging.error(f"Error en {pdf_path}: {e}")
    return filas

# --------------------------------------------------
# 2. TRANSFORMACIÓN Y REGLAS
# --------------------------------------------------
def procesa_dataframe(raw_rows, indicadores):
    # ---- carga básica
    df = pd.DataFrame(raw_rows, columns=required_columns)
    df["Remuneración"] = pd.to_numeric(df["Remuneración"], errors="coerce")
    df["Cod."]         = pd.to_numeric(df["Cod."], errors="coerce")
    df = df[~df["RUT"].str.contains("|".join(specific_headers), case=False, na=False)]

    # ---- fechas y días de cada LICENCIA original
    df["Fecha Inicio"]  = pd.to_datetime(df["Fecha Inicio"],  dayfirst=True, errors="coerce")
    df["Fecha Término"] = pd.to_datetime(df["Fecha Término"], dayfirst=True, errors="coerce")
    df["Dias_lic"]      = (df["Fecha Término"] - df["Fecha Inicio"]).dt.days + 1

    # ---- tipo de renta
    df["Tipo_Renta"] = df.apply(
        lambda r: "Renta Tope"
        if (
            pd.notna(r["Fecha Inicio"])
            and (per := r["Fecha Inicio"].strftime("%Y%m")) in indicadores
            and r["Remuneración"] >= float(
                indicadores[per]["rentas_topes_imponibles"]["valor"][0]
                .replace("$", "").replace(".", "").replace(",", ".")
            )
        )
        else "Renta No Tope",
        axis=1,
    )

    # ---- SEGMENTACIÓN por mes según reglas
    segmentos = []
    for _, r in df.iterrows():
        if pd.isna(r["Fecha Inicio"]) or pd.isna(r["Fecha Término"]):
            continue

        rut, nom, ren, cod = r["RUT"], r["Apellido Paterno, Materno, Nombres"], r["Remuneración"], r["Cod."]
        afp = normaliza_afp(r["AFP"])

        if r["Tipo_Renta"] == "Renta Tope":
            cur, end = r["Fecha Inicio"], r["Fecha Término"]
            while cur <= end:
                fin_mes = pd.Timestamp(cur.year, cur.month, ultimo_dia_mes(cur))
                bloque_fin = min(fin_mes, end)
                dias = (bloque_fin - cur).days + 1
                segmentos.append([rut, nom, ren, cod, afp,
                                  cur.strftime("%Y%m"), dias, "Renta Tope"])
                cur = bloque_fin + pd.Timedelta(days=1)
        else:
            if r["Dias_lic"] >= 10:
                continue
            start, end = r["Fecha Inicio"], r["Fecha Término"]
            if start.month == end.month and start.year == end.year:
                segmentos.append([rut, nom, ren, cod, afp,
                                  start.strftime("%Y%m"), r["Dias_lic"], "Renta No Tope"])
            else:
                dias_origen = ultimo_dia_mes(start) - start.day + 1
                segmentos.append([rut, nom, ren, cod, afp,
                                  start.strftime("%Y%m"), dias_origen, "Renta No Tope"])

    seg_df = pd.DataFrame(segmentos, columns=[
        "RUT","Nombre","Remuneración","Cod.","AFP","Periodo","Dias_segmento","Tipo_Renta"
    ])

    # ---- AGRUPACIÓN por RUT + Periodo
    agrup = (
        seg_df
        .groupby(["RUT", "Periodo"], as_index=False)
        .agg({
            "Nombre": "first",
            "Remuneración": "max",
            "Cod.": "first",
            "AFP": lambda x: x.mode()[0] if len(x) else "",
            "Tipo_Renta": "first",
            "Dias_segmento": "sum"
        })
        .rename(columns={"Dias_segmento": "Dias_mes"})
    )

    # ---- Rem_Días
    agrup["Rem_Días"] = agrup.apply(
        lambda r: r["Dias_mes"] if r["Tipo_Renta"] == "Renta Tope"
        else min(3, r["Dias_mes"]) if r["Dias_mes"] < 10
        else 0,
        axis=1
    )

    agrup = agrup[agrup["Rem_Días"] > 0]      # descarta filas sin pago


    # ---- Añade justo DESPUÉS del bloque de agrupación y ANTES de filtrar Rem_Días
    agrup["Tasa_diaria"] = agrup["Remuneración"] / 30

    def calc_rem_pesos(row):
        if row["Tipo_Renta"] == "Renta Tope":
            dias_pagables = row["Dias_mes"]                # se pagan todos
        else:
            dias_pagables = min(3, row["Dias_mes"]) if row["Dias_mes"] < 10 else 0
        return round(dias_pagables * row["Tasa_diaria"], 0)

    agrup["Rem_Días"] = agrup.apply(calc_rem_pesos, axis=1)
    agrup = agrup[agrup["Rem_Días"] > 0]                   # descarta sin pago


    # ---- Columnas monetarias
    with open(json_path, encoding="utf-8") as f:
        ind = json.load(f)

    def pct_afp(row):
        per, afp = row["Periodo"], row["AFP"]
        try:
            idx = [a.lower() for a in ind[per]["afp"]["afp"]].index(afp.lower())
            return float(ind[per]["afp"]["tasa_afp_dependientes"][idx]
                         .replace("%","").replace(",",".")) - 10.0
        except Exception:
            return 0.0

    agrup["Porcentaje"] = agrup.apply(pct_afp, axis=1)
    agrup["Pensión"]    = (agrup["Rem_Días"] * 0.10).round()
    agrup["Comisión"]   = ((agrup["Porcentaje"]/100) * agrup["Rem_Días"]).round()
    agrup["Total_AFP"]  = (agrup["Pensión"] + agrup["Comisión"]).astype(int)

    # ---- Etiquetas y fechas
    agrup["Análisis_Individual"] = "CORRESPONDE"
    agrup["Análisis_Grupo"]      = "CORRESPONDE"
    agrup["Resumen"]             = "Individual: CORRESPONDE, Grupo: CORRESPONDE"
    agrup["Fecha Inicio"]        = agrup["Periodo"].str[4:] + "-01-" + agrup["Periodo"].str[:4]
    agrup["Fecha Término"]       = ""

    agrup.rename(columns={"Nombre":"Apellido Paterno, Materno, Nombres",
                          "Dias_mes":"Días"}, inplace=True)

    # ---- Depuración: mostrar columnas clave
    print("\n>> DEBUG – primeros registros con columnas monetarias")
    print(
        agrup[[
            "RUT","Periodo","Tipo_Renta","Días","Rem_Días",
            "Porcentaje","Pensión","Comisión","Total_AFP"
        ]].head(10)
    )
    print("Filas con pago en este PDF:", len(agrup))
    print("----------------------------------------------------------\n")

    # ---- Asegura estructura completa
    FULL_COLS = [
        "RUT","Apellido Paterno, Materno, Nombres","Remuneración","Cod.",
        "Fecha Inicio","Fecha Término","AFP","Días","Rem_Días",
        "Pensión","Comisión","Total_AFP","Tipo_Renta","Periodo",
        "Análisis_Individual","Análisis_Grupo","Resumen"
    ]
    agrup = agrup.reindex(columns=FULL_COLS)

    # ---- Depuración: listado de columnas finales
    print(">> DEBUG – columnas del DataFrame final:", agrup.columns.tolist(), "\n")

    return agrup

# --------------------------------------------------
# 3. FLUJO PRINCIPAL
# --------------------------------------------------
def main():
    os.makedirs(output_folder, exist_ok=True)

    # Cargar indicadores una sola vez
    with open(json_path, encoding="utf-8") as f:
        indicadores = json.load(f)

    dataframes = []
    for file in tqdm(os.listdir(input_folder), desc="PDFs", unit="archivo"):
        if file.lower().endswith(".pdf"):
            raws = extrae_tablas(os.path.join(input_folder, file))
            if raws:
                dataframes.append(procesa_dataframe(raws, indicadores))

    if not dataframes:
        print("No se extrajo ningún dato de los PDFs.")
        return

    df = pd.concat(dataframes, ignore_index=True)

    expected = ["Pensión","Comisión","Total_AFP"]
    missing  = [c for c in expected if c not in df.columns]
    print("Faltan:", missing)



    df.to_excel(combined_excel_path, index=False)
    print(f"Archivo combinado guardado: {combined_excel_path}")

    # ----- RESUMEN (mismo criterio que antes) -----
    resumen = df[(df["Cod."] == 3) & (df["Remuneración"] > 0)]
    if resumen.empty:
        print("No hay registros que cumplan criterio de resumen.")
        return
    resumen.to_excel(summary_excel_path, index=False)
    print(f"Resumen guardado en: {summary_excel_path}")

    for afp, g in resumen.groupby("AFP"):
        fname = f"AFP_{afp.replace(' ','_')}.xlsx"
        g.to_excel(os.path.join(output_folder, fname), index=False)
    print("Archivos por AFP generados correctamente.")

    

if __name__ == "__main__":
    main()
