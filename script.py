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

    # ---- fechas originales
    df["Fecha Inicio"]  = pd.to_datetime(df["Fecha Inicio"],  dayfirst=True, errors="coerce")
    df["Fecha Término"] = pd.to_datetime(df["Fecha Término"], dayfirst=True, errors="coerce")
    df["Dias_lic"]      = (df["Fecha Término"] - df["Fecha Inicio"]).dt.days + 1

    # ---- tipo de renta
    df["Tipo_Renta"] = df.apply(
        lambda r: "Renta Tope"
        if (
            pd.notna(r["Fecha Inicio"])
            and (p := r["Fecha Inicio"].strftime("%Y%m")) in indicadores
            and r["Remuneración"] >= float(
                indicadores[p]["rentas_topes_imponibles"]["valor"][0]
                .replace("$", "").replace(".", "").replace(",", ".")
            )
        )
        else "Renta No Tope",
        axis=1,
    )

    # -----------------------------------------------------------------
    #  Estructura para informe: (rut, periodo) -> [dict(...)]
    # -----------------------------------------------------------------
    detalle_lic = {}

    # ---- SEGMENTACIÓN por mes
    segmentos = []
    for _, r in df.iterrows():
        if pd.isna(r["Fecha Inicio"]) or pd.isna(r["Fecha Término"]):
            continue

        rut, nom, ren, cod = r["RUT"], r["Apellido Paterno, Materno, Nombres"], r["Remuneración"], r["Cod."]
        afp = normaliza_afp(r["AFP"])

        # --- Renta Tope → pagar todos los días por bloque mensual
        if r["Tipo_Renta"] == "Renta Tope":
            cur, end = r["Fecha Inicio"], r["Fecha Término"]
            while cur <= end:
                fin_mes = pd.Timestamp(cur.year, cur.month, ultimo_dia_mes(cur))
                bloc_fin = min(fin_mes, end)
                dias = (bloc_fin - cur).days + 1
                per  = cur.strftime("%Y%m")

                segmentos.append([rut, nom, ren, cod, afp, per, dias, "Renta Tope"])
                detalle_lic.setdefault((rut, per), []).append(
                    {"ini": cur.strftime("%d-%m-%Y"),
                     "fin": bloc_fin.strftime("%d-%m-%Y"),
                     "dias_orig": dias,
                     "dias_pag": None,
                     "motivo": ""}
                )
                cur = bloc_fin + pd.Timedelta(days=1)
        # --- Renta No Tope (sólo licencias <10d)
        else:
            if r["Dias_lic"] >= 10:
                continue
            start, end = r["Fecha Inicio"], r["Fecha Término"]

            if start.month == end.month and start.year == end.year:
                dias = r["Dias_lic"]
                per  = start.strftime("%Y%m")
                segmentos.append([rut, nom, ren, cod, afp, per, dias, "Renta No Tope"])
                detalle_lic.setdefault((rut, per), []).append(
                    {"ini": start.strftime("%d-%m-%Y"),
                     "fin": end.strftime("%d-%m-%Y"),
                     "dias_orig": dias,
                     "dias_pag": None,
                     "motivo": ""}
                )
            else:
                dias_origen = ultimo_dia_mes(start) - start.day + 1
                per  = start.strftime("%Y%m")
                segmentos.append([rut, nom, ren, cod, afp, per, dias_origen, "Renta No Tope"])
                detalle_lic.setdefault((rut, per), []).append(
                    {"ini": start.strftime("%d-%m-%Y"),
                     "fin": pd.Timestamp(start.year, start.month, ultimo_dia_mes(start)).strftime("%d-%m-%Y"),
                     "dias_orig": dias_origen,
                     "dias_pag": None,
                     "motivo": ""}
                )

    # ---------- DataFrame de segmentos ----------
    seg_df = pd.DataFrame(segmentos, columns=[
        "RUT","Nombre","Remuneración","Cod.","AFP","Periodo","Dias_segmento","Tipo_Renta"
    ])

    # ---------- AGRUPACIÓN mínima -------------
    agrup = (
        seg_df
        .groupby(["RUT", "Periodo"], as_index=False)
        .agg({
            "Nombre": "first",
            "Remuneración": "max",
            "Cod.": "first",
            "AFP":  lambda x: x.mode()[0] if len(x) else "",
            "Tipo_Renta": "first",
            "Dias_segmento": "sum"
        })
        .rename(columns={"Dias_segmento": "Dias_mes"})
    )

    # ---------- Rem_Días (monto) -------------
    agrup["Tasa_diaria"] = agrup["Remuneración"] / 30

    def calc_monto(row):
        if row["Tipo_Renta"] == "Renta Tope":
            dias_pag = row["Dias_mes"]
        else:
            dias_pag = min(3, row["Dias_mes"]) if row["Dias_mes"] < 10 else 0
        return round(dias_pag * row["Tasa_diaria"], 0)

    agrup["Rem_Días"] = agrup.apply(calc_monto, axis=1)
    agrup = agrup[agrup["Rem_Días"] > 0]

    # ---------- REPARTO de días pagados a cada licencia ----------
    for _, row in agrup.iterrows():
        clave = (row["RUT"], row["Periodo"])
        lic_list = detalle_lic.get(clave, [])
        if not lic_list:
            continue

        if row["Tipo_Renta"] == "Renta Tope":
            for lic in lic_list:
                lic["dias_pag"] = lic["dias_orig"]
                lic["motivo"]   = "RT-1 (pago completo)"
        else:
            saldo = min(3, row["Dias_mes"]) if row["Dias_mes"] < 10 else 0
            for lic in sorted(lic_list, key=lambda x: pd.to_datetime(x["ini"], dayfirst=True)):
                if saldo > 0:
                    pagar = min(lic["dias_orig"], saldo)
                    lic["dias_pag"] = pagar
                    lic["motivo"]   = "Pagado (RN-1/RN-2)"
                    saldo -= pagar
                else:
                    lic["dias_pag"] = 0
                    lic["motivo"]   = "RN-1/RN-2 → 0"

    # ---------- Columnas monetarias ----------
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

    # ---------- Fechas y columnas finales ----------
    agrup["Fecha Inicio"]  = agrup["Periodo"].str[4:] + "-01-" + agrup["Periodo"].str[:4]
    agrup["Fecha Término"] = ""
    agrup.rename(columns={"Nombre":"Nombre completo", "Dias_mes":"Días"}, inplace=True)

    FINAL_COLS = [
        "RUT","Nombre completo","Remuneración","Cod.",
        "Periodo","Fecha Inicio","Fecha Término","AFP",
        "Días","Tipo_Renta","Rem_Días","Pensión","Comisión","Total_AFP"
    ]
    agrup = agrup.reindex(columns=FINAL_COLS)

    return agrup, detalle_lic

# --------------------------------------------------
# 3. FLUJO PRINCIPAL
# --------------------------------------------------
from datetime import datetime

def main():
    os.makedirs(output_folder, exist_ok=True)

    with open(json_path, encoding="utf-8") as f:
        indicadores = json.load(f)

    dataframes, detalles_global, pdfs = [], {}, []

    # ---------- recorrer PDFs ----------
    for file in os.listdir(input_folder):
        if not file.lower().endswith(".pdf"):
            continue
        pdf_path = os.path.join(input_folder, file)
        raws = extrae_tablas(pdf_path)
        if not raws:
            continue
        df_res, det_pdf = procesa_dataframe(raws, indicadores)
        dataframes.append(df_res)
        # merge de detalles
        for k, v in det_pdf.items():
            detalles_global.setdefault(k, []).extend(v)
        pdfs.append(file)

    if not dataframes:
        print("No se extrajo ningún dato.")
        return

    df = pd.concat(dataframes, ignore_index=True)

    # ---------- RESUMEN ----------
    resumen = df[(df["Cod."] == 3) & (df["Remuneración"] > 0)]
    if resumen.empty:
        print("Sin registros que cumplan criterio.")
        return

    resumen.to_excel(summary_excel_path, index=False)
    print("Resumen guardado en:", summary_excel_path)

    # ---------- Archivos por AFP ----------
    for afp, g in resumen.groupby("AFP"):
        g.to_excel(os.path.join(output_folder, f"AFP_{afp.replace(' ','_')}.xlsx"), index=False)
    print("Archivos por AFP generados.")

    # ---------- TXT de justificación ----------
    txt_path = os.path.join(output_folder,
                f"justificacion_{datetime.now():%Y%m%d_%H%M}.txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("Fecha de procesamiento: " + datetime.now().isoformat() + "\n")
        f.write("PDFs procesados: " + ", ".join(pdfs) + "\n")
        f.write(f"Registros con pago: {len(resumen)}\n\n")

        # reglas
        f.write("REGLAS APLICADAS\n")
        f.write("  RN-1  Licencia <10 días paga máx. 3 días.\n")
        f.write("  RN-2  Suma mensual <10 días paga máx. 3 días en total.\n")
        f.write("  RN-3  Si cruza mes (<10d) sólo paga el mes origen.\n")
        f.write("  RT-1  Renta Tope paga todos los días.\n\n")

        # detalle por RUT-Periodo
        for _, row in resumen.iterrows():
            clave = (row["RUT"], row["Periodo"])
            f.write(f"{row['RUT']} – {row['Periodo']}\n")
            f.write(f"  • Monto pagable: ${int(row['Rem_Días']):,}\n")
            f.write(f"    Regla mes: {'RT-1 (Tope)' if row['Tipo_Renta']=='Renta Tope' else 'RN-1/RN-2'}\n")
            f.write(f"    Licencias analizadas:\n")
            for lic in detalles_global.get(clave, []):
                f.write(
                    f"      - {lic['ini']} → {lic['fin']}  "
                    f"(orig: {lic['dias_orig']} d, pagados: {lic['dias_pag']} d)  "
                    f"{lic['motivo']}\n"
                )
            f.write("\n")

    print("Justificación generada:", txt_path)

if __name__ == "__main__":
    main()
    

