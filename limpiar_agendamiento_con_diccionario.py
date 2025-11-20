import pandas as pd
import json
import os

# ---------------------------------------------------------
# 1. Cargar diccionarios oficiales
# ---------------------------------------------------------

RUTA_DICC = "diccionarios/"

def cargar_diccionario(nombre):
    ruta = os.path.join(RUTA_DICC, nombre)
    with open(ruta, "r", encoding="utf-8") as f:
        return json.load(f)

upz_to_zona = cargar_diccionario("upz_to_zona.json")
zona_to_upz = cargar_diccionario("zona_to_upz.json")

print("Diccionarios cargados correctamente ✔")


# ---------------------------------------------------------
# 2. Funciones de corrección inteligente UPZ ↔ Zona
# ---------------------------------------------------------

def corregir_upz_zona(upz, zona):
    """
    Corrige UPZ y Zona incluso si una de las dos está mal.
    Retorna: (upz_corregida, zona_corregida, estado)
    estados posibles:
      - OK_AMBAS
      - OK_UPZ_CORRIGE_ZONA
      - OK_ZONA_CORRIGE_UPZ
      - ERROR_AMBAS
    """

    upz_orig = upz
    zona_orig = zona

    upz_cor = upz
    zona_cor = zona

    # Caso 1 – UPZ es válida → corregimos zona
    if upz in upz_to_zona:
        zona_cor = upz_to_zona[upz]
        if zona not in zona_to_upz or zona_to_upz[zona] != upz:
            return upz_cor, zona_cor, "OK_UPZ_CORRIGE_ZONA"
        else:
            return upz_cor, zona_cor, "OK_AMBAS"

    # Caso 2 – Zona es válida → corregimos UPZ
    if zona in zona_to_upz:
        upz_cor = zona_to_upz[zona]
        return upz_cor, zona_cor, "OK_ZONA_CORRIGE_UPZ"

    # Caso 3 – Ambas inválidas
    return upz_orig, zona_orig, "ERROR_AMBAS"


# ---------------------------------------------------------
# 3. Cargar archivo de agendamiento
# ---------------------------------------------------------

def cargar_archivo_agendamiento(ruta):
    df = pd.read_excel(ruta, dtype=str)
    print(f"Archivo cargado: {ruta}")
    return df


# ---------------------------------------------------------
# 4. Aplicar correcciones fila por fila
# ---------------------------------------------------------

def limpiar_dataframe(df):
    registros_error = []

    df["UPZ_Corregida"] = ""
    df["Zona_Corregida"] = ""
    df["Estado_Corrección"] = ""

    for idx, row in df.iterrows():
        upz = (row.get("UPZ", "") or "").strip()
        zona = (row.get("Zona a la Que Pertenece la Actividad", "") or "").strip()

        upz_new, zona_new, estado = corregir_upz_zona(upz, zona)

        df.at[idx, "UPZ_Corregida"] = upz_new
        df.at[idx, "Zona_Corregida"] = zona_new
        df.at[idx, "Estado_Corrección"] = estado

        if estado == "ERROR_AMBAS":
            registros_error.append({
                "fila": idx+1,
                "upz_original": upz,
                "zona_original": zona
            })

    return df, registros_error


# ---------------------------------------------------------
# 5. Guardar resultados
# ---------------------------------------------------------

def guardar_resultados(df, errores):
    os.makedirs("salidas", exist_ok=True)

    ruta_salida = "salidas/agendamiento_limpio.csv"
    df.to_csv(ruta_salida, index=False, encoding="utf-8-sig")

    ruta_incons = "salidas/agendamiento_inconsistencias.csv"
    pd.DataFrame(errores).to_csv(ruta_incons, index=False, encoding="utf-8-sig")

    print("✔ Archivo limpio generado:", ruta_salida)
    print("✔ Archivo de inconsistencias:", ruta_incons)


# ---------------------------------------------------------
# 6. Ejecución principal
# ---------------------------------------------------------

if __name__ == "__main__":

    # 👉 Cambia esto por el nombre de tu archivo
    ARCHIVO = "Agendamiento_Actividades.xlsx"

    df = cargar_archivo_agendamiento(ARCHIVO)
    df_limpio, inconsistencias = limpiar_dataframe(df)
    guardar_resultados(df_limpio, inconsistencias)

    print("\n🔥 Proceso completado con éxito.")