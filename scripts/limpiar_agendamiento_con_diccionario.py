import pandas as pd
import json
import os

# -------------------------------------------------------
# 1) Cargar diccionario UPZ ↔ ZONAS
# -------------------------------------------------------
def cargar_diccionario():
    ruta = os.path.join("scripts", "diccionario_upz_zonas.json")
    with open(ruta, "r", encoding="utf-8") as f:
        return json.load(f)

# -------------------------------------------------------
# 2) Función para normalizar UPZ (corrige texto)
# -------------------------------------------------------
def limpiar_nombre_upz(upz, correcciones):
    if pd.isna(upz):
        return None

    original = upz
    upz = upz.strip().upper()

    # Aplicar correcciones exactas
    if upz in correcciones:
        return correcciones[upz]

    # Correcciones “suaves”
    for incorrecto, correcto in correcciones.items():
        if upz.replace(" ", "") == incorrecto.replace(" ", ""):
            return correcto

    return upz


# -------------------------------------------------------
# 3) Asignar zonas según el diccionario
# -------------------------------------------------------
def asignar_zonas(codigo_upz, diccionario):
    codigo_upz = str(codigo_upz).strip()

    if codigo_upz in diccionario:
        return ", ".join(diccionario[codigo_upz])

    return "SIN ZONA"


# -------------------------------------------------------
# 4) PROCESO PRINCIPAL
# -------------------------------------------------------
def procesar_archivo():
    print(">>> Iniciando limpieza del archivo de actividades...")

    # 1. Cargar diccionario
    dicc = cargar_diccionario()

    dicc_upz_zonas = dicc  # Diccionario del archivo JSON

    # 2. Cargar archivo original (Google Sheets exportado)
    df = pd.read_csv("fact_actividades.csv")

    # 3. Limpiar UPZ
    print("✓ Corrigiendo nombres de UPZ...")
    if "Nombre_UPZ" in df.columns:
        correcciones = {}
        df["Nombre_UPZ"] = df["Nombre_UPZ"].apply(lambda x: limpiar_nombre_upz(x, correcciones))

    # 4. Asignar zonas basado en Código_UPZ
    print("✓ Asignando zonas desde diccionario oficial...")

    if "Codigo_UPZ" in df.columns:
        df["Zonas_Asignadas"] = df["Codigo_UPZ"].apply(lambda x: asignar_zonas(x, dicc_upz_zonas))
    else:
        df["Zonas_Asignadas"] = "SIN ZONA"

    # 5. Exportar archivo limpio
    df.to_csv("fact_actividades_limpio.csv", index=False, encoding="utf-8-sig")

    print("🎉 Archivo fact_actividades_limpio.csv generado correctamente")


# -------------------------------------------------------
# EJECUCIÓN DIRECTA
# -------------------------------------------------------
if __name__ == "__main__":
    procesar_archivo()
