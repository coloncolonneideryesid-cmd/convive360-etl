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
    dicc_upz_zonas = cargar_diccionario()

    # 2. Cargar archivo original (Google Sheets exportado)
    df = pd.read_csv("fact_actividades.csv")
    print(f"📥 Archivo cargado: {df.shape[0]} registros")

    # -------------------------------------------------------
    # 🔥 2.1 DEDUPLICACIÓN
    # -------------------------------------------------------
    columnas_clave = ["Nombre de la actividad", "Fecha de la actividad", "Hora de inicio"]

    print("🧹 Verificando duplicados...")

    if all(col in df.columns for col in columnas_clave):
        antes = df.shape[0]
        df = df.drop_duplicates(subset=columnas_clave, keep="first")
        despues = df.shape[0]
        eliminados = antes - despues

        print(f"✓ Deduplicación aplicada: {eliminados} duplicados eliminados")

        # Guardar duplicados eliminados (auditoría)
        if eliminados > 0:
            df_duplicados = df[df.duplicated(subset=columnas_clave, keep=False)]
            df_duplicados.to_csv("duplicados_detectados.csv", index=False, encoding="utf-8-sig")
            print("⚠ Archivo duplicados_detectados.csv generado (auditoría)")

    else:
        print("⚠ No fue posible aplicar deduplicación: faltan columnas clave")

    # -------------------------------------------------------
    # 3. Limpiar UPZ
    # -------------------------------------------------------
    print("✓ Corrigiendo nombres de UPZ...")

    if "Nombre_UPZ" in df.columns:
        correcciones = {}
        df["Nombre_UPZ"] = df["Nombre_UPZ"].apply(lambda x: limpiar_nombre_upz(x, correcciones))

    # -------------------------------------------------------
    # 4. Asignar zonas
    # -------------------------------------------------------
    print("✓ Asignando zonas desde diccionario oficial...")

    if "Codigo_UPZ" in df.columns:
        df["Zonas_Asignadas"] = df["Codigo_UPZ"].apply(lambda x: asignar_zonas(x, dicc_upz_zonas))
    else:
        df["Zonas_Asignadas"] = "SIN ZONA"

    # -------------------------------------------------------
    # VALIDACIÓN UPZ
    # -------------------------------------------------------
    print("🔍 Validando UPZ...")

    errores = []
    upz_validas = set(dicc_upz_zonas.keys())

    for i, row in df.iterrows():
        codigo = str(row.get("Codigo_UPZ", "")).strip()
        nombre = str(row.get("Nombre_UPZ", "")).strip()

        if codigo == "" or codigo.lower() == "nan":
            errores.append({
                "Fila": i + 1,
                "Codigo_UPZ": codigo,
                "Nombre_UPZ": nombre,
                "Error": "UPZ vacía o nula"
            })
            continue

        if codigo not in upz_validas:
            errores.append({
                "Fila": i + 1,
                "Codigo_UPZ": codigo,
                "Nombre_UPZ": nombre,
                "Error": "Código de UPZ NO existe en el diccionario"
            })
            continue

    # Export errores
    if len(errores) > 0:
        df_err = pd.DataFrame(errores)
        df_err.to_csv("errores_upz.csv", index=False, encoding="utf-8-sig")
        print(f"⚠ Se detectaron {len(errores)} errores de UPZ. Archivo generado: errores_upz.csv")
    else:
        print("✓ No se encontraron errores de UPZ")

    # -------------------------------------------------------
    # EXPORT FINAL
    # -------------------------------------------------------
    df.to_csv("fact_actividades_limpio.csv", index=False, encoding="utf-8-sig")

    print("🎉 Archivo fact_actividades_limpio.csv generado correctamente")


# -------------------------------------------------------
# EJECUCIÓN DIRECTA
# -------------------------------------------------------
if __name__ == "__main__":
    procesar_archivo()
