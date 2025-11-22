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

    # Correcciones "suaves"
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
# 3B) Validar compatibilidad UPZ ↔ ZONA
# -------------------------------------------------------
def validar_upz_zona(upz, zona, diccionario):
    """
    Valida que la UPZ y Zona sean compatibles.
    Retorna: (upz_correcta, zona_correcta, mensaje_error)
    """
    # Si no hay UPZ, no podemos validar
    if pd.isna(upz) or str(upz).strip() == "":
        if pd.isna(zona) or str(zona).strip() == "":
            return upz, zona, "UPZ y Zona vacías"
        return upz, zona, "UPZ vacía pero tiene Zona"
    
    upz_str = str(upz).strip()
    zona_str = str(zona).strip() if not pd.isna(zona) else ""
    
    # Buscar UPZ en diccionario
    if upz_str not in diccionario:
        return upz, zona, f"UPZ '{upz_str}' no existe en diccionario"
    
    # Obtener zonas válidas para esta UPZ
    zonas_validas = diccionario[upz_str]
    
    # Si no hay zona declarada, asignar todas las válidas
    if not zona_str or zona_str == "":
        zona_correcta = ", ".join(zonas_validas)
        return upz, zona_correcta, None
    
    # Si hay zona declarada, validar que sea compatible
    if zona_str in zonas_validas:
        return upz, zona, None  # Todo correcto
    else:
        # CONFLICTO: La zona declarada NO corresponde a la UPZ
        zona_correcta = ", ".join(zonas_validas)
        return upz, zona_correcta, f"CONFLICTO: Zona '{zona_str}' no corresponde a UPZ '{upz_str}'. Zonas correctas: {zona_correcta}"

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
    # 🔥 2.1 DEDUPLICACIÓN (MEJORADO - EXCLUIR VACÍOS)
    # -------------------------------------------------------
    columnas_clave = [
        "Nombre_Actividad", 
        "Responsable_Principal", 
        "Direccion_Actividad", 
        "Fecha_Actividad", 
        "Hora_Inicio"
    ]

    print("🧹 Verificando duplicados...")

    if all(col in df.columns for col in columnas_clave):
        antes = df.shape[0]
        
        # PASO 1: Identificar registros con información completa
        columnas_minimas = ["Nombre_Actividad", "Fecha_Actividad", "Hora_Inicio"]
        tiene_info_completa = df[columnas_minimas].notna().all(axis=1)
        
        registros_completos = df[tiene_info_completa].copy()
        registros_incompletos = df[~tiene_info_completa].copy()
        
        print(f"  → Registros con información completa: {len(registros_completos)}")
        print(f"  → Registros con información incompleta: {len(registros_incompletos)}")
        
        # PASO 2: Aplicar deduplicación SOLO a registros completos
        mascara_duplicados = registros_completos.duplicated(subset=columnas_clave, keep=False)
        
        if mascara_duplicados.sum() > 0:
            df_duplicados = registros_completos[mascara_duplicados].copy()
            df_duplicados.to_csv("duplicados_detectados.csv", index=False, encoding="utf-8-sig")
            print(f"⚠ Se detectaron {mascara_duplicados.sum()} registros duplicados")
        
        # PASO 3: Eliminar duplicados (mantener el primero)
        registros_completos = registros_completos.drop_duplicates(subset=columnas_clave, keep="first")
        
        # PASO 4: Recombinar registros completos (sin duplicados) + incompletos
        df = pd.concat([registros_completos, registros_incompletos], ignore_index=True)
        
        despues = df.shape[0]
        eliminados = antes - despues

        print(f"✓ Deduplicación aplicada: {eliminados} registros duplicados eliminados")
        
        if eliminados > 0:
            print(f"📄 Archivo duplicados_detectados.csv generado (auditoría)")

    else:
        faltantes = [col for col in columnas_clave if col not in df.columns]
        print(f"⚠ No fue posible aplicar deduplicación: faltan columnas {faltantes}")

    # -------------------------------------------------------
    # 3. Limpiar UPZ
    # -------------------------------------------------------
    print("✓ Corrigiendo nombres de UPZ...")

    if "Nombre_UPZ" in df.columns:
        correcciones = {}
        df["Nombre_UPZ"] = df["Nombre_UPZ"].apply(lambda x: limpiar_nombre_upz(x, correcciones))
    else:
        print("⚠ No se encontró columna 'Nombre_UPZ'")

    # -------------------------------------------------------
    # 4. Asignar zonas (CORREGIDO)
    # -------------------------------------------------------
    print("✓ Asignando zonas desde diccionario oficial...")

    # Intentar con diferentes nombres de columna
    col_upz = None
    if "Codigo_UPZ" in df.columns:
        col_upz = "Codigo_UPZ"
    elif "Nombre_UPZ" in df.columns:
        col_upz = "Nombre_UPZ"

    if col_upz:
        df["Zonas_Asignadas"] = df[col_upz].apply(lambda x: asignar_zonas(x, dicc_upz_zonas))
        print(f"  → Usando columna: {col_upz}")
    else:
        print("⚠ No se encontró columna de UPZ, asignando 'SIN ZONA'")
        df["Zonas_Asignadas"] = "SIN ZONA"

    # -------------------------------------------------------
    # VALIDACIÓN Y CORRECCIÓN UPZ ↔ ZONA
    # -------------------------------------------------------
    print("🔍 Validando compatibilidad UPZ ↔ Zona...")

    tiene_codigo = "Codigo_UPZ" in df.columns
    tiene_nombre = "Nombre_UPZ" in df.columns
    tiene_zona = "Zona" in df.columns

    errores = []
    conflictos = []
    correcciones_count = 0

    if tiene_nombre and tiene_zona:
        for i, row in df.iterrows():
            upz = row.get("Nombre_UPZ", "")
            zona = row.get("Zona", "")
            
            upz_correcta, zona_correcta, error = validar_upz_zona(upz, zona, dicc_upz_zonas)
            
            if error:
                if "CONFLICTO" in error:
                    conflictos.append({
                        "Fila": i + 1,
                        "UPZ_Original": upz,
                        "Zona_Original": zona,
                        "Zona_Correcta": zona_correcta,
                        "Error": error
                    })
                    # CORREGIR automáticamente
                    df.at[i, "Zona"] = zona_correcta
                    df.at[i, "Zonas_Asignadas"] = zona_correcta
                    correcciones_count += 1
                else:
                    errores.append({
                        "Fila": i + 1,
                        "UPZ": upz,
                        "Zona": zona,
                        "Error": error
                    })
        
        # Exportar conflictos detectados
        if len(conflictos) > 0:
            df_conflictos = pd.DataFrame(conflictos)
            df_conflictos.to_csv("conflictos_upz_zona.csv", index=False, encoding="utf-8-sig")
            print(f"⚠ Se detectaron {len(conflictos)} conflictos UPZ-Zona. Archivo: conflictos_upz_zona.csv")
            print(f"✓ {correcciones_count} zonas corregidas automáticamente")
        
        # Exportar errores (UPZ vacías, etc.)
        if len(errores) > 0:
            df_err = pd.DataFrame(errores)
            df_err.to_csv("errores_upz.csv", index=False, encoding="utf-8-sig")
            print(f"⚠ Se detectaron {len(errores)} errores de UPZ. Archivo: errores_upz.csv")
        
        if len(conflictos) == 0 and len(errores) == 0:
            print("✓ Todas las UPZ y Zonas son compatibles")
    
    else:
        print("⚠ No hay columnas suficientes para validar UPZ ↔ Zona")

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