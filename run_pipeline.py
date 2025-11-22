# run_pipeline.py
# Orquestador principal del ETL Convive360°

import os
import json
from datetime import datetime

# Importamos las funciones necesarias
from scripts.generar_dimensiones import generar_todas_las_dimensiones
from scripts.importar_desde_sheets import importar_desde_sheets
from scripts.mapear_columnas import mapear_columnas_sheets


# ---------------------------------------------------------
# 1. Cargar configuración
# ---------------------------------------------------------
def cargar_config():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)

# ---------------------------------------------------------
# 2. Cargar diccionario UPZ → ZONA
# ---------------------------------------------------------
def cargar_diccionario_upz_zonas():
    ruta = os.path.join("scripts", "diccionario_upz_zonas.json")
    if not os.path.exists(ruta):
        raise FileNotFoundError(f"❌ No se encontró el archivo {ruta}")
    with open(ruta, "r", encoding="utf-8") as f:
        print("✓ Diccionario UPZ-ZONA cargado correctamente")
        return json.load(f)

# ---------------------------------------------------------
# 3. Importar datos desde Google Sheets
# ---------------------------------------------------------
def importar_datos_sheets():
    print(">>> Importando datos desde Google Sheets...")
    exito = importar_desde_sheets()
    if not exito:
        raise Exception("❌ Error al importar desde Google Sheets")
    print("✓ Importación completada")

# ---------------------------------------------------------
# 4. Mapear nombres de columnas
# ---------------------------------------------------------
def mapear_columnas():
    print(">>> Mapeando nombres de columnas...")
    mapear_columnas_sheets()
    print("✓ Mapeo completado")

# ---------------------------------------------------------
# 5. Ejecutar limpieza principal
# ---------------------------------------------------------
def ejecutar_limpieza(diccionario):
    print(">>> Ejecutando limpieza con diccionario UPZ-ZONA...")
    os.system("python scripts/limpiar_agendamiento_con_diccionario.py")
    print("✓ Limpieza completada")

# ---------------------------------------------------------
# 6. Registrar ejecución del pipeline
# ---------------------------------------------------------
def registrar_log():
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("pipeline_log.txt", "a", encoding="utf-8") as f:
        f.write(f"Pipeline ejecutado: {fecha}\n")
    print(f"✓ Pipeline registrado en log ({fecha})")

# ---------------------------------------------------------
#        EJECUCIÓN PRINCIPAL
# ---------------------------------------------------------
if __name__ == "__main__":
    print("\n🚀 Iniciando ETL Convive360°...\n")

    # 1. Cargar config general
    config = cargar_config()

    # 2. Cargar diccionario UPZ-ZONA
    diccionario = cargar_diccionario_upz_zonas()

    # 3. Importar desde Google Sheets
    importar_datos_sheets()

    # 4. Mapear columnas a nombres estándar
    mapear_columnas()

    # 5. Ejecutar limpieza principal
    ejecutar_limpieza(diccionario)

    # 6. Generar dimensiones
    print(">>> Generando dimensiones...")
    generar_todas_las_dimensiones()
    print("✓ Dimensiones generadas")

    # 7. Registrar log
    registrar_log()

    print("\n🎉 Pipeline completado exitosamente\n")