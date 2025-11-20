# run_pipeline.py
# Orquestador principal de todo el ETL Convive360°

import os
import json
from datetime import datetime

# 1. Cargar configuración
def cargar_config():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)

# 2. Ejecutar limpieza principal
def ejecutar_limpieza():
    print(">>> Ejecutando limpieza con diccionario...")
    os.system("python limpiar_agendamiento_con_diccionario.py")

# 3. Guardar resumen de ejecución
def guardar_resumen():
    resumen = {
        "ejecutado_en": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "estado": "OK",
        "mensaje": "Pipeline ejecutado correctamente."
    }
    with open("resumen_pipeline.json", "w", encoding="utf-8") as f:
        json.dump(resumen, f, indent=4, ensure_ascii=False)

# 4. Orquestación
def main():
    print("=== Iniciando pipeline Convive360° ===")

    config = cargar_config()
    print("Configuración cargada:", config)

    ejecutar_limpieza()
    guardar_resumen()

    print("=== Pipeline completado exitosamente ===")

if __name__ == "__main__":
    main()
