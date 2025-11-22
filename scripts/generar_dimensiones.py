import pandas as pd
import numpy as np
import os

# ============================
#  DICCIONARIOS EN ESPAÑOL
# ============================
dias_semana_es = {
    "Monday": "Lunes",
    "Tuesday": "Martes",
    "Wednesday": "Miércoles",
    "Thursday": "Jueves",
    "Friday": "Viernes",
    "Saturday": "Sábado",
    "Sunday": "Domingo",
}

meses_es = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

# ============================
#   CREAR CARPETA OUTPUT
# ============================
os.makedirs("dimensiones", exist_ok=True)

# ============================
#   FUNCIÓN: DIM FECHA
# ============================
def generar_dim_fecha(df):
    print(">>> Generando Dimensión FECHA...")
    
    if "Fecha_Actividad" not in df.columns:
        raise KeyError("❌ No se encontró la columna 'Fecha_Actividad' en fact_actividades_limpio.csv")
    
    df_fecha = df.copy()
    df_fecha["Fecha_Actividad"] = pd.to_datetime(df_fecha["Fecha_Actividad"], errors="coerce")
    df_fecha = df_fecha.dropna(subset=["Fecha_Actividad"])
    
    dim_fecha = pd.DataFrame()
    dim_fecha["ID_Fecha"] = df_fecha["Fecha_Actividad"].dt.strftime("%Y%m%d").astype(int)
    dim_fecha["Fecha"] = df_fecha["Fecha_Actividad"]
    dim_fecha["Año"] = df_fecha["Fecha_Actividad"].dt.year
    dim_fecha["Mes"] = df_fecha["Fecha_Actividad"].dt.month
    dim_fecha["Nombre_Mes"] = dim_fecha["Mes"].map(meses_es)
    dim_fecha["Día"] = df_fecha["Fecha_Actividad"].dt.day
    
    # ✅ SIN locale - Usar diccionario español manual
    dim_fecha["Día_Semana"] = (
        df_fecha["Fecha_Actividad"]
        .dt.day_name()  # Sin locale
        .map(dias_semana_es)
    )
    
    dim_fecha = dim_fecha.drop_duplicates()
    
    os.makedirs("dimensiones", exist_ok=True)
    dim_fecha.to_csv("dimensiones/dim_fecha.csv", index=False, encoding="utf-8-sig")
    print("✓ Dimensión FECHA generada correctamente")

# ============================
#   FUNCIÓN: DIM UBICACIÓN
# ============================
# ============================
#   FUNCIÓN: DIM UBICACIÓN
# ============================
def generar_dim_ubicacion(df):
    print(">>> Generando Dimensión UBICACIÓN...")
    
    cols = [
        "Direccion_Actividad", 
        "UPZ_Enriquecida", 
        "Zona_Enriquecida",
        "Barrio_Extraido"
    ]
    
    cols_existentes = [c for c in cols if c in df.columns]
    
    if not cols_existentes:
        print("⚠ No se encontraron columnas para dimensión ubicación")
        return
    
    dim = df[cols_existentes].drop_duplicates().reset_index(drop=True)
    dim["id_ubicacion"] = dim.index + 1
    col_order = ["id_ubicacion"] + cols_existentes
    dim = dim[col_order]
    
    os.makedirs("dimensiones", exist_ok=True)
    dim.to_csv("dimensiones/dim_ubicacion.csv", index=False, encoding="utf-8-sig")
    print("✓ dim_ubicacion.csv generado correctamente")

# ============================
#   FUNCIÓN: DIM ACTIVIDAD
# ============================
def generar_dim_actividad(df):
    print(">>> Generando Dimensión ACTIVIDAD...")
    
    cols = [
        "Nombre_Actividad",
        "Descripcion_Actividad",
        "Responsable_Principal",
        "Estrategia",
        "Lineas_Seguridad_Texto",
        "Lineas_Convivencia_Texto",
        "Lineas_Justicia_Texto",
        "Emoji_Actividad"
    ]
    
    cols_existentes = [c for c in cols if c in df.columns]
    
    if not cols_existentes:
        print("⚠ No se encontraron columnas para dimensión actividad")
        return
    
    dim = df[cols_existentes].drop_duplicates().reset_index(drop=True)
    dim["id_actividad"] = dim.index + 1
    col_order = ["id_actividad"] + cols_existentes
    dim = dim[col_order]
    
    os.makedirs("dimensiones", exist_ok=True)
    dim.to_csv("dimensiones/dim_actividad.csv", index=False, encoding="utf-8-sig")
    print("✓ dim_actividad.csv generado correctamente")

def generar_todas_las_dimensiones():
    """
    Función que se llama desde run_pipeline.py
    """
    print(">>> Generando dimensiones...")
    
    # Cargar archivo limpio generado en el paso anterior
    if not os.path.exists("fact_actividades_limpio.csv"):
        print("❌ No se encontró fact_actividades_limpio.csv")
        return
    
    df = pd.read_csv("fact_actividades_limpio.csv")
    print(f"📊 Registros cargados: {len(df)}")
    
    # Generar dimensiones
    generar_dim_fecha(df)
    generar_dim_ubicacion(df)
    generar_dim_actividad(df)
    
    print("✓ Todas las dimensiones generadas correctamente")

# ============================
#   EJECUCIÓN DIRECTA (OPCIONAL)
# ============================
if __name__ == "__main__":
    # Si ejecutas este archivo directamente (no desde run_pipeline.py)
    if os.path.exists("Reporte Actividades.xlsx"):
        df = pd.read_excel("Reporte Actividades.xlsx")
        
        generar_dim_fecha(df)
        generar_dim_ubicacion(df)
        generar_dim_actividad(df)
        
        print("\n🎉 ETL COMPLETO: Todas las dimensiones fueron generadas exitosamente.")
    else:
        print("❌ No se encontró Reporte Actividades.xlsx")