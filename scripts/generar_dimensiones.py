import pandas as pd
import os

def asegurar_directorio():
    if not os.path.exists("dimensiones"):
        os.makedirs("dimensiones")

# ---------------------------------------------------------
# DIMENSIÓN FECHA
# ---------------------------------------------------------
def generar_dim_fecha(df):
    print(">>> Generando Dimensión FECHA...")

    # La columna real en el CSV es Fecha_Actividad
    if "Fecha_Actividad" not in df.columns:
        raise KeyError("❌ No se encontró la columna 'Fecha_Actividad' en fact_actividades_limpio.csv")

    df_fecha = df.copy()

    # Convertimos a datetime
    df_fecha["Fecha_Actividad"] = pd.to_datetime(df_fecha["Fecha_Actividad"], errors="coerce")

    # Eliminamos nulos
    df_fecha = df_fecha.dropna(subset=["Fecha_Actividad"])

    # Creamos la dimensión fecha
    dim_fecha = pd.DataFrame()
    dim_fecha["ID_Fecha"] = df_fecha["Fecha_Actividad"].dt.strftime("%Y%m%d").astype(int)
    dim_fecha["Fecha"] = df_fecha["Fecha_Actividad"]
    dim_fecha["Año"] = df_fecha["Fecha_Actividad"].dt.year
    dim_fecha["Mes"] = df_fecha["Fecha_Actividad"].dt.month
    dim_fecha["Día"] = df_fecha["Fecha_Actividad"].dt.day
    dim_fecha["Día_Semana"] = df_fecha["Fecha_Actividad"].dt.day_name(locale="es_ES")

    dim_fecha = dim_fecha.drop_duplicates()

    dim_fecha.to_csv("dim_fecha.csv", index=False, encoding="utf-8")
    print("✓ Dimensión FECHA generada correctamente")


# ---------------------------------------------------------
# DIMENSIÓN LUGAR
# ---------------------------------------------------------
def generar_dim_lugar(df):
    df_dim = df[[
        "Dirección donde se realiza la actividad",
        "Codigo_UPZ",
        "Nombre_UPZ",
        "Zonas_Asignadas"
    ]].drop_duplicates()

    df_dim = df_dim.rename(columns={
        "Dirección donde se realiza la actividad": "id_lugar"
    })

    df_dim.to_csv("dimensiones/dim_lugar.csv", index=False, encoding="utf-8-sig")
    print("✓ dim_lugar generada")

# ---------------------------------------------------------
# DIMENSIÓN RESPONSABLE
# ---------------------------------------------------------
def generar_dim_responsable(df):
    df_dim = df[[
        "Responsable de la actividad",
        "Número del responsable"
    ]].drop_duplicates()

    df_dim = df_dim.rename(columns={
        "Responsable de la actividad": "id_responsable"
    })

    df_dim.to_csv("dimensiones/dim_responsable.csv", index=False, encoding="utf-8-sig")
    print("✓ dim_responsable generada")

# ---------------------------------------------------------
# DIMENSIÓN UPZ
# ---------------------------------------------------------
def generar_dim_upz(df):
    df_dim = df[[
        "Codigo_UPZ",
        "Nombre_UPZ"
    ]].drop_duplicates()

    df_dim = df_dim.rename(columns={
        "Codigo_UPZ": "id_upz"
    })

    df_dim.to_csv("dimensiones/dim_upz.csv", index=False, encoding="utf-8-sig")
    print("✓ dim_upz generada")

# ---------------------------------------------------------
# DIMENSIÓN ZONA
# ---------------------------------------------------------
def generar_dim_zona(df):
    df_dim = df[["Zonas_Asignadas"]].drop_duplicates()
    df_dim = df_dim.rename(columns={"Zonas_Asignadas": "id_zona"})

    df_dim.to_csv("dimensiones/dim_zona.csv", index=False, encoding="utf-8-sig")
    print("✓ dim_zona generada")

# ---------------------------------------------------------
# DIMENSIÓN ACTIVIDAD
# ---------------------------------------------------------
def generar_dim_actividad(df):
    df_dim = df[[
        "Nombre de la actividad",
        "Descripción de la actividad",
        "Enfoque de la actividad",
        "Estrategia a impactar"
    ]].drop_duplicates()

    df_dim = df_dim.rename(columns={
        "Nombre de la actividad": "id_actividad"
    })

    df_dim.to_csv("dimensiones/dim_actividad.csv", index=False, encoding="utf-8-sig")
    print("✓ dim_actividad generada")

# ---------------------------------------------------------
# EJECUCIÓN PRINCIPAL
# ---------------------------------------------------------
def generar_todas_las_dimensiones():
    asegurar_directorio()

    df = pd.read_csv("fact_actividades_limpio.csv")

    generar_dim_fecha(df.rename(columns={"Fecha_Actividad": "Fecha_Actividad"}))
    generar_dim_lugar(df)
    generar_dim_responsable(df)
    generar_dim_upz(df)
    generar_dim_zona(df)
    generar_dim_actividad(df)

    print("\n🎯 Todas las dimensiones fueron generadas correctamente\n")


if __name__ == "__main__":
    generar_todas_las_dimensiones()
