import pandas as pd
import os

def asegurar_directorio():
    if not os.path.exists("dimensiones"):
        os.makedirs("dimensiones")

# ---------------------------------------------------------
# DIMENSIÓN FECHA
# ---------------------------------------------------------
def generar_dim_fecha(df):
    df_fecha = df.copy()

    df_fecha["Fecha de la actividad"] = pd.to_datetime(df_fecha["Fecha de la actividad"], errors="coerce")
    
    df_fecha["anio"] = df_fecha["Fecha de la actividad"].dt.year
    df_fecha["mes"] = df_fecha["Fecha de la actividad"].dt.month
    df_fecha["dia"] = df_fecha["Fecha de la actividad"].dt.day
    df_fecha["mes_texto"] = df_fecha["Fecha de la actividad"].dt.strftime("%B")
    df_fecha["dia_semana"] = df_fecha["Fecha de la actividad"].dt.day_name()

    df_dim = df_fecha[["Fecha de la actividad", "anio", "mes", "dia", "mes_texto", "dia_semana"]].drop_duplicates()
    df_dim = df_dim.rename(columns={"Fecha de la actividad": "id_fecha"})

    df_dim.to_csv("dimensiones/dim_fecha.csv", index=False, encoding="utf-8-sig")
    print("✓ dim_fecha generada")

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

    generar_dim_fecha(df)
    generar_dim_lugar(df)
    generar_dim_responsable(df)
    generar_dim_upz(df)
    generar_dim_zona(df)
    generar_dim_actividad(df)

    print("\n🎯 Todas las dimensiones fueron generadas correctamente\n")


if __name__ == "__main__":
    generar_todas_las_dimensiones()
