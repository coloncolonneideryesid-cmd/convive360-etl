import pandas as pd
from datetime import datetime

def deduplicar_agendamiento(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplica actividades de agendamiento usando una llave compuesta inteligente.
    La llave evita que actividades idénticas aparezcan duplicadas.

    Llave de deduplicación:
    - Nombre de la actividad
    - Dirección donde se realiza la actividad
    - Fecha de la actividad
    - Hora de inicio

    Si existen duplicados:
    → Conserva la fila más nueva según 'Marca temporal'
    """

    columnas_clave = [
        "1. Nombre de la actividad",
        "7. Dirección donde se realiza la actividad",
        "10. Fecha de la actividad",
        "11. Hora de inicio"
    ]

    # Validación: asegurar que las columnas existen
    for col in columnas_clave + ["Marca temporal"]:
        if col not in df.columns:
            raise ValueError(f"Falta la columna requerida para deduplicar: {col}")

    # Marcar duplicados
    df["_llave"] = (
        df[columnas_clave]
        .astype(str)
        .apply(lambda x: "||".join(x), axis=1)
    )

    # Convertir marca temporal a datetime para ordenar
    df["Marca temporal"] = pd.to_datetime(df["Marca temporal"], errors="coerce")

    # Ordenar de más reciente → menos reciente
    df = df.sort_values(by="Marca temporal", ascending=False)

    # Eliminar duplicados dejando solo el más reciente
    df_sin_duplicados = df.drop_duplicates(subset="_llave", keep="first")

    # Limpiar columnas auxiliares
    df_sin_duplicados = df_sin_duplicados.drop(columns=["_llave"])

    return df_sin_duplicados


if __name__ == "__main__":
    # Modo de prueba manual
    print("🔍 Módulo de deduplicación listo para usarse dentro del pipeline.")
