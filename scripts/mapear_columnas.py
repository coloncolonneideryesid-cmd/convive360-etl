import pandas as pd

# ============================
#   MAPEO DE COLUMNAS
# ============================
# Diccionario que traduce nombres del Google Sheet a nombres estándar del pipeline

MAPEO_COLUMNAS = {
    # FECHA
    "10. Fecha de la actividad": "Fecha_Actividad",
    "Fecha de Actividad": "Fecha_Actividad",
    "Fecha_Actividad.1": "Fecha_Actividad",  # ← AGREGAR ESTA
    "Marca temporal": "Marca_Temporal",
    
    # HORA
    "11. Hora de inicio": "Hora_Inicio",
    "Hora de Inicio de Actividad": "Hora_Inicio",
    "Hora_Inicio.1": "Hora_Inicio",  # ← AGREGAR ESTA
    
    # NOMBRE Y DESCRIPCIÓN
    "1. Nombre de la actividad": "Nombre_Actividad",
    "Nombre de la Actividad": "Nombre_Actividad",
    "Nombre_Actividad.1": "Nombre_Actividad",  # ← AGREGAR ESTA
    "2. Descripción de la actividad": "Descripcion_Actividad",
    "2. Descripción de la actividad  ": "Descripcion_Actividad",
    "Descripción de la Actividad": "Descripcion_Actividad",
    "Descripcion_Actividad.1": "Descripcion_Actividad",  # ← AGREGAR ESTA
    
    # RESPONSABLE
    "4. Responsable de la actividad": "Responsable_Principal",
    "4. Responsable de la actividad*": "Responsable_Principal",
    "Responsable_Principal.1": "Responsable_Principal",  # ← AGREGAR ESTA
    "3. Responsables de la actividad": "Responsables_Actividad",
    "Responsables de la actividad": "Responsables_Actividad",
    "Responsables_Actividad.1": "Responsables_Actividad",  # ← AGREGAR ESTA
    "5. Número del responsable": "Numero_Responsable",
    
    # UBICACIÓN
    "7. Dirección donde se realiza la actividad": "Direccion_Actividad",
    "7. Dirección donde se realiza la actividad  ": "Direccion_Actividad",
    "Dirección donde se realiza la actividad": "Direccion_Actividad",
    "Direccion_Actividad.1": "Direccion_Actividad",  # ← AGREGAR ESTA
    
    # UPZ Y ZONA
    "8. UPZ a la Que Pertenece la Actividad": "Nombre_UPZ",
    "UPZ a la Que Pertenece la Actividad": "Nombre_UPZ",
    "Nombre_UPZ.1": "Nombre_UPZ",  # ← AGREGAR ESTA
    "9. Zona a la que Pertenece la Actividad": "Zona",
    "Zona a la que Pertenece la Actividad": "Zona",
    "Zona.1": "Zona",  # ← AGREGAR ESTA
    
    # ESTRATEGIA
    "5. Enfoque de la actividad": "Enfoque",
    "Enfoque de la actividad*": "Enfoque",
    "Enfoque Estratégico": "Enfoque",
    "Enfoque.1": "Enfoque",  # ← AGREGAR ESTA
    "Enfoque.2": "Enfoque",  # ← AGREGAR ESTA
    "6. Estrategia a impactar": "Estrategia_Impactar",
    "Estrategia de Impacto": "Estrategia_Impactar",
    "Estrategia_Impactar.1": "Estrategia_Impactar",  # ← AGREGAR ESTA
    
    # LÍNEAS ESTRATÉGICAS
    "6.1. Líneas Estratégicas de Seguridad": "Linea_Seguridad",
    "6.2. Líneas Estratégicas de Convivencia": "Linea_Convivencia",
    "Líneas Estratégicas de Convivencia": "Linea_Convivencia",
    "Linea_Convivencia.1": "Linea_Convivencia",  # ← AGREGAR ESTA
    "6.3. Líneas Estratégicas de Justicia": "Linea_Justicia",
    
    # ARTICULACIÓN
    "4. Con quien va articular": "Con_Quien_Articula",
    "Con quién va a articular": "Con_Quien_Articula",
    "Con_Quien_Articula.1": "Con_Quien_Articula",  # ← AGREGAR ESTA
    
    # OTROS
    "Dirección de correo electrónico": "Email_Responsable",
    "¿Deseas recibir un correo de confirmación?": "Confirmacion_Email",
    "¿Deseas recibir un correo de confirmación?  ": "Confirmacion_Email",
    "12. ¿Deseas recibir un correo de confirmación?": "Confirmacion_Email",
    "12. ¿Deseas recibir un correo de confirmación?  ": "Confirmacion_Email",
    "Confirmacion_Email.1": "Confirmacion_Email",  # ← AGREGAR ESTA
    "Puntuación": "Puntuacion",
    "Hoja_Origen": "Hoja_Origen",
    "Zonas_Asignadas": "Zonas_Asignadas"
}

# ============================
#   FUNCIÓN PRINCIPAL
# ============================
def mapear_columnas_sheets():
    """
    Lee fact_actividades.csv y renombra las columnas según el mapeo
    """
    print(">>> Mapeando nombres de columnas...")
    
    # Leer archivo
    df = pd.read_csv("fact_actividades.csv")
    print(f"📊 Registros antes del mapeo: {len(df)}")
    print(f"📋 Columnas antes: {len(df.columns)}")
    
    # Renombrar columnas usando el diccionario
    df_renamed = df.rename(columns=MAPEO_COLUMNAS)
    
    # Eliminar espacios en blanco de los nombres de columnas
    df_renamed.columns = df_renamed.columns.str.strip()
    
    # Mostrar cambios
    columnas_cambiadas = [
        (old, new) 
        for old, new in MAPEO_COLUMNAS.items() 
        if old in df.columns
    ]
    
    if columnas_cambiadas:
        print(f"✓ {len(columnas_cambiadas)} columnas renombradas:")
        for old, new in columnas_cambiadas[:5]:  # Mostrar solo primeras 5
            print(f"  • '{old}' → '{new}'")
        if len(columnas_cambiadas) > 5:
            print(f"  ... y {len(columnas_cambiadas) - 5} más")
    
    # Guardar archivo mapeado
    df_renamed.to_csv("fact_actividades.csv", index=False, encoding="utf-8-sig")
    
    print(f"✓ Archivo guardado con columnas estandarizadas")
    print(f"📋 Columnas después: {len(df_renamed.columns)}")
    
    return df_renamed

# ============================
#   EJECUCIÓN DIRECTA
# ============================
if __name__ == "__main__":
    mapear_columnas_sheets()