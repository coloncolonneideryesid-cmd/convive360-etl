import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import os

# ============================
#   CONFIGURACIÓN
# ============================
SHEET_NAME = "AGENDAMIENTO DE ACTIVIDADES EN TERRITORIO (CALENDAR POWER BI)"
SHEET_ID = "15DIXQnQfS9_gbYC4h3j5inIgEVEUJC79ceCc4uYv_ts"
HOJAS = [
    "Respuestas de formulario 1",
    "Respuestas de formulario 2"
]

# ============================
#   FUNCIÓN: CONECTAR A GOOGLE SHEETS
# ============================
def conectar_google_sheets():
    """
    Conecta con Google Sheets usando credenciales
    """
    print(">>> Conectando con Google Sheets...")
    
    # Intentar leer credenciales desde variable de entorno (GitHub Actions)
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    
    if creds_json:
        # Estamos en GitHub Actions
        print("✓ Credenciales obtenidas desde GitHub Secrets")
        creds_dict = json.loads(creds_json)
        
        # Guardar temporalmente para gspread
        with open('/tmp/credentials.json', 'w') as f:
            json.dump(creds_dict, f)
        creds_path = '/tmp/credentials.json'
    else:
        # Estamos en local (desarrollo)
        print("⚠ Credenciales no encontradas en entorno")
        print("💡 Para pruebas locales, crea 'google_credentials.json' en la raíz")
        
        # Buscar archivo local
        if os.path.exists('google_credentials.json'):
            creds_path = 'google_credentials.json'
            print("✓ Usando credenciales locales")
        else:
            raise FileNotFoundError(
                "❌ No se encontraron credenciales. "
                "Crea 'google_credentials.json' o configura GOOGLE_CREDENTIALS en GitHub Secrets"
            )
    
    # Autenticar
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    
    print("✓ Conexión establecida con Google Sheets")
    return client

# ============================
#   FUNCIÓN: LEER HOJAS
# ============================
def leer_hojas_google_sheets():
    """
    Lee las dos hojas del Google Sheet y las combina
    """
    print(">>> Leyendo datos desde Google Sheets...")
    
    # Conectar
    client = conectar_google_sheets()
    
    # Abrir el documento
    try:
        sheet = client.open(SHEET_NAME)
        print(f"✓ Documento '{SHEET_NAME}' abierto correctamente")
    except Exception as e:
        print(f"❌ Error al abrir el documento: {e}")
        raise
    
    # Mapeo de columnas común para todas las hojas
    MAPEO_COMUN = {
        "Marca temporal": "Marca_Temporal",
        "Dirección de correo electrónico": "Email_Responsable",
        "1. Nombre de la actividad": "Nombre_Actividad",
        "Nombre de la Actividad": "Nombre_Actividad",  # ← AGREGAR ESTA
        "2. Descripción de la actividad": "Descripcion_Actividad",
        "2. Descripción de la actividad  ": "Descripcion_Actividad",
        "Descripción de la Actividad": "Descripcion_Actividad",  # ← AGREGAR ESTA
        "4. Responsable de la actividad": "Responsable_Principal",
        "4. Responsable de la actividad*": "Responsable_Principal",
        "3. Responsables de la actividad": "Responsables_Actividad",
        "Responsables de la actividad": "Responsables_Actividad",
        "5. Número del responsable": "Numero_Responsable",
        "7. Dirección donde se realiza la actividad": "Direccion_Actividad",
        "7. Dirección donde se realiza la actividad  ": "Direccion_Actividad",
        "Dirección donde se realiza la actividad": "Direccion_Actividad",
        "10. Fecha de la actividad": "Fecha_Actividad",
        "Fecha de Actividad": "Fecha_Actividad",
        "11. Hora de inicio": "Hora_Inicio",
        "Hora de Inicio de Actividad": "Hora_Inicio",
        "8. UPZ a la Que Pertenece la Actividad": "Nombre_UPZ",
        "UPZ a la Que Pertenece la Actividad": "Nombre_UPZ",
        "9. Zona a la que Pertenece la Actividad": "Zona",
        "Zona a la que Pertenece la Actividad": "Zona",
        "5. Enfoque de la actividad": "Enfoque",
        "Enfoque de la actividad*": "Enfoque",
        "Enfoque Estratégico": "Enfoque",
        "6. Estrategia a impactar": "Estrategia_Impactar",
        "Estrategia de Impacto": "Estrategia_Impactar",
        "6.1. Líneas Estratégicas de Seguridad": "Linea_Seguridad",
        "6.2. Líneas Estratégicas de Convivencia": "Linea_Convivencia",
        "Líneas Estratégicas de Convivencia": "Linea_Convivencia",
        "6.3. Líneas Estratégicas de Justicia": "Linea_Justicia",
        "4. Con quien va articular": "Con_Quien_Articula",
        "Con quién va a articular": "Con_Quien_Articula",
        "¿Deseas recibir un correo de confirmación?": "Confirmacion_Email",
        "¿Deseas recibir un correo de confirmación?  ": "Confirmacion_Email",
        "12. ¿Deseas recibir un correo de confirmación?": "Confirmacion_Email",
        "12. ¿Deseas recibir un correo de confirmación?  ": "Confirmacion_Email",
    }
    
    # Leer cada hoja
    dataframes = []
    
    for nombre_hoja in HOJAS:
        try:
            print(f"  → Leyendo '{nombre_hoja}'...")
            worksheet = sheet.worksheet(nombre_hoja)
            
            # Obtener todos los datos
            datos = worksheet.get_all_records()
            
            if datos:
                df = pd.DataFrame(datos)
                
                # RENOMBRAR COLUMNAS ANTES DE COMBINAR
                df = df.rename(columns=MAPEO_COMUN)
                df.columns = df.columns.str.strip()  # Eliminar espacios
                
                # COMBINAR COLUMNAS DUPLICADAS (tomar el primer valor no nulo)
                # Esto maneja casos donde múltiples columnas originales mapean al mismo nombre
                cols_duplicadas = df.columns[df.columns.duplicated()].unique()
                if len(cols_duplicadas) > 0:
                    print(f"    ⚠ Combinando {len(cols_duplicadas)} columnas duplicadas")
                    for col in cols_duplicadas:
                        # Obtener todas las columnas con este nombre
                        cols_con_mismo_nombre = df.columns == col
                        # Combinar tomando el primer valor no nulo
                        df[col] = df.loc[:, cols_con_mismo_nombre].bfill(axis=1).iloc[:, 0]
                    # Eliminar columnas duplicadas
                    df = df.loc[:, ~df.columns.duplicated()]
                
                df['Hoja_Origen'] = nombre_hoja  # Marcar de dónde viene
                dataframes.append(df)
                print(f"    ✓ {len(df)} registros leídos")
            else:
                print(f"    ⚠ Hoja vacía")
                
        except Exception as e:
            print(f"    ❌ Error al leer '{nombre_hoja}': {e}")
    
    # Combinar todas las hojas
    if dataframes:
        df_final = pd.concat(dataframes, ignore_index=True)
        print(f"\n✓ Total de registros combinados: {len(df_final)}")
        
        # FUSIONAR COLUMNAS DUPLICADAS (ej: Nombre_Actividad + Nombre_Actividad.1)
        print(">>> Fusionando columnas duplicadas...")
        columnas_base = [col for col in df_final.columns if not col.endswith('.1')]
        
        fusiones = 0
        for col_base in columnas_base:
            col_duplicada = f"{col_base}.1"
            if col_duplicada in df_final.columns:
                print(f"  → Fusionando '{col_base}' con '{col_duplicada}'")
                # Combinar: si col_base está vacía, usar col_duplicada
                df_final[col_base] = df_final[col_base].fillna(df_final[col_duplicada])
                # Eliminar columna duplicada
                df_final = df_final.drop(columns=[col_duplicada])
                fusiones += 1
        
        if fusiones > 0:
            print(f"✓ {fusiones} columnas fusionadas correctamente")
        print(f"✓ Columnas finales: {len(df_final.columns)}")
        
        return df_final
    else:
        raise ValueError("❌ No se pudo leer ninguna hoja del documento")

# ============================
#   FUNCIÓN: GUARDAR ARCHIVO
# ============================
def guardar_archivo_fact(df):
    """
    Guarda el DataFrame como CSV para el pipeline
    """
    print(">>> Guardando archivo fact_actividades.csv...")
    
    # Guardar
    df.to_csv('fact_actividades.csv', index=False, encoding='utf-8-sig')
    
    print(f"✓ Archivo guardado correctamente ({len(df)} registros)")
    print(f"✓ Columnas: {', '.join(df.columns[:5])}... (+ {len(df.columns)-5} más)")

# ============================
#   FUNCIÓN PRINCIPAL
# ============================
def importar_desde_sheets():
    """
    Función principal que ejecuta todo el proceso
    """
    try:
        # Leer datos
        df = leer_hojas_google_sheets()
        
        # Guardar
        guardar_archivo_fact(df)
        
        print("\n🎉 Importación desde Google Sheets completada exitosamente")
        return True
        
    except Exception as e:
        print(f"\n❌ Error en la importación: {e}")
        return False

# ============================
#   EJECUCIÓN DIRECTA
# ============================
if __name__ == "__main__":
    importar_desde_sheets()