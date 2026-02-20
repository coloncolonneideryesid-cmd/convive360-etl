#!/usr/bin/env python3
"""
Pipeline ETL para Convive360 - Alcaldía Local de San Cristóbal
Extrae datos de Google Sheets, los transforma y genera archivos CSV para Power BI
Versión CORREGIDA: Limpia cada hoja por separado antes del concat para evitar columnas duplicadas
"""

import os
import sys
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import logging
from datetime import datetime
from typing import Dict
import hashlib

# ========================================
# CONFIGURACIÓN
# ========================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_log.txt'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

SPREADSHEET_ID = os.getenv('SPREADSHEET_ID', '15DIXQnQfS9_gbYC4h3j5inIgEVEUJC79ceCc4uYv_ts')
SHEET_NAME_1 = os.getenv('SHEET_NAME_1', 'Respuestas de formulario 1')
SHEET_NAME_2 = os.getenv('SHEET_NAME_2', 'Respuestas de formulario 2')
CREDENTIALS_FILE = 'credentials.json'

# ========================================
# MAPEO DE COLUMNAS
# ========================================

COLUMN_MAPPING = {
    'Marca temporal': 'Marca_Temporal',
    'Dirección de correo electrónico': 'Email_Responsable',
    '2. Nombre de la actividad': 'Nombre_Actividad',
    'Nombre de la Actividad': 'Nombre_Actividad',
    '3. Descripción de la actividad': 'Descripcion_Actividad',
    'Descripción de la Actividad': 'Descripcion_Actividad',
    '4. Responsables de la actividad': 'Responsable_Principal',
    'Responsables de la actividad': 'Responsable_Principal',
    '5. Con quien va articular': 'Articulacion',
    'Con quién va a articular': 'Articulacion',
    '6. Responsable de la actividad': 'Responsable_Actividad',
    '4. Responsable de la actividad*': 'Responsable_Actividad',
    '5. Enfoque de la actividad': 'Enfoque_Actividad',
    'Enfoque de la actividad*': 'Enfoque_Actividad',
    'Enfoque Estratégico': 'Enfoque_Estrategico',
    '6. Estrategia a impactar': 'Estrategia_Impactar',
    'Estrategia de Impacto': 'Estrategia_Impactar',
    '1. Esta actividad se enmarca en:': 'Enmarca_En',
    '6.1. Líneas Estratégicas de Seguridad': 'Linea_Seguridad',
    'Líneas Estratégicas de Seguridad': 'Linea_Seguridad',
    '6.2. Líneas Estratégicas de Convivencia': 'Linea_Convivencia',
    'Líneas Estratégicas de Convivencia': 'Linea_Convivencia',
    '6.3. Líneas Estratégicas de Justicia': 'Linea_Justicia',
    'Líneas Estratégicas de Justicia': 'Linea_Justicia',
    '8. UPZ a la Que Pertenece la Actividad': 'UPZ',
    'UPZ a la Que Pertenece la Actividad': 'UPZ',
    'BARRIOS DE LA UPZ 32 - San Blas': 'Barrios_UPZ32',
    'BARRIOS DE LA UPZ 33 - Sosiego': 'Barrios_UPZ33',
    'BARRIOS DE LA UPZ 34 - 20 de Julio': 'Barrios_UPZ34',
    'BARRIOS DE LA UPZ 51 - Los Libertadores': 'Barrios_UPZ51',
    'BARRIOS DE LA UPZ 50 - La Gloria': 'Barrios_UPZ50',
    '9. Zona a la que Pertenece la Actividad': 'Zona',
    'Zona a la que Pertenece la Actividad': 'Zona',
    '7. Dirección donde se realiza la actividad': 'Direccion_Actividad',
    'Dirección donde se realiza la actividad': 'Direccion_Actividad',
    '10. Fecha de la actividad': 'Fecha_Actividad',
    'Fecha de Actividad': 'Fecha_Actividad',
    '11. Hora de inicio': 'Hora_Inicio',
    'Hora de Inicio de Actividad': 'Hora_Inicio',
    '12. ¿Deseas recibir un correo de confirmación?': 'Recibir_Correo',
    '¿Deseas recibir un correo de confirmación?': 'Recibir_Correo',
    'Estado': 'Estado',
    'ID del evento': 'ID_Evento',
    'Quien rechazó': 'Quien_Rechazo',
    'Fecha de cancelación': 'Fecha_Cancelacion'
}

# ========================================
# AUTENTICACIÓN
# ========================================

def autenticar_google_sheets() -> object:
    """Autentica con Google Sheets API usando service account"""
    try:
        if not os.path.exists(CREDENTIALS_FILE):
            raise FileNotFoundError(f"No se encontró el archivo de credenciales: {CREDENTIALS_FILE}")

        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE,
            scopes=[
                'https://www.googleapis.com/auth/spreadsheets.readonly',
                'https://www.googleapis.com/auth/drive.readonly'
            ]
        )
        service = build('sheets', 'v4', credentials=credentials)
        logger.info("✅ Autenticación exitosa con Google Sheets API")
        return service

    except Exception as e:
        logger.error(f"❌ Error en autenticación: {e}")
        raise

# ========================================
# EXTRACCIÓN
# ========================================

def extraer_datos(service: object, sheet_name: str) -> pd.DataFrame:
    """Extrae datos de una hoja específica del Google Sheets"""
    try:
        logger.info(f"📥 Extrayendo datos de: {sheet_name}")
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A:AB"
        ).execute()

        values = result.get('values', [])

        if not values:
            logger.warning(f"⚠️ No se encontraron datos en {sheet_name}")
            return pd.DataFrame()

        df = pd.DataFrame(values[1:], columns=values[0])
        logger.info(f"✅ Extraídos {len(df)} registros de {sheet_name}")
        return df

    except Exception as e:
        logger.error(f"❌ Error al extraer datos de {sheet_name}: {e}")
        raise

# ========================================
# TRANSFORMACIÓN
# ========================================

def normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """
    FIX PRINCIPAL: Renombra y consolida columnas en un DataFrame INDIVIDUAL
    antes del concat, evitando así columnas duplicadas tras la unión.
    """
    # Paso 1: Renombrar columnas según el mapeo, evitando crear duplicados
    nuevas_columnas = {}
    nombres_usados = {}  # nombre_final -> lista de columnas originales que mapean a él

    for col in df.columns:
        nombre_final = COLUMN_MAPPING.get(col, col)
        if nombre_final not in nombres_usados:
            nombres_usados[nombre_final] = []
        nombres_usados[nombre_final].append(col)

    for nombre_final, cols_originales in nombres_usados.items():
        if len(cols_originales) == 1:
            nuevas_columnas[cols_originales[0]] = nombre_final
        else:
            # Varias columnas originales mapean al mismo nombre final:
            # consolidar en la primera columna y eliminar las demás
            col_destino = cols_originales[0]
            for col_extra in cols_originales[1:]:
                df[col_destino] = df[col_destino].replace('', pd.NA).fillna(
                    df[col_extra].replace('', pd.NA)
                )
                df = df.drop(columns=[col_extra])
            nuevas_columnas[col_destino] = nombre_final

    df = df.rename(columns=nuevas_columnas)

    # Paso 2: Eliminar cualquier columna duplicada residual (mismo nombre)
    df = df.loc[:, ~df.columns.duplicated(keep='first')]

    return df


def limpiar_datos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y normaliza el DataFrame ya combinado.
    Asume que las columnas ya fueron normalizadas por hoja con normalizar_columnas().
    """
    try:
        logger.info("🧹 Iniciando limpieza de datos")

        # Eliminar filas completamente vacías
        df = df.dropna(how='all')
        logger.info("  ✓ Filas vacías eliminadas")

        # Eliminar columnas duplicadas residuales por si acaso
        df = df.loc[:, ~df.columns.duplicated(keep='first')]
        logger.info("  ✓ Columnas duplicadas eliminadas")

        # Convertir columnas de fecha
        for col_fecha in ['Marca_Temporal', 'Fecha_Actividad', 'Fecha_Cancelacion']:
            if col_fecha in df.columns:
                df[col_fecha] = pd.to_datetime(df[col_fecha], errors='coerce')
        logger.info("  ✓ Fechas convertidas")

        # Consolidar columna de barrio
        df['Barrio_Extraido'] = ''
        if 'UPZ' in df.columns:
            upz_barrio_map = {
                '32': 'Barrios_UPZ32',
                '33': 'Barrios_UPZ33',
                '34': 'Barrios_UPZ34',
                '51': 'Barrios_UPZ51',
                '50': 'Barrios_UPZ50',
            }
            for idx, row in df.iterrows():
                upz = str(row.get('UPZ', ''))
                for num, col_barrio in upz_barrio_map.items():
                    if num in upz and col_barrio in df.columns:
                        df.at[idx, 'Barrio_Extraido'] = row.get(col_barrio, '')
                        break
        logger.info("  ✓ Barrios consolidados")

        # Eliminar duplicados
        registros_antes = len(df)
        df = df.drop_duplicates()
        duplicados_eliminados = registros_antes - len(df)
        if duplicados_eliminados > 0:
            logger.info(f"  ✓ {duplicados_eliminados} duplicados eliminados")

        logger.info(f"✅ Limpieza completada: {len(df)} registros limpios")
        return df

    except Exception as e:
        logger.error(f"❌ Error al limpiar datos: {e}")
        raise


def generar_id_actividad(row) -> str:
    """Genera un ID único para cada actividad usando hash MD5"""
    campos = (
        f"{row.get('Marca_Temporal', '')}"
        f"{row.get('Nombre_Actividad', '')}"
        f"{row.get('Fecha_Actividad', '')}"
        f"{row.get('Hora_Inicio', '')}"
        f"{row.get('Direccion_Actividad', '')}"
        f"{row.get('Email_Responsable', '')}"
    )
    return hashlib.md5(campos.encode()).hexdigest()[:16].upper()


def crear_dimensiones(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Crea tablas de dimensiones a partir del DataFrame principal"""
    try:
        logger.info("🔨 Creando tablas de dimensiones")
        dimensiones = {}

        dims_simples = {
            'dim_upz': 'UPZ',
            'dim_zona': 'Zona',
            'dim_estrategia': 'Estrategia_Impactar',
            'dim_enfoque': 'Enfoque_Actividad',
            'dim_estado': 'Estado',
        }

        for nombre_dim, col in dims_simples.items():
            if col in df.columns:
                dim = df[[col]].dropna().drop_duplicates().reset_index(drop=True)
                dim.insert(0, f'{nombre_dim.replace("dim_", "")}_id', range(1, len(dim) + 1))
                dimensiones[nombre_dim] = dim
                logger.info(f"  ✓ {nombre_dim} creada: {len(dim)} registros")

        if 'Barrio_Extraido' in df.columns and 'UPZ' in df.columns:
            dim_barrio = (
                df[['Barrio_Extraido', 'UPZ']]
                .dropna()
                .drop_duplicates()
                .reset_index(drop=True)
            )
            dim_barrio.insert(0, 'barrio_id', range(1, len(dim_barrio) + 1))
            dimensiones['dim_barrio'] = dim_barrio
            logger.info(f"  ✓ dim_barrio creada: {len(dim_barrio)} registros")

        logger.info(f"✅ {len(dimensiones)} dimensiones creadas")
        return dimensiones

    except Exception as e:
        logger.error(f"❌ Error al crear dimensiones: {e}")
        raise


def crear_tabla_hechos(df: pd.DataFrame, dimensiones: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Crea la tabla de hechos con referencias a las dimensiones"""
    try:
        logger.info("📊 Creando tabla de hechos")
        fact = df.copy()

        joins = [
            ('dim_upz',        'UPZ',                 'UPZ'),
            ('dim_zona',       'Zona',                'Zona'),
            ('dim_estrategia', 'Estrategia_Impactar', 'Estrategia_Impactar'),
            ('dim_enfoque',    'Enfoque_Actividad',   'Enfoque_Actividad'),
            ('dim_estado',     'Estado',              'Estado'),
        ]

        for nombre_dim, col_fact, col_dim in joins:
            if nombre_dim in dimensiones and col_fact in fact.columns:
                fact = fact.merge(
                    dimensiones[nombre_dim],
                    left_on=col_fact,
                    right_on=col_dim,
                    how='left'
                )
                logger.info(f"  ✓ Join con {nombre_dim}")

        if 'dim_barrio' in dimensiones and 'Barrio_Extraido' in fact.columns:
            fact = fact.merge(
                dimensiones['dim_barrio'],
                on=['Barrio_Extraido', 'UPZ'],
                how='left'
            )
            logger.info("  ✓ Join con dim_barrio")

        logger.info(f"✅ Tabla de hechos creada: {len(fact)} registros")
        return fact

    except Exception as e:
        logger.error(f"❌ Error al crear tabla de hechos: {e}")
        raise

# ========================================
# CARGA
# ========================================

def guardar_archivos(fact: pd.DataFrame, dimensiones: Dict[str, pd.DataFrame]) -> None:
    """Guarda los archivos CSV en disco"""
    try:
        logger.info("💾 Guardando archivos CSV")

        fact.to_csv('fact_actividades.csv', index=False, encoding='utf-8-sig')
        logger.info("  ✓ fact_actividades.csv guardado")

        # Versión sin columnas _x / _y generadas por merges
        columnas_mantener = [col for col in fact.columns if not col.endswith('_x') and not col.endswith('_y')]
        fact_limpio = fact[columnas_mantener]
        fact_limpio.to_csv('fact_actividades_limpio.csv', index=False, encoding='utf-8-sig')
        logger.info("  ✓ fact_actividades_limpio.csv guardado")

        # Versión enriquecida con separador ; para Power BI
        fact_limpio.to_csv('fact_actividades_enriquecido.csv', index=False, encoding='utf-8-sig', sep=';')
        logger.info("  ✓ fact_actividades_enriquecido.csv guardado")

        os.makedirs('dimensiones', exist_ok=True)
        fact_limpio.to_csv('dimensiones/fact_actividades_enriquecido.csv', index=False, encoding='utf-8-sig', sep=';')
        logger.info("  ✓ dimensiones/fact_actividades_enriquecido.csv guardado")

        for nombre, df_dim in dimensiones.items():
            filepath = f'dimensiones/{nombre}.csv'
            df_dim.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.info(f"  ✓ {filepath} guardado")

        logger.info("✅ Todos los archivos guardados correctamente")

    except Exception as e:
        logger.error(f"❌ Error al guardar archivos: {e}")
        raise

# ========================================
# FUNCIÓN PRINCIPAL
# ========================================

def main():
    """Función principal del pipeline ETL"""
    inicio = datetime.now()

    try:
        logger.info("=" * 60)
        logger.info("🚀 INICIANDO PIPELINE ETL CONVIVE360")
        logger.info("=" * 60)
        logger.info(f"Spreadsheet ID: {SPREADSHEET_ID}")
        logger.info(f"Hoja 1: {SHEET_NAME_1}")
        logger.info(f"Hoja 2: {SHEET_NAME_2}")
        logger.info("")

        # 1. AUTENTICAR
        service = autenticar_google_sheets()

        # 2. EXTRAER
        df1 = extraer_datos(service, SHEET_NAME_1)
        df2 = extraer_datos(service, SHEET_NAME_2)

        # 3. NORMALIZAR COLUMNAS POR SEPARADO ← FIX PRINCIPAL
        #    Esto garantiza que cada hoja llega al concat con columnas ya unificadas,
        #    evitando el error "cannot assemble with duplicate keys"
        logger.info("🔀 Normalizando columnas por hoja")
        df1 = normalizar_columnas(df1)
        df2 = normalizar_columnas(df2)
        logger.info("  ✓ Columnas normalizadas en ambas hojas")

        # 4. COMBINAR
        logger.info("🔗 Combinando datos de ambas hojas")
        df = pd.concat([df1, df2], ignore_index=True)

        # Eliminar columnas duplicadas que puedan aparecer tras el concat
        df = df.loc[:, ~df.columns.duplicated(keep='first')]
        logger.info(f"✅ Datos combinados: {len(df)} registros totales")
        logger.info("")

        # 5. LIMPIAR
        df_limpio = limpiar_datos(df)
        logger.info("")

        # 6. GENERAR ID_ACTIVIDAD
        logger.info("🔑 Generando ID_Actividad único")
        df_limpio['ID_Actividad'] = df_limpio.apply(generar_id_actividad, axis=1)
        logger.info(f"  ✓ ID_Actividad generado para {len(df_limpio)} registros")
        logger.info("")

        # 7. CREAR DIMENSIONES
        dimensiones = crear_dimensiones(df_limpio)
        logger.info("")

        # 8. CREAR TABLA DE HECHOS
        fact = crear_tabla_hechos(df_limpio, dimensiones)
        logger.info("")

        # 9. GUARDAR
        guardar_archivos(fact, dimensiones)
        logger.info("")

        duracion = (datetime.now() - inicio).total_seconds()
        logger.info("=" * 60)
        logger.info("🎉 PIPELINE ETL COMPLETADO EXITOSAMENTE")
        logger.info("=" * 60)
        logger.info(f"⏱️  Duración: {duracion:.2f} segundos")
        logger.info(f"📊 Registros procesados: {len(fact)}")
        logger.info(f"📁 Archivos generados:")
        logger.info(f"   • fact_actividades.csv")
        logger.info(f"   • fact_actividades_limpio.csv")
        logger.info(f"   • fact_actividades_enriquecido.csv ⭐")
        logger.info(f"   • {len(dimensiones)} dimensiones en dimensiones/")
        logger.info("=" * 60)
        return 0

    except Exception as e:
        logger.error("=" * 60)
        logger.error("💥 ERROR EN EL PIPELINE ETL")
        logger.error("=" * 60)
        logger.error(f"Error: {str(e)}") 
        import traceback
        logger.error(traceback.format_exc())
        logger.error("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
