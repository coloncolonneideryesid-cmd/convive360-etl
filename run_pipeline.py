#!/usr/bin/env python3
"""
Pipeline ETL para Convive360 - Alcaldía Local de San Cristóbal
Extrae datos de Google Sheets, los transforma y genera archivos CSV para Power BI
Versión MEJORADA: Genera fact_actividades_enriquecido.csv con ID_Actividad
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

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_log.txt'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Variables de entorno (se configuran en GitHub Secrets)
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
    '1. Esta actividad se enmarca en:': 'Enmarca_En',
    '2. Nombre de la actividad': 'Nombre_Actividad',
    '3. Descripción de la actividad': 'Descripcion_Actividad',
    '4. Responsables de la actividad': 'Responsable_Principal',
    '5. Con quien va articular': 'Articulacion',
    '6. Responsable de la actividad': 'Responsable_Actividad',
    '5. Enfoque de la actividad': 'Enfoque_Actividad',
    '6. Estrategia a impactar': 'Estrategia_Impactar',
    '6.1. Líneas Estratégicas de Seguridad': 'Linea_Seguridad',
    '6.2. Líneas Estratégicas de Convivencia': 'Linea_Convivencia',
    '6.3. Líneas Estratégicas de Justicia': 'Linea_Justicia',
    '8. UPZ a la Que Pertenece la Actividad': 'UPZ',
    'BARRIOS DE LA UPZ 32 - San Blas': 'Barrios_UPZ32',
    'BARRIOS DE LA UPZ 33 - Sosiego': 'Barrios_UPZ33',
    'BARRIOS DE LA UPZ 34 - 20 de Julio': 'Barrios_UPZ34',
    'BARRIOS DE LA UPZ 51 - Los Libertadores': 'Barrios_UPZ51',
    'BARRIOS DE LA UPZ 50 - La Gloria': 'Barrios_UPZ50',
    '9. Zona a la que Pertenece la Actividad': 'Zona',
    '7. Dirección donde se realiza la actividad': 'Direccion_Actividad',
    '10. Fecha de la actividad': 'Fecha_Actividad',
    '11. Hora de inicio': 'Hora_Inicio',
    '12. ¿Deseas recibir un correo de confirmación?': 'Recibir_Correo',
    'Estado': 'Estado',
    'ID del evento': 'ID_Evento',
    'Quien rechazó': 'Quien_Rechazo',
    'Fecha de cancelación': 'Fecha_Cancelacion'
}

# ========================================
# FUNCIONES DE AUTENTICACIÓN
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
# FUNCIONES DE EXTRACCIÓN
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
        
        # Convertir a DataFrame
        df = pd.DataFrame(values[1:], columns=values[0])
        
        logger.info(f"✅ Extraídos {len(df)} registros de {sheet_name}")
        return df
        
    except Exception as e:
        logger.error(f"❌ Error al extraer datos de {sheet_name}: {e}")
        raise

# ========================================
# FUNCIONES DE TRANSFORMACIÓN
# ========================================

def limpiar_datos(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia y normaliza los datos extraídos"""
    try:
        logger.info("🧹 Iniciando limpieza de datos")
        
        # Eliminar filas completamente vacías
        df = df.dropna(how='all')
        logger.info(f"  ✓ Filas vacías eliminadas")
        
        # Renombrar columnas según el mapeo
        df = df.rename(columns=COLUMN_MAPPING)
        logger.info(f"  ✓ Columnas renombradas")
        
        # Convertir columnas de fecha
        if 'Marca_Temporal' in df.columns:
            df['Marca_Temporal'] = pd.to_datetime(df['Marca_Temporal'], errors='coerce')
        
        if 'Fecha_Actividad' in df.columns:
            df['Fecha_Actividad'] = pd.to_datetime(df['Fecha_Actividad'], errors='coerce')
        
        if 'Fecha_Cancelacion' in df.columns:
            df['Fecha_Cancelacion'] = pd.to_datetime(df['Fecha_Cancelacion'], errors='coerce')
        
        logger.info(f"  ✓ Fechas convertidas")
        
        # Consolidar columna de barrio
        df['Barrio_Extraido'] = ''
        
        if 'UPZ' in df.columns:
            for idx, row in df.iterrows():
                upz = str(row.get('UPZ', ''))
                
                if '32' in upz and 'Barrios_UPZ32' in df.columns:
                    df.at[idx, 'Barrio_Extraido'] = row.get('Barrios_UPZ32', '')
                elif '33' in upz and 'Barrios_UPZ33' in df.columns:
                    df.at[idx, 'Barrio_Extraido'] = row.get('Barrios_UPZ33', '')
                elif '34' in upz and 'Barrios_UPZ34' in df.columns:
                    df.at[idx, 'Barrio_Extraido'] = row.get('Barrios_UPZ34', '')
                elif '51' in upz and 'Barrios_UPZ51' in df.columns:
                    df.at[idx, 'Barrio_Extraido'] = row.get('Barrios_UPZ51', '')
                elif '50' in upz and 'Barrios_UPZ50' in df.columns:
                    df.at[idx, 'Barrio_Extraido'] = row.get('Barrios_UPZ50', '')
        
        logger.info(f"  ✓ Barrios consolidados")
        
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
    campos = f"{row.get('Nombre_Actividad', '')}{row.get('Fecha_Actividad', '')}{row.get('Hora_Inicio', '')}{row.get('Direccion_Actividad', '')}"
    return hashlib.md5(campos.encode()).hexdigest()[:12].upper()

def crear_dimensiones(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Crea tablas de dimensiones a partir del DataFrame principal"""
    try:
        logger.info("🔨 Creando tablas de dimensiones")
        dimensiones = {}
        
        # Dimensión UPZ
        if 'UPZ' in df.columns:
            dim_upz = df[['UPZ']].dropna().drop_duplicates().reset_index(drop=True)
            dim_upz.insert(0, 'upz_id', range(1, len(dim_upz) + 1))
            dimensiones['dim_upz'] = dim_upz
            logger.info(f"  ✓ dim_upz creada: {len(dim_upz)} registros")
        
        # Dimensión Zona
        if 'Zona' in df.columns:
            dim_zona = df[['Zona']].dropna().drop_duplicates().reset_index(drop=True)
            dim_zona.insert(0, 'zona_id', range(1, len(dim_zona) + 1))
            dimensiones['dim_zona'] = dim_zona
            logger.info(f"  ✓ dim_zona creada: {len(dim_zona)} registros")
        
        # Dimensión Estrategia
        if 'Estrategia_Impactar' in df.columns:
            dim_estrategia = df[['Estrategia_Impactar']].dropna().drop_duplicates().reset_index(drop=True)
            dim_estrategia.insert(0, 'estrategia_id', range(1, len(dim_estrategia) + 1))
            dimensiones['dim_estrategia'] = dim_estrategia
            logger.info(f"  ✓ dim_estrategia creada: {len(dim_estrategia)} registros")
        
        # Dimensión Enfoque
        if 'Enfoque_Actividad' in df.columns:
            dim_enfoque = df[['Enfoque_Actividad']].dropna().drop_duplicates().reset_index(drop=True)
            dim_enfoque.insert(0, 'enfoque_id', range(1, len(dim_enfoque) + 1))
            dimensiones['dim_enfoque'] = dim_enfoque
            logger.info(f"  ✓ dim_enfoque creada: {len(dim_enfoque)} registros")
        
        # Dimensión Estado
        if 'Estado' in df.columns:
            dim_estado = df[['Estado']].dropna().drop_duplicates().reset_index(drop=True)
            dim_estado.insert(0, 'estado_id', range(1, len(dim_estado) + 1))
            dimensiones['dim_estado'] = dim_estado
            logger.info(f"  ✓ dim_estado creada: {len(dim_estado)} registros")
        
        # Dimensión Barrio
        if 'Barrio_Extraido' in df.columns:
            dim_barrio = df[['Barrio_Extraido', 'UPZ']].dropna().drop_duplicates().reset_index(drop=True)
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
        
        # Hacer joins con las dimensiones
        if 'dim_upz' in dimensiones and 'UPZ' in fact.columns:
            fact = fact.merge(
                dimensiones['dim_upz'],
                left_on='UPZ',
                right_on='UPZ',
                how='left'
            )
            logger.info("  ✓ Join con dim_upz")
        
        if 'dim_zona' in dimensiones and 'Zona' in fact.columns:
            fact = fact.merge(
                dimensiones['dim_zona'],
                left_on='Zona',
                right_on='Zona',
                how='left'
            )
            logger.info("  ✓ Join con dim_zona")
        
        if 'dim_estrategia' in dimensiones and 'Estrategia_Impactar' in fact.columns:
            fact = fact.merge(
                dimensiones['dim_estrategia'],
                left_on='Estrategia_Impactar',
                right_on='Estrategia_Impactar',
                how='left'
            )
            logger.info("  ✓ Join con dim_estrategia")
        
        if 'dim_enfoque' in dimensiones and 'Enfoque_Actividad' in fact.columns:
            fact = fact.merge(
                dimensiones['dim_enfoque'],
                left_on='Enfoque_Actividad',
                right_on='Enfoque_Actividad',
                how='left'
            )
            logger.info("  ✓ Join con dim_enfoque")
        
        if 'dim_estado' in dimensiones and 'Estado' in fact.columns:
            fact = fact.merge(
                dimensiones['dim_estado'],
                left_on='Estado',
                right_on='Estado',
                how='left'
            )
            logger.info("  ✓ Join con dim_estado")
        
        if 'dim_barrio' in dimensiones and 'Barrio_Extraido' in fact.columns:
            fact = fact.merge(
                dimensiones['dim_barrio'],
                left_on=['Barrio_Extraido', 'UPZ'],
                right_on=['Barrio_Extraido', 'UPZ'],
                how='left'
            )
            logger.info("  ✓ Join con dim_barrio")
        
        logger.info(f"✅ Tabla de hechos creada: {len(fact)} registros")
        return fact
        
    except Exception as e:
        logger.error(f"❌ Error al crear tabla de hechos: {e}")
        raise

# ========================================
# FUNCIONES DE CARGA
# ========================================

def guardar_archivos(fact: pd.DataFrame, dimensiones: Dict[str, pd.DataFrame]) -> None:
    """Guarda los archivos CSV en disco"""
    try:
        logger.info("💾 Guardando archivos CSV")
        
        # Guardar tabla de hechos COMPLETA
        fact.to_csv('fact_actividades.csv', index=False, encoding='utf-8-sig')
        logger.info("  ✓ fact_actividades.csv guardado")
        
        # Crear versión LIMPIA (sin columnas duplicadas _x, _y)
        columnas_mantener = [col for col in fact.columns 
                           if not col.endswith('_x') and not col.endswith('_y')]
        fact_limpio = fact[columnas_mantener]
        fact_limpio.to_csv('fact_actividades_limpio.csv', index=False, encoding='utf-8-sig')
        logger.info("  ✓ fact_actividades_limpio.csv guardado")
        
        # ⭐ NUEVO: Guardar versión ENRIQUECIDA (con delimitador ; para Power BI)
        fact_enriquecido = fact_limpio.copy()
        fact_enriquecido.to_csv('fact_actividades_enriquecido.csv', index=False, encoding='utf-8-sig', sep=';')
        logger.info("  ✓ fact_actividades_enriquecido.csv guardado")
        
        # Guardar dimensiones
        os.makedirs('dimensiones', exist_ok=True)
        
        # También guardar fact_actividades_enriquecido en dimensiones/ (para compatibilidad)
        fact_enriquecido.to_csv('dimensiones/fact_actividades_enriquecido.csv', index=False, encoding='utf-8-sig', sep=';')
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
        
        # 2. EXTRAER DATOS
        df1 = extraer_datos(service, SHEET_NAME_1)
        df2 = extraer_datos(service, SHEET_NAME_2)
        
        # 3. COMBINAR DATOS
        logger.info("🔗 Combinando datos de ambas hojas")
        df = pd.concat([df1, df2], ignore_index=True)
        logger.info(f"✅ Datos combinados: {len(df)} registros totales")
        logger.info("")
        
        # 4. LIMPIAR DATOS
        df_limpio = limpiar_datos(df)
        logger.info("")
        
        # 4.5. GENERAR ID_ACTIVIDAD (⭐ CRÍTICO PARA POWER BI)
        logger.info("🔑 Generando ID_Actividad único")
        df_limpio['ID_Actividad'] = df_limpio.apply(generar_id_actividad, axis=1)
        logger.info(f"  ✓ ID_Actividad generado para {len(df_limpio)} registros")
        logger.info("")
        
        # 5. CREAR DIMENSIONES
        dimensiones = crear_dimensiones(df_limpio)
        logger.info("")
        
        # 6. CREAR TABLA DE HECHOS
        fact = crear_tabla_hechos(df_limpio, dimensiones)
        logger.info("")
        
        # 7. GUARDAR ARCHIVOS (incluye fact_actividades_enriquecido.csv)
        guardar_archivos(fact, dimensiones)
        logger.info("")
        
        # Resumen final
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
        logger.info(f"   • {len(dimensiones)} dimensiones")
        logger.info("=" * 60)
        
        return 0
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error("💥 ERROR EN EL PIPELINE ETL")
        logger.error("=" * 60)
        logger.error(f"Error: {str(e)}")
        logger.error("=" * 60)
        
        return 1

if __name__ == "__main__":
    sys.exit(main())
