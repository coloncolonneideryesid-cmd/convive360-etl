#!/usr/bin/env python3
"""
Pipeline ETL para Convive360 - Alcaldía Local de San Cristóbal
Versión CORREGIDA para Power BI:
  - Todos los CSV usan sep=';' y encoding='utf-8-sig'
  - Genera columnas con nombres exactos que espera el modelo Power BI
  - ID_Actividad garantizadamente único (incluye índice de fila)
  - Genera Zona_Enriquecida, UPZ_Enriquecida, Hoja_Origen, columna Estrategia
  - Genera Dim_Lineas_Estrategicas separada
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

SPREADSHEET_ID   = os.getenv('SPREADSHEET_ID', '15DIXQnQfS9_gbYC4h3j5inIgEVEUJC79ceCc4uYv_ts')
SHEET_NAME_1     = os.getenv('SHEET_NAME_1',   'Respuestas de formulario 1')
SHEET_NAME_2     = os.getenv('SHEET_NAME_2',   'Respuestas de formulario 2')
CREDENTIALS_FILE = 'credentials.json'

# Separador único para TODOS los CSV → Power BI lo leerá consistentemente
CSV_SEP = ';'
CSV_ENC = 'utf-8-sig'

# ========================================
# MAPEO DE COLUMNAS
# ========================================

COLUMN_MAPPING = {
    'Marca temporal':                                 'Marca_Temporal',
    'Dirección de correo electrónico':                'Email_Responsable',
    '2. Nombre de la actividad':                      'Nombre_Actividad',
    'Nombre de la Actividad':                         'Nombre_Actividad',
    '3. Descripción de la actividad':                 'Descripcion_Actividad',
    'Descripción de la Actividad':                    'Descripcion_Actividad',
    '4. Responsables de la actividad':                'Responsable_Principal',
    'Responsables de la actividad':                   'Responsable_Principal',
    '5. Con quien va articular':                      'Articulacion',
    'Con quién va a articular':                       'Articulacion',
    '6. Responsable de la actividad':                 'Responsable_Actividad',
    '4. Responsable de la actividad*':                'Responsable_Actividad',
    '5. Enfoque de la actividad':                     'Enfoque_Actividad',
    'Enfoque de la actividad*':                       'Enfoque_Actividad',
    'Enfoque Estratégico':                            'Enfoque_Estrategico',
    '6. Estrategia a impactar':                       'Estrategia_Impactar',
    'Estrategia de Impacto':                          'Estrategia_Impactar',
    '1. Esta actividad se enmarca en:':               'Enmarca_En',
    '6.1. Líneas Estratégicas de Seguridad':          'Linea_Seguridad',
    'Líneas Estratégicas de Seguridad':               'Linea_Seguridad',
    '6.2. Líneas Estratégicas de Convivencia':        'Linea_Convivencia',
    'Líneas Estratégicas de Convivencia':             'Linea_Convivencia',
    '6.3. Líneas Estratégicas de Justicia':           'Linea_Justicia',
    'Líneas Estratégicas de Justicia':                'Linea_Justicia',
    '8. UPZ a la Que Pertenece la Actividad':         'UPZ',
    'UPZ a la Que Pertenece la Actividad':            'UPZ',
    'BARRIOS DE LA UPZ 32 - San Blas':                'Barrios_UPZ32',
    'BARRIOS DE LA UPZ 33 - Sosiego':                 'Barrios_UPZ33',
    'BARRIOS DE LA UPZ 34 - 20 de Julio':             'Barrios_UPZ34',
    'BARRIOS DE LA UPZ 51 - Los Libertadores':        'Barrios_UPZ51',
    'BARRIOS DE LA UPZ 50 - La Gloria':               'Barrios_UPZ50',
    '9. Zona a la que Pertenece la Actividad':        'Zona',
    'Zona a la que Pertenece la Actividad':           'Zona',
    '7. Dirección donde se realiza la actividad':     'Direccion_Actividad',
    'Dirección donde se realiza la actividad':        'Direccion_Actividad',
    '10. Fecha de la actividad':                      'Fecha_Actividad',
    'Fecha de Actividad':                             'Fecha_Actividad',
    '11. Hora de inicio':                             'Hora_Inicio',
    'Hora de Inicio de Actividad':                    'Hora_Inicio',
    '12. ¿Deseas recibir un correo de confirmación?': 'Recibir_Correo',
    '¿Deseas recibir un correo de confirmación?':     'Recibir_Correo',
    'Estado':                                         'Estado',
    'ID del evento':                                  'ID_Evento',
    'Quien rechazó':                                  'Quien_Rechazo',
    'Fecha de cancelación':                           'Fecha_Cancelacion',
}

# ========================================
# AUTENTICACIÓN
# ========================================

def autenticar_google_sheets() -> object:
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(f"No se encontró: {CREDENTIALS_FILE}")
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=[
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            'https://www.googleapis.com/auth/drive.readonly',
        ]
    )
    service = build('sheets', 'v4', credentials=credentials)
    logger.info("✅ Autenticación exitosa")
    return service

# ========================================
# EXTRACCIÓN
# ========================================

def extraer_datos(service: object, sheet_name: str) -> pd.DataFrame:
    logger.info(f"📥 Extrayendo: {sheet_name}")
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!A:AB"
    ).execute()
    values = result.get('values', [])
    if not values:
        logger.warning(f"⚠️ Sin datos en {sheet_name}")
        return pd.DataFrame()
    df = pd.DataFrame(values[1:], columns=values[0])
    logger.info(f"✅ {len(df)} registros de {sheet_name}")
    return df

# ========================================
# NORMALIZACIÓN POR HOJA (FIX duplicados)
# ========================================

def normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra y consolida columnas en un DataFrame INDIVIDUAL antes del concat.
    Garantiza que cada hoja llega con nombres unificados, evitando el error
    'cannot assemble with duplicate keys' al hacer pd.to_datetime después del concat.
    """
    nombres_usados: Dict[str, list] = {}
    for col in df.columns:
        nombre_final = COLUMN_MAPPING.get(col, col)
        nombres_usados.setdefault(nombre_final, []).append(col)

    nuevas_columnas = {}
    for nombre_final, cols_originales in nombres_usados.items():
        if len(cols_originales) == 1:
            nuevas_columnas[cols_originales[0]] = nombre_final
        else:
            # Consolidar múltiples columnas → primera gana, rellena con las demás
            col_destino = cols_originales[0]
            for col_extra in cols_originales[1:]:
                df[col_destino] = (
                    df[col_destino].replace('', pd.NA)
                    .fillna(df[col_extra].replace('', pd.NA))
                )
                df = df.drop(columns=[col_extra])
            nuevas_columnas[col_destino] = nombre_final

    df = df.rename(columns=nuevas_columnas)
    df = df.loc[:, ~df.columns.duplicated(keep='first')]
    return df

# ========================================
# LIMPIEZA Y ENRIQUECIMIENTO
# ========================================

def limpiar_datos(df: pd.DataFrame, hoja_origen: str) -> pd.DataFrame:
    """Limpia y agrega las columnas enriquecidas que necesita Power BI."""
    try:
        logger.info(f"🧹 Limpiando hoja: {hoja_origen}")

        df = df.dropna(how='all')
        df = df.loc[:, ~df.columns.duplicated(keep='first')]

        # Columna requerida por Power BI
        df['Hoja_Origen'] = hoja_origen

        # Fechas
        for col in ['Marca_Temporal', 'Fecha_Actividad', 'Fecha_Cancelacion']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        logger.info("  ✓ Fechas convertidas")

        # Barrio consolidado desde columnas por UPZ
        df['Barrio_Extraido'] = ''
        upz_barrio_map = {
            '32': 'Barrios_UPZ32',
            '33': 'Barrios_UPZ33',
            '34': 'Barrios_UPZ34',
            '51': 'Barrios_UPZ51',
            '50': 'Barrios_UPZ50',
        }
        if 'UPZ' in df.columns:
            for idx, row in df.iterrows():
                upz = str(row.get('UPZ', ''))
                for num, col_barrio in upz_barrio_map.items():
                    if num in upz and col_barrio in df.columns:
                        df.at[idx, 'Barrio_Extraido'] = row.get(col_barrio, '')
                        break
        logger.info("  ✓ Barrios consolidados")

        # Columnas enriquecidas que busca el modelo Power BI
        df['UPZ_Enriquecida']  = df['UPZ'].astype(str).str.strip()  if 'UPZ'  in df.columns else ''
        df['Zona_Enriquecida'] = df['Zona'].astype(str).str.strip()  if 'Zona' in df.columns else ''
        # 'Estrategia' es el alias que espera dim_estrategias y las medidas DAX
        df['Estrategia'] = (
            df['Estrategia_Impactar'].astype(str).str.strip()
            if 'Estrategia_Impactar' in df.columns else ''
        )

        registros_antes = len(df)
        df = df.drop_duplicates()
        eliminados = registros_antes - len(df)
        if eliminados:
            logger.info(f"  ✓ {eliminados} duplicados eliminados")

        logger.info(f"✅ {hoja_origen}: {len(df)} registros limpios")
        return df

    except Exception as e:
        logger.error(f"❌ Error al limpiar {hoja_origen}: {e}")
        raise

# ========================================
# ID ÚNICO
# ========================================

def generar_id_actividad(row, idx: int) -> str:
    """
    Hash MD5 que incluye el índice de fila para garantizar unicidad absoluta,
    incluso cuando varios registros comparten los mismos valores de negocio.
    Soluciona: "ID_Actividad contiene un valor duplicado"
    """
    campos = (
        f"{idx}"
        f"{row.get('Marca_Temporal', '')}"
        f"{row.get('Nombre_Actividad', '')}"
        f"{row.get('Fecha_Actividad', '')}"
        f"{row.get('Hora_Inicio', '')}"
        f"{row.get('Direccion_Actividad', '')}"
        f"{row.get('Email_Responsable', '')}"
        f"{row.get('Hoja_Origen', '')}"
    )
    return hashlib.md5(campos.encode()).hexdigest()[:16].upper()

# ========================================
# DIMENSIONES
# ========================================

def crear_dimensiones(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Crea todas las tablas de dimensiones que necesita el modelo Power BI."""
    try:
        logger.info("🔨 Creando dimensiones")
        dimensiones = {}

        def dim_simple(col: str, id_col: str, nombre: str):
            if col not in df.columns:
                return
            d = df[[col]].dropna().drop_duplicates().reset_index(drop=True)
            d = d[d[col].astype(str).str.strip() != '']
            d.insert(0, id_col, range(1, len(d) + 1))
            dimensiones[nombre] = d
            logger.info(f"  ✓ {nombre}: {len(d)} registros")

        dim_simple('UPZ_Enriquecida',  'upz_id',       'dim_upz')
        dim_simple('Zona_Enriquecida', 'zona_id',       'dim_zonas')
        dim_simple('Estrategia',       'estrategia_id', 'dim_estrategias')
        dim_simple('Enfoque_Actividad','enfoque_id',    'dim_enfoques')
        dim_simple('Estado',           'estado_id',     'dim_estados')

        # dim_barrios
        if 'Barrio_Extraido' in df.columns and 'UPZ_Enriquecida' in df.columns:
            d = (
                df[['Barrio_Extraido', 'UPZ_Enriquecida']]
                .dropna()
                .drop_duplicates()
                .reset_index(drop=True)
            )
            d = d[d['Barrio_Extraido'].astype(str).str.strip() != '']
            d.insert(0, 'barrio_id', range(1, len(d) + 1))
            dimensiones['dim_barrios'] = d
            logger.info(f"  ✓ dim_barrios: {len(d)} registros")

        # Dim_Lineas_Estrategicas → requerida por USERELATIONSHIP en DAX
        lineas = []
        for col_linea, tipo in [
            ('Linea_Seguridad',   'Seguridad'),
            ('Linea_Convivencia', 'Convivencia'),
            ('Linea_Justicia',    'Justicia'),
        ]:
            if col_linea in df.columns:
                vals = (
                    df[['ID_Actividad', col_linea]]
                    .dropna(subset=[col_linea])
                    .rename(columns={col_linea: 'Linea_Estrategica'})
                    .copy()
                )
                vals = vals[vals['Linea_Estrategica'].astype(str).str.strip() != '']
                vals['Tipo_Linea'] = tipo
                lineas.append(vals)

        if lineas:
            dim_lineas = pd.concat(lineas, ignore_index=True)
            dim_lineas.insert(0, 'linea_id', range(1, len(dim_lineas) + 1))
            dimensiones['Dim_Lineas_Estrategicas'] = dim_lineas
            logger.info(f"  ✓ Dim_Lineas_Estrategicas: {len(dim_lineas)} registros")

        logger.info(f"✅ {len(dimensiones)} dimensiones creadas")
        return dimensiones

    except Exception as e:
        logger.error(f"❌ Error al crear dimensiones: {e}")
        raise

# ========================================
# TABLA DE HECHOS
# ========================================

def crear_tabla_hechos(df: pd.DataFrame, dimensiones: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    try:
        logger.info("📊 Creando tabla de hechos")
        fact = df.copy()

        joins = [
            ('dim_upz',        'UPZ_Enriquecida', 'UPZ_Enriquecida'),
            ('dim_zonas',      'Zona_Enriquecida','Zona_Enriquecida'),
            ('dim_estrategias','Estrategia',       'Estrategia'),
            ('dim_enfoques',   'Enfoque_Actividad','Enfoque_Actividad'),
            ('dim_estados',    'Estado',           'Estado'),
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

        if 'dim_barrios' in dimensiones and 'Barrio_Extraido' in fact.columns:
            fact = fact.merge(
                dimensiones['dim_barrios'],
                on=['Barrio_Extraido', 'UPZ_Enriquecida'],
                how='left'
            )
            logger.info("  ✓ Join con dim_barrios")

        # Limpiar columnas _x / _y generadas por merges
        cols_limpias = [c for c in fact.columns if not c.endswith('_x') and not c.endswith('_y')]
        fact = fact[cols_limpias]

        logger.info(f"✅ Tabla de hechos: {len(fact)} registros, {len(fact.columns)} columnas")
        return fact

    except Exception as e:
        logger.error(f"❌ Error al crear tabla de hechos: {e}")
        raise

# ========================================
# CARGA
# ========================================

def guardar_csv(df: pd.DataFrame, path: str) -> None:
    """Todos los CSV con el mismo separador y encoding para Power BI."""
    df.to_csv(path, index=False, encoding=CSV_ENC, sep=CSV_SEP)
    logger.info(f"  ✓ {path}")


def guardar_archivos(fact: pd.DataFrame, dimensiones: Dict[str, pd.DataFrame]) -> None:
    try:
        logger.info("💾 Guardando archivos")
        os.makedirs('dimensiones', exist_ok=True)

        guardar_csv(fact, 'fact_actividades.csv')
        guardar_csv(fact, 'fact_actividades_enriquecido.csv')
        guardar_csv(fact, 'dimensiones/fact_actividades_enriquecido.csv')

        for nombre, df_dim in dimensiones.items():
            guardar_csv(df_dim, f'dimensiones/{nombre}.csv')

        logger.info("✅ Todos los archivos guardados")

    except Exception as e:
        logger.error(f"❌ Error al guardar: {e}")
        raise

# ========================================
# MAIN
# ========================================

def main():
    inicio = datetime.now()
    try:
        logger.info("=" * 60)
        logger.info("🚀 INICIANDO PIPELINE ETL CONVIVE360")
        logger.info("=" * 60)

        # 1. Autenticar
        service = autenticar_google_sheets()

        # 2. Extraer
        df1 = extraer_datos(service, SHEET_NAME_1)
        df2 = extraer_datos(service, SHEET_NAME_2)

        # 3. Normalizar columnas POR SEPARADO ← evita duplicate keys en concat
        logger.info("🔀 Normalizando columnas por hoja")
        df1 = normalizar_columnas(df1)
        df2 = normalizar_columnas(df2)

        # 4. Limpiar y enriquecer cada hoja
        df1 = limpiar_datos(df1, hoja_origen='Formulario_1')
        df2 = limpiar_datos(df2, hoja_origen='Formulario_2')

        # 5. Combinar
        logger.info("🔗 Combinando hojas")
        df = pd.concat([df1, df2], ignore_index=True)
        df = df.loc[:, ~df.columns.duplicated(keep='first')]
        logger.info(f"✅ Total: {len(df)} registros")

        # 6. ID único garantizado
        logger.info("🔑 Generando ID_Actividad")
        df['ID_Actividad'] = [
            generar_id_actividad(row, idx)
            for idx, row in df.iterrows()
        ]
        duplicados = df['ID_Actividad'].duplicated().sum()
        if duplicados:
            logger.warning(f"  ⚠️ {duplicados} IDs duplicados — revisar datos fuente")
        else:
            logger.info(f"  ✓ {len(df)} IDs únicos")

        # 7. Dimensiones
        dimensiones = crear_dimensiones(df)

        # 8. Tabla de hechos
        fact = crear_tabla_hechos(df, dimensiones)

        # 9. Guardar
        guardar_archivos(fact, dimensiones)

        duracion = (datetime.now() - inicio).total_seconds()
        logger.info("=" * 60)
        logger.info("🎉 PIPELINE COMPLETADO EXITOSAMENTE")
        logger.info(f"⏱️  Duración    : {duracion:.2f} s")
        logger.info(f"📊 Registros   : {len(fact)}")
        logger.info(f"📁 Dimensiones : {len(dimensiones)}")
        logger.info("=" * 60)
        return 0

    except Exception as e:
        import traceback
        logger.error("=" * 60)
        logger.error("💥 ERROR EN EL PIPELINE")
        logger.error(str(e))
        logger.error(traceback.format_exc())
        logger.error("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
