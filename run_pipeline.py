#!/usr/bin/env python3
"""
Pipeline ETL para Convive360 - Alcaldía Local de San Cristóbal
Versión CORREGIDA v4.1:
  - FIX: 'Area Solicitante' mapeada a Responsable_Principal
  - FIX: Rango de extracción ampliado de A:AB a A:AD (30 columnas)
  - 0 registros perdidos: drop_duplicates solo por clave de negocio real
  - 0 celdas en blanco artificiales: columnas exclusivas de cada formulario
    se rellenan con 'N/A' o valor por defecto tras el concat
  - Todos los CSV con sep=';' y encoding='utf-8-sig'
  - ID_Actividad único garantizado (incluye índice de fila)
  - Genera todas las columnas/tablas que necesita el modelo Power BI
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

CSV_SEP = ';'
CSV_ENC = 'utf-8-sig'

# Clave de negocio para detectar duplicados REALES
CLAVE_DUPLICADO = ['Marca_Temporal', 'Email_Responsable', 'Nombre_Actividad']

# Columnas que SOLO existen en Formulario 1
SOLO_FORMULARIO_1 = [
    'Enmarca_En',
    'Barrios_UPZ32',
    'Barrios_UPZ33',
    'Barrios_UPZ34',
    'Barrios_UPZ51',
    'Barrios_UPZ50',
    'Linea_Seguridad',
    'Linea_Justicia',
    'Linea_Convivencia',
]

# Columnas que SOLO existen en Formulario 2
SOLO_FORMULARIO_2 = [
    'Enfoque_Estrategico',
]

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
    # ── FIX: nueva columna insertada en abril 2026 ──────────────────────────
    'Area Solicitante':                               'Responsable_Principal',
    'Área Solicitante':                               'Responsable_Principal',
    # ────────────────────────────────────────────────────────────────────────
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
        range=f"{sheet_name}!A:AD"      # ← FIX: era A:AB (28 cols), ahora A:AD (30 cols)
    ).execute()
    values = result.get('values', [])
    if not values:
        logger.warning(f"⚠️ Sin datos en {sheet_name}")
        return pd.DataFrame()
    # Normalizar filas con menos columnas que el header
    header = values[0]
    rows = [row + [''] * (len(header) - len(row)) for row in values[1:]]
    df = pd.DataFrame(rows, columns=header)
    logger.info(f"✅ {len(df)} registros de {sheet_name}")
    return df

# ========================================
# NORMALIZACIÓN POR HOJA
# ========================================

def normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra y consolida columnas en un DataFrame INDIVIDUAL antes del concat.
    Evita el error 'cannot assemble with duplicate keys'.
    También elimina espacios extras en nombres de columnas.
    """
    df.columns = [c.strip() for c in df.columns]

    nombres_usados: Dict[str, list] = {}
    for col in df.columns:
        nombre_final = COLUMN_MAPPING.get(col, col)
        nombres_usados.setdefault(nombre_final, []).append(col)

    nuevas_columnas = {}
    for nombre_final, cols_originales in nombres_usados.items():
        if len(cols_originales) == 1:
            nuevas_columnas[cols_originales[0]] = nombre_final
        else:
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
    logger.info(f"🧹 Limpiando: {hoja_origen} ({len(df)} registros)")

    df = df.replace('', pd.NA)
    df = df.dropna(how='all')
    df = df.replace(pd.NA, '')

    df['Hoja_Origen'] = hoja_origen

    for col in ['Marca_Temporal', 'Fecha_Actividad', 'Fecha_Cancelacion']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

    logger.info(f"  ✓ {hoja_origen}: {len(df)} registros limpios")
    return df


def rellenar_columnas_cruzadas(df: pd.DataFrame) -> pd.DataFrame:
    defaults = {
        'Enmarca_En':            'No especificado',
        'Barrios_UPZ32':         'N/A',
        'Barrios_UPZ33':         'N/A',
        'Barrios_UPZ34':         'N/A',
        'Barrios_UPZ51':         'N/A',
        'Barrios_UPZ50':         'N/A',
        'Linea_Seguridad':       'No aplica',
        'Linea_Justicia':        'No aplica',
        'Linea_Convivencia':     'No aplica',
        'Enfoque_Estrategico':   'No especificado',
        'Articulacion':          'N/A',
        'Estado':                'Pendiente',
        'ID_Evento':             '',
        'Quien_Rechazo':         '',
        'Fecha_Cancelacion':     '',
        'Recibir_Correo':        'No',
        'Enfoque_Actividad':     'No especificado',
        'Responsable_Actividad': 'No especificado',
    }

    for col, default_val in defaults.items():
        if col in df.columns:
            df[col] = df[col].replace('', pd.NA).fillna(default_val)
        else:
            df[col] = default_val

    logger.info("  ✓ Columnas cruzadas rellenadas con valores por defecto")
    return df


def enriquecer_datos(df: pd.DataFrame) -> pd.DataFrame:
    df['UPZ_Enriquecida']  = df['UPZ'].astype(str).str.strip()  if 'UPZ'  in df.columns else 'Sin UPZ'
    df['Zona_Enriquecida'] = df['Zona'].astype(str).str.strip()  if 'Zona' in df.columns else 'Sin Zona'
    df['Estrategia'] = (
        df['Estrategia_Impactar'].astype(str).str.strip()
        if 'Estrategia_Impactar' in df.columns else 'Sin estrategia'
    )

    df['Barrio_Extraido'] = 'Sin barrio'
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
                    valor = str(row.get(col_barrio, '')).strip()
                    if valor and valor not in ('N/A', 'nan', ''):
                        df.at[idx, 'Barrio_Extraido'] = valor
                    break

    logger.info("  ✓ Columnas enriquecidas generadas")
    return df

# ========================================
# ID ÚNICO
# ========================================

def generar_id_actividad(row, idx: int) -> str:
    campos = (
        f"{idx}"
        f"|{row.get('Marca_Temporal', '')}"
        f"|{row.get('Nombre_Actividad', '')}"
        f"|{row.get('Fecha_Actividad', '')}"
        f"|{row.get('Hora_Inicio', '')}"
        f"|{row.get('Direccion_Actividad', '')}"
        f"|{row.get('Email_Responsable', '')}"
        f"|{row.get('Hoja_Origen', '')}"
    )
    return hashlib.md5(campos.encode()).hexdigest()[:16].upper()

# ========================================
# DIMENSIONES
# ========================================

def crear_dimensiones(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    logger.info("🔨 Creando dimensiones")
    dimensiones = {}

    def dim_simple(col: str, id_col: str, nombre: str):
        if col not in df.columns:
            logger.warning(f"  ⚠️ Columna '{col}' no encontrada para {nombre}")
            return
        d = df[[col]].copy()
        d[col] = d[col].astype(str).str.strip()
        d = d[d[col].notna() & (d[col] != '') & (d[col] != 'nan')]
        d = d.drop_duplicates().reset_index(drop=True)
        d.insert(0, id_col, range(1, len(d) + 1))
        dimensiones[nombre] = d
        logger.info(f"  ✓ {nombre}: {len(d)} valores únicos")

    dim_simple('UPZ_Enriquecida',   'upz_id',       'dim_upz')
    dim_simple('Zona_Enriquecida',  'zona_id',       'dim_zonas')
    dim_simple('Estrategia',        'estrategia_id', 'dim_estrategias')
    dim_simple('Enfoque_Actividad', 'enfoque_id',    'dim_enfoques')
    dim_simple('Estado',            'estado_id',     'dim_estados')

    # dim_areas → nueva dimensión para Responsable_Principal (áreas)
    if 'Responsable_Principal' in df.columns:
        d = df[['Responsable_Principal']].copy()
        d['Responsable_Principal'] = d['Responsable_Principal'].astype(str).str.strip()
        d = d[
            d['Responsable_Principal'].notna() &
            (d['Responsable_Principal'] != '') &
            (d['Responsable_Principal'] != 'nan') &
            (d['Responsable_Principal'] != 'No especificado')
        ]
        d = d.drop_duplicates().reset_index(drop=True)
        d.insert(0, 'area_id', range(1, len(d) + 1))
        dimensiones['dim_areas'] = d
        logger.info(f"  ✓ dim_areas: {len(d)} valores únicos")

    # dim_barrios
    if 'Barrio_Extraido' in df.columns and 'UPZ_Enriquecida' in df.columns:
        d = df[['Barrio_Extraido', 'UPZ_Enriquecida']].copy()
        d['Barrio_Extraido'] = d['Barrio_Extraido'].astype(str).str.strip()
        d['UPZ_Enriquecida'] = d['UPZ_Enriquecida'].astype(str).str.strip()
        d = d[d['Barrio_Extraido'].notna() & (d['Barrio_Extraido'] != '') & (d['Barrio_Extraido'] != 'Sin barrio')]
        d = d.drop_duplicates().reset_index(drop=True)
        d.insert(0, 'barrio_id', range(1, len(d) + 1))
        dimensiones['dim_barrios'] = d
        logger.info(f"  ✓ dim_barrios: {len(d)} valores únicos")

    # Dim_Lineas_Estrategicas
    lineas = []
    for col_linea, tipo in [
        ('Linea_Seguridad',   'Seguridad'),
        ('Linea_Convivencia', 'Convivencia'),
        ('Linea_Justicia',    'Justicia'),
    ]:
        if col_linea in df.columns:
            vals = df[['ID_Actividad', col_linea]].copy()
            vals[col_linea] = vals[col_linea].astype(str).str.strip()
            vals = vals[
                vals[col_linea].notna() &
                (vals[col_linea] != '') &
                (vals[col_linea] != 'nan') &
                (vals[col_linea] != 'No aplica')
            ]
            vals = vals.rename(columns={col_linea: 'Linea_Estrategica'})
            vals['Tipo_Linea'] = tipo
            lineas.append(vals)

    if lineas:
        dim_lineas = pd.concat(lineas, ignore_index=True)
        dim_lineas.insert(0, 'linea_id', range(1, len(dim_lineas) + 1))
        dimensiones['Dim_Lineas_Estrategicas'] = dim_lineas
        logger.info(f"  ✓ Dim_Lineas_Estrategicas: {len(dim_lineas)} registros")

    logger.info(f"✅ {len(dimensiones)} dimensiones creadas")
    return dimensiones

# ========================================
# TABLA DE HECHOS
# ========================================

def crear_tabla_hechos(df: pd.DataFrame, dimensiones: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    logger.info("📊 Creando tabla de hechos")
    fact = df.copy()

    joins = [
        ('dim_upz',         'UPZ_Enriquecida',       'UPZ_Enriquecida'),
        ('dim_zonas',       'Zona_Enriquecida',       'Zona_Enriquecida'),
        ('dim_estrategias', 'Estrategia',              'Estrategia'),
        ('dim_enfoques',    'Enfoque_Actividad',       'Enfoque_Actividad'),
        ('dim_estados',     'Estado',                  'Estado'),
        ('dim_areas',       'Responsable_Principal',   'Responsable_Principal'),  # ← nuevo join
    ]

    for nombre_dim, col_fact, col_dim in joins:
        if nombre_dim in dimensiones and col_fact in fact.columns:
            fact = fact.merge(
                dimensiones[nombre_dim],
                left_on=col_fact,
                right_on=col_dim,
                how='left'
            )

    if 'dim_barrios' in dimensiones and 'Barrio_Extraido' in fact.columns:
        fact = fact.merge(
            dimensiones['dim_barrios'],
            on=['Barrio_Extraido', 'UPZ_Enriquecida'],
            how='left'
        )

    cols_limpias = [c for c in fact.columns if not c.endswith('_x') and not c.endswith('_y')]
    fact = fact[cols_limpias]

    logger.info(f"✅ Tabla de hechos: {len(fact)} registros, {len(fact.columns)} columnas")
    return fact

# ========================================
# CARGA
# ========================================

def guardar_csv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False, encoding=CSV_ENC, sep=CSV_SEP)
    logger.info(f"  ✓ {path}  ({len(df)} registros)")


def guardar_archivos(fact: pd.DataFrame, dimensiones: Dict[str, pd.DataFrame]) -> None:
    logger.info("💾 Guardando archivos")
    os.makedirs('dimensiones', exist_ok=True)

    guardar_csv(fact, 'fact_actividades.csv')
    guardar_csv(fact, 'fact_actividades_enriquecido.csv')
    guardar_csv(fact, 'dimensiones/fact_actividades_enriquecido.csv')

    for nombre, df_dim in dimensiones.items():
        guardar_csv(df_dim, f'dimensiones/{nombre}.csv')

    logger.info("✅ Todos los archivos guardados")

# ========================================
# MAIN
# ========================================

def main():
    inicio = datetime.now()
    try:
        logger.info("=" * 60)
        logger.info("🚀 INICIANDO PIPELINE ETL CONVIVE360 v4.1")
        logger.info("=" * 60)

        # 1. Autenticar
        service = autenticar_google_sheets()

        # 2. Extraer
        df1 = extraer_datos(service, SHEET_NAME_1)
        df2 = extraer_datos(service, SHEET_NAME_2)
        logger.info(f"  F1: {len(df1)} | F2: {len(df2)} | Esperado: {len(df1)+len(df2)}")

        # 3. Normalizar columnas POR SEPARADO
        logger.info("🔀 Normalizando columnas por hoja")
        df1 = normalizar_columnas(df1)
        df2 = normalizar_columnas(df2)

        # 4. Limpiar cada hoja individualmente
        df1 = limpiar_datos(df1, hoja_origen='Formulario_1')
        df2 = limpiar_datos(df2, hoja_origen='Formulario_2')

        # 5. Combinar
        logger.info("🔗 Combinando hojas")
        df = pd.concat([df1, df2], ignore_index=True)
        df = df.loc[:, ~df.columns.duplicated(keep='first')]
        logger.info(f"  ✓ Tras concat: {len(df)} registros")

        # 6. Rellenar columnas cruzadas
        df = rellenar_columnas_cruzadas(df)

        # 7. Eliminar duplicados REALES
        registros_antes = len(df)
        cols_dedup = [c for c in CLAVE_DUPLICADO if c in df.columns]
        if cols_dedup:
            df = df.drop_duplicates(subset=cols_dedup, keep='first')
            eliminados = registros_antes - len(df)
            if eliminados:
                logger.info(f"  ✓ {eliminados} duplicado(s) real(es) eliminado(s)")
            else:
                logger.info(f"  ✓ Sin duplicados reales — todos los registros conservados")
        logger.info(f"  ✓ Registros finales: {len(df)}")

        # 8. Enriquecer
        df = enriquecer_datos(df)

        # 9. ID único
        logger.info("🔑 Generando ID_Actividad")
        df = df.reset_index(drop=True)
        df['ID_Actividad'] = [
            generar_id_actividad(row, idx)
            for idx, row in df.iterrows()
        ]
        duplicados = df['ID_Actividad'].duplicated().sum()
        if duplicados:
            logger.warning(f"  ⚠️ {duplicados} IDs duplicados — revisar datos fuente")
        else:
            logger.info(f"  ✓ {len(df)} IDs únicos generados")

        # 10. Dimensiones
        dimensiones = crear_dimensiones(df)

        # 11. Tabla de hechos
        fact = crear_tabla_hechos(df, dimensiones)

        # 12. Guardar
        guardar_archivos(fact, dimensiones)

        duracion = (datetime.now() - inicio).total_seconds()
        logger.info("=" * 60)
        logger.info("🎉 PIPELINE COMPLETADO EXITOSAMENTE")
        logger.info(f"⏱️  Duración     : {duracion:.2f} s")
        logger.info(f"📊 Registros    : {len(fact)}")
        logger.info(f"📁 Dimensiones  : {len(dimensiones)}")
        logger.info(f"📋 Columnas fact: {len(fact.columns)}")
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
