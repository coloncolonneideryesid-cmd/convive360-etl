#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enriquece fact_actividades con barrios extraídos desde direcciones
Usa 4 métodos de extracción + validación Zona-UPZ
VERSIÓN 2: Corrige TODAS las zonas duplicadas
"""

import pandas as pd
import json
import re
from pathlib import Path
from difflib import get_close_matches

# Rutas
BASE_DIR = Path(__file__).resolve().parents[1]
DICT_FILE = BASE_DIR / "scripts" / "diccionario_barrios_completo.json"
FACT_FILE = BASE_DIR / "fact_actividades_limpio_fixed.csv"
OUTPUT_FILE = BASE_DIR / "dimensiones" / "fact_actividades_enriquecido.csv"

print("\n" + "="*80)
print("🔍 ENRIQUECIENDO ACTIVIDADES CON BARRIOS V2")
print("="*80)

# =====================================================================
# CARGAR DATOS CON MANEJO ROBUSTO DE CSV
# =====================================================================
print("\n📥 Cargando datos...")

# Cargar fact_actividades con manejo robusto de comillas y delimitadores
try:
    df = pd.read_csv(
        FACT_FILE, 
        encoding='utf-8',
        sep=';',
        quotechar='"',
        escapechar='\\',
        on_bad_lines='warn',
        low_memory=False
    )
    print(f"✅ Actividades cargadas: {len(df)} registros")
except Exception as e:
    print(f"⚠️  Error al cargar CSV: {e}")
    print("🔧 Intentando con parámetros alternativos...")
    df = pd.read_csv(FACT_FILE, encoding='utf-8', sep=';', engine='python')
    print(f"✅ Actividades cargadas: {len(df)} registros")

# Cargar diccionario
with open(DICT_FILE, 'r', encoding='utf-8') as f:
    diccionario = json.load(f)

barrio_a_upz = diccionario['barrio_a_upz']
barrio_a_zonas = diccionario['barrio_a_zonas']
barrios_conocidos = list(barrio_a_upz.keys())

print(f"✅ Diccionario cargado: {len(barrios_conocidos)} barrios")

# =====================================================================
# FUNCIONES DE EXTRACCIÓN
# =====================================================================

def normalizar(texto):
    """Normaliza texto para comparación"""
    if pd.isna(texto):
        return ""
    import unicodedata
    texto = str(texto)
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto)
                    if unicodedata.category(c) != 'Mn')
    return texto.lower().strip()

def extraer_barrio(direccion):
    """
    Extrae barrio usando 4 métodos:
    1. Exacto
    2. Patrón (BARRIO X, BRR. X, B. X)
    3. Aproximado (fuzzy 85%+)
    4. Compuesto (2+ palabras)
    
    Retorna: (barrio, upz, zonas_lista, metodo)
    """
    if pd.isna(direccion) or str(direccion).strip() == "":
        return None, None, None, None
    
    direccion_norm = normalizar(direccion)
    
    # MÉTODO 1: Búsqueda exacta
    for barrio in barrios_conocidos:
        if barrio in direccion_norm:
            upz = barrio_a_upz.get(barrio)
            zonas = barrio_a_zonas.get(barrio, [])
            return barrio, upz, zonas, 'Exacto'
    
    # MÉTODO 2: Patrones comunes
    patrones = [
        r'barrio\s+([a-z\s]+?)(?:\s+calle|\s+carrera|\s+diagonal|\s+transversal|$)',
        r'brr\.?\s+([a-z\s]+?)(?:\s+calle|\s+carrera|\s+diagonal|\s+transversal|$)',
        r'b\.?\s+([a-z\s]+?)(?:\s+calle|\s+carrera|\s+diagonal|\s+transversal|$)',
    ]
    
    for patron in patrones:
        match = re.search(patron, direccion_norm)
        if match:
            barrio_extraido = match.group(1).strip()
            matches = get_close_matches(barrio_extraido, barrios_conocidos, n=1, cutoff=0.80)
            if matches:
                barrio = matches[0]
                upz = barrio_a_upz.get(barrio)
                zonas = barrio_a_zonas.get(barrio, [])
                return barrio, upz, zonas, 'Patron'
    
    # MÉTODO 3: Aproximado (fuzzy)
    palabras = re.findall(r'\b[a-z]{4,}\b', direccion_norm)
    for palabra in palabras:
        matches = get_close_matches(palabra, barrios_conocidos, n=1, cutoff=0.85)
        if matches:
            barrio = matches[0]
            upz = barrio_a_upz.get(barrio)
            zonas = barrio_a_zonas.get(barrio, [])
            return barrio, upz, zonas, 'Aproximado'
    
    # MÉTODO 4: Compuesto (2+ palabras coincidentes)
    for barrio in barrios_conocidos:
        palabras_barrio = barrio.split()
        if len(palabras_barrio) >= 2:
            coincidencias = sum(1 for p in palabras_barrio if p in direccion_norm)
            if coincidencias >= 2:
                upz = barrio_a_upz.get(barrio)
                zonas = barrio_a_zonas.get(barrio, [])
                return barrio, upz, zonas, 'Compuesto'
    
    return None, None, None, None


def seleccionar_zona_correcta(barrio, upz, zonas_posibles):
    """
    Selecciona la zona correcta cuando un barrio está en múltiples zonas.
    Usa el UPZ y el nombre del barrio para determinar la zona precisa.
    """
    if not zonas_posibles:
        return None
    
    # Si solo hay una zona, devolver esa
    if len(zonas_posibles) == 1:
        return zonas_posibles[0].upper()
    
    # Normalizar UPZ
    upz_norm = normalizar(upz) if upz else ""
    barrio_norm = normalizar(barrio) if barrio else ""
    
    # Mapeo específico UPZ 32 (SAN BLAS) - Zona 1 vs Zona 2
    if "32" in upz_norm or "san blas" in upz_norm:
        barrios_zona1 = {
            'aguas claras', 'amapolas', 'amapolas ii sector', 'buenavista suroriental',
            'corinto', 'el balcon de la castana', 'el futuro', 'el ramajal', 
            'la castana', 'la cecilia', 'la gran colombia', 'la sagrada familia',
            'las acacias', 'las mercedes', 'los alpes', 'los laureles sur oriental i sector',
            'los laureles sur oriental ii sector', 'macarena de los alpes', 'manila',
            'montecarlo', 'nueva espana', 'ramajal', 'san blas', 'san blas ii sector',
            'san cristobal alto', 'triangulo alto', 'vitelma'
        }
        if barrio_norm in barrios_zona1:
            return "ZONA 1"
        else:
            return "ZONA 2"
    
    # Mapeo específico UPZ 50 (LA GLORIA) - Zona 3 vs Zona 8
    elif "50" in upz_norm or "la gloria" in upz_norm:
        barrios_zona3 = {
            'altamira chiquita', 'altamira sector san jose', 'bellavista sur oriental',
            'el poblado', 'la arboleda', 'la gloria', 'la grovana', 'la nueva gloria',
            'los alpes del zipa', 'los altos del zuque', 'los puentes', 'miraflores',
            'moralva', 'panorama', 'puente colorado', 'quindio', 'quindio ii sector',
            'san jose oriental', 'villa anita sur oriental'
        }
        if barrio_norm in barrios_zona3:
            return "ZONA 3"
        else:
            return "ZONA 8"
    
    # Mapeo específico UPZ 51 (LOS LIBERTADORES) - Zona 5 vs Zona 7
    elif "51" in upz_norm or "los libertadores" in upz_norm:
        barrios_zona5 = {
            'bosque de san jose', 'ciudad de londres', 'juan rey', 'juan rey ii',
            'la arboleda', 'la belleza', 'la nueva gloria', 'la nueva gloria ii sector',
            'los libertadores', 'los libertadores bosque diamante triangulo',
            'los libertadores sector el tesoro', 'los libertadores sector la colina',
            'los libertadores sector san ignacio', 'los libertadores sector san isidro',
            'los libertadores sector san jose', 'los libertadores sector san luis',
            'los libertadores sector san miguel', 'los pinos', 'nueva delly',
            'nueva delly parte alta', 'republica del canada el pinar', 'san manuel',
            'san rafael sur oriental', 'sierras del sur oriente', 'valparaiso',
            'villa aurora', 'villa begonia'
        }
        if barrio_norm in barrios_zona5:
            return "ZONA 5"
        else:
            return "ZONA 7"
    
    # UPZ sin ambigüedad
    elif "33" in upz_norm or "sosiego" in upz_norm:
        return "ZONA 6"
    elif "34" in upz_norm or "20 de julio" in upz_norm:
        return "ZONA 4"
    
    # Por defecto, tomar la primera zona disponible
    return zonas_posibles[0].upper() if zonas_posibles else None

def validar_zona_upz(zona, upz):
    """Valida consistencia Zona-UPZ"""
    upz_zonas_validas = {
        "32 - san blas": ["zona 1", "zona 2"],
        "33 - sosiego": ["zona 6"],
        "34 - 20 de julio": ["zona 4"],
        "50 - la gloria": ["zona 3", "zona 8"],
        "51 - los libertadores": ["zona 5", "zona 7"]
    }
    
    if pd.isna(zona) or pd.isna(upz):
        return True, None
    
    zona_norm = normalizar(zona)
    upz_norm = normalizar(upz)
    
    if upz_norm in upz_zonas_validas:
        zonas_esperadas = upz_zonas_validas[upz_norm]
        if zona_norm in zonas_esperadas:
            return True, None
        else:
            return False, f"Inconsistencia: {zona} no corresponde con {upz}"
    
    return True, None

# =====================================================================
# APLICAR EXTRACCIÓN
# =====================================================================
print("\n🔍 Extrayendo barrios de direcciones...")

# Crear columnas nuevas
df['Barrio_Extraido'] = None
df['UPZ_Enriquecida'] = df['Nombre_UPZ']
df['Zona_Enriquecida'] = df['Zona']
df['Zonas_Posibles'] = None
df['Metodo_Extraccion'] = None
df['Validacion_Zona_UPZ'] = None
df['Observaciones'] = None

extracciones_exitosas = 0
upz_completadas = 0
zona_completadas = 0

for idx, row in df.iterrows():
    direccion = row['Direccion_Actividad']
    
    # Extraer barrio
    barrio, upz_ext, zonas_ext, metodo = extraer_barrio(direccion)
    
    if barrio:
        df.at[idx, 'Barrio_Extraido'] = barrio.title()
        df.at[idx, 'Metodo_Extraccion'] = metodo
        extracciones_exitosas += 1
        
        # Guardar zonas posibles
        if zonas_ext:
            df.at[idx, 'Zonas_Posibles'] = ', '.join(zonas_ext)
        
        # Completar UPZ si está vacía
        upz_actual = row['Nombre_UPZ']
        if pd.isna(upz_actual) or str(upz_actual).strip() == "":
            df.at[idx, 'UPZ_Enriquecida'] = upz_ext
            upz_completadas += 1

# =====================================================================
# CORRECCIÓN MASIVA DE ZONAS DUPLICADAS (TODOS LOS REGISTROS)
# =====================================================================
print("\n🔧 Corrigiendo todas las zonas duplicadas...")

zonas_corregidas = 0

for idx, row in df.iterrows():
    zona_actual = row['Zona_Enriquecida']
    upz_enriquecida = row['UPZ_Enriquecida']
    barrio = row['Barrio_Extraido']
    
    # Si la zona tiene coma (duplicada)
    if pd.notna(zona_actual) and ',' in str(zona_actual):
        # Parsear las zonas
        zonas_duplicadas = [z.strip() for z in str(zona_actual).split(',')]
        
        # Corregir usando UPZ + Barrio (si existe)
        zona_corregida = seleccionar_zona_correcta(barrio, upz_enriquecida, zonas_duplicadas)
        
        if zona_corregida:
            df.at[idx, 'Zona_Enriquecida'] = zona_corregida
            if barrio:
                df.at[idx, 'Observaciones'] = f"Zona duplicada corregida con barrio: {zona_actual} → {zona_corregida}"
            else:
                df.at[idx, 'Observaciones'] = f"Zona duplicada corregida con UPZ: {zona_actual} → {zona_corregida}"
            zonas_corregidas += 1
            zona_completadas += 1

print(f"✅ Zonas duplicadas corregidas: {zonas_corregidas}")

# =====================================================================
# VALIDACIÓN ZONA-UPZ
# =====================================================================
print("\n🔍 Validando consistencia Zona-UPZ...")

inconsistencias = 0

for idx, row in df.iterrows():
    zona_final = df.at[idx, 'Zona_Enriquecida']
    upz_final = df.at[idx, 'UPZ_Enriquecida']
    
    es_valido, mensaje = validar_zona_upz(zona_final, upz_final)
    
    if not es_valido:
        df.at[idx, 'Validacion_Zona_UPZ'] = 'Inconsistente'
        if not pd.notna(df.at[idx, 'Observaciones']):
            df.at[idx, 'Observaciones'] = mensaje
        inconsistencias += 1
    else:
        df.at[idx, 'Validacion_Zona_UPZ'] = 'Válido'

# =====================================================================
# ESTADÍSTICAS
# =====================================================================
print("\n" + "="*80)
print("📊 RESULTADOS DE EXTRACCIÓN")
print("="*80)

print(f"\n✅ Barrios extraídos: {extracciones_exitosas}/{len(df)} ({extracciones_exitosas/len(df)*100:.1f}%)")
print(f"✅ UPZ completadas: {upz_completadas}")
print(f"✅ Zonas completadas/corregidas: {zona_completadas}")
print(f"⚠️  Inconsistencias finales: {inconsistencias}")

# Desglose por método
if 'Metodo_Extraccion' in df.columns:
    print("\n📋 Por método de extracción:")
    metodos = df['Metodo_Extraccion'].value_counts()
    for metodo, count in metodos.items():
        print(f"   {metodo}: {count} ({count/len(df)*100:.1f}%)")

# =====================================================================
# AGREGAR ID_ACTIVIDAD CON PREFIJO DE HOJA
# =====================================================================
print("\n🔢 Agregando ID_Actividad con prefijo de hoja...")

# Crear ID basado en Hoja_Origen y número de fila
def generar_id(row, idx_por_hoja):
    hoja = row.get('Hoja_Origen', 'Hoja 1')
    
    # Determinar prefijo
    if 'formulario 1' in str(hoja).lower() or hoja == 'Hoja 1':
        prefijo = 'H1'
    elif 'formulario 2' in str(hoja).lower() or hoja == 'Hoja 2':
        prefijo = 'H2'
    else:
        prefijo = 'H1'  # Default
    
    # Obtener número de fila para esta hoja
    numero = idx_por_hoja[hoja]
    idx_por_hoja[hoja] += 1
    
    return f"{prefijo}_{numero}"

# Contador por hoja
idx_por_hoja = {}
for hoja in df['Hoja_Origen'].unique():
    idx_por_hoja[hoja] = 1

# Generar IDs
df.insert(0, 'ID_Actividad', df.apply(lambda row: generar_id(row, idx_por_hoja), axis=1))

print(f"✅ ID_Actividad agregado con prefijos H1_ y H2_")
print(f"   Hoja 1: {(df['ID_Actividad'].str.startswith('H1_')).sum()} registros")
print(f"   Hoja 2: {(df['ID_Actividad'].str.startswith('H2_')).sum()} registros")

# =====================================================================
# GUARDAR CON DELIMITADOR PUNTO Y COMA
# =====================================================================
df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8', sep=';')
print(f"\n💾 Guardado: {OUTPUT_FILE}")

# Calidad de datos
print("\n📊 Calidad de datos:")
total = len(df)
completitud = {
    'Barrio': f"{(df['Barrio_Extraido'].notna().sum()/total*100):.1f}%",
    'UPZ': f"{(df['UPZ_Enriquecida'].notna().sum()/total*100):.1f}%",
    'Zona': f"{(df['Zona_Enriquecida'].notna().sum()/total*100):.1f}%",
    'Validación': f"{((df['Validacion_Zona_UPZ'] == 'Válido').sum()/total*100):.1f}%"
}

for campo, porcentaje in completitud.items():
    print(f"   {campo}: {porcentaje}")

print("\n" + "="*80)
print("✅ ENRIQUECIMIENTO COMPLETADO CON ÉXITO")
print("="*80)