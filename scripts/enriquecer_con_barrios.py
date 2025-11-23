#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enriquece fact_actividades con barrios extraídos desde direcciones
Usa 4 métodos de extracción + validación Zona-UPZ
"""

import pandas as pd
import json
import re
from pathlib import Path
from difflib import get_close_matches

# Rutas
BASE_DIR = Path(__file__).resolve().parents[1]
DICT_FILE = BASE_DIR / "scripts" / "diccionario_barrios_completo.json"
FACT_FILE = BASE_DIR / "fact_actividades_limpio.csv"
OUTPUT_FILE = BASE_DIR / "fact_actividades_enriquecido.csv"

print("\n" + "="*80)
print("🔍 ENRIQUECIENDO ACTIVIDADES CON BARRIOS")
print("="*80)

# =====================================================================
# CARGAR DATOS
# =====================================================================
print("\n📥 Cargando datos...")

# Cargar fact_actividades
df = pd.read_csv(FACT_FILE, encoding='utf-8')
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
inconsistencias = 0

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
        
        # Completar Zona si está vacía
        zona_actual = row['Zona']
        if pd.isna(zona_actual) or str(zona_actual).strip() == "":
            if zonas_ext and len(zonas_ext) > 0:
                # Tomar primera zona si hay múltiples
                df.at[idx, 'Zona_Enriquecida'] = zonas_ext[0]
                zona_completadas += 1
                if len(zonas_ext) > 1:
                    df.at[idx, 'Observaciones'] = f"Barrio en múltiples zonas: {', '.join(zonas_ext)}"
    
    # Validar consistencia Zona-UPZ
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
# ESTADÍSTICAS Y GUARDADO
# =====================================================================
print("\n" + "="*80)
print("📊 RESULTADOS DE EXTRACCIÓN")
print("="*80)

print(f"\n✅ Barrios extraídos: {extracciones_exitosas}/{len(df)} ({extracciones_exitosas/len(df)*100:.1f}%)")
print(f"✅ UPZ completadas: {upz_completadas}")
print(f"✅ Zonas completadas: {zona_completadas}")
print(f"⚠️  Inconsistencias: {inconsistencias}")

# Desglose por método
if 'Metodo_Extraccion' in df.columns:
    print("\n📋 Por método de extracción:")
    metodos = df['Metodo_Extraccion'].value_counts()
    for metodo, count in metodos.items():
        print(f"   {metodo}: {count} ({count/len(df)*100:.1f}%)")

# Guardar
df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
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
