#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mejora la extracción de barrios usando UPZ/Zona cuando no hay barrio en dirección
"""

import pandas as pd
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
FACT_FILE = BASE_DIR / "fact_actividades_enriquecido.csv"
DICT_BARRIOS = BASE_DIR / "scripts" / "diccionario_barrios_completo.json"

print("\n" + "="*80)
print("🔧 MEJORANDO EXTRACCIÓN DE BARRIOS USANDO UPZ/ZONA")
print("="*80)

# Cargar datos
df = pd.read_csv(FACT_FILE, encoding='utf-8', sep=';')
print(f"✅ Cargadas {len(df)} actividades")

with open(DICT_BARRIOS, 'r', encoding='utf-8') as f:
    dict_barrios = json.load(f)

barrios_por_upz = dict_barrios['barrios_por_upz']
barrios_por_zona = dict_barrios['barrios_por_zona']

# Análisis inicial
sin_barrio = df['Barrio_Extraido'].isna().sum()
print(f"⚠️  Actividades sin barrio: {sin_barrio}/{len(df)} ({sin_barrio/len(df)*100:.1f}%)")

# =====================================================================
# ESTRATEGIA DE ASIGNACIÓN
# =====================================================================
print("\n📋 Aplicando estrategias de asignación...")

asignaciones = {
    'upz_unico': 0,
    'zona_upz': 0,
    'zona_unica': 0,
    'sin_asignar': 0
}

for idx, row in df.iterrows():
    # Si ya tiene barrio, continuar
    if pd.notna(row['Barrio_Extraido']):
        continue
    
    upz = row.get('UPZ_Enriquecida') or row.get('Nombre_UPZ')
    zona = row.get('Zona_Enriquecida') or row.get('Zona')
    
    # Normalizar UPZ (manejo de mayúsculas)
    if pd.notna(upz):
        upz_norm = None
        for upz_key in barrios_por_upz.keys():
            if upz.upper() in upz_key.upper():
                upz_norm = upz_key
                break
    else:
        upz_norm = None
    
    # ESTRATEGIA 1: UPZ con un solo barrio principal
    if upz_norm and upz_norm in barrios_por_upz:
        barrios = barrios_por_upz[upz_norm]
        if len(barrios) == 1:
            df.at[idx, 'Barrio_Extraido'] = barrios[0]
            df.at[idx, 'Metodo_Extraccion'] = 'UPZ Único'
            asignaciones['upz_unico'] += 1
            continue
    
    # ESTRATEGIA 2: Cruce Zona-UPZ (barrio más común)
    if pd.notna(zona) and upz_norm:
        # Obtener barrios de la zona
        zona_norm = f"ZONA {zona.split()[-1]}" if 'zona' not in zona.lower() else zona.upper()
        barrios_zona = barrios_por_zona.get(zona_norm, [])
        
        # Filtrar por UPZ
        barrios_upz = barrios_por_upz.get(upz_norm, [])
        
        # Intersección
        barrios_comunes = [b for b in barrios_zona if b in barrios_upz]
        
        if len(barrios_comunes) == 1:
            df.at[idx, 'Barrio_Extraido'] = barrios_comunes[0]
            df.at[idx, 'Metodo_Extraccion'] = 'Zona-UPZ'
            asignaciones['zona_upz'] += 1
            continue
        elif len(barrios_comunes) > 1:
            # Tomar el primero (más representativo)
            df.at[idx, 'Barrio_Extraido'] = barrios_comunes[0]
            df.at[idx, 'Metodo_Extraccion'] = 'Zona-UPZ (Múltiple)'
            df.at[idx, 'Observaciones'] = f"Posibles: {', '.join(barrios_comunes[:3])}"
            asignaciones['zona_upz'] += 1
            continue
    
    # ESTRATEGIA 3: Solo zona (barrio más representativo)
    if pd.notna(zona):
        zona_norm = f"ZONA {zona.split()[-1]}" if 'zona' not in zona.lower() else zona.upper()
        barrios_zona = barrios_por_zona.get(zona_norm, [])
        
        if len(barrios_zona) == 1:
            df.at[idx, 'Barrio_Extraido'] = barrios_zona[0]
            df.at[idx, 'Metodo_Extraccion'] = 'Zona Única'
            asignaciones['zona_unica'] += 1
            continue
    
    # No se pudo asignar
    asignaciones['sin_asignar'] += 1

# =====================================================================
# RESULTADOS
# =====================================================================
print("\n" + "="*80)
print("📊 RESULTADOS DE MEJORA")
print("="*80)

barrios_final = df['Barrio_Extraido'].notna().sum()
print(f"\n✅ Barrios totales: {barrios_final}/{len(df)} ({barrios_final/len(df)*100:.1f}%)")
print(f"   📈 Mejora: +{barrios_final - (len(df) - sin_barrio)} barrios")

print("\n📋 Por estrategia de asignación:")
for estrategia, count in asignaciones.items():
    if count > 0:
        print(f"   {estrategia}: {count} ({count/len(df)*100:.1f}%)")

print("\n📋 Por método de extracción (todos):")
metodos = df['Metodo_Extraccion'].value_counts()
for metodo, count in metodos.items():
    print(f"   {metodo}: {count} ({count/len(df)*100:.1f}%)")

# Guardar
output_file = BASE_DIR / "fact_actividades_enriquecido.csv"
df.to_csv(output_file, index=False, encoding='utf-8')
print(f"\n💾 Guardado: {output_file}")

print("\n" + "="*80)
print("✅ MEJORA COMPLETADA")
print("="*80)
