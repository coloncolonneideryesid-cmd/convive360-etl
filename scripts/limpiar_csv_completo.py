#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Limpia fact_actividades_limpio.csv:
- Remueve BOM
- Corrige encoding
- Valida estructura de columnas
"""

import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_FILE = BASE_DIR / "fact_actividades_limpio.csv"
OUTPUT_FILE = BASE_DIR / "fact_actividades_limpio_fixed.csv"

print("🔧 Limpiando archivo CSV...")

# Intentar con utf-8-sig (maneja BOM automáticamente)
try:
    df = pd.read_csv(
        INPUT_FILE, 
        encoding='utf-8-sig',  # Maneja BOM automáticamente
        quotechar='"',
        escapechar='\\',
        on_bad_lines='warn',
        low_memory=False
    )
    print(f"✅ Leído con utf-8-sig: {len(df)} registros")
except Exception as e:
    print(f"⚠️  Error con utf-8-sig: {e}")
    print("🔧 Intentando con latin-1...")
    df = pd.read_csv(
        INPUT_FILE, 
        encoding='latin-1',
        quotechar='"',
        escapechar='\\',
        on_bad_lines='warn',
        low_memory=False
    )
    print(f"✅ Leído con latin-1: {len(df)} registros")

print(f"\n📊 Columnas encontradas ({len(df.columns)}):")
for i, col in enumerate(df.columns, 1):
    print(f"  {i:2}. {col}")

# Verificar columnas esperadas
columnas_esperadas = ['Zona', 'Nombre_UPZ', 'Zonas_Asignadas', 'Direccion_Actividad']
columnas_faltantes = [c for c in columnas_esperadas if c not in df.columns]

if columnas_faltantes:
    print(f"\n⚠️  Columnas faltantes: {columnas_faltantes}")
else:
    print("\n✅ Todas las columnas críticas están presentes")

# Guardar con delimitador punto y coma y UTF-8 limpio
df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8', sep=';')

print(f"\n💾 Guardado: {OUTPUT_FILE}")
print(f"\n📊 Muestra de datos en columna Zona:")
print(df['Zona'].value_counts().head(15))

print(f"\n📊 Total de zonas vacías: {df['Zona'].isna().sum()}")
print(f"📊 Total de zonas con coma: {df['Zona'].astype(str).str.contains(',').sum()}")

print("\n✅ Limpieza completada")
