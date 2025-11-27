#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnóstico: Comparar conteos Google Sheets vs archivos locales
"""

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path

# Rutas
BASE_DIR = Path(__file__).resolve().parents[1]
CREDS_FILE = BASE_DIR / "config" / "credentials.json"

print("\n" + "="*80)
print("🔍 DIAGNÓSTICO DE CONTEO DE REGISTROS")
print("="*80)

# =====================================================================
# 1. CONTAR EN GOOGLE SHEETS
# =====================================================================
print("\n📊 CONTANDO EN GOOGLE SHEETS...")

try:
    # Autenticar
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/drive.readonly'
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    
    # Abrir spreadsheet (reemplaza con tu ID)
    SPREADSHEET_ID = "TU_ID_DE_SPREADSHEET"  # NECESITAS PONER EL ID CORRECTO
    sh = gc.open_by_key(SPREADSHEET_ID)
    
    # Hoja 1
    hoja1 = sh.worksheet("Respuestas de formulario 1")
    datos_h1 = hoja1.get_all_values()
    count_h1 = len(datos_h1) - 1  # -1 por el encabezado
    
    # Hoja 2
    hoja2 = sh.worksheet("Respuestas de formulario 2")
    datos_h2 = hoja2.get_all_values()
    count_h2 = len(datos_h2) - 1
    
    print(f"✅ Hoja 1: {count_h1} registros")
    print(f"✅ Hoja 2: {count_h2} registros")
    print(f"📊 TOTAL GOOGLE SHEETS: {count_h1 + count_h2} registros")
    
except Exception as e:
    print(f"❌ Error al leer Google Sheets: {e}")
    count_h1 = None
    count_h2 = None

# =====================================================================
# 2. CONTAR EN ARCHIVOS LOCALES
# =====================================================================
print("\n📁 CONTANDO EN ARCHIVOS LOCALES...")

archivos = {
    "fact_actividades.csv": BASE_DIR / "fact_actividades.csv",
    "fact_actividades_limpio.csv": BASE_DIR / "fact_actividades_limpio.csv",
    "fact_actividades_enriquecido.csv": BASE_DIR / "fact_actividades_enriquecido.csv"
}

for nombre, ruta in archivos.items():
    if ruta.exists():
        df = pd.read_csv(ruta, encoding='utf-8')
        print(f"✅ {nombre}: {len(df)} registros")
        
        # Verificar estrategia
        if 'Estrategia' in df.columns:
            sin_estrategia = df['Estrategia'].isna().sum() + (df['Estrategia'] == 'Sin estrategia').sum()
            print(f"   ⚠️  Sin estrategia: {sin_estrategia}")
    else:
        print(f"❌ {nombre}: No existe")

# =====================================================================
# 3. BUSCAR DUPLICADOS
# =====================================================================
print("\n🔍 BUSCANDO DUPLICADOS EN fact_actividades_enriquecido.csv...")

archivo_enriquecido = BASE_DIR / "fact_actividades_enriquecido.csv"
if archivo_enriquecido.exists():
    df = pd.read_csv(archivo_enriquecido, encoding='utf-8')
    
    # Duplicados por ID_Actividad
    if 'ID_Actividad' in df.columns:
        duplicados_id = df['ID_Actividad'].duplicated().sum()
        print(f"⚠️  Duplicados por ID_Actividad: {duplicados_id}")
    
    # Duplicados por contenido completo
    duplicados_completos = df.duplicated().sum()
    print(f"⚠️  Filas completamente duplicadas: {duplicados_completos}")
    
    # Mostrar algunos duplicados si existen
    if duplicados_completos > 0:
        print("\n📋 Muestra de duplicados:")
        dups = df[df.duplicated(keep=False)].sort_values('ID_Actividad').head(10)
        print(dups[['ID_Actividad', 'Nombre_Actividad', 'Fecha_Actividad']].to_string())

print("\n" + "="*80)
print("✅ DIAGNÓSTICO COMPLETADO")
print("="*80)
