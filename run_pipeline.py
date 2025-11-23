#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline ETL Completo - Convive360
Orquesta todas las fases del proceso
"""

import sys
import subprocess
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent

print("\n" + "="*80)
print("🚀 INICIANDO PIPELINE ETL COMPLETO - CONVIVE360")
print("="*80)
print(f"📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📂 Directorio: {BASE_DIR}")
print("="*80)

def ejecutar_script(script_path, descripcion):
    """Ejecuta un script y maneja errores"""
    print(f"\n>>> {descripcion}...")
    try:
        resultado = subprocess.run(
            [sys.executable, str(script_path)],
            check=True,
            capture_output=True,
            text=True
        )
        print(f"✓ {descripcion} completado")
        if resultado.stdout:
            print(resultado.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error en {descripcion}")
        print(f"   {e.stderr}")
        return False
    except Exception as e:
        print(f"❌ Error inesperado en {descripcion}: {e}")
        return False

# =====================================================================
# FASE 1: EXTRACCIÓN
# =====================================================================
print("\n" + "="*80)
print("📥 FASE 1: EXTRACCIÓN DE DATOS")
print("="*80)

if not ejecutar_script(
    BASE_DIR / "scripts" / "descargar_google_sheets.py",
    "Descarga desde Google Sheets"
):
    sys.exit(1)

# =====================================================================
# FASE 2: LIMPIEZA Y TRANSFORMACIÓN
# =====================================================================
print("\n" + "="*80)
print("🧹 FASE 2: LIMPIEZA Y TRANSFORMACIÓN")
print("="*80)

if not ejecutar_script(
    BASE_DIR / "scripts" / "limpiar_datos.py",
    "Limpieza de datos"
):
    sys.exit(1)

if not ejecutar_script(
    BASE_DIR / "scripts" / "validar_upz_zonas.py",
    "Validación UPZ y Zonas"
):
    sys.exit(1)

if not ejecutar_script(
    BASE_DIR / "scripts" / "deduplicar_actividades.py",
    "Deduplicación de actividades"
):
    sys.exit(1)

# =====================================================================
# FASE 3: ENRIQUECIMIENTO CON BARRIOS
# =====================================================================
print("\n" + "="*80)
print("📍 FASE 3: ENRIQUECIMIENTO CON BARRIOS")
print("="*80)

# Crear diccionario de barrios (solo primera vez o si no existe)
dict_file = BASE_DIR / "scripts" / "diccionario_barrios_completo.json"
if not dict_file.exists():
    print("📚 Creando diccionario de barrios...")
    if not ejecutar_script(
        BASE_DIR / "scripts" / "crear_diccionario_barrios.py",
        "Creación de diccionario de barrios"
    ):
        print("⚠️  Continuando sin diccionario completo...")
else:
    print("✓ Diccionario de barrios ya existe")

# Enriquecer actividades con barrios
if not ejecutar_script(
    BASE_DIR / "scripts" / "enriquecer_con_barrios.py",
    "Enriquecimiento con barrios"
):
    print("⚠️  Continuando sin enriquecimiento de barrios...")

# =====================================================================
# FASE 4: GENERACIÓN DE DIMENSIONES
# =====================================================================
print("\n" + "="*80)
print("📊 FASE 4: GENERACIÓN DE DIMENSIONES")
print("="*80)

if not ejecutar_script(
    BASE_DIR / "scripts" / "generar_dim_fecha.py",
    "Generación de dim_fecha"
):
    sys.exit(1)

if not ejecutar_script(
    BASE_DIR / "scripts" / "generar_dimensiones.py",
    "Generación de dimensiones restantes"
):
    sys.exit(1)

# =====================================================================
# FASE 5: MODELO DIMENSIONAL COMPLETO
# =====================================================================
print("\n" + "="*80)
print("🎯 FASE 5: MODELO DIMENSIONAL COMPLETO")
print("="*80)

if not ejecutar_script(
    BASE_DIR / "scripts" / "generar_modelo_completo.py",
    "Generación de modelo dimensional completo"
):
    print("⚠️  Continuando sin modelo completo...")

# =====================================================================
# FASE 6: VERIFICACIÓN Y REPORTE
# =====================================================================
print("\n" + "="*80)
print("✅ FASE 6: VERIFICACIÓN Y REPORTE")
print("="*80)

if not ejecutar_script(
    BASE_DIR / "scripts" / "verificar_datos.py",
    "Verificación de integridad"
):
    print("⚠️  Continuando sin verificación completa...")

if not ejecutar_script(
    BASE_DIR / "scripts" / "generar_reporte.py",
    "Generación de reporte final"
):
    print("⚠️  Continuando sin reporte...")

# =====================================================================
# RESUMEN FINAL
# =====================================================================
print("\n" + "="*80)
print("🎉 PIPELINE COMPLETADO CON ÉXITO")
print("="*80)

# Contar archivos generados
dimensiones_dir = BASE_DIR / "dimensiones"
if dimensiones_dir.exists():
    archivos = list(dimensiones_dir.glob("*.csv"))
    print(f"\n📊 Archivos generados en dimensiones/: {len(archivos)}")
    for archivo in sorted(archivos):
        print(f"   ✓ {archivo.name}")

fact_file = BASE_DIR / "fact_actividades_limpio.csv"
if fact_file.exists():
    import pandas as pd
    df = pd.read_csv(fact_file)
    print(f"\n📄 fact_actividades_limpio.csv: {len(df)} registros")

enriquecido_file = BASE_DIR / "fact_actividades_enriquecido.csv"
if enriquecido_file.exists():
    df_enr = pd.read_csv(enriquecido_file)
    barrios_extraidos = df_enr['Barrio_Extraido'].notna().sum()
    print(f"📄 fact_actividades_enriquecido.csv: {barrios_extraidos} con barrio ({barrios_extraidos/len(df_enr)*100:.1f}%)")

print("\n" + "="*80)
print(f"⏱️  Completado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)
