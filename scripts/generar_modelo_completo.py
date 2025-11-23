#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para generar TODAS las tablas dimensionales y de hechos faltantes
a partir de fact_actividades_limpio.csv y diccionarios
"""

import pandas as pd
import json
import re
import hashlib
from pathlib import Path
from datetime import datetime

# =====================================================================
# CONFIGURACIÓN DE RUTAS (CORREGIDO)
# =====================================================================
BASE_DIR = Path(__file__).resolve().parent.parent  # Raíz del proyecto
DIMENSIONES_DIR = BASE_DIR / "dimensiones"
SCRIPTS_DIR = BASE_DIR / "scripts"

# Crear carpeta si no existe
DIMENSIONES_DIR.mkdir(exist_ok=True)

# Archivos de entrada
FACT_ACTIVIDADES = BASE_DIR / "fact_actividades_limpio.csv"
DICT_UPZ_ZONAS = SCRIPTS_DIR / "diccionario_upz_zonas.json"

print("\n" + "="*80)
print("🚀 GENERANDO MODELO DIMENSIONAL COMPLETO PARA POWER BI")
print("="*80)

# =====================================================================
# 1. CARGAR DATOS BASE
# =====================================================================
print("\n📥 Cargando datos base...")

if not FACT_ACTIVIDADES.exists():
    print(f"❌ ERROR: No se encontró {FACT_ACTIVIDADES}")
    exit(1)

df_actividades = pd.read_csv(FACT_ACTIVIDADES, encoding='utf-8-sig')
print(f"✅ fact_actividades_limpio.csv cargado: {len(df_actividades)} registros")

with open(DICT_UPZ_ZONAS, 'r', encoding='utf-8') as f:
    upz_zonas = json.load(f)
print(f"✅ Diccionario UPZ-Zonas cargado: {len(upz_zonas)} UPZ")

# Agregar ID_Actividad único si no existe
if 'ID_Actividad' not in df_actividades.columns:
    print("⚠️  Generando ID_Actividad único...")
    df_actividades['ID_Actividad'] = df_actividades.apply(
        lambda row: hashlib.md5(
            f"{row['Nombre_Actividad']}{row['Fecha_Actividad']}{row['Hora_Inicio']}{row['Responsable_Principal']}".encode()
        ).hexdigest()[:12].upper(),
        axis=1
    )
    # Guardar fact_actividades con ID
    df_actividades.to_csv(FACT_ACTIVIDADES, index=False, encoding='utf-8-sig')
    print(f"✅ ID_Actividad generado para {len(df_actividades)} registros")

# =====================================================================
# 2. GENERAR DIM_ZONAS
# =====================================================================
print("\n" + "="*80)
print("📊 GENERANDO dim_zonas.csv")
print("="*80)

zonas_data = []
for i in range(1, 9):
    zonas_data.append({
        'ID_Zona': i,
        'Zona': f'Zona {i}',
        'Nombre_Zona': f'Zona {i}',
        'Numero_Zona': i
    })

df_dim_zonas = pd.DataFrame(zonas_data)
output_path = DIMENSIONES_DIR / "dim_zonas.csv"
df_dim_zonas.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ dim_zonas.csv generado ({len(df_dim_zonas)} zonas)")

# =====================================================================
# 3. GENERAR DIM_UPZ
# =====================================================================
print("\n" + "="*80)
print("📊 GENERANDO dim_upz.csv")
print("="*80)

upz_data = []
upz_info = {
    "32 - SAN BLAS": {"codigo": 32, "nombre": "San Blas", "zona_principal": "Zona 1"},
    "33 - SOSIEGO": {"codigo": 33, "nombre": "Sosiego", "zona_principal": "Zona 6"},
    "34 - 20 DE JULIO": {"codigo": 34, "nombre": "20 de Julio", "zona_principal": "Zona 4"},
    "50 - LA GLORIA": {"codigo": 50, "nombre": "La Gloria", "zona_principal": "Zona 3"},
    "51 - LOS LIBERTADORES": {"codigo": 51, "nombre": "Los Libertadores", "zona_principal": "Zona 5"}
}

for upz_key, info in upz_info.items():
    # Buscar en diccionario (mayúsculas y minúsculas)
    zonas_asoc = []
    for key in upz_zonas.keys():
        if str(info['codigo']) in key or info['nombre'].upper() in key.upper():
            zonas_asoc = upz_zonas[key]
            break
    
    upz_data.append({
        'ID_UPZ': info['codigo'],
        'UPZ': upz_key,
        'Codigo_UPZ': info['codigo'],
        'Nombre_UPZ': info['nombre'],
        'Zona_Principal': info['zona_principal'],
        'Zonas_Asociadas': ', '.join(zonas_asoc) if zonas_asoc else ''
    })

df_dim_upz = pd.DataFrame(upz_data)
output_path = DIMENSIONES_DIR / "dim_upz.csv"
df_dim_upz.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ dim_upz.csv generado ({len(df_dim_upz)} UPZ)")

# =====================================================================
# 4. GENERAR BRIDGE_UPZ_ZONAS
# =====================================================================
print("\n" + "="*80)
print("📊 GENERANDO bridge_upz_zonas.csv")
print("="*80)

bridge_upz_zonas_data = []
for upz_key, info in upz_info.items():
    # Buscar zonas asociadas
    zonas_asoc = []
    for key in upz_zonas.keys():
        if str(info['codigo']) in key:
            zonas_asoc = upz_zonas[key]
            break
    
    for zona in zonas_asoc:
        bridge_upz_zonas_data.append({
            'UPZ': upz_key,
            'Codigo_UPZ': info['codigo'],
            'Zona': zona,
            'Es_Zona_Principal': 'Sí' if zona == info['zona_principal'] else 'No'
        })

df_bridge_upz_zonas = pd.DataFrame(bridge_upz_zonas_data)
output_path = DIMENSIONES_DIR / "bridge_upz_zonas.csv"
df_bridge_upz_zonas.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ bridge_upz_zonas.csv generado ({len(df_bridge_upz_zonas)} relaciones)")

# =====================================================================
# 5. GENERAR DIM_BARRIOS (desde fact_actividades)
# =====================================================================
print("\n" + "="*80)
print("📊 GENERANDO dim_barrios.csv")
print("="*80)

def extraer_barrio_mejorado(direccion):
    """Extrae posible nombre de barrio de dirección"""
    if pd.isna(direccion):
        return None
    direccion = str(direccion).upper()
    
    # Patrones de búsqueda
    patrones = [
        r'BARRIO\s+([A-ZÁÉÍÓÚÑ\s]+?)(?:\s+|$|,)',
        r'BRR\.?\s+([A-ZÁÉÍÓÚÑ\s]+?)(?:\s+|$|,)',
        r'B\.?\s+([A-ZÁÉÍÓÚÑ\s]+?)(?:\s+|$|,)',
    ]
    
    for patron in patrones:
        match = re.search(patron, direccion)
        if match:
            barrio = match.group(1).strip()
            # Limpiar palabras comunes que no son barrios
            if len(barrio) > 3 and barrio not in ['SUR', 'ESTE', 'OESTE', 'NORTE']:
                return barrio
    
    return None

# Extraer barrios únicos
barrios_encontrados = set()
for _, row in df_actividades.iterrows():
    barrio = extraer_barrio_mejorado(row.get('Direccion_Actividad', ''))
    if barrio:
        barrios_encontrados.add(barrio)

# Agregar barrios desde otras fuentes si existen
if 'Barrio' in df_actividades.columns:
    barrios_adicionales = df_actividades['Barrio'].dropna().unique()
    barrios_encontrados.update(barrios_adicionales)

barrios_data = []
for i, barrio in enumerate(sorted(barrios_encontrados), start=1):
    barrios_data.append({
        'ID_Barrio': i,
        'Barrio': barrio.title(),
        'Barrio_Normalizado': barrio.lower().strip()
    })

df_dim_barrios = pd.DataFrame(barrios_data) if barrios_data else pd.DataFrame(columns=['ID_Barrio', 'Barrio', 'Barrio_Normalizado'])
output_path = DIMENSIONES_DIR / "dim_barrios.csv"
df_dim_barrios.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ dim_barrios.csv generado ({len(df_dim_barrios)} barrios)")

# =====================================================================
# 6. GENERAR DIM_ESTRATEGIAS
# =====================================================================
print("\n" + "="*80)
print("📊 GENERANDO dim_estrategias.csv")
print("="*80)

estrategias_data = [
    {'ID_Estrategia': 1, 'Estrategia': 'Seguridad', 'Descripcion': 'Actividades de Seguridad Ciudadana'},
    {'ID_Estrategia': 2, 'Estrategia': 'Convivencia', 'Descripcion': 'Actividades de Convivencia y Reconciliación'},
    {'ID_Estrategia': 3, 'Estrategia': 'Justicia', 'Descripcion': 'Actividades de Justicia y Acceso a Derechos'}
]

df_dim_estrategias = pd.DataFrame(estrategias_data)
output_path = DIMENSIONES_DIR / "dim_estrategias.csv"
df_dim_estrategias.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ dim_estrategias.csv generado ({len(df_dim_estrategias)} estrategias)")

# =====================================================================
# 7. GENERAR FACT_ESTRATEGIAS (desde columna Estrategia_Impactar)
# =====================================================================
print("\n" + "="*80)
print("📊 GENERANDO fact_estrategias.csv")
print("="*80)

fact_estrategias_rows = []

for _, row in df_actividades.iterrows():
    id_actividad = row['ID_Actividad']
    estrategias_str = row.get('Estrategia_Impactar', '')
    
    if pd.notna(estrategias_str) and estrategias_str.strip():
        # Normalizar y obtener ID
        estrategia_norm = estrategias_str.strip().title()
        
        id_estrategia = None
        if 'seguridad' in estrategia_norm.lower():
            id_estrategia = 1
        elif 'convivencia' in estrategia_norm.lower():
            id_estrategia = 2
        elif 'justicia' in estrategia_norm.lower():
            id_estrategia = 3
        
        if id_estrategia:
            fact_estrategias_rows.append({
                'ID_Actividad': id_actividad,
                'ID_Estrategia': id_estrategia,
                'Estrategia': estrategia_norm
            })

df_fact_estrategias = pd.DataFrame(fact_estrategias_rows)
output_path = DIMENSIONES_DIR / "fact_estrategias.csv"
df_fact_estrategias.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ fact_estrategias.csv generado ({len(df_fact_estrategias)} relaciones)")

# =====================================================================
# 8. GENERAR FACT_LINEAS (desde Linea_Seguridad, Linea_Convivencia, Linea_Justicia)
# =====================================================================
print("\n" + "="*80)
print("📊 GENERANDO fact_lineas.csv")
print("="*80)

fact_lineas_rows = []

columnas_lineas = {
    1: 'Linea_Seguridad',
    2: 'Linea_Convivencia',
    3: 'Linea_Justicia'
}

for id_estrategia, col_linea in columnas_lineas.items():
    if col_linea in df_actividades.columns:
        for _, row in df_actividades.iterrows():
            id_actividad = row['ID_Actividad']
            lineas_str = row.get(col_linea, '')
            
            if pd.notna(lineas_str) and lineas_str.strip() and lineas_str.strip().upper() != 'N/A':
                # Separar por coma si hay múltiples
                lineas_lista = [l.strip() for l in str(lineas_str).split(',') if l.strip()]
                
                for linea in lineas_lista:
                    if linea and linea.upper() != 'N/A':
                        fact_lineas_rows.append({
                            'ID_Actividad': id_actividad,
                            'ID_Estrategia': id_estrategia,
                            'Linea_Estrategica': linea.strip()
                        })

df_fact_lineas = pd.DataFrame(fact_lineas_rows)
output_path = DIMENSIONES_DIR / "fact_lineas.csv"
df_fact_lineas.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ fact_lineas.csv generado ({len(df_fact_lineas)} líneas)")

# =====================================================================
# 9. GENERAR BRIDGE_BARRIOS_ZONAS (desde fact_actividades)
# =====================================================================
print("\n" + "="*80)
print("📊 GENERANDO bridge_barrios_zonas.csv")
print("="*80)

bridge_barrios_zonas_rows = []

# Extraer relaciones barrio-zona desde direcciones y zonas asignadas
for _, row in df_actividades.iterrows():
    barrio = extraer_barrio_mejorado(row.get('Direccion_Actividad', ''))
    zona = row.get('Zona', '')
    
    if barrio and pd.notna(zona) and zona.strip():
        bridge_barrios_zonas_rows.append({
            'Barrio': barrio.title(),
            'Zona': zona.strip()
        })

# Eliminar duplicados
df_bridge_barrios_zonas = pd.DataFrame(bridge_barrios_zonas_rows).drop_duplicates()
output_path = DIMENSIONES_DIR / "bridge_barrios_zonas.csv"
df_bridge_barrios_zonas.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ bridge_barrios_zonas.csv generado ({len(df_bridge_barrios_zonas)} relaciones)")

# =====================================================================
# 10. RESUMEN FINAL
# =====================================================================
print("\n" + "="*80)
print("📊 RESUMEN DE TABLAS GENERADAS")
print("="*80)

tablas_generadas = [
    ("dim_zonas.csv", len(df_dim_zonas)),
    ("dim_upz.csv", len(df_dim_upz)),
    ("dim_barrios.csv", len(df_dim_barrios)),
    ("dim_estrategias.csv", len(df_dim_estrategias)),
    ("fact_estrategias.csv", len(df_fact_estrategias)),
    ("fact_lineas.csv", len(df_fact_lineas)),
    ("bridge_upz_zonas.csv", len(df_bridge_upz_zonas)),
    ("bridge_barrios_zonas.csv", len(df_bridge_barrios_zonas))
]

print("\n✅ Archivos creados en dimensiones/:\n")
for nombre, filas in tablas_generadas:
    print(f"   📄 {nombre:<30} {filas:>5} registros")

print("\n" + "="*80)
print("🎉 MODELO DIMENSIONAL COMPLETO GENERADO CON ÉXITO")
print("="*80)
print(f"\n📂 Ubicación: {DIMENSIONES_DIR}")
print("\n🚀 Próximos pasos:")
print("   1. Revisar archivos en dimensiones/")
print("   2. Agregar al run_pipeline.py")
print("   3. Configurar GitHub Actions")
print("   4. Importar en Power BI")
