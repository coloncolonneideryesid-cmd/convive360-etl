#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para generar TODAS las tablas dimensionales y de hechos faltantes
VERSIÓN FINAL CORREGIDA - Usa fact_actividades_enriquecido.csv
"""

import pandas as pd
import json
import re
from pathlib import Path
from datetime import datetime
import hashlib

# =====================================================================
# CONFIGURACIÓN DE RUTAS (CORREGIDAS)
# =====================================================================
BASE_DIR = Path(__file__).resolve().parents[1]
DIMENSIONES_DIR = BASE_DIR / "dimensiones"
SCRIPTS_DIR = BASE_DIR / "scripts"

# Crear carpeta si no existe
DIMENSIONES_DIR.mkdir(exist_ok=True)

# ✅ ARCHIVOS DE ENTRADA CORREGIDOS
FACT_ACTIVIDADES = BASE_DIR / "fact_actividades_enriquecido.csv"  # ✅ ENRIQUECIDO
DICT_UPZ_ZONAS = SCRIPTS_DIR / "diccionario_upz_zonas.json"
DICT_BARRIOS = SCRIPTS_DIR / "diccionario_barrios_completo.json"

print("\n" + "="*80)
print("🚀 GENERANDO MODELO DIMENSIONAL COMPLETO PARA POWER BI")
print("="*80)

# =====================================================================
# 1. CARGAR DATOS BASE
# =====================================================================
print("\n📥 Cargando datos base...")

# Verificar que existe el archivo enriquecido
if not FACT_ACTIVIDADES.exists():
    print(f"❌ Error: No se encuentra {FACT_ACTIVIDADES}")
    print("   Ejecuta primero: python scripts/enriquecer_con_barrios.py")
    print("   Luego: python scripts/mejorar_extraccion_barrios.py")
    exit(1)

df_actividades = pd.read_csv(FACT_ACTIVIDADES, encoding='utf-8')
print(f"✅ fact_actividades_enriquecido.csv cargado: {len(df_actividades)} registros")

# Verificar columna Barrio_Extraido
if 'Barrio_Extraido' in df_actividades.columns:
    barrios_con_dato = df_actividades['Barrio_Extraido'].notna().sum()
    print(f"   �� Barrios extraídos: {barrios_con_dato}/{len(df_actividades)} ({barrios_con_dato/len(df_actividades)*100:.1f}%)")
else:
    print("⚠️  Columna 'Barrio_Extraido' no encontrada")

# Cargar diccionario UPZ-Zonas
if DICT_UPZ_ZONAS.exists():
    with open(DICT_UPZ_ZONAS, 'r', encoding='utf-8') as f:
        upz_zonas = json.load(f)
    print(f"✅ Diccionario UPZ-Zonas cargado: {len(upz_zonas)} UPZ")
else:
    print("⚠️  Diccionario UPZ-Zonas no encontrado, creando versión básica...")
    upz_zonas = {
        "32 - San Blas": ["Zona 1", "Zona 2"],
        "33 - Sosiego": ["Zona 6"],
        "34 - 20 de Julio": ["Zona 4"],
        "50 - La Gloria": ["Zona 3", "Zona 8"],
        "51 - Los Libertadores": ["Zona 5", "Zona 7"]
    }

# Cargar diccionario completo de barrios
if DICT_BARRIOS.exists():
    with open(DICT_BARRIOS, 'r', encoding='utf-8') as f:
        dict_barrios = json.load(f)
    print(f"✅ Diccionario de barrios cargado: {dict_barrios['metadata']['total_barrios']} barrios oficiales")
else:
    print("❌ Diccionario de barrios no encontrado")
    print("   Ejecuta primero: python scripts/crear_diccionario_barrios.py")
    exit(1)

# =====================================================================
# 2. GENERAR ID_ACTIVIDAD si no existe
# =====================================================================
if 'ID_Actividad' not in df_actividades.columns:
    print("\n⚠️  Generando ID_Actividad único...")
    
    def generar_id(row):
        # Crear ID único basado en campos clave
        campos = f"{row.get('Nombre_Actividad', '')}{row.get('Fecha_Actividad', '')}{row.get('Hora_Inicio', '')}{row.get('Direccion_Actividad', '')}"
        return hashlib.md5(campos.encode()).hexdigest()[:12].upper()
    
    df_actividades['ID_Actividad'] = df_actividades.apply(generar_id, axis=1)
    print(f"✅ ID_Actividad generado para {len(df_actividades)} registros")

# =====================================================================
# 3. GENERAR dim_zonas.csv
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
# 4. GENERAR dim_upz.csv
# =====================================================================
print("\n" + "="*80)
print("📊 GENERANDO dim_upz.csv")
print("="*80)

upz_data = []
upz_info = {
    "32 - San Blas": {"codigo": 32, "nombre": "San Blas", "zona_principal": "Zona 1"},
    "33 - Sosiego": {"codigo": 33, "nombre": "Sosiego", "zona_principal": "Zona 6"},
    "34 - 20 de Julio": {"codigo": 34, "nombre": "20 de Julio", "zona_principal": "Zona 4"},
    "50 - La Gloria": {"codigo": 50, "nombre": "La Gloria", "zona_principal": "Zona 3"},
    "51 - Los Libertadores": {"codigo": 51, "nombre": "Los Libertadores", "zona_principal": "Zona 5"}
}

for upz_key, info in upz_info.items():
    zonas_asoc = upz_zonas.get(upz_key, [])
    upz_data.append({
        'ID_UPZ': info['codigo'],
        'UPZ': upz_key,
        'Codigo_UPZ': info['codigo'],
        'Nombre_UPZ': info['nombre'],
        'Zona_Principal': info['zona_principal'],
        'Zonas_Asociadas': ', '.join(zonas_asoc)
    })

df_dim_upz = pd.DataFrame(upz_data)
output_path = DIMENSIONES_DIR / "dim_upz.csv"
df_dim_upz.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ dim_upz.csv generado ({len(df_dim_upz)} UPZ)")

# =====================================================================
# 5. GENERAR bridge_upz_zonas.csv
# =====================================================================
print("\n" + "="*80)
print("📊 GENERANDO bridge_upz_zonas.csv")
print("="*80)

bridge_upz_zonas_data = []
for upz, zonas in upz_zonas.items():
    zona_principal = upz_info.get(upz, {}).get('zona_principal', '')
    for zona in zonas:
        bridge_upz_zonas_data.append({
            'UPZ': upz,
            'Zona': zona,
            'Es_Zona_Principal': 'Sí' if zona == zona_principal else 'No'
        })

df_bridge_upz_zonas = pd.DataFrame(bridge_upz_zonas_data)
output_path = DIMENSIONES_DIR / "bridge_upz_zonas.csv"
df_bridge_upz_zonas.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ bridge_upz_zonas.csv generado ({len(df_bridge_upz_zonas)} relaciones)")

# =====================================================================
# 6. GENERAR dim_barrios.csv (DESDE DICCIONARIO OFICIAL + EXTRAÍDOS)
# =====================================================================
print("\n" + "="*80)
print("📊 GENERANDO dim_barrios.csv")
print("="*80)

barrios_por_upz = dict_barrios['barrios_por_upz']
barrio_a_upz = dict_barrios['barrio_a_upz']

# Crear dim_barrios desde diccionario oficial (197 barrios)
barrios_data = []
id_counter = 1

print("   📋 Procesando barrios oficiales del diccionario...")
for upz, barrios in barrios_por_upz.items():
    for barrio in barrios:
        barrios_data.append({
            'ID_Barrio': id_counter,
            'Barrio': barrio,
            'Barrio_Normalizado': barrio.lower().strip(),
            'UPZ': upz,
            'Fuente': 'Diccionario Oficial'
        })
        id_counter += 1

print(f"   ✅ {id_counter - 1} barrios oficiales agregados")

# AGREGAR barrios extraídos que no están en el diccionario oficial
if 'Barrio_Extraido' in df_actividades.columns:
    print("   📋 Buscando barrios adicionales en actividades...")
    barrios_extraidos = df_actividades['Barrio_Extraido'].dropna().unique()
    barrios_oficiales_norm = {b.lower().strip() for b in [item['Barrio'] for item in barrios_data]}
    
    nuevos_barrios = 0
    for barrio_ext in barrios_extraidos:
        barrio_norm = str(barrio_ext).lower().strip()
        if barrio_norm and barrio_norm not in barrios_oficiales_norm:
            # Barrio encontrado en actividades pero no en diccionario oficial
            barrios_data.append({
                'ID_Barrio': id_counter,
                'Barrio': str(barrio_ext).title(),
                'Barrio_Normalizado': barrio_norm,
                'UPZ': None,
                'Fuente': 'Extraído de Actividades'
            })
            id_counter += 1
            nuevos_barrios += 1
    
    if nuevos_barrios > 0:
        print(f"   ✅ {nuevos_barrios} barrios adicionales encontrados en actividades")

df_dim_barrios = pd.DataFrame(barrios_data)
output_path = DIMENSIONES_DIR / "dim_barrios.csv"
df_dim_barrios.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"\n✅ dim_barrios.csv generado ({len(df_dim_barrios)} barrios)")
print(f"   • Oficiales: {len([b for b in barrios_data if b['Fuente'] == 'Diccionario Oficial'])}")
print(f"   • Extraídos: {len([b for b in barrios_data if b['Fuente'] == 'Extraído de Actividades'])}")

# =====================================================================
# 7. GENERAR dim_estrategias.csv
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
# 8. GENERAR fact_estrategias.csv
# =====================================================================
print("\n" + "="*80)
print("📊 GENERANDO fact_estrategias.csv")
print("="*80)

fact_estrategias_rows = []

# Columna de estrategias
col_estrategias = 'Estrategia_Impactar'

if col_estrategias in df_actividades.columns:
    for idx, row in df_actividades.iterrows():
        id_actividad = row['ID_Actividad']
        estrategias_str = row.get(col_estrategias, '')
        
        if pd.notna(estrategias_str) and str(estrategias_str).strip():
            # Separar por comas
            estrategias_lista = [e.strip() for e in str(estrategias_str).split(',')]
            
            for estrategia in estrategias_lista:
                if estrategia:
                    # Normalizar nombre
                    estrategia_norm = estrategia.title()
                    
                    fact_estrategias_rows.append({
                        'ID_Actividad': id_actividad,
                        'Estrategia': estrategia_norm,
                        'ID_Estrategia': 1 if 'seguridad' in estrategia.lower() else 
                                       2 if 'convivencia' in estrategia.lower() else 
                                       3 if 'justicia' in estrategia.lower() else None
                    })

df_fact_estrategias = pd.DataFrame(fact_estrategias_rows)
output_path = DIMENSIONES_DIR / "fact_estrategias.csv"
df_fact_estrategias.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ fact_estrategias.csv generado ({len(df_fact_estrategias)} relaciones)")

# =====================================================================
# 9. GENERAR fact_lineas.csv
# =====================================================================
print("\n" + "="*80)
print("📊 GENERANDO fact_lineas.csv")
print("="*80)

fact_lineas_rows = []

# Columnas de líneas estratégicas
columnas_lineas = {
    'Seguridad': 'Linea_Seguridad',
    'Convivencia': 'Linea_Convivencia',
    'Justicia': 'Linea_Justicia'
}

for tipo_estrategia, col_linea in columnas_lineas.items():
    if col_linea in df_actividades.columns:
        for idx, row in df_actividades.iterrows():
            id_actividad = row['ID_Actividad']
            lineas_str = row.get(col_linea, '')
            
            if pd.notna(lineas_str) and str(lineas_str).strip() and str(lineas_str) != 'N/A':
                # Separar por comas
                lineas_lista = [l.strip() for l in str(lineas_str).split(',')]
                
                for linea in lineas_lista:
                    if linea and linea != 'N/A':
                        fact_lineas_rows.append({
                            'ID_Actividad': id_actividad,
                            'Tipo_Estrategia': tipo_estrategia,
                            'Linea_Estrategica': linea,
                            'ID_Estrategia': 1 if tipo_estrategia == 'Seguridad' else 
                                           2 if tipo_estrategia == 'Convivencia' else 3
                        })

df_fact_lineas = pd.DataFrame(fact_lineas_rows)
output_path = DIMENSIONES_DIR / "fact_lineas.csv"
df_fact_lineas.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"✅ fact_lineas.csv generado ({len(df_fact_lineas)} líneas)")

# =====================================================================
# 10. GENERAR bridge_barrios_zonas.csv
# =====================================================================
print("\n" + "="*80)
print("📊 GENERANDO bridge_barrios_zonas.csv")
print("="*80)

# Usar diccionario oficial
barrios_por_zona = dict_barrios['barrios_por_zona']

bridge_barrios_zonas_rows = []

print("   📋 Procesando barrios por zona del diccionario...")
for zona, barrios in barrios_por_zona.items():
    for barrio in barrios:
        bridge_barrios_zonas_rows.append({
            'Barrio': barrio,
            'Zona': zona,
            'Fuente': 'Diccionario Oficial'
        })

print(f"   ✅ {len(bridge_barrios_zonas_rows)} relaciones del diccionario")

# Agregar relaciones de actividades enriquecidas
if 'Barrio_Extraido' in df_actividades.columns and 'Zona_Enriquecida' in df_actividades.columns:
    print("   📋 Agregando relaciones de actividades...")
    relaciones_actividades = df_actividades[['Barrio_Extraido', 'Zona_Enriquecida']].dropna().drop_duplicates()
    
    agregadas = 0
    for _, row in relaciones_actividades.iterrows():
        barrio = str(row['Barrio_Extraido']).strip()
        zona = str(row['Zona_Enriquecida']).strip()
        
        # Normalizar zona
        if 'zona' not in zona.lower():
            zona = f"ZONA {zona.split()[-1]}"
        else:
            zona = zona.upper()
        
        # Verificar si ya existe
        existe = any(
            r['Barrio'].lower().strip() == barrio.lower().strip() and 
            r['Zona'].upper().strip() == zona.upper().strip() 
            for r in bridge_barrios_zonas_rows
        )
        
        if not existe:
            bridge_barrios_zonas_rows.append({
                'Barrio': barrio.title(),
                'Zona': zona,
                'Fuente': 'Actividades'
            })
            agregadas += 1
    
    print(f"   ✅ {agregadas} relaciones adicionales de actividades")

df_bridge_barrios_zonas = pd.DataFrame(bridge_barrios_zonas_rows)
output_path = DIMENSIONES_DIR / "bridge_barrios_zonas.csv"
df_bridge_barrios_zonas.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"\n✅ bridge_barrios_zonas.csv generado ({len(df_bridge_barrios_zonas)} relaciones)")
print(f"   • Del diccionario: {len([r for r in bridge_barrios_zonas_rows if r['Fuente'] == 'Diccionario Oficial'])}")
print(f"   • De actividades: {len([r for r in bridge_barrios_zonas_rows if r['Fuente'] == 'Actividades'])}")

# =====================================================================
# 11. ACTUALIZAR fact_actividades con ID_Barrio
# =====================================================================
print("\n" + "="*80)
print("📊 ACTUALIZANDO fact_actividades con ID_Barrio")
print("="*80)

# Crear mapeo barrio → ID_Barrio
barrio_to_id = dict(zip(
    df_dim_barrios['Barrio_Normalizado'],
    df_dim_barrios['ID_Barrio']
))

# Agregar columna ID_Barrio
if 'Barrio_Extraido' in df_actividades.columns:
    df_actividades['ID_Barrio'] = df_actividades['Barrio_Extraido'].apply(
        lambda x: barrio_to_id.get(str(x).lower().strip()) if pd.notna(x) else None
    )
    
    barrios_mapeados = df_actividades['ID_Barrio'].notna().sum()
    print(f"✅ ID_Barrio asignado a {barrios_mapeados}/{len(df_actividades)} actividades ({barrios_mapeados/len(df_actividades)*100:.1f}%)")
    
    # Guardar fact_actividades actualizado
    output_path = BASE_DIR / "fact_actividades_enriquecido.csv"
    df_actividades.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"💾 fact_actividades_enriquecido.csv actualizado con ID_Barrio")

# =====================================================================
# 12. RESUMEN FINAL
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
print("   1. Subir archivos a GitHub")
print("   2. Actualizar GitHub Actions")
print("   3. Importar en Power BI desde GitHub URLs")
print("="*80)
