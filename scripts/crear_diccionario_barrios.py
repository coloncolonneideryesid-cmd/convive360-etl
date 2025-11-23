	#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crea diccionario maestro de barrios → UPZ → Zonas
Basado en documentación oficial de San Cristóbal
"""

import json
from pathlib import Path

# Rutas
BASE_DIR = Path(__file__).resolve().parents[1]
DICT_FILE = BASE_DIR / "scripts" / "diccionario_barrios_completo.json"

print("\n" + "="*80)
print("📚 CREANDO DICCIONARIO MAESTRO DE BARRIOS")
print("="*80)

# =====================================================================
# DICCIONARIO OFICIAL: 206 BARRIOS → UPZ (desde PDF oficial)
# =====================================================================
barrios_por_upz = {
    "32 - SAN BLAS": [
        "AGUAS CLARAS", "ALTOS DEL ZIPA", "AMAPOLAS", "AMAPOLAS II",
        "BALCON DE LA CASTANA", "BELLA VISTA SECTOR LUCERO",
        "BELLAVISTA PARTE BAJA", "BELLAVISTA SUR", "BOSQUE DE LOS ALPES",
        "BUENAVISTA SURORIENTAL", "CAMINO VIEJO SAN CRISTOBAL",
        "CERROS DE SAN VICENTE", "CIUDAD DE LONDRES", "CORINTO",
        "EL BALCON DE LA CASTANA", "EL FUTURO", "EL RAMAJAL",
        "EL RAMAJAL SAN PEDRO", "GRAN COLOMBIA", "HORACIO ORJUELA",
        "LA CASTANA", "LA CECILIA", "LA GRAN COLOMBIA", "LA HERRADURA",
        "LA JOYITA CENTRO", "LA PLAYA", "LA ROCA", "LA SAGRADA FAMILIA",
        "LAS ACACIAS", "LAS MERCEDES", "LAURELES SUR ORIENTAL II SECTOR",
        "LOS ALPES", "LOS ALPES FUTURO", "LOS ARRAYANES SECTOR SANTA INES",
        "LOS LAURELES SUR ORIENTAL I SECTOR", "MACARENA DE LOS ALPES",
        "MANANTIAL", "MANILA", "MIRAFLORES", "MOLINOS DE ORIENTE",
        "MONTECARLO", "NUEVA ESPANA", "NUEVA ESPANA PARTE ALTA",
        "RAMAJAL", "RINCON DE LA VICTORIA BELLAVISTA", "SAGRADA FAMILIA",
        "SAN BLAS", "SAN BLAS PARCELAS", "SAN BLAS II SECTOR",
        "SAN CRISTOBAL ALTO", "SAN CRISTOBAL VIEJO", "SAN PEDRO",
        "SAN VICENTE", "SAN VICENTE ALTO", "SAN VICENTE BAJO",
        "SAN VICENTE SUR ORIENTAL", "SANTA INES", "SANTA INES SUR",
        "TERRAZAS DE ORIENTE", "TRIANGULO", "TRIANGULO ALTO",
        "TRIANGULO BAJO", "VEREDA ALTOS DE SAN BLAS", "VITELMA"
    ],
    
    "33 - SOSIEGO": [
        "GOLCONDA", "PRIMERO DE MAYO", "BUENOS AIRES", "CALVO SUR",
        "CAMINO VIEJO DE SAN CRISTOBAL", "LA MARIA", "LAS BRISAS",
        "LOS DOS LEONES", "MODELO SUR", "NARINO SUR", "QUINTA RAMOS",
        "REPUBLICA DE VENEZUELA", "SAN CRISTOBAL SUR", "SAN JAVIER",
        "SANTA ANA", "SANTA ANA SUR", "SOSIEGO", "VELODROMO",
        "VILLA ALBANIA", "VILLA JAVIER"
    ],
    
    "34 - 20 DE JULIO": [
        "ATENAS", "20 DE JULIO", "ATENAS I", "AYACUCHO", "BARCELONA",
        "BARCELONA SUR", "BARCELONA SUR ORIENTAL", "BELLO HORIZONTE",
        "BELLO HORIZONTE III SECTOR", "CORDOBA", "EL ANGULO", "EL ENCANTO",
        "GRANADA SUR", "GRANADA SUR III SECTOR", "LA JOYITA", "LA SERAFINA",
        "LAS LOMAS", "MANAGUA", "MONTEBELLO", "SAN ISIDRO",
        "SAN ISIDRO I Y II", "SAN ISIDRO SUR"
    ],
    
    "50 - LA GLORIA": [
        "ALTAMIRA", "ALTAMIRA CHIQUITA", "ALTOS DEL POBLADO",
        "ALTOS DEL VIRREY", "ALTOS DEL ZUQUE", "BELLAVISTA PARTE ALTA",
        "BELLAVISTA SUR ORIENTAL", "BUENOS AIRES", "CIUDADELA SANTA ROSA",
        "EL QUINDIO", "EL RECODO REPUBLICA DE CANADA", "EL RODEO",
        "LA COLMENA", "LA GLORIA", "LA GLORIA BAJA", "LA GLORIA MZ 11",
        "LA GLORIA OCCIDENTAL", "LA GLORIA ORIENTAL", "LA GLORIA SAN MIGUEL",
        "LA GROVANA", "LA VICTORIA", "LA VICTORIA II SECTOR",
        "LA VICTORIA III SECTOR", "LAS GAVIOTAS", "LAS GUACAMAYAS",
        "LAS GUACAMAYAS I II Y III", "LAS LOMAS", "LOS PUENTES",
        "MALVINAS", "MIRAFLORES", "MORALVA", "PANORAMA",
        "PASEITO III", "PUENTE COLORADO", "QUINDIO", "QUINDIO I Y II",
        "QUINDIO II", "SAN JOSE", "SAN JOSE ORIENTAL", "SAN JOSE SUR ORIENTAL",
        "SAN MARTIN DE LOBA I Y II", "SAN MARTIN SUR"
    ],
    
    "51 - LOS LIBERTADORES": [
        "ANTIOQUIA", "CANADA LA GUIRA", "CANADA LA GUIRA II SECTOR",
        "CANADA SAN LUIS", "CHIGUAZA", "CIUDAD DE LONDRES", "EL PARAISO",
        "EL PINAR", "EL TRIUNFO", "JUAN REY", "LA BELLEZA", "LA NUEVA GLORIA",
        "LA NUEVA GLORIA II SECTOR", "LA PENINSULA", "LA SIERRA",
        "LAS GAVIOTAS", "LOS LIBERTADORES", "LOS LIBERTADORES SECTOR EL TESORO",
        "LOS LIBERTADORES SECTOR LA COLINA", "LOS LIBERTADORES SECTOR SAN IGNACIO",
        "LOS LIBERTADORES SECTOR SAN ISIDRO", "LOS LIBERTADORES SECTOR SAN JOSE",
        "LOS LIBERTADORES SECTOR SAN LUIS", "LOS LIBERTADORES SECTOR SAN MIGUEL",
        "LOS LIBERTADORES BOSQUE DIAMANTE TRIANGULO", "LOS PINARES",
        "LOS PINOS", "LOS PUENTES", "NUEVA DELLY", "NUEVA GLORIA",
        "NUEVA ROMA", "NUEVAS MALVINAS", "REPUBLICA DEL CANADA",
        "REPUBLICA DEL CANADA EL PINAR", "SAN JACINTO", "SAN MANUEL",
        "SAN RAFAEL SUR ORIENTAL", "SAN RAFAEL USME", "SANTA RITA I II Y III",
        "SANTA RITA SUR ORIENTAL", "VALPARAISO", "VILLA ANGELICA",
        "VILLA AURORA", "VILLA DEL CERRO", "VILLABELL", "YOMASA",
        "EL PARAISO SUR ORIENTAL I SECTOR", "JUAN REY I Y II", "VILLA BEGONIA"
    ]
}

# =====================================================================
# DICCIONARIO: BARRIOS → ZONAS (desde mapas oficiales)
# =====================================================================
barrios_por_zona = {
    "ZONA 1": [
        "AGUAS CLARAS", "AMAPOLAS", "AMAPOLAS II SECTOR", "ARBOLEDA DE LOS ALPES",
        "BALCONES DE SAN PEDRO III", "BUENAVISTA II SECTOR", "BUENAVISTA SURORIENTAL",
        "CORINTO", "EL BALCON DE LA CASTANA", "EL FUTURO", "EL MANANTIAL",
        "EL RAMAJAL", "EL RAMAJAL SAN PEDRO", "EL TRIANGULO",
        "GRANJAS Y HUERTAS EL RAMAJAL", "LA CASTANA", "LA CECILIA",
        "LA GRAN COLOMBIA", "LA SAGRADA FAMILIA", "LAS ACACIAS", "LAS MERCEDES",
        "LOS ALPES", "LOS ALPES DEL ZIPA", "LOS LAURELES SUR ORIENTAL I SECTOR",
        "LOS LAURELES SUR ORIENTAL II SECTOR", "MACARENA DE LOS ALPES",
        "MANILA", "MANILA 2", "MONTECARLO", "NUEVA ESPANA", "NUEVA ESPANA PARTE ALTA",
        "RAMAJAL", "SAN BLAS", "SAN BLAS II SECTOR", "SAN CRISTOBAL ALTO",
        "SAN CRISTOBAL SUR VIEJO", "SAN JERONIMO DEL YUSTE", "TORRES DE GRATAMIRA",
        "TRIANGULO ALTO", "VITELMA"
    ],
    
    "ZONA 2": [
        "ALTOS DE LA MARIA", "ALTOS DEL SOL", "BALKANES", "BELLAVISTA SECTOR LUCERO",
        "BELLAVISTA SUR ORIENTAL", "BUENOS AIRES II SECTOR", "BUENOS AIRES III SECTOR",
        "CAMINO VIEJO", "CASAPANDA", "CIUDAD MARBELLA SECTOR II",
        "EL RINCON DE LA VICTORIA BELLAVISTA", "LA PLAYA", "LOS ARRAYANES SANTA INES",
        "LOS BALCANES VITELMA", "LOS DOS LEONES", "LOS FAROLES DE SANTA INES",
        "MEDIA LUNA", "SAN BLAS", "SAN BLAS II SECTOR", "SAN CRISTOBAL SUR VIEJO",
        "SAN VICENTE", "SAN VICENTE SUR ORIENTAL", "SANTA INES", "SANTA INES SUR",
        "SANTA SOFIA DE VITELMA", "SIDEL TERRAZAS DE ORIENTE", "TORRES DE BUENOS AIRES",
        "VITELMA", "VITELMA CIUDADELA PARQUE DE LA ROCA"
    ],
    
    "ZONA 3": [
        "ALTAMIRA CHIQUITA", "ALTAMIRA SECTOR SAN JOSE", "BELLAVISTA SUR ORIENTAL",
        "EL POBLADO", "LA ARBOLEDA", "LA GLORIA", "LA GROVANA", "LA NUEVA GLORIA",
        "LOS ALPES DEL ZIPA", "LOS ALTOS DEL ZUQUE", "LOS PUENTES", "MIRAFLORES",
        "MORALVA", "PANORAMA", "PUENTE COLORADO", "QUINDIO", "QUINDIO II SECTOR",
        "SAN JOSE ORIENTAL", "VILLA ANITA SUR ORIENTAL"
    ],
    
    "ZONA 4": [
        "ATENAS", "ATENAS I", "BARCELONA SUR ORIENTAL", "BELLO HORIZONTE",
        "BELLO HORIZONTE II SECTOR", "BELLO HORIZONTE III SECTOR",
        "CIUDADELA MARIA MICAELA", "CORDOBA", "EL ANGULO", "EL REFUGIO SUR",
        "EL SOSIEGO", "GRANADA SUR", "GRANADA SUR III SECTOR",
        "LA JOYITA BELLO HORIZONTE", "MANAGUA", "SAN ISIDRO", "SURAMERICA",
        "UNIFAMILIARES AVENIDA DECIMA", "VILLA DE LOS ALPES",
        "VILLA DE LOS ALPES II SECTOR", "VILLA NATALY 20 DE JULIO"
    ],
    
    "ZONA 5": [
        "BOSQUE DE SAN JOSE", "CIUDAD DE LONDRES", "EL PINAR REPUBLICA DE CANADA II SECTOR",
        "EL RECODO REPUBLICA DE CANADA", "JUAN REY", "JUAN REY II", "LA ARBOLEDA",
        "LA BELLEZA", "LA NUEVA GLORIA", "LA NUEVA GLORIA II SECTOR", "LOS LIBERTADORES",
        "LOS LIBERTADORES BOSQUE DIAMANTE TRIANGULO", "LOS LIBERTADORES SECTOR EL TESORO",
        "LOS LIBERTADORES SECTOR LA COLINA", "LOS LIBERTADORES SECTOR SAN IGNACIO",
        "LOS LIBERTADORES SECTOR SAN ISIDRO", "LOS LIBERTADORES SECTOR SAN JOSE",
        "LOS LIBERTADORES SECTOR SAN LUIS", "LOS LIBERTADORES SECTOR SAN MIGUEL",
        "LOS PINOS", "NUEVA DELLY", "NUEVA DELLY PARTE ALTA",
        "REPUBLICA DEL CANADA EL PINAR", "SAN MANUEL", "SAN RAFAEL SUR ORIENTAL",
        "SIERRAS DEL SUR ORIENTE", "VALPARAISO", "VILLA AURORA", "VILLA BEGONIA"
    ],
    
    "ZONA 6": [
        "CAJA DE VIVIENDA POPULAR", "CAMINO VIEJO DE SAN CRISTOBAL",
        "CIUDAD MARBELLA SECTOR I", "CIUDAD MARBELLA SECTOR II",
        "EL ALTO DE LAS BRISAS", "EL ALTO DE LAS BRISAS II",
        "EL DIAMANTE BOSQUE DE SAN CRISTOBAL", "EL PARQUE ETAPA I", "EL SOSIEGO",
        "NARINO SUR", "PRIMERO DE MAYO", "QUINTA RAMOS", "SAN BERNARDO DEL VIENTO",
        "SAN CRISTOBAL SUR VIEJO", "SANTA ANA", "SANTA ANA SUR", "SANTA ANITA",
        "TAPAS LA LIBERTAD", "VELODROMO", "VILLA ALBANIA", "VILLA JAVIER"
    ],
    
    "ZONA 7": [
        "CANADA LA GUIRA II SECTOR", "CANADA SAN LUIS", "EL OASIS DEL SUR",
        "EL PARAISO SUR ORIENTAL I SECTOR", "EL PARAISO URB ANTIOQUIA",
        "LA BELLEZA", "LA NUEVA GLORIA", "LA NUEVA GLORIA II SECTOR", "LA PENINSULA",
        "LA SIERRA", "LOS LIBERTADORES", "NUEVAS MALVINAS EL TRIUNFO",
        "SAN JACINTO", "SANTA RITA SUR ORIENTAL", "SANTA RITA SUR ORIENTAL II ETAPA",
        "VILLA ANGELICA", "VILLABEL"
    ],
    
    "ZONA 8": [
        "GUACAMAYAS III SECTOR", "LA COLMENA", "LA GLORIA", "LA GLORIA BAJA",
        "LA VICTORIA", "LA VICTORIA II SECTOR", "LA VICTORIA III SECTOR",
        "LAS GUACAMAYAS", "MALVINAS", "SAN JOSE ORIENTAL",
        "SAN JOSE SUR ORIENTAL", "SAN MARTIN DE LOBA"
    ]
}

# =====================================================================
# CREAR MAPEOS INVERTIDOS (para búsqueda rápida)
# =====================================================================
print("\n📊 Procesando diccionarios...")

def normalizar(texto):
    """Normaliza texto para búsqueda"""
    import unicodedata
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto)
                    if unicodedata.category(c) != 'Mn')
    return texto.lower().strip()

# Mapeo: barrio_normalizado → UPZ
barrio_a_upz = {}
for upz, barrios in barrios_por_upz.items():
    for barrio in barrios:
        barrio_norm = normalizar(barrio)
        barrio_a_upz[barrio_norm] = upz
        # También guardar versión con mayúsculas
        barrio_a_upz[barrio.lower()] = upz

# Mapeo: barrio_normalizado → [zonas] (puede ser múltiple)
barrio_a_zonas = {}
for zona, barrios in barrios_por_zona.items():
    for barrio in barrios:
        barrio_norm = normalizar(barrio)
        if barrio_norm not in barrio_a_zonas:
            barrio_a_zonas[barrio_norm] = []
        if zona not in barrio_a_zonas[barrio_norm]:
            barrio_a_zonas[barrio_norm].append(zona)

# =====================================================================
# GUARDAR DICCIONARIO COMPLETO
# =====================================================================
diccionario_completo = {
    "barrios_por_upz": barrios_por_upz,
    "barrios_por_zona": barrios_por_zona,
    "barrio_a_upz": barrio_a_upz,
    "barrio_a_zonas": barrio_a_zonas,
    "metadata": {
        "total_barrios": sum(len(b) for b in barrios_por_upz.values()),
        "total_upz": len(barrios_por_upz),
        "total_zonas": len(barrios_por_zona),
        "fecha_creacion": "2025-01-22"
    }
}

with open(DICT_FILE, 'w', encoding='utf-8') as f:
    json.dump(diccionario_completo, f, ensure_ascii=False, indent=2)

print(f"✅ Diccionario guardado: {DICT_FILE}")
print(f"\n📊 Estadísticas:")
print(f"   • Total barrios oficiales: {diccionario_completo['metadata']['total_barrios']}")
print(f"   • Total UPZ: {diccionario_completo['metadata']['total_upz']}")
print(f"   • Total Zonas: {diccionario_completo['metadata']['total_zonas']}")
print(f"   • Variantes normalizadas: {len(barrio_a_upz)}")

print("\n" + "="*80)
print("✅ DICCIONARIO MAESTRO CREADO CON ÉXITO")
print("="*80)
