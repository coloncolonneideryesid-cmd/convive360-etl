import pandas as pd

# Leer con manejo correcto de comillas
df = pd.read_csv('fact_actividades_limpio.csv', encoding='utf-8', quotechar='"', escapechar='\\')

# Guardar con punto y coma
df.to_csv('fact_actividades_limpio_fixed.csv', index=False, encoding='utf-8', sep=';')

print(f"✅ Limpiado: {len(df)} registros")
print(f"✅ Columnas: {list(df.columns)}")
