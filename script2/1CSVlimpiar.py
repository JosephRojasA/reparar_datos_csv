import pandas as pd

# === Cargar archivo CSV con punto y coma como separador ===
df = pd.read_csv("../PERSONA_CAPTUREPRO2.csv", sep=";", dtype=str)

# === Renombrar columnas para trabajar sin espacios ===
df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
# Esperamos: nombre, identificacion, tipo_doc

# === Limpiar espacios al inicio y final en todas las celdas ===
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# === Reemplazar celdas vacías o solo espacios por NaN ===
df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)

# === Eliminar filas con identificacion o tipo_doc vacíos ===
df = df.dropna(subset=["identificacion", "tipo_doc"])

# === Normalizar múltiples espacios internos (por si hay dobles espacios en nombres) ===
df = df.applymap(lambda x: " ".join(x.split()) if isinstance(x, str) else x)

# === Guardar archivo limpio ===
df.to_csv("../salida/PERSONA_CAPTUREPRO_limpios_final.csv", index=False)

print("✅ Archivo limpio guardado como datos_limpios_final.csv")
