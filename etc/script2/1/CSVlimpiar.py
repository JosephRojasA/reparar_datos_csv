import pandas as pd

# === Cargar archivo CSV con punto y coma como separador ===
df = pd.read_csv("../t_tercero 2(t_tercero).csv", sep=";", dtype=str)

# === Eliminar espacios al inicio y final en todas las celdas ===
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# === Reemplazar celdas vacías o con solo espacios por NaN ===
df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)

# === Eliminar filas donde tipo_documento o identificacion estén vacíos ===
df = df.dropna(subset=["tipo_documento", "identificacion"])

# === Normalizar múltiples espacios internos ===
df = df.applymap(lambda x: " ".join(x.split()) if isinstance(x, str) else x)

# === Guardar archivo limpio ===
df.to_csv("../salida/datos_limpios_final.csv", index=False)

print("✅ Archivo limpio guardado como datos_limpios_final.csv")
