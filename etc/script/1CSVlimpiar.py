import pandas as pd
import re

# === Cargar archivo CSV con punto y coma ===
df = pd.read_csv("../t_tercero_PARTE_1.csv", sep=";", dtype=str, encoding="utf-8")

# === Quitar columnas completamente vacías o sobrantes ===
df = df.dropna(axis=1, how="all")  # columnas 100% vacías
df = df.loc[:, ~df.columns.duplicated()]  # columnas duplicadas

# === Eliminar espacios alrededor de todas las celdas ===
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# === Reemplazar campos vacíos o con solo espacios por NaN ===
df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)

# === Eliminar filas donde identificacion o tipo_documento estén vacíos ===
df = df.dropna(subset=["identificacion", "tipo_documento"])

# === Normalizar espacios múltiples en textos ===
df = df.applymap(lambda x: " ".join(x.split()) if isinstance(x, str) else x)

# === Eliminar filas con nombres no legibles (vacíos, punto, símbolo �) ===
def nombre_valido(nombre):
    if pd.isna(nombre):
        return False
    nombre = nombre.strip()
    if nombre in [".", "�", ""] or not re.search(r"[A-Za-zÁÉÍÓÚáéíóúÑñ]", nombre):
        return False
    return True

df = df[df["nombres"].apply(nombre_valido)]

# === Guardar resultado limpio ===
df.to_csv("../salida/1tercer_parte1_datos_limpios_final.csv", index=False)

print("✅ Archivo limpio guardado como: datos_limpios_final.csv")
