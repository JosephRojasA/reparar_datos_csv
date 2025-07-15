import pandas as pd
import re

# === Cargar archivo ya limpio o el que tengas como base ===
df = pd.read_csv("../salida/datos_final_sin_duplicados.csv", dtype=str)

# === Limpiar espacios en celdas ===
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# === Definir función: ¿el nombre empieza con letra? ===
def nombre_valido(texto):
    if pd.isna(texto) or texto.strip() == "":
        return False
    # Solo válido si empieza con letra (letras con tildes y Ñ/ñ incluidas)
    return bool(re.match(r"^[A-Za-zÁÉÍÓÚáéíóúÑñ]", texto.strip()))

# === Filtrar filas que tienen nombre válido ===
df = df[df["nombres"].apply(nombre_valido)]

# === Guardar archivo resultante ===
df.to_csv("../salida/datos_final_nombre_valido.csv", index=False)

print("✅ Archivo guardado como: datos_final_nombre_valido.csv (nombres válidos únicamente)")
