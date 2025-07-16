import pandas as pd

# === Cargar archivo CSV con separador correcto y codificación ===
df = pd.read_csv("../salida/2tercer_parte1_sin_duplicados.csv", sep=";", encoding="latin1", dtype=str)

# === Eliminar símbolo � de columnas de texto ===
def limpiar_caracteres(texto):
    if pd.isna(texto):
        return texto
    return texto.replace("�", "").strip()

# Limpiar columnas relevantes si existen
for col in ["nombres", "apellidos"]:
    if col in df.columns:
        df[col] = df[col].apply(limpiar_caracteres)

# === Guardar en codificación UTF-8 ===
df.to_csv("../salida/3tercer_parte1.csv", index=False, encoding="utf-8")

print("✅ Archivo reparado y guardado como: 8PERSONA_utf8_reparado.csv")
