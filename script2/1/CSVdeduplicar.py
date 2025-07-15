import pandas as pd
import re

# === Cargar archivo previamente limpiado ===
df = pd.read_csv("../salida/datos_limpios_final.csv", dtype=str)

# === Limpiar espacios en todas las celdas ===
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# === Validar campo 'identificacion' ===
def identificacion_valida(ident):
    if pd.isna(ident):
        return False
    ident = ident.strip()
    # Solo permitir números (mínimo 6, máximo 15 dígitos)
    if not re.fullmatch(r"\d{6,15}", ident):
        return False
    # Excluir identificaciones compuestas solo por ceros
    if re.fullmatch(r"0+", ident):
        return False
    return True

# === Filtrar solo identificaciones válidas ===
df = df[df["identificacion"].apply(identificacion_valida)]

# === Validar legibilidad de nombres y apellidos (solo letras, tildes, espacios) ===
def es_legible(texto):
    if pd.isna(texto) or texto.strip() == "":
        return False
    return bool(re.fullmatch(r"[A-Za-zÁÉÍÓÚáéíóúÑñ ]+", texto.strip()))

# === Crear columna auxiliar de legibilidad ===
df["legible"] = df.apply(
    lambda row: es_legible(row["nombres"]) and (pd.isna(row["apellidos"]) or es_legible(row["apellidos"])),
    axis=1
)

# === Ordenar por identificacion y legibilidad (primero los más legibles) ===
df = df.sort_values(by=["identificacion", "legible"], ascending=[True, False])

# === Eliminar duplicados por 'identificacion', conservando el más legible ===
df = df.drop_duplicates(subset="identificacion", keep="first")

# === Eliminar la columna auxiliar ===
df = df.drop(columns=["legible"])

# === Guardar el archivo final limpio ===
df.to_csv("../salida/datos_final_sin_duplicados.csv", index=False)

print("✅ Archivo final limpio guardado como: datos_final_sin_duplicados.csv")
