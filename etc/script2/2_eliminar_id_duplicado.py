import pandas as pd
import re

# === Cargar archivo previamente limpiado ===
df = pd.read_csv("../salida/PERSONA_CAPTUREPRO_limpios_final.csv", dtype=str)

# === Limpiar espacios en todas las celdas ===
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# === Validar campo 'identificacion' ===
def identificacion_valida(ident):
    if pd.isna(ident):
        return False
    ident = ident.strip()
    # Solo permitir números de 6 a 15 dígitos
    if not re.fullmatch(r"\d{6,15}", ident):
        return False
    if re.fullmatch(r"0+", ident):  # solo ceros
        return False
    return True

df = df[df["identificacion"].apply(identificacion_valida)]

# === Validar legibilidad del campo 'nombre' (solo letras y espacios) ===
def es_nombre_legible(nombre):
    if pd.isna(nombre) or nombre.strip() == "":
        return False
    return bool(re.fullmatch(r"[A-Za-zÁÉÍÓÚáéíóúÑñ. ]+", nombre.strip()))

# === Crear columna auxiliar de legibilidad ===
df["legible"] = df["nombre"].apply(es_nombre_legible)

# === Ordenar: primero por identificacion, luego por legibilidad (True primero) ===
df = df.sort_values(by=["identificacion", "legible"], ascending=[True, False])

# === Eliminar duplicados, conservar la fila más legible por identificacion ===
df = df.drop_duplicates(subset="identificacion", keep="first")

# === Eliminar la columna auxiliar ===
df = df.drop(columns=["legible"])

# === Guardar archivo limpio final ===
df.to_csv("../salida/PERSONA_CAPTUREPRO_empresas_sin_duplicados.csv", index=False)

print("✅ Archivo limpio guardado como: datos_final_empresas_sin_duplicados.csv")
