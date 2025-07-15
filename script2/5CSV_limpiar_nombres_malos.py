import pandas as pd
import re
import unicodedata

# === Cargar archivo ===
df = pd.read_csv("../salida/PERSONA_CAPTUREPRO_empresas_sin_duplicados.csv", dtype=str)

# === Limpiar espacios ===
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# === Reparar caracteres mal codificados en 'nombre' (ej: ñ, á, é) ===
def reparar_codificacion(texto):
    if pd.isna(texto):
        return texto
    # Arreglar errores comunes de codificación mal decodificada (UTF-8 mal leído como latin1)
    try:
        return texto.encode('latin1').decode('utf-8')
    except UnicodeEncodeError:
        return texto  # Dejarlo igual si no se puede

df["nombre"] = df["nombre"].apply(reparar_codificacion)

# === Eliminar filas donde el nombre esté vacío, empiece con punto o número, o solo tenga símbolos ===
def nombre_valido(nombre):
    if pd.isna(nombre) or nombre.strip() == "":
        return False
    nombre = nombre.strip()
    # No debe comenzar con punto ni número
    if re.match(r"^[\.\d]", nombre):
        return False
    # Debe contener al menos una letra legible
    if not re.search(r"[A-Za-zÁÉÍÓÚáéíóúÑñ]", nombre):
        return False
    return True

# === Filtrar solo nombres válidos ===
df = df[df["nombre"].apply(nombre_valido)]

# === Guardar archivo final limpio ===
df.to_csv("../salida/3PERSONA_CAPTUREPRO_nombres_limpios.csv", index=False)

print("✅ Archivo limpio guardado como: datos_final_nombres_limpios.csv")
