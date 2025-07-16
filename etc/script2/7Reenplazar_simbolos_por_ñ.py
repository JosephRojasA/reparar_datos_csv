import pandas as pd
import re

# === Cargar archivo CSV (ajusta el encoding si UTF-8 no funciona) ===
df = pd.read_csv("../salida/6PERSONA_CAPTUREPRO_codificacion_reparada.csv", dtype=str, encoding="utf-8")

# === Función para reemplazar secuencias mal codificadas por Ñ o ñ ===
def reemplazar_malas_ñ(texto):
    if pd.isna(texto):
        return texto

    texto = str(texto)

    # Reemplazos comunes de codificación mal interpretada
    texto = re.sub(r"(Ã‘|Ãƒâ€˜|Ãƒ')", "Ñ", texto)  # Ñ mayúscula
    texto = re.sub(r"(Ã±|ÃƒÂ±|ÃƒÂ‘)", "ñ", texto)  # ñ minúscula

    return texto

# === Aplicar reparación sobre la columna 'nombre' ===
df["nombre"] = df["nombre"].apply(reemplazar_malas_ñ)

# === Guardar archivo corregido ===
df.to_csv("../salida/7PERSONA_nombre_reparado.csv", index=False)

print("✅ Reemplazo completado y archivo guardado como: 7PERSONA_nombre_reparado.csv")
