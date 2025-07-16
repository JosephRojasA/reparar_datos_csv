import pandas as pd
import re

# === Cargar archivo con codificación Latin-1 por si ayuda a detectar mejor ===
df = pd.read_csv("../salida/3PERSONA_CAPTUREPRO_nombres_limpios.csv", dtype=str, encoding="latin1")

# === Reparación automática ===
def reparar_texto(texto):
    if pd.isna(texto):
        return texto
    try:
        # Intento automático
        reparado = texto.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        reparado = texto

    # Reemplazos manuales adicionales si quedaron residuos
    reemplazos = {
        "Ã‘": "Ñ", "Ã‘": "Ñ", "Ã‘": "Ñ",
        "Ã±": "ñ", "Ã¡": "á", "ÃÁ": "Á",
        "Ã©": "é", "Ã‰": "É",
        "Ã­": "í", "ÃÍ": "Í",
        "Ã³": "ó", "Ã“": "Ó",
        "Ãº": "ú", "Ãš": "Ú",
        "Ã¼": "ü", "Ãœ": "Ü",
        "â€“": "-", "â€œ": '"', "â€": '"', "â€˜": "'", "â€™": "'",
    }

    for erroneo, correcto in reemplazos.items():
        reparado = reparado.replace(erroneo, correcto)

    return reparado

# === Aplicar reparación a todo el DataFrame ===
df = df.applymap(reparar_texto)

# === Guardar archivo corregido ===
df.to_csv("../salida/6PERSONA_CAPTUREPRO_codificacion_reparada.csv", index=False)

print("✅ Codificación reparada y archivo guardado como: 5PERSONA_CAPTUREPRO_codificacion_reparada.csv")
