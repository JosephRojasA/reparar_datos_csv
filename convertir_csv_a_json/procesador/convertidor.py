import pandas as pd
import unicodedata

def limpiar_texto_para_iso(texto):
    try:
        # Normaliza a ASCII-compatible, elimina caracteres no convertibles
        texto = unicodedata.normalize('NFKD', texto).encode('latin-1', 'ignore').decode('latin-1')
        return texto
    except:
        return ""  # En caso de fallo, devolver cadena vacía

def limpiar_dataframe_para_iso(df):
    for col in df.columns:
        df[col] = df[col].apply(lambda x: limpiar_texto_para_iso(str(x)))
    return df

def guardar_json(df, ruta_salida, encoding='utf-8'):
    # Si se está guardando como ISO-8859-1, limpiar caracteres incompatibles
    if encoding.lower() in ['latin-1', 'iso-8859-1']:
        df = limpiar_dataframe_para_iso(df)

    with open(ruta_salida, 'w', encoding=encoding) as f:
        df.to_json(f, orient='records', indent=2, force_ascii=False)
