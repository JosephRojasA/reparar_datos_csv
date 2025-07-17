import pandas as pd
from .utilidades import estandarizar_columnas

def procesar_archivos_csv(ruta_csv):
    df = pd.read_csv(ruta_csv, sep=';')
    df = estandarizar_columnas(df)
    return df
