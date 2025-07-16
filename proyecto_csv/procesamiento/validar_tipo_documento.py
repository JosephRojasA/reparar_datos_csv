import pandas as pd
import logging

# Configurar logging
logger = logging.getLogger(__name__)

# Palabras clave mal codificadas y su corrección
CORRECCIONES_PARCIALES = {
    "EXTRANJER�A": "EXTRANJERIA",
    "DEFUNSI�N": "DEFUNCION",
    "IDENTIFICACI�N": "IDENTIFICACION",
    "N�MERO �NICO": "NUMERO UNICO"
}

def corregir_tipo_documento(valor: str) -> str:
    if not isinstance(valor, str):
        return valor

    corregido = valor
    for incorrecto, correcto in CORRECCIONES_PARCIALES.items():
        if incorrecto in corregido:
            corregido = corregido.replace(incorrecto, correcto)
    return corregido.strip()

def validar_tipo_documento_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if 'tipo_documento' not in df.columns:
        logger.warning("No se encontró la columna 'tipo_documento'")
        return df

    df['tipo_documento'] = df['tipo_documento'].apply(corregir_tipo_documento)
    logger.info("✔️ Correcciones aplicadas a la columna 'tipo_documento'")
    return df
