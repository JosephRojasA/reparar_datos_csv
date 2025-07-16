import pandas as pd
import re
from typing import Optional, Tuple
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PALABRAS_INVALIDAS = {
    "ok", "na", "n/a", "anonimo", "desconocido", "empresa", "distribuidora",
    "proveedor", "cliente", "razon social", "no aplica", "sin nombre"
}

CARACTERES_INVALIDOS = "�¦�¿�¡�"

def limpiar_basura(texto: str) -> str:
    texto = re.sub(rf"[{re.escape(CARACTERES_INVALIDOS)}]", "", texto)
    texto = re.sub(r"[^\x20-\x7EÁÉÍÓÚáéíóúÑñüÜ ]", "", texto)
    texto = ' '.join(texto.split())
    return texto

def normalizar_nombre_o_apellido(campo: str) -> Optional[str]:
    if pd.isna(campo) or not isinstance(campo, str):
        return None

    campo = campo.strip().lower()
    campo = limpiar_basura(campo)

    if campo in PALABRAS_INVALIDAS:
        return None

    campo = re.sub(r'[^\wÁÉÍÓÚáéíóúÑñüÜ ]', '', campo)
    campo = ' '.join(campo.split())

    if len(re.sub(r'[^A-Za-zÁÉÍÓÚáéíóúÑñüÜ]', '', campo)) < 2:
        return None

    campo = re.sub(r'\b([a-záéíóúñü])', lambda m: m.group(1).upper(), campo)
    campo = re.sub(r'\bMc(\w)', lambda m: 'Mc' + m.group(1).capitalize(), campo)
    campo = re.sub(r"\bO'(\w)", lambda m: "O'" + m.group(1).capitalize(), campo)

    return campo if campo else None

def validar_nombres_y_apellidos_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    reporte = {
        'total_registros': len(df),
        'nombres_invalidos': 0,
        'apellidos_invalidos': 0,
        'nombres_corregidos': 0,
        'apellidos_corregidos': 0,
        'registros_validos': 0
    }

    try:
        columnas = [col.lower().strip() for col in df.columns]
        if 'nombres' not in columnas:
            logger.warning("DataFrame sin columna obligatoria 'nombres'")
            return df, reporte

        col_nombres = df.columns[columnas.index('nombres')]
        col_apellidos = df.columns[columnas.index('apellidos')] if 'apellidos' in columnas else None

        nombres_originales = df[col_nombres].copy()
        apellidos_originales = df[col_apellidos].copy() if col_apellidos else None

        df['nombres'] = df[col_nombres].apply(normalizar_nombre_o_apellido)
        if col_apellidos:
            df['apellidos'] = df[col_apellidos].apply(normalizar_nombre_o_apellido)
        else:
            df['apellidos'] = None

        reporte['nombres_invalidos'] = df['nombres'].isna().sum()
        if col_apellidos:
            reporte['apellidos_invalidos'] = df['apellidos'].isna().sum()

        reporte['nombres_corregidos'] = (nombres_originales != df['nombres']).sum()
        if col_apellidos and apellidos_originales is not None:
            reporte['apellidos_corregidos'] = (apellidos_originales != df['apellidos']).sum()

        df_validos = df[df['nombres'].notna()].copy()
        reporte['registros_validos'] = len(df_validos)

        return df_validos, reporte

    except Exception as e:
        logger.error(f"Error al validar nombres y apellidos en DataFrame: {str(e)}")
        return df, reporte

def validar_nombres_y_apellidos(archivo_entrada: str, archivo_salida: str) -> Optional[str]:
    try:
        with open(archivo_entrada, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            delimiter = ';' if ';' in first_line else ','

        df = pd.read_csv(archivo_entrada, dtype=str, delimiter=delimiter, keep_default_na=False, na_values=['', ' '])
        columnas = [col.lower().strip() for col in df.columns]

        if 'nombres' not in columnas:
            logger.error(f"El archivo {archivo_entrada} no contiene la columna obligatoria 'nombres'")
            return None

        col_nombres = df.columns[columnas.index('nombres')]
        col_apellidos = df.columns[columnas.index('apellidos')] if 'apellidos' in columnas else None

        df['nombres_normalizados'] = df[col_nombres].apply(normalizar_nombre_o_apellido)
        if col_apellidos:
            df['apellidos_normalizados'] = df[col_apellidos].apply(normalizar_nombre_o_apellido)
        else:
            df['apellidos_normalizados'] = None

        df_validos = df[df['nombres_normalizados'].notna()].copy()
        df_invalidos = df[df['nombres_normalizados'].isna()]

        df_validos[col_nombres] = df_validos['nombres_normalizados']
        if col_apellidos:
            df_validos[col_apellidos] = df_validos['apellidos_normalizados']

        df_validos.drop(columns=['nombres_normalizados', 'apellidos_normalizados'], inplace=True)

        df_validos.to_csv(archivo_salida, index=False, sep=delimiter)

        if not df_invalidos.empty:
            invalidos_path = archivo_salida.replace('.csv', '_nombres_apellidos_invalidos.csv')
            df_invalidos.to_csv(invalidos_path, index=False, sep=delimiter)
            logger.info(f"Registros inválidos guardados en: {invalidos_path}")

        logger.info(f"Archivo validado guardado en: {archivo_salida}")
        return archivo_salida

    except Exception as e:
        logger.error(f"Error al validar nombres y apellidos en {archivo_entrada}: {str(e)}")
        return None
