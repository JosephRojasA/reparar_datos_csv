import pandas as pd
import re
from typing import Optional, Tuple
import logging
import unicodedata

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def normalizar_nombre(nombre: str) -> str:
    """
    Normaliza un nombre propio corrigiendo formatos comunes.
    
    Args:
        nombre: Cadena con el nombre a normalizar
        
    Returns:
        str: Nombre normalizado o None si no es válido
    """
    if pd.isna(nombre) or not isinstance(nombre, str):
        return None
    
    # Eliminar espacios extraños y caracteres no deseados
    nombre = nombre.strip()
    nombre = re.sub(r'[^\wÁÉÍÓÚáéíóúÑñ ]', '', nombre, flags=re.IGNORECASE)
    
    # Eliminar múltiples espacios
    nombre = ' '.join(nombre.split())
    
    # Corregir formatos comunes
    nombre = re.sub(r'\b([a-zÁÉÍÓÚáéíóúñÑ])', lambda m: m.group(1).upper(), nombre.lower())
    
    # Corregir casos específicos
    nombre = re.sub(r'\bMc(\w)', r'Mc\1', nombre)  # McDonald -> McDonald
    nombre = re.sub(r'\bO\'(\w)', r'O\'\1', nombre)  # O'conner -> O'Conner
    
    # Validar que tenga al menos 2 caracteres válidos
    if len(re.sub(r'[^A-Za-zÁÉÍÓÚáéíóúÑñ]', '', nombre)) < 2:
        return None
    
    return nombre if nombre else None

def validar_nombres(archivo_entrada: str, archivo_salida: str) -> Optional[str]:
    """
    Valida y repara nombres en un archivo CSV, normalizando formatos.
    
    Args:
        archivo_entrada: Ruta al archivo CSV de entrada
        archivo_salida: Ruta donde se guardará el archivo validado
        
    Returns:
        str: Ruta del archivo generado o None si hubo error
    """
    try:
        # Leer archivo manteniendo strings como están
        df = pd.read_csv(archivo_entrada, dtype=str, keep_default_na=False, na_values=['', ' '])
        
        # Verificar si existe columna de nombres
        if 'nombres' not in df.columns:
            logger.error(f"El archivo {archivo_entrada} no tiene columna 'nombres'")
            return None
        
        # Aplicar normalización
        df['nombres_normalizados'] = df['nombres'].apply(normalizar_nombre)
        
        # Filtrar nombres inválidos
        df_valido = df[df['nombres_normalizados'].notna()].copy()
        df_invalido = df[df['nombres_normalizados'].isna()]
        
        # Reemplazar columna original con nombres normalizados
        df_valido['nombres'] = df_valido['nombres_normalizados']
        df_valido = df_valido.drop(columns=['nombres_normalizados'])
        
        # Guardar resultados
        df_valido.to_csv(archivo_salida, index=False)
        
        # Opcional: Guardar registros inválidos
        if not df_invalido.empty:
            invalidos_path = archivo_salida.replace('.csv', '_nombres_invalidos.csv')
            df_invalido.to_csv(invalidos_path, index=False)
            logger.info(f"Registros con nombres inválidos guardados en: {invalidos_path}")
        
        logger.info(f"Archivo con nombres validados guardado en: {archivo_salida}")
        return archivo_salida
        
    except Exception as e:
        logger.error(f"Error al validar nombres en {archivo_entrada}: {str(e)}")
        return None

# Versión para trabajar con DataFrames (recomendada para pipelines en memoria)
def validar_nombres_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """
    Valida y repara nombres en un DataFrame, normalizando formatos.
    
    Args:
        df: DataFrame con los datos a validar
        
    Returns:
        Tuple: (DataFrame validado, reporte de validación)
    """
    reporte = {
        'total_registros': len(df),
        'nombres_invalidos': 0,
        'nombres_corregidos': 0
    }
    
    try:
        if df.empty or 'nombres' not in df.columns:
            logger.warning("DataFrame vacío o sin columna 'nombres'")
            return df, reporte
        
        # Aplicar normalización
        nombres_originales = df['nombres'].copy()
        df['nombres'] = df['nombres'].apply(normalizar_nombre)
        
        # Filtrar nombres inválidos
        mascara_validos = df['nombres'].notna()
        df_valido = df[mascara_validos].copy()
        df_invalido = df[~mascara_validos]
        
        # Generar reporte
        reporte['nombres_invalidos'] = len(df_invalido)
        reporte['nombres_corregidos'] = (nombres_originales[mascara_validos] != df_valido['nombres']).sum()
        reporte['registros_validos'] = len(df_valido)
        
        return df_valido, reporte
        
    except Exception as e:
        logger.error(f"Error al validar nombres en DataFrame: {str(e)}")
        return df, reporte