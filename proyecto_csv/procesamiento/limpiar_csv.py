import pandas as pd
import chardet
from typing import Tuple

def detectar_configuracion_archivo(ruta_archivo: str) -> Tuple[str, str]:
    """
    Detecta automáticamente la codificación y el delimitador de un archivo CSV.
    Mantiene el delimitador original sin forzar cambio a coma.
    """
    # Detectar codificación
    with open(ruta_archivo, 'rb') as f:
        rawdata = f.read(10000)
        resultado = chardet.detect(rawdata)
        encoding = resultado['encoding']
    
    # Detectar delimitador manteniendo el original
    with open(ruta_archivo, 'r', encoding=encoding) as f:
        primera_linea = f.readline()
        
        # Priorizar punto y coma si existe, sino usar el detectado
        if ';' in primera_linea:
            delimitador = ';'
        else:
            delimitadores = [',', '\t', '|']
            conteo = {delim: primera_linea.count(delim) for delim in delimitadores}
            delimitador = max(conteo.items(), key=lambda x: x[1])[0]
            if conteo[delimitador] == 0:
                delimitador = ';'  # Valor por defecto
    
    return encoding, delimitador

def limpiar_csv(archivo_entrada: str, archivo_salida: str) -> str:
    """
    Limpia un archivo CSV manteniendo su delimitador original (; si existe).
    """
    try:
        # 1. Detectar configuración del archivo
        encoding, delimitador = detectar_configuracion_archivo(archivo_entrada)
        
        # 2. Leer el archivo
        df = pd.read_csv(
            archivo_entrada,
            sep=delimitador,
            dtype=str,
            encoding=encoding,
            keep_default_na=False,
            na_values=['', ' ', 'NA', 'N/A', 'NaN', 'NULL']
        )
        
        # 3. Limpieza de datos
        # Eliminar espacios alrededor de valores
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        
        # Reemplazar valores vacíos por NaN
        df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)
        
        # Normalizar espacios internos múltiples
        df = df.map(lambda x: ' '.join(x.split()) if isinstance(x, str) else x)
        
        # Eliminar filas completamente vacías
        df.dropna(how='all', inplace=True)
        
        # Eliminar filas sin datos clave (si existen las columnas)
        columnas_clave = ['tipo_documento', 'identificacion']
        columnas_presentes = [col for col in columnas_clave if col in df.columns]
        if columnas_presentes:
            df.dropna(subset=columnas_presentes, inplace=True)
        
        # 4. Guardar manteniendo el delimitador original
        df.to_csv(archivo_salida, index=False, sep=delimitador, encoding='utf-8')
        
        return archivo_salida
    
    except Exception as e:
        print(f"❌ Error al limpiar {archivo_entrada}: {str(e)}")
        raise

def limpiar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Versión para limpieza en memoria (mantiene misma lógica pero sin delimitador).
    """
    try:
        df_clean = df.copy()
        df_clean = df_clean.astype(str)
        
        # Aplicar mismas transformaciones de limpieza
        df_clean = df_clean.map(lambda x: x.strip() if isinstance(x, str) else x)
        df_clean.replace(r'^\s*$', pd.NA, regex=True, inplace=True)
        df_clean = df_clean.map(lambda x: ' '.join(x.split()) if isinstance(x, str) else x)
        df_clean.dropna(how='all', inplace=True)
        
        columnas_clave = ['tipo_documento', 'identificacion']
        columnas_presentes = [col for col in columnas_clave if col in df_clean.columns]
        if columnas_presentes:
            df_clean.dropna(subset=columnas_presentes, inplace=True)
        
        return df_clean
    
    except Exception as e:
        print(f"❌ Error al limpiar DataFrame: {str(e)}")
        raise