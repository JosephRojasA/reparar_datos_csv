import pandas as pd
import chardet
from typing import Optional, Dict
import warnings
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

def detectar_codificacion(ruta_archivo: str, muestra_bytes: int = 10000) -> str:
    """
    Detecta la codificación más probable de un archivo.
    
    Args:
        ruta_archivo: Ruta al archivo a analizar
        muestra_bytes: Cantidad de bytes a leer para la detección
        
    Returns:
        str: Codificación detectada (utf-8 por defecto si no se puede determinar)
    """
    try:
        with open(ruta_archivo, 'rb') as f:
            rawdata = f.read(muestra_bytes)
            resultado = chardet.detect(rawdata)
            return resultado['encoding'] if resultado['confidence'] > 0.7 else 'utf-8'
    except Exception:
        return 'utf-8'

def reparar_codificacion(archivo_entrada: str, archivo_salida: str) -> Optional[str]:
    """
    Corrige problemas de codificación en un archivo CSV, manteniendo el delimitador original.
    
    Args:
        archivo_entrada: Ruta al archivo CSV de entrada
        archivo_salida: Ruta donde se guardará el archivo corregido
        
    Returns:
        str: Ruta del archivo generado o None si hubo error
    """
    try:
        # 1. Detectar codificación y delimitador
        encoding = detectar_codificacion(archivo_entrada)
        
        # 2. Leer archivo con codificación detectada
        try:
            df = pd.read_csv(archivo_entrada, dtype=str, encoding=encoding)
        except UnicodeDecodeError:
            # Si falla, intentar con otras codificaciones comunes
            for enc in ['latin1', 'iso-8859-1', 'cp1252', 'utf-16']:
                try:
                    df = pd.read_csv(archivo_entrada, dtype=str, encoding=enc)
                    break
                except:
                    continue
            else:
                raise ValueError("No se pudo determinar la codificación del archivo")

        # 3. Tabla de reemplazos para caracteres mal codificados
        reemplazos: Dict[str, str] = {
            # Caracteres mal codificados comúnmente
            "Ã¡": "á", "Ã©": "é", "Ã­": "í", "Ã³": "ó", "Ãº": "ú",
            "Ã±": "ñ", "Ã¼": "ü", "Ã§": "ç", "Ã£": "ã", "Ãµ": "õ",
            "Ã€": "À", "Ã": "Á", "Ã‰": "É", "Ã": "Í", "Ã“": "Ó",
            "Ãš": "Ú", "Ã‘": "Ñ", "Ãœ": "Ü", "Ã‡": "Ç", "Ãƒ": "Ã",
            "Ã–": "Ö", "Ã„": "Ä", "Ã…": "Å", "Ãˆ": "È", "Ã‰": "É",
            "ÃŠ": "Ê", "Ã‹": "Ë", "ÃŒ": "Ì", "Ã": "Í", "ÃŽ": "Î",
            "Ã‰": "Ï", "Ã‘": "Ñ", "Ã’": "Ò", "Ã“": "Ó", "Ã”": "Ô",
            "Ã•": "Õ", "Ã˜": "Ø", "Ã™": "Ù", "Ãš": "Ú", "Ã›": "Û",
            "Ãœ": "Ü", "Ã": "Ý", "Ãž": "Þ", "ÃŸ": "ß", "Ã¡": "á",
            "Ã¢": "â", "Ã£": "ã", "Ã¤": "ä", "Ã¥": "å", "Ã¦": "æ",
            "Ã§": "ç", "Ã¨": "è", "Ã©": "é", "Ãª": "ê", "Ã«": "ë",
            "Ã¬": "ì", "Ã­": "í", "Ã®": "î", "Ã¯": "ï", "Ã°": "ð",
            "Ã±": "ñ", "Ã²": "ò", "Ã³": "ó", "Ã´": "ô", "Ãµ": "õ",
            "Ã¶": "ö", "Ã·": "÷", "Ã¸": "ø", "Ã¹": "ù", "Ãº": "ú",
            "Ã»": "û", "Ã½": "ý", "Ã¾": "þ", "Ã¿": "ÿ",
            # Caracteres inválidos/sucio
            "�": "", "\x00": "", "\ufeff": "", "ï»¿": ""
        }

        # 4. Función para aplicar los reemplazos
        def corregir_caracteres(texto: str) -> str:
            if pd.isna(texto):
                return texto
            for malo, bueno in reemplazos.items():
                texto = texto.replace(malo, bueno)
            return texto

        # 5. Aplicar corrección a todo el DataFrame
        df = df.applymap(corregir_caracteres)

        # 6. Guardar archivo corregido en UTF-8
        df.to_csv(archivo_salida, index=False, encoding='utf-8')
        
        return archivo_salida

    except Exception as e:
        print(f"❌ Error al reparar codificación en {archivo_entrada}: {str(e)}")
        return None

# Versión para trabajar con DataFrames (recomendada para pipelines en memoria)
def reparar_codificacion_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Corrige problemas de codificación en un DataFrame.
    
    Args:
        df: DataFrame con posibles problemas de codificación
        
    Returns:
        pd.DataFrame: DataFrame con caracteres corregidos
    """
    try:
        # Hacer copia para no modificar el original
        df_clean = df.copy()
        
        # Tabla de reemplazos (misma que la versión de archivo)
        reemplazos = {
            "Ã¡": "á", "Ã©": "é", "Ã­": "í", "Ã³": "ó", "Ãº": "ú",
            "Ã±": "ñ", "Ã¼": "ü", "Ã§": "ç", "Ã£": "ã", "Ãµ": "õ",
            "Ã€": "À", "Ã": "Á", "Ã‰": "É", "Ã": "Í", "Ã“": "Ó",
            "Ãš": "Ú", "Ã‘": "Ñ", "Ãœ": "Ü", "Ã‡": "Ç", "Ãƒ": "Ã",
            "�": "", "\x00": "", "\ufeff": "", "ï»¿": ""
        }
        
        # Aplicar corrección
        df_clean = df_clean.applymap(lambda x: x if pd.isna(x) else 
                                    ''.join([reemplazos.get(c, c) for c in str(x)]))
        
        return df_clean
    
    except Exception as e:
        print(f"❌ Error al reparar codificación en DataFrame: {str(e)}")
        raise