import pandas as pd
import chardet
from typing import Optional, Tuple, List
import os

def detectar_delimitador_y_codificacion(ruta_archivo: str) -> Tuple[str, str]:
    """
    Detecta automáticamente el delimitador y codificación de un archivo CSV.
    Prioriza punto y coma si está presente, sino detecta el más común.
    """
    # Detectar codificación
    with open(ruta_archivo, 'rb') as f:
        rawdata = f.read(10000)
        resultado = chardet.detect(rawdata)
        encoding = resultado['encoding']
    
    # Detectar delimitador
    with open(ruta_archivo, 'r', encoding=encoding) as f:
        primera_linea = f.readline()
        
        # Priorizar punto y coma si existe
        if ';' in primera_linea:
            return ';', encoding
        
        # Si no, detectar el delimitador más común
        delimitadores = [',', '\t', '|']
        conteo = {delim: primera_linea.count(delim) for delim in delimitadores}
        delimitador = max(conteo.items(), key=lambda x: x[1])[0]
        
        return (delimitador, encoding) if conteo[delimitador] > 0 else (';', encoding)

def estandarizar_columnas(archivo_entrada: str, archivo_salida: str) -> Optional[str]:
    """
    Estandariza los nombres de columnas de un archivo CSV manteniendo el delimitador original.
    
    Args:
        archivo_entrada: Ruta al archivo CSV de entrada
        archivo_salida: Ruta donde se guardará el archivo estandarizado
        
    Returns:
        str: Ruta del archivo generado o None si hubo error
    """
    errores: List[Tuple[int, str]] = []
    
    try:
        # 1. Detectar configuración del archivo
        delimitador, encoding = detectar_delimitador_y_codificacion(archivo_entrada)
        
        # 2. Validar estructura básica del archivo
        with open(archivo_entrada, 'r', encoding=encoding) as f:
            for i, line in enumerate(f, 1):
                if line.count(delimitador) < 2:  # mínimo 3 campos (2 delimitadores)
                    errores.append((i, line.strip()))
        
        # 3. Leer archivo CSV
        df = pd.read_csv(
            archivo_entrada,
            sep=delimitador,
            dtype=str,
            encoding=encoding,
            on_bad_lines='skip',
            engine='python',
            keep_default_na=False,
            na_values=['', ' ', 'NA', 'N/A', 'NaN', 'NULL']
        )
        
        print(f"🧪 Columnas originales: {list(df.columns)}")
        
        # 4. Mapeo de columnas a estandarizar
        mapeo_columnas = {
            # Nombres
            'nombres': 'nombres',
            'nombre': 'nombres',
            'nombres_apellidos': 'nombres',
            'primer_nombre': 'nombres',
            'person_name': 'nombres',
            
            # Apellidos
            'apellidos': 'apellidos',
            'apellido': 'apellidos',
            'segundo_apellido': 'apellidos',
            'last_name': 'apellidos',
            
            # Tipo documento
            'tipo_doc': 'tipo_documento',
            'tipo_documento': 'tipo_documento',
            'tipo': 'tipo_documento',
            'document_type': 'tipo_documento',
            'tdoc': 'tipo_documento',
            
            # Identificación
            'identificacion': 'identificacion',
            'ident': 'identificacion',
            'documento': 'identificacion',
            'document': 'identificacion',
            'id': 'identificacion',
            'numero_documento': 'identificacion',
            'doc_number': 'identificacion',
            'cedula': 'identificacion',
            'num_id': 'identificacion'
        }
        
        # 5. Limpiar y estandarizar nombres de columnas
        df.columns = [col.lower().strip().replace('ï»¿', '').replace('"', '') for col in df.columns]
        df.rename(columns={col: mapeo_columnas.get(col, col) for col in df.columns}, inplace=True)
        
        print(f"✅ Columnas estandarizadas: {list(df.columns)}")
        
        # 6. Guardar archivo estandarizado
        df.to_csv(archivo_salida, index=False, sep=delimitador, encoding='utf-8')
        
        # 7. Guardar errores si los hay
        if errores:
            nombre_base = os.path.splitext(os.path.basename(archivo_salida))[0]
            errores_path = os.path.join(os.path.dirname(archivo_salida), f"{nombre_base}_errores.csv")
            
            pd.DataFrame(errores, columns=['linea', 'contenido'])\
              .to_csv(errores_path, index=False, sep=',', encoding='utf-8')
            
            print(f"⚠️  Se guardaron {len(errores)} líneas problemáticas en: {errores_path}")
        
        return archivo_salida
    
    except Exception as e:
        print(f"❌ Error procesando {archivo_entrada}: {str(e)}")
        return None

# Versión para trabajar con DataFrames (recomendada para pipelines en memoria)
def estandarizar_columnas_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza los nombres de columnas de un DataFrame.
    
    Args:
        df: DataFrame a estandarizar
        
    Returns:
        pd.DataFrame: DataFrame con columnas estandarizadas
    """
    try:
        # Hacer copia para no modificar el original
        df_std = df.copy()
        
        # Mapeo de columnas (mismo que la versión de archivo)
        mapeo_columnas = {
            'nombres': 'nombres',
            'nombre': 'nombres',
            'apellidos': 'apellidos',
            'apellido': 'apellidos',
            'tipo_doc': 'tipo_documento',
            'tipo_documento': 'tipo_documento',
            'tipo': 'tipo_documento',
            'identificacion': 'identificacion',
            'ident': 'identificacion',
            'documento': 'identificacion',
            'document': 'identificacion',
            'id': 'identificacion',
            'numero_documento': 'identificacion'
        }
        
        # Limpiar y estandarizar nombres de columnas
        df_std.columns = [str(col).lower().strip().replace('ï»¿', '') for col in df_std.columns]
        df_std.rename(columns=mapeo_columnas, inplace=True)
        
        return df_std
    
    except Exception as e:
        print(f"❌ Error estandarizando DataFrame: {str(e)}")
        raise