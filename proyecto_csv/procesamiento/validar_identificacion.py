import pandas as pd
import re
from typing import Optional, Tuple
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validar_identificaciones(input_path: str, output_path: str) -> Optional[str]:
    """
    Valida y limpia identificaciones en un archivo CSV, eliminando duplicados y registros inválidos.
    Mantiene el registro con mejor información cuando hay duplicados.
    
    Args:
        input_path: Ruta al archivo CSV de entrada
        output_path: Ruta donde se guardará el archivo validado
        
    Returns:
        str: Ruta del archivo generado o None si hubo error
    """
    try:
        # 1. Leer archivo
        df = pd.read_csv(input_path, dtype=str, keep_default_na=False, na_values=['', ' '])
        
        # Verificar si el DataFrame está vacío
        if df.empty or df.shape[1] == 0:
            logger.warning(f"Archivo vacío o sin columnas: {input_path}")
            pd.DataFrame().to_csv(output_path, index=False)
            return output_path

        # 2. Verificar columna de identificación
        if 'identificacion' not in df.columns:
            logger.error(f"El archivo {input_path} no tiene columna 'identificacion'")
            return None

        # 3. Función para validar identificaciones
        def es_identificacion_valida(ident: str) -> bool:
            """Valida que la identificación cumpla con los requisitos básicos"""
            if pd.isna(ident) or str(ident).strip() == '':
                return False
            
            ident = str(ident).strip()
            
            # Validar formato (6-15 dígitos, no todos ceros)
            return (re.fullmatch(r"\d{6,15}", ident) is not None and 
                    not re.fullmatch(r"0+", ident))

        # 4. Función para evaluar calidad del registro
        def evaluar_calidad_registro(row: pd.Series) -> int:
            """Asigna un puntaje de calidad al registro (mayor es mejor)"""
            puntaje = 0
            
            # Verificar nombres completos
            if 'nombres' in row and not pd.isna(row['nombres']) and str(row['nombres']).strip():
                puntaje += 1
                if re.search(r"[a-zA-ZáéíóúñÁÉÍÓÚÑ]{2,}", str(row['nombres'])):
                    puntaje += 1
            
            # Verificar apellidos completos
            if 'apellidos' in row and not pd.isna(row['apellidos']) and str(row['apellidos']).strip():
                puntaje += 1
                if re.search(r"[a-zA-ZáéíóúñÁÉÍÓÚÑ]{2,}", str(row['apellidos'])):
                    puntaje += 1
            
            # Verificar tipo documento
            if 'tipo_documento' in row and not pd.isna(row['tipo_documento']) and str(row['tipo_documento']).strip():
                puntaje += 1
            
            return puntaje

        # 5. Filtrar identificaciones inválidas
        mascara_validas = df['identificacion'].apply(es_identificacion_valida)
        df_validas = df[mascara_validas].copy()
        df_invalidas = df[~mascara_validas]
        
        if not df_invalidas.empty:
            logger.warning(f"Se encontraron {len(df_invalidas)} identificaciones inválidas en {input_path}")

        # 6. Procesar duplicados
        df_validas['calidad'] = df_validas.apply(evaluar_calidad_registro, axis=1)
        
        # Ordenar por calidad descendente y mantener el mejor registro por ID
        df_final = df_validas.sort_values(
            by=['identificacion', 'calidad'], 
            ascending=[True, False]
        ).drop_duplicates(
            subset=['identificacion'], 
            keep='first'
        ).drop(columns=['calidad'])

        # 7. Guardar resultados
        df_final.to_csv(output_path, index=False)
        
        # Opcional: Guardar registros inválidos para análisis
        if not df_invalidas.empty:
            invalidos_path = output_path.replace('.csv', '_invalidos.csv')
            df_invalidas.to_csv(invalidos_path, index=False)
            logger.info(f"Registros inválidos guardados en: {invalidos_path}")

        logger.info(f"Archivo validado guardado en: {output_path}")
        return output_path

    except Exception as e:
        logger.error(f"Error al validar identificaciones en {input_path}: {str(e)}")
        return None

# Versión para trabajar con DataFrames (recomendada para pipelines en memoria)
def validar_identificaciones_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """
    Valida identificaciones en un DataFrame, eliminando duplicados y registros inválidos.
    
    Args:
        df: DataFrame con los datos a validar
        
    Returns:
        Tuple: (DataFrame validado, reporte de validación)
    """
    reporte = {
        'total_registros': len(df),
        'identificaciones_invalidas': 0,
        'duplicados_eliminados': 0,
        'registros_validos': 0
    }
    
    try:
        if df.empty:
            logger.warning("DataFrame vacío recibido para validación")
            return df, reporte

        # 1. Verificar columna de identificación
        if 'identificacion' not in df.columns:
            logger.error("El DataFrame no tiene columna 'identificacion'")
            return df, reporte

        # 2. Función de validación
        def es_identificacion_valida(ident: str) -> bool:
            if pd.isna(ident) or str(ident).strip() == '':
                return False
            ident = str(ident).strip()
            return (re.fullmatch(r"\d{6,15}", ident) is not None and 
                    not re.fullmatch(r"0+", ident))

        # 3. Evaluar calidad del registro
        def evaluar_calidad_registro(row: pd.Series) -> int:
            puntaje = 0
            if 'nombres' in row and not pd.isna(row['nombres']) and str(row['nombres']).strip():
                puntaje += 1
                if re.search(r"[a-zA-ZáéíóúñÁÉÍÓÚÑ]{2,}", str(row['nombres'])):
                    puntaje += 1
            if 'apellidos' in row and not pd.isna(row['apellidos']) and str(row['apellidos']).strip():
                puntaje += 1
                if re.search(r"[a-zA-ZáéíóúñÁÉÍÓÚÑ]{2,}", str(row['apellidos'])):
                    puntaje += 1
            if 'tipo_documento' in row and not pd.isna(row['tipo_documento']) and str(row['tipo_documento']).strip():
                puntaje += 1
            return puntaje

        # 4. Filtrar y procesar
        mascara_validas = df['identificacion'].apply(es_identificacion_valida)
        df_validas = df[mascara_validas].copy()
        df_invalidas = df[~mascara_validas]
        
        reporte['identificaciones_invalidas'] = len(df_invalidas)
        
        if not df_validas.empty:
            df_validas['calidad'] = df_validas.apply(evaluar_calidad_registro, axis=1)
            
            # Contar duplicados antes de eliminarlos
            duplicados = df_validas.duplicated(subset=['identificacion'], keep=False)
            reporte['duplicados_eliminados'] = duplicados.sum() - df_validas['identificacion'].nunique()
            
            df_final = df_validas.sort_values(
                by=['identificacion', 'calidad'], 
                ascending=[True, False]
            ).drop_duplicates(
                subset=['identificacion'], 
                keep='first'
            ).drop(columns=['calidad'])
            
            reporte['registros_validos'] = len(df_final)
            
            return df_final, reporte
        
        return pd.DataFrame(columns=df.columns), reporte

    except Exception as e:
        logger.error(f"Error al validar identificaciones en DataFrame: {str(e)}")
        return df, reporte