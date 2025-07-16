import pandas as pd
import re
from typing import Optional, Tuple
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validar_identificaciones(input_path: str, output_path: str) -> Optional[str]:
    try:
        df = pd.read_csv(input_path, dtype=str, keep_default_na=False, na_values=['', ' '])

        if df.empty or df.shape[1] == 0:
            logger.warning(f"Archivo vacío o sin columnas: {input_path}")
            pd.DataFrame().to_csv(output_path, index=False)
            return output_path

        if 'identificacion' not in df.columns or 'tipo_documento' not in df.columns:
            logger.error("Faltan columnas necesarias: 'identificacion' o 'tipo_documento'")
            return None

        # Convertir a string y limpiar espacios
        df['identificacion'] = df['identificacion'].astype(str).str.strip().fillna('')
        df['tipo_documento'] = df['tipo_documento'].astype(str).str.strip().fillna('')

        def es_identificacion_valida(ident) -> bool:
            try:
                ident = str(ident).strip()
                if not ident or pd.isna(ident):
                    return False
                return re.fullmatch(r"\d{6,15}", ident) is not None and not re.fullmatch(r"0+", ident)
            except Exception:
                return False

        def evaluar_calidad_registro(row: pd.Series) -> int:
            puntaje = 0
            nombres = str(row.get('nombres', '')).strip()
            apellidos = str(row.get('apellidos', '')).strip()
            tipo_doc = str(row.get('tipo_documento', '')).strip()

            if nombres:
                puntaje += 1
                if re.search(r"[a-zA-ZáéíóúñÁÉÍÓÚÑ]{2,}", nombres):
                    puntaje += 1
            if apellidos:
                puntaje += 1
                if re.search(r"[a-zA-ZáéíóúñÁÉÍÓÚÑ]{2,}", apellidos):
                    puntaje += 1
            if tipo_doc:
                puntaje += 1

            return puntaje

        # Validar identificaciones
        mascara_validas = df['identificacion'].apply(es_identificacion_valida)
        df_validas = df[mascara_validas].copy()
        df_invalidas = df[~mascara_validas]

        if not df_invalidas.empty:
            logger.warning(f"{len(df_invalidas)} identificaciones inválidas")

        df_validas['id_combinado'] = (
            df_validas['tipo_documento'].str.upper() + "_" + df_validas['identificacion']
        )
        df_validas['calidad'] = df_validas.apply(evaluar_calidad_registro, axis=1)

        df_final = df_validas.sort_values(
            by=['id_combinado', 'calidad'],
            ascending=[True, False]
        ).drop_duplicates(
            subset=['id_combinado'],
            keep='first'
        ).drop(columns=['id_combinado', 'calidad'])

        df_final.to_csv(output_path, index=False)

        if not df_invalidas.empty:
            invalidos_path = output_path.replace('.csv', '_invalidos.csv')
            df_invalidas.to_csv(invalidos_path, index=False)
            logger.info(f"Registros inválidos guardados en: {invalidos_path}")

        logger.info(f"Archivo validado guardado en: {output_path}")
        return output_path

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return None


def validar_identificaciones_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    reporte = {
        'total_registros': len(df),
        'identificaciones_invalidas': 0,
        'duplicados_eliminados': 0,
        'registros_validos': 0
    }

    try:
        if df.empty or 'identificacion' not in df.columns or 'tipo_documento' not in df.columns:
            logger.warning("DataFrame vacío o faltan columnas necesarias")
            return df, reporte

        # Normalizar columnas como string
        df['identificacion'] = df['identificacion'].astype(str).str.strip().fillna('')
        df['tipo_documento'] = df['tipo_documento'].astype(str).str.strip().fillna('')

        def es_identificacion_valida(ident) -> bool:
            try:
                ident = str(ident).strip()
                if not ident or pd.isna(ident):
                    return False
                return re.fullmatch(r"\d{6,15}", ident) is not None and not re.fullmatch(r"0+", ident)
            except Exception:
                return False

        def evaluar_calidad_registro(row: pd.Series) -> int:
            puntaje = 0
            nombres = str(row.get('nombres', '')).strip()
            apellidos = str(row.get('apellidos', '')).strip()
            tipo_doc = str(row.get('tipo_documento', '')).strip()

            if nombres:
                puntaje += 1
                if re.search(r"[a-zA-ZáéíóúñÁÉÍÓÚÑ]{2,}", nombres):
                    puntaje += 1
            if apellidos:
                puntaje += 1
                if re.search(r"[a-zA-ZáéíóúñÁÉÍÓÚÑ]{2,}", apellidos):
                    puntaje += 1
            if tipo_doc:
                puntaje += 1

            return puntaje

        # Validar
        mascara_validas = df['identificacion'].apply(es_identificacion_valida)
        df_validas = df[mascara_validas].copy()
        df_invalidas = df[~mascara_validas]
        reporte['identificaciones_invalidas'] = len(df_invalidas)

        if not df_validas.empty:
            df_validas['id_combinado'] = (
                df_validas['tipo_documento'].str.upper() + "_" + df_validas['identificacion']
            )

            df_validas['calidad'] = df_validas.apply(evaluar_calidad_registro, axis=1)

            duplicados = df_validas.duplicated(subset='id_combinado', keep=False)
            reporte['duplicados_eliminados'] = duplicados.sum() - df_validas['id_combinado'].nunique()

            df_final = df_validas.sort_values(
                by=['id_combinado', 'calidad'],
                ascending=[True, False]
            ).drop_duplicates(
                subset='id_combinado',
                keep='first'
            ).drop(columns=['id_combinado', 'calidad'])

            reporte['registros_validos'] = len(df_final)
            return df_final, reporte

        return pd.DataFrame(columns=df.columns), reporte

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return df, reporte
