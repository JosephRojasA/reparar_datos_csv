import os
import pandas as pd
from datetime import datetime
import logging
import shutil

from procesamiento.correccion_codificacion import reparar_codificacion
from procesamiento.estandarizar_columnas import estandarizar_columnas
from procesamiento.limpiar_csv import limpiar_csv
from procesamiento.validar_identificacion import validar_identificaciones_dataframe
from procesamiento.validar_nombres_y_apellidos import validar_nombres_y_apellidos_dataframe

# Configuración de logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuración de directorios
ENTRADA_DIR = "entrada"
SALIDA_DIR = "salida"
MEMORIA_DIR = "memoria"
ERRORES_DIR = "errores"

# Crear directorios si no existen
os.makedirs(SALIDA_DIR, exist_ok=True)
os.makedirs(MEMORIA_DIR, exist_ok=True)
os.makedirs(ERRORES_DIR, exist_ok=True)

def leer_csv(ruta: str) -> pd.DataFrame:
    """Lee un archivo CSV con delimitador ; o , y manejo robusto de errores"""
    try:
        with open(ruta, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            delimiter = ';' if ';' in first_line else ','

        return pd.read_csv(ruta, sep=delimiter, dtype=str, engine='python',
                           on_bad_lines='warn', encoding='utf-8')
    except Exception as e:
        logger.error(f"No se pudo leer el archivo {ruta}: {str(e)}")
        return pd.DataFrame()

def procesar_archivo(ruta_entrada: str) -> bool:
    """Procesa un archivo CSV paso a paso"""
    try:
        nombre_archivo = os.path.basename(ruta_entrada)
        nombre_base = os.path.splitext(nombre_archivo)[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        logger.info(f"\n{'='*50}")
        logger.info(f"🔹 Procesando archivo: {nombre_archivo}")

        # Paso 0: Leer archivo
        df = leer_csv(ruta_entrada)
        if df.empty:
            logger.error("❌ No se pudieron leer datos del archivo")
            return False

        # Paso 1: Reparar codificación y espacios
        df = df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)

        # Guardar copia del original
        ruta_original = os.path.join(MEMORIA_DIR, f"{nombre_base}_original_{timestamp}.csv")
        df.to_csv(ruta_original, index=False, sep=';')

        # Paso 2: Estandarizar nombres de columnas
        df.columns = [col.strip().lower() for col in df.columns]
        posibles_nombres = {
            'nombres': ['nombres', 'nombre'],
            'apellidos': ['apellidos', 'apellido'],
            'identificacion': ['identificacion', 'id', 'documento'],
            'tipo_documento': ['tipo_documento', 'tipo id']
        }

        mapeo = {}
        for clave, posibles in posibles_nombres.items():
            for posible in posibles:
                if posible in df.columns:
                    mapeo[posible] = clave
                    break
        df.rename(columns=mapeo, inplace=True)  #validar tipo documento texto
        from procesamiento.validar_tipo_documento import validar_tipo_documento_dataframe

        df = validar_tipo_documento_dataframe(df)

        # Paso 3: Limpieza básica
        df = df.dropna(how='all')
        df = df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)

        # Paso 4: Validar identificaciones
        if 'identificacion' not in df.columns:
            logger.error("❌ No existe columna 'identificacion'")
            return False

        df, reporte_ident = validar_identificaciones_dataframe(df)
        logger.info(f"🆔 Validación de identificaciones: {reporte_ident}")

        if df.empty:
            logger.warning("⚠️ No quedaron registros válidos tras validar identificaciones")
            return False

        # Paso 5: Validar nombres (apellidos pueden ser opcionales)
        if 'nombres' not in df.columns:
            logger.error("❌ No existe columna 'nombres'")
            return False

        df, reporte_nombres_apellidos = validar_nombres_y_apellidos_dataframe(df)
        logger.info(f"🔤 Validación de nombres y apellidos: {reporte_nombres_apellidos}")

        if df.empty:
            logger.warning("⚠️ No quedaron registros válidos tras validar nombres")
            return False

        # Paso final: Guardar archivo procesado
        ruta_salida = os.path.join(SALIDA_DIR, f"{nombre_base}_procesado_{timestamp}.csv")
        df.to_csv(ruta_salida, index=False, sep=';')

        logger.info(f"✅ Procesado correctamente. Registros válidos: {len(df)}")
        return True

    except Exception as e:
        logger.error(f"❌ Error al procesar {nombre_archivo}: {str(e)}")
        with open(os.path.join(ERRORES_DIR, f"error_{nombre_base}_{timestamp}.txt"), 'w') as f:
            f.write(f"Error procesando {nombre_archivo}:\n{str(e)}")
        return False

def limpiar_memoria():
    """Elimina todos los archivos en la carpeta de memoria"""
    try:
        for filename in os.listdir(MEMORIA_DIR):
            file_path = os.path.join(MEMORIA_DIR, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                logger.error(f"Error al eliminar {file_path}: {e}")
        logger.info("🧹 Carpeta de memoria limpiada exitosamente")
    except Exception as e:
        logger.error(f"Error al limpiar memoria: {e}")

def main():
    """Función principal que ejecuta el pipeline"""
    logger.info("="*50)
    logger.info("🚀 INICIANDO PROCESAMIENTO DE ARCHIVOS CSV")
    logger.info("="*50)

    archivos_procesados = 0
    archivos_fallidos = 0

    for archivo in sorted(os.listdir(ENTRADA_DIR)):
        if archivo.lower().endswith(".csv"):
            ruta_entrada = os.path.join(ENTRADA_DIR, archivo)
            if procesar_archivo(ruta_entrada):
                archivos_procesados += 1
            else:
                archivos_fallidos += 1

    limpiar_memoria()

    logger.info("\n" + "="*50)
    logger.info("📊 RESUMEN FINAL DEL PROCESAMIENTO")
    logger.info(f"✅ Archivos procesados correctamente: {archivos_procesados}")
    logger.info(f"❌ Archivos con errores: {archivos_fallidos}")
    logger.info(f"📂 Resultados en: {os.path.abspath(SALIDA_DIR)}")
    logger.info(f"📝 Errores detallados en: {os.path.abspath(ERRORES_DIR)}")
    logger.info("🧹 Memoria limpiada")
    logger.info("🎉 PROCESAMIENTO COMPLETADO")
    logger.info("="*50)

if __name__ == "__main__":
    main()
