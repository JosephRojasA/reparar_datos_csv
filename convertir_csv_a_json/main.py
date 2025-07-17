import os
from procesador.lector_csv import procesar_archivos_csv
from procesador.convertidor import guardar_json

def main():
    carpeta_entrada = "entrada"
    carpeta_salida_utf8 = os.path.join("salida", "utf8")
    carpeta_salida_iso = os.path.join("salida", "iso8859")

    # Crear carpetas de salida si no existen
    os.makedirs(carpeta_salida_utf8, exist_ok=True)
    os.makedirs(carpeta_salida_iso, exist_ok=True)

    archivos = [f for f in os.listdir(carpeta_entrada) if f.endswith('.csv')]

    for archivo in archivos:
        ruta_csv = os.path.join(carpeta_entrada, archivo)
        print(f"[INFO] Procesando {archivo}...")

        df = procesar_archivos_csv(ruta_csv)

        nombre_salida = archivo.replace('.csv', '.json')

        # Guardar en UTF-8
        ruta_utf8 = os.path.join(carpeta_salida_utf8, nombre_salida)
        guardar_json(df, ruta_utf8, encoding='utf-8')
        print(f"  ↳ Guardado en: {ruta_utf8} (UTF-8)")

        # Guardar en ISO-8859-1
        ruta_iso = os.path.join(carpeta_salida_iso, nombre_salida)
        guardar_json(df, ruta_iso, encoding='iso-8859-1')
        print(f"  ↳ Guardado en: {ruta_iso} (ISO-8859-1)")

if __name__ == "__main__":
    main()
