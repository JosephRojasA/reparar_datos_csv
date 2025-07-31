import csv
import os

CARPETA_ENTRADA = 'entrada'
CARPETA_SALIDA = 'salida'
BASE_DATOS = 'gdocxhl'

IDS_POR_UPDATE = 1000          # Cantidad de IDs por cada UPDATE
UPDATES_POR_ARCHIVO = 10       # Cantidad de bloques UPDATE por archivo
IDS_POR_ARCHIVO = IDS_POR_UPDATE * UPDATES_POR_ARCHIVO

def limpiar(texto):
    return texto.replace("'", "''").strip()

def escribir_bloques(archivo, bloques):
    for bloque in bloques:
        archivo.write("UPDATE t_tercero\n")
        archivo.write("SET tipo_documento = 'NIT'\n")
        archivo.write("WHERE identificacion IN (\n")
        archivo.write(",\n".join(f"    '{id}'" for id in bloque))
        archivo.write("\n);\nGO\n\n")

def generar_update_sql():
    if not os.path.exists(CARPETA_ENTRADA):
        print(f"❌ Carpeta de entrada no encontrada: {CARPETA_ENTRADA}")
        return

    os.makedirs(CARPETA_SALIDA, exist_ok=True)

    all_ids = []
    for archivo_csv in os.listdir(CARPETA_ENTRADA):
        if not archivo_csv.lower().endswith('.csv'):
            continue

        ruta = os.path.join(CARPETA_ENTRADA, archivo_csv)
        with open(ruta, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) < 2:
                    continue
                identificacion = limpiar(row[0])
                tipo_doc = limpiar(row[1]).upper()
                if tipo_doc == 'NIT' and identificacion:
                    all_ids.append(identificacion)

    if not all_ids:
        print("⚠️ No se encontraron identificaciones válidas con tipo NIT.")
        return

    parte = 1
    total = len(all_ids)
    idx = 0

    while idx < total:
        path = os.path.join(CARPETA_SALIDA, f"update_tipo_documento_parte{parte}.sql")
        with open(path, 'w', encoding='utf-8') as archivo:
            archivo.write(f"USE {BASE_DATOS};\nGO\n\n")
            print(f"📝 Escribiendo archivo: {path}")

            bloques_ids = all_ids[idx:idx + IDS_POR_ARCHIVO]
            bloques = [bloques_ids[i:i + IDS_POR_UPDATE] for i in range(0, len(bloques_ids), IDS_POR_UPDATE)]
            escribir_bloques(archivo, bloques)

        idx += IDS_POR_ARCHIVO
        parte += 1

    print(f"\n✅ Archivos generados en '{CARPETA_SALIDA}'")
    print(f"🔁 Total identificaciones procesadas: {len(all_ids)}")
    print(f"📁 Total archivos SQL: {parte - 1}")

if __name__ == '__main__':
    generar_update_sql()
