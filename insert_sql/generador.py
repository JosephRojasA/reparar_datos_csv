import csv
import os

CARPETA_SALIDA = 'salida'
ARCHIVO_ENTRADA = 'validos.csv'
BASE_DATOS = 'gdocxhl'

TAMANO_INSERT_SQLSERVER = 1000
TAMANO_POR_ARCHIVO = 100000

def limpiar(texto):
    return texto.replace("'", "''").strip()

def escribir_bloques(sqlfile, valores):
    for i in range(0, len(valores), TAMANO_INSERT_SQLSERVER):
        bloque = valores[i:i+TAMANO_INSERT_SQLSERVER]
        sqlfile.write("INSERT INTO t_tercero (\n    identificacion,\n    nombres,\n    tipo_documento,\n    apellidos\n) VALUES\n")
        sqlfile.write(",\n".join(bloque))
        sqlfile.write(";\nGO\n\n")

def generar_insert_sql():
    if not os.path.exists(ARCHIVO_ENTRADA):
        print(f"Archivo no encontrado: {ARCHIVO_ENTRADA}")
        return

    os.makedirs(CARPETA_SALIDA, exist_ok=True)

    valores_por_archivo = []
    total_insertados = 0
    parte_actual = 1

    def abrir_nuevo_archivo(parte):
        path = os.path.join(CARPETA_SALIDA, f"insert_terceros_parte{parte}.sql")
        archivo = open(path, 'w', encoding='utf-8')
        archivo.write(f"USE {BASE_DATOS};\nGO\n\n")
        print(f"Escribiendo archivo: {path}")
        return archivo

    archivo_actual = abrir_nuevo_archivo(parte_actual)

    with open(ARCHIVO_ENTRADA, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            identificacion = limpiar(row['identificacion'])
            nombres = limpiar(row['nombres'])
            tipo_documento = limpiar(row['tipo_documento'])
            apellidos = ''

            valor = f"('{identificacion}', '{nombres}', '{tipo_documento}', '{apellidos}')"
            valores_por_archivo.append(valor)
            total_insertados += 1

            if len(valores_por_archivo) >= TAMANO_POR_ARCHIVO:
                escribir_bloques(archivo_actual, valores_por_archivo)
                archivo_actual.close()
                valores_por_archivo = []
                parte_actual += 1
                archivo_actual = abrir_nuevo_archivo(parte_actual)

    if valores_por_archivo:
        escribir_bloques(archivo_actual, valores_por_archivo)
        archivo_actual.close()

    print(f"\nArchivos generados correctamente en '{CARPETA_SALIDA}'")
    print(f"Total de registros insertados: {total_insertados}")
    print(f"Total de archivos SQL: {parte_actual}")

if __name__ == '__main__':
    generar_insert_sql()
