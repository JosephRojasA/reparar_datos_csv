import csv
import os
import re

CARPETA_ENTRADA = 'entrada'
CARPETA_IDS = 'id'
ARCHIVO_VALIDOS = 'validos.csv'

def cargar_identificaciones_existentes():
    ids_existentes = set()
    for nombre_archivo in os.listdir(CARPETA_IDS):
        if not nombre_archivo.lower().endswith('.csv'):
            continue
        with open(os.path.join(CARPETA_IDS, nombre_archivo), newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if row:
                    ids_existentes.add(row[0].strip().strip('"'))
    return ids_existentes

def nombre_valido(nombre):
    nombre = nombre.strip()
    if len(nombre) <= 1:
        return False
    if re.fullmatch(r'\W+', nombre):  # solo símbolos
        return False
    if re.fullmatch(r'\d+', nombre):  # solo números
        return False
    if nombre.count('-') > 3:
        return False
    return True

def verificar_nombres():
    ids_existentes = cargar_identificaciones_existentes()
    validos = []

    for archivo in os.listdir(CARPETA_ENTRADA):
        if not archivo.lower().endswith('.csv'):
            continue
        ruta = os.path.join(CARPETA_ENTRADA, archivo)
        with open(ruta, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                identificacion = row.get('identificacion', '').strip().strip('"')
                tipo_documento = row.get('nombredocumento', '').strip()
                nombre = row.get('nombre', '').strip().upper()
                if not identificacion or not nombre_valido(nombre):
                    continue
                if identificacion in ids_existentes:
                    continue
                validos.append((identificacion, nombre, tipo_documento))

    with open(ARCHIVO_VALIDOS, 'w', newline='', encoding='utf-8') as out:
        writer = csv.writer(out)
        writer.writerow(['identificacion', 'nombres', 'tipo_documento'])
        writer.writerows(validos)

    print("Verificación completada. Registros válidos:", len(validos))

if __name__ == '__main__':
    verificar_nombres()
