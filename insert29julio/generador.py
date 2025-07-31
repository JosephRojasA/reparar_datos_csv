import csv
import os
import re
from textwrap import dedent

# === CONFIGURACIÓN GENERAL ===
REGISTROS_POR_INSERT = 1000
INSERTS_POR_ARCHIVO = 100
REGISTROS_POR_ARCHIVO = REGISTROS_POR_INSERT * INSERTS_POR_ARCHIVO
NOMBRE_TABLA = "t_tercero"
CARPETA_ENTRADA = "entrada"
CARPETA_SALIDA = "salida"
CARPETA_IDS = "id"
BASE_DATOS = "gdocxhl"

def limpiar(valor):
    return str(valor).replace("'", "''").strip() if valor else ""

def nombre_valido(nombre):
    if not nombre or len(nombre) < 2:
        return False
    nombre = nombre.strip()
    if re.fullmatch(r"[\W\d_]+", nombre):  # Solo símbolos o números
        return False
    if re.fullmatch(r"[A-Za-z]$", nombre):  # Solo una letra
        return False
    if nombre.count("-") > 3:
        return False
    if nombre in {".", "-", "--", "..."}:
        return False
    return True

def cargar_ids_existentes():
    existentes = set()
    for archivo in os.listdir(CARPETA_IDS):
        if archivo.lower().endswith(".csv"):
            with open(os.path.join(CARPETA_IDS, archivo), "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                for row in reader:
                    if row:
                        existentes.add(row[0].strip())
    return existentes

def generar_sql():
    os.makedirs(CARPETA_SALIDA, exist_ok=True)
    ids_existentes = cargar_ids_existentes()

    datos = []
    for archivo_csv in os.listdir(CARPETA_ENTRADA):
        if archivo_csv.lower().endswith(".csv"):
            ruta_csv = os.path.join(CARPETA_ENTRADA, archivo_csv)
            with open(ruta_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    identificacion = limpiar(row.get("identificacion", ""))
                    tipo_documento = limpiar(row.get("nombredocumento", ""))
                    nombre = limpiar(row.get("nombre", ""))

                    if not identificacion or identificacion in ids_existentes:
                        continue
                    if not nombre_valido(nombre):
                        continue

                    datos.append((identificacion, nombre, tipo_documento, ""))  # Apellidos en blanco
                    ids_existentes.add(identificacion)  # Evita duplicados dentro del lote

    total_registros = len(datos)
    total_archivos = (total_registros + REGISTROS_POR_ARCHIVO - 1) // REGISTROS_POR_ARCHIVO

    for i in range(total_archivos):
        parte = i + 1
        nombre_archivo = os.path.join(CARPETA_SALIDA, f"insert_terceros_parte{parte}.sql")

        with open(nombre_archivo, "w", encoding="utf-8") as archivo_sql:
            archivo_sql.write(f"-- Inserciones para {NOMBRE_TABLA}\n")
            archivo_sql.write(f"USE {BASE_DATOS};\nGO\n\n")

            for j in range(INSERTS_POR_ARCHIVO):
                inicio = i * REGISTROS_POR_ARCHIVO + j * REGISTROS_POR_INSERT
                fin = min(inicio + REGISTROS_POR_INSERT, total_registros)
                if inicio >= total_registros:
                    break

                valores = []
                for r in datos[inicio:fin]:
                    valores.append(f"('{r[0]}', '{r[1]}', '{r[2]}', '{r[3]}')")

                if valores:
                    insert_sql = dedent(f"""
                        INSERT INTO {NOMBRE_TABLA} (
                            identificacion, nombres, tipo_documento, apellidos
                        ) VALUES
                        {",\n                        ".join(valores)};
                        GO
                    """).strip()
                    archivo_sql.write(insert_sql + "\n\n")

    print(f"✅ Archivos generados en '{CARPETA_SALIDA}'. Total registros: {total_registros}")
