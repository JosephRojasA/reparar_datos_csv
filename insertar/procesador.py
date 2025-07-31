import csv
import os
import re
from textwrap import dedent

# === CONFIGURACIÓN GENERAL ===
REGISTROS_POR_INSERT = 1000
INSERTS_POR_ARCHIVO = 100
REGISTROS_POR_ARCHIVO = REGISTROS_POR_INSERT * INSERTS_POR_ARCHIVO
NOMBRE_TABLA = "gdocxhl.dbo.t_tercero"

# === Validaciones de nombre ===
REGEX_VALIDO = re.compile(r"^[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ \-]{1,}$", re.IGNORECASE)
REGEX_SOLO_NUMEROS = re.compile(r"^\d+$")
REGEX_SOLO_UNA_LETRA = re.compile(r"^[A-ZÁÉÍÓÚÑ]$", re.IGNORECASE)
REGEX_COMIENZA_SIMBOLO = re.compile(r"^[^A-ZÁÉÍÓÚÑ]", re.IGNORECASE)

def limpiar(valor):
    return str(valor).replace("'", "''").strip() if valor else ""

def separar_nombre_apellido(nombre_completo):
    partes = nombre_completo.strip().split()
    if len(partes) >= 4:
        return ' '.join(partes[:2]), ' '.join(partes[2:])
    elif len(partes) == 3:
        return partes[0], ' '.join(partes[1:])
    elif len(partes) == 2:
        return partes[0], partes[1]
    elif len(partes) == 1:
        return partes[0], ''
    return '', ''

def es_valido(texto):
    if not texto:
        return False
    texto = texto.strip()
    if REGEX_SOLO_NUMEROS.match(texto):
        return False
    if REGEX_SOLO_UNA_LETRA.match(texto):
        return False
    if REGEX_COMIENZA_SIMBOLO.match(texto):
        return False
    if not REGEX_VALIDO.match(texto):
        return False
    return True

def cargar_ids_existentes(archivo_ids):
    existentes = set()
    with open(archivo_ids, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None)  # Saltar encabezado si lo hay
        for row in reader:
            if row and row[0].strip():
                existentes.add(row[0].strip())
    return existentes

def generar_sql_desde_csv_condicional(archivo_datos, archivo_ids_existentes, carpeta_salida):
    ids_existentes = cargar_ids_existentes(archivo_ids_existentes)

    with open(archivo_datos, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        datos = list(reader)

    os.makedirs(carpeta_salida, exist_ok=True)
    errores = []

    datos_filtrados = []
    for registro in datos:
        identificacion = limpiar(registro.get("numDocumento", ""))
        if not identificacion or identificacion in ids_existentes:
            continue

        nombre_completo = limpiar(registro.get("nomApellido", ""))
        nombres, apellidos = separar_nombre_apellido(nombre_completo)
        nombres = limpiar(nombres)
        apellidos = limpiar(apellidos)

        if not (es_valido(nombres) and es_valido(apellidos)):
            errores.append({
                "numDocumento": identificacion,
                "nombres": nombres,
                "apellidos": apellidos
            })
            continue

        datos_filtrados.append((identificacion, nombres, apellidos, ""))  # tipo_documento vacío
        ids_existentes.add(identificacion)

    total_registros = len(datos_filtrados)
    total_archivos = (total_registros + REGISTROS_POR_ARCHIVO - 1) // REGISTROS_POR_ARCHIVO

    for i in range(total_archivos):
        parte = i + 1
        nombre_archivo = os.path.join(carpeta_salida, f"parte{parte}.sql")

        with open(nombre_archivo, "w", encoding="utf-8") as archivo_sql:
            archivo_sql.write(f"-- Inserciones para {NOMBRE_TABLA} (sin validación EXISTS)\n")
            archivo_sql.write("USE gdocxhl;\nGO\n\n")

            for j in range(INSERTS_POR_ARCHIVO):
                inicio = i * REGISTROS_POR_ARCHIVO + j * REGISTROS_POR_INSERT
                fin = min(inicio + REGISTROS_POR_INSERT, total_registros)
                if inicio >= total_registros:
                    break

                values = [
                    f"('{id_}', '{n}', '{a}', '{td}')"
                    for id_, n, a, td in datos_filtrados[inicio:fin]
                ]

                if values:
                    insert_sql = dedent(f"""
                        INSERT INTO {NOMBRE_TABLA} (
                            identificacion, nombres, apellidos, tipo_documento
                        )
                        VALUES
                        {",\n                        ".join(values)};
                        GO
                    """).strip()

                    archivo_sql.write(insert_sql + "\n\n")

    if errores:
        archivo_errores = os.path.join(carpeta_salida, "errores.csv")
        with open(archivo_errores, "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["numDocumento", "nombres", "apellidos"])
            writer.writeheader()
            writer.writerows(errores)
        print(f"⚠️  {len(errores)} registros inválidos guardados en '{archivo_errores}'.")

    print(f"✅ SQL generado en '{carpeta_salida}' con {total_archivos} archivo(s).")
