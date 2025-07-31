import csv
import re

# Solo se permiten letras, espacios y guiones medios
REGEX_VALIDO = re.compile(r"^[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ \-]{1,}$", re.IGNORECASE)
REGEX_SOLO_NUMEROS = re.compile(r"^\d+$")
REGEX_SOLO_UNA_LETRA = re.compile(r"^[A-ZÁÉÍÓÚÑ]$", re.IGNORECASE)
REGEX_COMIENZA_SIMBOLO = re.compile(r"^[^A-ZÁÉÍÓÚÑ]", re.IGNORECASE)

def es_nombre_valido(texto):
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

def validar_csv_entrada(archivo_entrada, archivo_salida_valido, archivo_salida_invalido):
    with open(archivo_entrada, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        registros_validos = []
        registros_invalidos = []

        for fila in reader:
            nombre = fila.get("nombres", "").strip()
            apellido = fila.get("apellidos", "").strip()

            if es_nombre_valido(nombre) and es_nombre_valido(apellido):
                registros_validos.append(fila)
            else:
                registros_invalidos.append(fila)

    # Guardar válidos
    with open(archivo_salida_valido, "w", newline="", encoding="utf-8") as f_val:
        writer = csv.DictWriter(f_val, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(registros_validos)

    # Guardar inválidos
    with open(archivo_salida_invalido, "w", newline="", encoding="utf-8") as f_inv:
        writer = csv.DictWriter(f_inv, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(registros_invalidos)

    print(f"✅ Validación completada.")
    print(f"✔ Registros válidos: {len(registros_validos)} guardados en: {archivo_salida_valido}")
    print(f"❌ Registros inválidos: {len(registros_invalidos)} guardados en: {archivo_salida_invalido}")
