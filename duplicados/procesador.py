import pandas as pd
import os
import math

def calidad_registro(row):
    nombres = str(row['nombres']).strip()
    apellidos = str(row['apellidos']).strip()

    nombre_partes = nombres.split()
    apellido_partes = apellidos.split()

    score = 0

    # Nombre vacío o inválido
    if nombres == "" or nombres.lower() in ["nan", "none"]:
        score += 3

    # Apellido vacío
    if apellidos == "" or apellidos.lower() in ["nan", "none"]:
        score += 2

    # Nombre con muchas palabras (posibles apellidos mezclados)
    if len(nombre_partes) > 2:
        score += 1
    if len(nombre_partes) >= 4:
        score += 2  # penalización adicional

    # Apellido sospechoso
    if len(apellido_partes) == 0:
        score += 2
    if len(apellido_partes) > 4:
        score += 2

    # Apellido = nombre (probable error)
    if nombres == apellidos:
        score += 3

    return score

def procesar_duplicados_y_generar_sql(archivo_csv, carpeta_salida):
    # CSV estándar: coma y comillas dobles
    df = pd.read_csv(archivo_csv, delimiter=",", quotechar='"', encoding="utf-8")

    # Validar existencia de columnas esperadas
    columnas_requeridas = ['id', 'identificacion', 'nombres', 'apellidos']
    for col in columnas_requeridas:
        if col not in df.columns:
            raise ValueError(f"❌ Columna faltante en el CSV: '{col}'")

    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df = df.dropna(subset=['id'])  # Quita filas sin ID válido
    df['id'] = df['id'].astype(int)

    print(f"✅ Total registros cargados: {len(df)}")

    # Duplicados por 'identificacion'
    duplicados = df[df.duplicated(subset=['identificacion'], keep=False)]
    print(f"🔁 Total duplicados detectados: {len(duplicados)}")

    ids_a_eliminar = []

    for identificacion, grupo in duplicados.groupby('identificacion'):
        if len(grupo) <= 1:
            continue

        grupo = grupo.copy()
        grupo['score'] = grupo.apply(calidad_registro, axis=1)

        # Ordenar por score (menor es mejor)
        grupo_ordenado = grupo.sort_values(by='score')
        mejor = grupo_ordenado.iloc[0]
        ids_malos = grupo_ordenado.iloc[1:]['id'].tolist()
        ids_a_eliminar.extend(ids_malos)

    print(f"🗑️ Total IDs a eliminar: {len(ids_a_eliminar)}")

    # Crear carpeta si no existe
    os.makedirs(carpeta_salida, exist_ok=True)

    total_deletes = math.ceil(len(ids_a_eliminar) / 1000)
    archivos = math.ceil(total_deletes / 100)

    for i in range(archivos):
        nombre_archivo = os.path.join(carpeta_salida, f"delete_parte_{i+1}.sql")
        with open(nombre_archivo, "w", encoding="utf-8") as f:
            for j in range(100):
                start = (i * 100 + j) * 1000
                end = start + 1000
                ids_lote = ids_a_eliminar[start:end]
                if not ids_lote:
                    break
                ids_str = ", ".join(map(str, ids_lote))
                f.write(f"DELETE FROM t_tercero WHERE id IN ({ids_str});\n")
        print(f"📄 Archivo generado: {nombre_archivo}")
