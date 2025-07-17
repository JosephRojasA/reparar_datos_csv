def estandarizar_columnas(df):
    columnas = [col.lower().strip() for col in df.columns]

    # Mapear nombres esperados a estándar
    mapeo = {
        'nombres': 'nombres',
        'apellidos': 'apellidos',
        'identificacion': 'identificacion',
        'tipo_documento': 'tipo_documento',
    }

    columnas_estandar = {}
    for col in columnas:
        clave = col.lower()
        for key in mapeo:
            if key in clave:
                columnas_estandar[col] = mapeo[key]

    df = df.rename(columns=columnas_estandar)

    # Reordenar columnas
    columnas_final = ['nombres', 'apellidos', 'identificacion', 'tipo_documento']
    for col in columnas_final:
        if col not in df.columns:
            df[col] = None  # Rellenar si falta

    return df[columnas_final]
