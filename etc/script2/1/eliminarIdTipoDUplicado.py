import pandas as pd
import re

# === Cargar archivo limpio ===
df = pd.read_csv("../salida/datos_final_nombre_valido.csv", dtype=str)

# === Limpiar espacios ===
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# === Función para quitar tipo_documento duplicado del inicio de identificacion ===
def limpiar_identificacion(tipo, identificacion):
    if pd.isna(tipo) or pd.isna(identificacion):
        return identificacion
    tipo = tipo.strip()
    identificacion = identificacion.strip()

    # Buscar patrón tipo: 'RC 123456' o 'RC123456'
    patron = r'^' + re.escape(tipo) + r'[\s\-]*'
    nueva_ident = re.sub(patron, '', identificacion)
    return nueva_ident

# === Aplicar limpieza fila por fila ===
df["identificacion"] = df.apply(lambda row: limpiar_identificacion(row["tipo_documento"], row["identificacion"]), axis=1)

# === Guardar archivo corregido ===
df.to_csv("../salida/datos_final_documentos_limpios.csv", index=False)

print("✅ Identificaciones corregidas y archivo guardado como: datos_final_documentos_limpios.csv")
