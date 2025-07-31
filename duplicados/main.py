from procesador import procesar_duplicados_y_generar_sql

def main():
    archivo_entrada = "duplicados.csv"
    carpeta_salida = "deletes"
    procesar_duplicados_y_generar_sql(archivo_entrada, carpeta_salida)

if __name__ == "__main__":
    main()
