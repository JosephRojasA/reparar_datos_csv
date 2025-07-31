
from procesador import generar_sql_desde_csv_condicional

generar_sql_desde_csv_condicional(
    archivo_datos="entrada/datosconsultaduplicados-1753720069062.csv",
    archivo_ids_existentes="t_tercerotodoslosid.csv",
    carpeta_salida="salida_sql"
)
from validador_nombres import validar_csv_entrada


