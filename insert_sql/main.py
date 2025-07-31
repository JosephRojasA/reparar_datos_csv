import subprocess
import os

def ejecutar(script):
    print(f"\n--- Ejecutando: {script} ---")
    resultado = subprocess.run(['python', script], capture_output=True, text=True)
    print(resultado.stdout)
    if resultado.stderr:
        print(f"Errores:\n{resultado.stderr}")

def main():
    print("Iniciando proceso completo...")

    if not os.path.exists('entrada'):
        print("No se encontró la carpeta 'entrada'. Crea una y coloca los CSV a procesar.")
        return

    if not os.path.exists('id'):
        print("No se encontró la carpeta 'id'. Allí deben estar los identificadores existentes.")
        return

    ejecutar('verificarNombres.py')

    if not os.path.exists('validos.csv'):
        print("No se generó el archivo 'validos.csv'. Verifica si hubo errores.")
        return

    ejecutar('generador.py')
    print("\n✅ Proceso finalizado correctamente.")

if __name__ == '__main__':
    main()
