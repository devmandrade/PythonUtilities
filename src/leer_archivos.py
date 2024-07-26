"""Módulo leer_archivos.

Este módulo contiene herramientas para leer archivos de texto
y cargarlos a Oracle.

"""

import csv


def obtener_tupla_ancho_fijo(linea: str, mapa_posiciones: dict):
    """Obtener una tupla de entradas a partir de una línea de ancho fijo.

    Argumentos:
      - linea: línea proveniente de un archivo de ancho fijo
      - mapa_posiciones: mapa que a cada columna le asocia un par `(ini, fin)`
            de posiciones en la línea

    Retorna:
        Una tupla de `str`
    """
    tupla = []
    for ini, fin in mapa_posiciones.values():
        entrada = linea[ini:fin].strip()
        tupla.append(entrada)
    return tupla


def transformar_tupla(tupla: list, mapa_transformadores: dict):
    """Transformar una tupla con un mapa de funciones.

    Argumentos:
        - tupla: una lista de valores
        - mapa_transformadores: un diccionario que asocia a cada entrada
          de la tupla una función a ser aplicada

    Retorna:
        Una tupla cuyos valores han sido transformados.
    """
    columnas = list(mapa_transformadores.keys())
    valores_transformados = [
        mapa_transformadores[columna](valor) if len(valor) > 0 else None
        for columna, valor in zip(columnas, tupla)
    ]
    return valores_transformados


def cargar_bloque_tabla(bloque, tabla, conexion, cursor):
    especificacion_columnas = ",".join([f":{i+1}" for i in range(len(bloque[0]))])
    consulta = f"INSERT INTO {tabla} VALUES ({especificacion_columnas})"

    try:
        print("Insertando bloque...", end="")
        cursor.executemany(consulta, bloque)
        conexion.commit()
        print(" listo.")
    except Exception as e:
        print(bloque)
        raise e


def cargar_csv_por_bloques(
    archivo,
    tabla,
    tamaño_bloque=1000,
    mapa_transformadores=None,
    conexion=None,
    codificacion=None,
):
    """Leer un archivo CSV, cargando los contenidos a una tabla Oracle.

    Argumentos:
        - archivo: ruta del archivo CSV a cargar
        - tabla: nombre de la tabla a escribir
        - tamaño_bloque: cantidad de filas a escribir en cada iteración
        - mapa_transformadores (opcional): diccionario que asocia a cada columna una función
          a ser aplicada para preparar el valor
        - conexion (opcional): objeto de conexión a Oracle
        - codificacion (opcional): codificación de texto con la cual leer el archivo
          (usualmente es "utf-8")

    Retorna:
        Nada
    """
    cursor = conexion.cursor()

    with open(archivo, "r", encoding=codificacion) as archivo:
        lector = csv.reader(archivo, delimiter=";")

        # leer cabecera
        columnas = next(lector)
        n_columnas = len(columnas)

        bloque = []
        i_bloque = 0
        while True:
            # obtener siguiente fila:
            valores = next(lector, None)
            if valores is None:
                break

            valores_transformados = transformar_tupla(valores, mapa_transformadores)
            bloque.append(valores_transformados)

            # insertar bloque
            if len(bloque) >= tamaño_bloque:
                cargar_bloque_tabla(bloque, tabla, conexion, cursor)
                print(f"Bloque {i_bloque} listo!")
                bloque = []
                i_bloque += 1

        # vaciar bloque:
        if len(bloque) > 0:
            cargar_bloque_tabla(bloque, tabla, conexion, cursor)
            bloque = []
