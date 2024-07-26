"""Módulo compresión

Artefactos para comprimir y descomprimir archivos.

Autores:
- @mandrade
- @atorres
"""

import os
import zipfile
from pathlib import Path

import py7zr
import rarfile


def comprimir_archivo(ruta_origen: str, ruta_destino: str, nombre: str = None):
    """Función para comprimir archivos.

    Argumentos:
        ruta_origen (str): Ruta del archivo incluido el nombre y su extensión.
        ruta_destino (str): Ruta de destino incluido el nombre del archivo y su extensión.
        nombre (str, optional): Nombre del archivo comprimido, puede ser vacío
        y por defecto tomará el nombre que se le de en ruta_destino.

    Retorna:
        bool: `True` si tuvo éxito la operación.
    """
    if not ruta_destino.endswith(".zip"):
        ruta_destino += ".zip"
    nombre = nombre or os.path.basename(ruta_origen)
    with zipfile.ZipFile(ruta_destino, "w", zipfile.ZIP_DEFLATED) as archivo_zip:
        archivo_zip.write(ruta_origen, nombre)
    return True


def descomprimir_archivo(archivo, destino, password=None):
    """Descomprime archivos .zip, .rar y .7z.

    Argumentos:
        archivo: ruta del archivo de origen
        destino: el destino
        password: opcionalmente la clave

    Retorna:
        tupla con el nombre del archivo descomprimido, la ruta completa del archivo y el tamaño del archivo.
    """
    archivo_path = Path(archivo)
    archivo_nombre = ""
    archivo_ruta_completa = ""
    archivo_tamaño = 0

    if archivo_path.suffix == ".zip":
        with zipfile.ZipFile(archivo_path, "r") as zip_ref:
            if password:
                zip_ref.setpassword(password.encode())
            zip_ref.extractall(destino, pwd=password.encode() if password else None)
            archivo_nombre = zip_ref.namelist()[0]
            archivo_ruta_completa = Path(destino) / archivo_nombre
            archivo_tamaño = archivo_ruta_completa.stat().st_size
            print(f"Archivo {archivo_path.name} descomprimido en {destino}")
            print(f"- {archivo_nombre}")

    elif archivo_path.suffix == ".rar":
        with rarfile.RarFile(archivo_path, "r") as rar_ref:
            if password:
                rar_ref.setpassword(password)
            rar_ref.extractall(destino)
            archivo_nombre = rar_ref.namelist()[0]
            archivo_ruta_completa = Path(destino) / archivo_nombre
            archivo_tamaño = archivo_ruta_completa.stat().st_size
            print(f"Archivo {archivo_path.name} descomprimido en {destino}")
            print(f"- {archivo_nombre}")

    elif archivo_path.suffix == ".7z":
        with py7zr.SevenZipFile(archivo_path, "r", password=password) as sevenzip_ref:
            sevenzip_ref.extractall(path=destino)
            archivo_nombre = sevenzip_ref.getnames()[0]
            archivo_ruta_completa = Path(destino) / archivo_nombre
            archivo_tamaño = archivo_ruta_completa.stat().st_size
            print(f"Archivo {archivo_path.name} descomprimido en {destino}")
            print(f"- {archivo_nombre}")

    else:
        print(f"Formato de archivo no soportado: {archivo_path.suffix}")

    return archivo_nombre, archivo_ruta_completa, archivo_tamaño
