"""Módulo log

Artefacto de log.

"""

import logging
import os
from datetime import datetime


def configurar_log(dir_log: str, nombre: str):
    """Configurar el módulo logging.

    Argumentos:
        dir_log: Directorio donde se guardará el log.
        nombre: Nombre del archivo log.
    """
    if not os.path.exists(dir_log):
        os.makedirs(dir_log)

    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = dir_log + nombre + "_" + fecha + ".log"
    print(log_file)
    logging.basicConfig(
        filename=log_file,
        filemode="w",
        level=logging.INFO,
        format="[%(asctime)s] [%(module)s.%(funcName)s] [%(levelname)s]: %(message)s",
    )


def print_log(msg: str):
    """Guarda mensajes en archivo log.

    Argumentos:
        msg: mensaje a guardar en log.
    """
    logging.info(msg)
    print(msg)
