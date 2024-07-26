"""
@autor: Madison Andrade M.
Fecha creación: 2024-06-17
Descripción: Formatea a un número legible fácilmente.
"""


def formato_tamano(tamano_bytes):
    """
    Convierte un tamaño en bytes a una representación en unidades más grandes (KB, MB, GB).

    Argumentos:
        tamano_bytes (int): El tamaño en bytes para ser convertido.

    Retorna:
        str: Una cadena de texto: bytes (B), kilobytes (KB), megabytes (MB) o gigabytes (GB).
    """
    KB = 1024
    MB = KB * 1024
    GB = MB * 1024

    if tamano_bytes >= GB:
        return f"{tamano_bytes / GB:.1f} GB"
    elif tamano_bytes >= MB:
        return f"{tamano_bytes / MB:.1f} MB"
    elif tamano_bytes >= KB:
        return f"{tamano_bytes / KB:.1f} KB"
    else:
        return f"{tamano_bytes} B"
