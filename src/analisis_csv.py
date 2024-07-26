import csv
import re
import chardet
import csv
import dask.dataframe as dd
from dask.distributed import Client, get_client
import os
import time
import numpy as np
from datetime import datetime, timedelta
import win32api
import win32con
import pdfkit
import time
import hashlib
import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import mimetypes
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders
from email.message import Message

now = datetime.now()
import shutil
import zipfile
from unrar import rarfile
from os import remove
import pyzipper

cabecera = ""
delimiter_1 = []
delimiter_2 = []
# contar_columnas_tipos=[][]
carpeta_archivos = "\\\\nas05\\Repositorio_Datos_ADM\\30_AUTOMATIZACIONES\\11_CHECK_ARCHIVOS\\carpetas\\pendientes\\"
carpeta_archivos_procesados = "\\\\nas05\\Repositorio_Datos_ADM\\30_AUTOMATIZACIONES\\11_CHECK_ARCHIVOS\\carpetas\\procesados\\"
carpeta_archivos_error = "\\\\nas05\\Repositorio_Datos_ADM\\30_AUTOMATIZACIONES\\11_CHECK_ARCHIVOS\\carpetas\\errores\\"
carpeta_archivos_error_descomprimir = "\\\\nas05\\Repositorio_Datos_ADM\\30_AUTOMATIZACIONES\\11_CHECK_ARCHIVOS\\carpetas\\error_descomprimir\\"
dir_gobierno_datos = "\\\\nas05\\Repositorio_Datos_ADM\\30_AUTOMATIZACIONES\\03_GOBIERNO_DATOS_ANALISIS_FUENTES\\pendientes\\"
# log = "kjkljkl"


def html_desc(nom_dir_archivo, nombre_archivo):
    global nombre
    global crea_fecha
    global modi_fecha
    global fecha_informe
    global Checksum
    global peso
    global extension
    global crea_dia
    global crea_mes
    global crea_anio
    global modi_dia
    global modi_mes
    global modi_anio
    global me
    global you
    global cc
    global otrocc

    otrocc = "carce@desarrollosocial.cl"
    # otrocc=""
    me = "atorres@desarrollosocial.cl"
    you = "pamela.soto@desarrollosocial.gob.cl;"
    # you = "atorres@desarrollosocial.cl"
    cc = "atorres@desarrollosocial.cl"
    archivo_split = archivo.split(".")
    extension = archivo_split[1]
    Checksum = md5(nom_dir_archivo + nombre_archivo)
    html = ""
    nombre = Obtener_owner(nom_dir_archivo + nombre_archivo)
    crea_fecha, modi_fecha = Obtener_Fechas(nom_dir_archivo + nombre_archivo)
    crea_dia, crea_mes, crea_anio = Convertir_Fechas(crea_fecha)
    modi_dia, modi_mes, modi_anio = Convertir_Fechas(modi_fecha)
    peso = os.stat(nom_dir_archivo + nombre_archivo).st_size
    months = ("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
    month = months[now.month - 1]
    fecha_informe = str(now.day) + "-" + str(month) + "-" + str(now.year)
    html = html + "<div  >"
    html = html + "<br></br>"
    html = html + "<h1 style='text-align: left'>Metadatos del Archivo </h1>"
    html = (
        html
        + '<table   style="border:1px solid #000000;border-collapse: collapse;text-align: left" >'
    )
    html = (
        html
        + '<tr ><td style="text-align: left"><strong>Creado (propietario):</strong></td> <td style="text-align: left">'
        + nombre
        + "</td></tr>"
    )
    html = (
        html
        + '<tr ><td style="text-align: left"><strong>Fecha Creación:</strong></td> <td style="text-align: left">'
        + crea_dia
        + "-"
        + crea_mes
        + "-"
        + crea_anio
        + "</td></tr>"
    )
    html = (
        html
        + '<tr ><td style="text-align: left"><strong>Fecha Modificación:</strong></td> <td style="text-align: left">'
        + modi_dia
        + "-"
        + modi_mes
        + "-"
        + modi_anio
        + "</td></tr>"
    )
    html = (
        html
        + '<tr><td style="text-align: left"><strong>Nombre archivo:</strong></td><td style="text-align: left">'
        + nombre_archivo
        + "</td></tr>"
    )
    html = (
        html
        + '<tr><td style="text-align: left"><strong>Fecha Generación informe:</strong></td><td style="text-align: left">'
        + fecha_informe
        + "</td></tr>"
    )
    html = (
        html
        + '<tr><td style="text-align: left"><strong>Checksum:</strong></td><td style="text-align: left">'
        + Checksum
        + "</td></tr>"
    )
    html = (
        html
        + '<tr><td style="text-align: left"><strong>Tamaño:</strong></td><td style="text-align: left">{:,.0f}'.format(
            peso
        ).replace(
            ",", "."
        )
        + " bytes</td></tr>"
    )
    html = (
        html
        + '<tr><td style="text-align: left"><strong>Extensión</strong></td><td style="text-align: left">'
        + extension
        + " </td></tr>"
    )
    html = html + "</table></div>"

    return html


def html_desc2(
    estado,
    encoding,
    delimitador,
    num_col,
    total_reg,
    mensaje,
    lineas_error,
    contador_lineas_error,
):

    html = "<div  >"
    html = html + "<br>"
    html = html + "<h1 style='text-align: left'>Formato del Archivo </h1>"
    html = (
        html
        + '<table   style="border:1px solid #000000;border-collapse: collapse;text-align: left" >'
    )
    if estado:
        html = (
            html
            + '<tr ><td style="text-align: left"><strong>Estado:</strong></td> <td style="text-align: left"><b>VÁLIDO</b></td></tr>'
        )
    else:
        html = (
            html
            + '<tr ><td style="text-align: left"><strong>Estado:</strong></td> <td style="text-align: left;color:red"><b>INCORRECTO</b></td></tr>'
        )

    html = (
        html
        + '<tr ><td style="text-align: left"><strong>Encoding:</strong></td> <td style="text-align: left">'
        + encoding
        + "</td></tr>"
    )
    html = (
        html
        + '<tr ><td style="text-align: left"><strong>Delimitador:</strong></td> <td style="text-align: left">'
        + delimiter
        + "</td></tr>"
    )
    html = (
        html
        + '<tr ><td style="text-align: left"><strong>Número de Columnas:</strong></td> <td style="text-align: left">'
        + str(num_col)
        + "</td></tr>"
    )
    html = (
        html
        + '<tr><td style="text-align: left"><strong>Total Registros:</strong></td><td style="text-align: left">{:,.0f}'.format(
            total_reg
        ).replace(
            ",", "."
        )
        + "</td></tr>"
    )
    html = (
        html
        + '<tr><td style="text-align: left"><strong>Informe de errores:</strong></td><td style="text-align: left">'
        + mensaje
        + "</td></tr>"
    )
    if not estado:
        html = (
            html
            + '<tr><td style="text-align: left"><strong>TOP 10 líneas con errores:</strong></td><td style="text-align: left">'
            + str(lineas_error)
            + "</td></tr>"
        )
        html = (
            html
            + '<tr><td style="text-align: left"><strong>TOTAL líneas con errores:</strong></td><td style="text-align: left">'
            + str(contador_lineas_error)
            + "</td></tr>"
        )
    html = html + "</table></div>"

    return html


def md5(file1):
    md5h = hashlib.md5()
    with open(file1, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5h.update(chunk)
    return md5h.hexdigest()


def Convertir_Fechas(fecha):
    h = datetime.strptime(str(fecha), "%a %b %d %H:%M:%S %Y")
    f = str(h).split()
    d = f[0].split("-")
    dia = d[2]
    mes = d[1]
    anio = d[0]

    if mes == "Jan":
        mes = "01"
    if mes == "Feb":
        mes = "02"
    if mes == "Mar":
        mes = "03"
    if mes == "Apr":
        mes = "04"
    if mes == "May":
        mes = "05"
    if mes == "Jun":
        mes = "06"
    if mes == "Jul":
        mes = "07"
    if mes == "Aug":
        mes = "08"
    if mes == "Sep":
        mes = "09"
    if mes == "Oct":
        mes = "10"
    if mes == "Nov":
        mes = "11"
    if mes == "Dec":
        mes = "12"
    return dia, mes, anio


def Obtenerpeso(dir_nombre):
    peso = os.stat(dir_nombre).st_size
    return peso


def Obtener_Fechas(FILENAME):
    ti_c = os.path.getctime(FILENAME)
    ti_m = os.path.getmtime(FILENAME)
    c_ti = time.ctime(ti_c)
    m_ti = time.ctime(ti_m)
    return c_ti, m_ti


def Obtener_owner(FILENAME):
    name_split = str(win32api.GetUserNameEx(win32con.NameSamCompatible)).split("\\")
    return name_split[1]


def TotalRegistros(archivo_dir):
    with open(archivo_dir, "rb") as f:
        cont = 0
        for ln in f:
            cont = cont + 1

    print("--------------- 3.4.1 Total registros : " + str(cont))
    # log=log+"--------------- 3.4.1 Total registros : "+str(cont)+"+\n"
    return cont


def DetEnconder(archivo_dir, total_reg_csv):
    decoded = ""
    dec = True
    list_decode = []
    muestra = round(total_reg_csv * 0.3)
    print("--------------- 3.4.2 La muestra es : " + str(muestra))
    # log=log+"--------------- 3.4.2 La muestra es : "+str(muestra)+"+\n"

    with open(archivo_dir, "rb") as f:
        cont = 0
        utf8 = 0
        utf82 = 0
        iso8859 = 0
        cp1252 = 0
        cp850 = 0
        entro = False
        utf8_caracter_1 = 0
        utf8_caracter_2 = 0
        utf8_caracter_3 = 0
        comillas = 0

        for ln in f:

            cont = cont + 1
            line = ""

            for cp in ("latin-1", "UTF8", "ISO-8859-1", "cp1252", "cp850"):
                try:
                    line = ln.decode(cp)
                    if "Ã‰" in line:
                        utf8_caracter_1 = utf8_caracter_1 + 1
                    if "Ã" in line:
                        utf8_caracter_2 = utf8_caracter_2 + 1
                    if "Ãš" in line:
                        utf8_caracter_3 = utf8_caracter_3 + 1
                    if '"' in line:
                        comillas = comillas + 1

                    if cp == "latin-1":
                        utf8 = utf8 + 1
                    if cp == "UTF8":
                        utf82 = utf82 + 1
                    if cp == "ISO-8859-1":
                        iso8859 = iso8859 + 1
                    if cp == "cp1252":
                        cp1252 = cp1252 + 1
                    if cp == "cp850":
                        cp850 = cp850 + 1
                    # decoded=cp
                    # break
                except UnicodeDecodeError:
                    det = False
                    pass
                if cont == muestra:
                    entro = True
                    list_decode.append(utf8)
                    list_decode.append(utf82)
                    list_decode.append(iso8859)
                    list_decode.append(cp1252)
                    list_decode.append(cp850)
                    item_pos = list_decode.index(max(list_decode))
                    if item_pos == 0:
                        decoded = "latin-1"
                    if item_pos == 1:
                        decoded = "UTF8"
                    if item_pos == 2:
                        decoded = "ISO-8859-1"
                    if item_pos == 3:
                        decoded = "cp1252"
                    if item_pos == 4:
                        decoded = "cp850"
                    break

    if not entro:
        list_decode.append(utf8)
        list_decode.append(utf82)
        list_decode.append(iso8859)
        list_decode.append(cp1252)
        list_decode.append(cp850)
        item_pos = list_decode.index(max(list_decode))
        if item_pos == 0:
            decoded = "latin-1"
        if item_pos == 1:
            decoded = "UTF8"
        if item_pos == 2:
            decoded = "ISO-8859-1"
        if item_pos == 3:
            decoded = "cp1252"
        if item_pos == 4:
            decoded = "cp850"
    if utf8_caracter_1 > 0 or utf8_caracter_2 > 0 or utf8_caracter_3 > 0:
        decoded = "UTF8"
    comi = False
    if comillas > muestra:
        comi = True

    print("--------------- 3.4.3 El enconding es : " + str(decoded))
    # log=log+"--------------- 3.4.3 El enconding es : "+str(decoded)+"+\n"
    return decoded, comi


def ObtenerCabecera(archivo_dir):
    with open(archivo_dir, encoding=decoded, errors="ignore") as file_obj:
        reader_obj = csv.reader(file_obj, delimiter="ψ")
        for row in reader_obj:
            cabecera = str(row[0])

            break
    return cabecera


def ObtenerEncoder2(archivo_dir):
    dat = open(archivo_dir, "rb").read()
    result = chardet.detect(dat)
    charenc = result["encoding"]
    return charenc


def ObtenerDelimitador(cabecera):
    delimiter_1.clear()
    delimiter_1.append(len([s.start() for s in re.finditer(";", cabecera)]))
    delimiter_1.append(len([s.start() for s in re.finditer("\|", cabecera)]))
    delimiter_1.append(len([s.start() for s in re.finditer(",", cabecera)]))
    delimiter_1.append(len([s.start() for s in re.finditer("\t", cabecera)]))
    # delimiter_1.append(len([s.start()  for s in re.finditer(" ",cabecera )]))
    delimiter_1.append(len([s.start() for s in re.finditer("~", cabecera)]))
    # delimiter_1.append(len([s.start()  for s in re.finditer("	",cabecera )]))

    delimiter = ""
    delimiter_pos = delimiter_1.index(max(delimiter_1))
    total_columnas = max(delimiter_1) + 1
    if delimiter_pos == 0:
        delimiter = ";"
    if delimiter_pos == 1:
        delimiter = "|"
    if delimiter_pos == 2:
        delimiter = ","
    if delimiter_pos == 3:
        delimiter = "Tab"
    if delimiter_pos == 4:
        delimiter = "~"
    print("--------------- 3.4.4 El delimitador es : " + str(delimiter))
    print("--------------- 3.4.5 La cantidad de columnas es : " + str(total_columnas))
    return delimiter, total_columnas


def ValidarDato(i):
    tipo = "str"
    entro = False
    try:
        int(i)
        if i[0] == "0":
            tipo = 8
        else:
            tipo = 1
        entro = True
    except:
        pass

    try:
        if i == "0":
            tipo = 1
            entro = True
    except:
        pass

    if not entro:
        try:
            float(i)
            tipo = 2
            entro = True
        except:
            pass

    if not entro:
        try:
            float(i.replace(",", "."))
            tipo = 3
            entro = True
        except:
            pass
    if not entro:
        try:
            fecha = datetime.strptime(i, "%d/%m/%Y")
            tipo = 4
            entro = True
        except:
            pass

    if not entro:
        try:
            fecha = datetime.strptime(i, "%Y/%m/%d")
            tipo = 5
            entro = True
        except:
            pass

    if not entro:
        try:
            fecha = datetime.strptime(i, "%d-%m-%Y")
            tipo = 6
            entro = True
        except:
            pass

    if not entro:
        try:
            fecha = datetime.strptime(i, "%Y-%m-%d")
            tipo = 7
            entro = True
        except:
            pass

    if not entro:
        tipo = 8

    return tipo


def AnalisiClausure(fila1, delimiter, caracter_especial, columnas):
    filax = fila1.replace(
        "" + ("\t" if delimiter == "Tab" else delimiter) + "", caracter_especial
    )

    if len(filax.split(caracter_especial)) == columnas:
        fila1 = filax.split(caracter_especial)
    else:
        fila1 = fila1.split("\t" if delimiter == "Tab" else delimiter)
        fila2 = []
        for a in range(columnas):
            fila2.append("")
        p = 0
        councomilla = 0
        for af in fila1:
            if ('"' in af) or councomilla == 1:
                if councomilla == 0:
                    councomilla = 1
                    fila2[p] = af
                else:

                    if '"' in af:
                        fila2[p] = (
                            fila2[p] + ("\t" if delimiter == "Tab" else delimiter) + af
                        )
                        councomilla = 0
                        p = p + 1
                    else:
                        fila2[p] = fila2[p] + af
            else:
                fila2[p] = af
                p = p + 1

        fila1 = fila2

    return fila1


def ValidarLargoColumnas(
    archivo_dir, num_col, validar_cabecera, encod, total_reg, cabecera_array, cabecera
):
    validado = True
    muestra = round(total_reg * 0.05)
    muestra2 = round(total_reg * 0.1)
    contador_lineas_error = 0
    with open(archivo_dir, encoding=encod, errors="ignore") as file_obj:
        reader_obj = csv.reader(file_obj, delimiter="ψ")
        h = 0
        delimiter_2.clear()
        print("num colum" + str(num_col))
        contar_columnas_tipos = np.empty((num_col, 8), dtype=int)
        comillas_columnas = np.empty((num_col), dtype=int)
        count_cc = 0
        for con in comillas_columnas:
            comillas_columnas[count_cc] = 0
            count_cc = count_cc + 1

        x = 0

        for item in contar_columnas_tipos:
            v = 0
            for s_item in item:
                contar_columnas_tipos[x][v] = 0
                v = v + 1
            x = x + 1

        ta = 0
        html2 = "<div  >"
        html2 = html2 + "<br>"
        html2 = html2 + "<h1 style='text-align: left'>Top 10 filas </h1>"
        html2 = (
            html2
            + '<table   style="border:1px solid #000000;border-collapse: collapse;text-align: left" >'
        )
        html3 = "<div><br><h1 style='text-align: left'>Líneas con error </h1><table   style='border:1px solid #000000;border-collapse: collapse;text-align: left' >"
        cabecera_html = ""
        count_row = 0
        for row in reader_obj:

            count_row = count_row + 1
            if count_row < 12:
                html2 = html2 + "<tr>"

            h = h + 1

            ###intenta averiguar cual de los dos arreglos viene la informacion, si en row o row[]
            ro_split_2 = str(row).split("\t" if delimiter == "Tab" else delimiter)
            ro_split = str(row[0]).split("\t" if delimiter == "Tab" else delimiter)

            ro = len(ro_split_2)
            ro2 = len(ro_split)

            if ro2 >= ro:
                ro = ro2
                if '"' in row[0]:
                    ro_split = AnalisiClausure(row[0], delimiter, "ψ", cabecera2)
                    ro = len(ro_split)
            else:
                if '"' in row[0]:
                    ro_split_2 = AnalisiClausure(row[0], delimiter, "ψ", cabecera2)
                    ro_split = AnalisiClausure(row[0], delimiter, "ψ", cabecera2)
                    ro = len(ro_split_2)

            if ro != cabecera2 and not ('"' in row):

                contador_lineas_error = contador_lineas_error + 1
                if ta < 11:
                    if validar_cabecera and ta == 0:
                        html3 = (
                            html3
                            + "<tr><td style='text-align: left;color:red'>Línea error</td><b>"
                            + cabecera_html
                            + "</b></tr>"
                        )

                    if not validar_cabecera and ta == 0:
                        html3 = (
                            html3
                            + "<tr><td style='text-align: left;color:red'>Cabecera</td>"
                        )
                        if len(ro_split) >= len(ro_split_2):
                            cont_colum_error = 0
                            for dato in ro_split:
                                cont_colum_error = cont_colum_error + 1
                                if cont_colum_error > ro:
                                    html3 = (
                                        html3 + '<td style="text-align: left"> </td>'
                                    )
                                else:
                                    html3 = (
                                        html3
                                        + '<td style="text-align: left">Columna '
                                        + str(cont_colum_error)
                                        + " </td>"
                                    )
                        else:
                            cont_colum_error = 0
                            for dato in ro_split_2:
                                cont_colum_error = cont_colum_error + 1
                                if cont_colum_error > ro:
                                    html3 = (
                                        html3 + '<td style="text-align: left"> </td>'
                                    )
                                else:
                                    html3 = (
                                        html3
                                        + '<td style="text-align: left">Columna '
                                        + str(cont_colum_error)
                                        + " </td>"
                                    )
                        html3 = html3 + "</tr>"

                    html3 = html3 + "<tr>"
                    html3 = (
                        html3
                        + '<td style="text-align: left"><b>Línea '
                        + str(h)
                        + "</b></td>"
                    )
                    w = 0

                    for dato in ro_split:
                        if (h < muestra) and ('"' in dato):
                            comillas_columnas[w] = comillas_columnas[w] + 1

                        if dato.strip() == "":
                            html3 = (
                                html3
                                + '<td style="text-align: left">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>'
                            )
                        else:
                            html3 = (
                                html3
                                + '<td style="text-align: left"><b>'
                                + dato
                                + "</b></td>"
                            )

                    html3 = html3 + "</tr>"
                    delimiter_2.append(h)
                    ta = ta + 1
                validado = False
            else:

                if h < muestra2 and h > 0:
                    w = 0
                    contador_columna = 0
                    for dato in ro_split:

                        contador_columna = contador_columna + 1
                        if (h < muestra) and ('"' in dato):
                            comillas_columnas[w] = comillas_columnas[w] + 1
                        if count_row < 12:
                            if not validar_cabecera and count_row == 1:
                                html2 = (
                                    html2
                                    + '<td style="text-align: left">Columna '
                                    + str(contador_columna)
                                    + "</td>"
                                )
                            elif validar_cabecera and count_row == 1:
                                cabecera_html = (
                                    cabecera_html
                                    + '<td style="text-align: left"><b>('
                                    + str(w + 1)
                                    + ")"
                                    + dato
                                    + "</b></td>"
                                )
                                html2 = (
                                    html2
                                    + '<td style="text-align: left"><b>('
                                    + str(w + 1)
                                    + ")"
                                    + dato
                                    + "</b></td>"
                                )
                            else:
                                html2 = (
                                    html2
                                    + '<td style="text-align: left">'
                                    + dato
                                    + "</td>"
                                )

                        if h < muestra2 and h > 1:
                            if not dato.strip() == "":
                                tipo = ValidarDato(dato.strip().replace('"', ""))
                                if tipo == 1:
                                    contar_columnas_tipos[w][0] = (
                                        contar_columnas_tipos[w][0] + 1
                                    )
                                if tipo == 2:
                                    contar_columnas_tipos[w][1] = (
                                        contar_columnas_tipos[w][1] + 1
                                    )
                                if tipo == 3:
                                    contar_columnas_tipos[w][2] = (
                                        contar_columnas_tipos[w][2] + 1
                                    )
                                if tipo == 4:
                                    contar_columnas_tipos[w][3] = (
                                        contar_columnas_tipos[w][3] + 1
                                    )
                                if tipo == 5:
                                    contar_columnas_tipos[w][4] = (
                                        contar_columnas_tipos[w][4] + 1
                                    )
                                if tipo == 6:
                                    contar_columnas_tipos[w][5] = (
                                        contar_columnas_tipos[w][5] + 1
                                    )
                                if tipo == 7:
                                    contar_columnas_tipos[w][6] = (
                                        contar_columnas_tipos[w][6] + 1
                                    )
                                if tipo == 8:
                                    contar_columnas_tipos[w][7] = (
                                        contar_columnas_tipos[w][7] + 1
                                    )
                        w = w + 1

            if count_row < 12:
                html2 = html2 + "</tr>"
        html2 = html2 + "</table></div>"
        html3 = html3 + "</table></div>"
        a_cont = 1
        b_cont = 0
        item_max = []
        html = "<div  >"
        html = html + "<br>"
        html = html + "<h1 style='text-align: left'>Descripción columnas </h1>"
        html = (
            html
            + '<table   style="border:1px solid #000000;border-collapse: collapse;text-align: left" >'
        )
        if validar_cabecera:
            html = (
                html
                + '<tr><td style="text-align: left"><strong>Cabecera</strong></td><td style="text-align: left"><strong>SI</strong></td></tr>'
            )
        else:
            html = (
                html
                + '<tr><td style="text-align: left"><strong>Cabecera</strong></td><td style="text-align: left;color:red"><strong>NO</strong></td></tr>'
            )

        print("--------------- 3.4.6 Descripción de columnas")
        for res_rows in contar_columnas_tipos:
            item_max.clear()
            for item in res_rows:
                item_max.append(item)
            item_pos = item_max.index(max(item_max))
            if max(item_max) == 0:
                item_pos = 7
            if item_max[7] > 0:
                item_pos = 7
            if item_pos == 0 and item_max[1] > 0:
                item_pos = 1
            if item_pos == 0 and item_max[2] > 0:
                item_pos = 2

            colum_name_num = "Columna " + str(a_cont)

            if validar_cabecera:
                colum_name_num = "(" + str(a_cont) + ")" + cabecera_array[a_cont - 1]

            if item_pos == 0:
                print(
                    "-------------------- La columna "
                    + colum_name_num
                    + " es tipo int "
                )
                if comillas_columnas[a_cont - 1] >= (muestra - 1):
                    html = (
                        html
                        + '<tr><td style="text-align: left"><strong>'
                        + colum_name_num
                        + '</strong></td><td style="text-align: left">INT (ENCERRADO EN COMILLAS)</td></tr>'
                    )
                else:
                    html = (
                        html
                        + '<tr><td style="text-align: left"><strong>'
                        + colum_name_num
                        + '</strong></td><td style="text-align: left">INT</td></tr>'
                    )

            if item_pos == 1:
                print(
                    "-------------------- La columna "
                    + colum_name_num
                    + " es tipo float con punto "
                )
                if comillas_columnas[a_cont - 1] >= (muestra - 1):
                    html = (
                        html
                        + '<tr><td style="text-align: left"><strong>'
                        + colum_name_num
                        + '</strong></td><td style="text-align: left;color:red">FLOAT CON PUNTO (ENCERRADO EN COMILLAS)</td></tr>'
                    )
                else:
                    html = (
                        html
                        + '<tr><td style="text-align: left"><strong>'
                        + colum_name_num
                        + '</strong></td><td style="text-align: left;color:red">FLOAT CON PUNTO</td></tr>'
                    )

            if item_pos == 2:
                print(
                    "-------------------- La columna "
                    + colum_name_num
                    + " es tipo float  con coma "
                )
                if comillas_columnas[a_cont - 1] >= (muestra - 1):
                    html = (
                        html
                        + '<tr><td style="text-align: left"><strong>'
                        + colum_name_num
                        + '</strong></td><td style="text-align: left;color:red">FLOAT CON COMA (ENCERRADO EN COMILLAS)</td></tr>'
                    )
                else:
                    html = (
                        html
                        + '<tr><td style="text-align: left"><strong>'
                        + colum_name_num
                        + '</strong></td><td style="text-align: left;color:red">FLOAT CON COMA</td></tr>'
                    )

            if item_pos == 3:
                print(
                    "-------------------- La columna "
                    + colum_name_num
                    + " es tipo Fecha formato dd/mm/yyyy "
                )
                if comillas_columnas[a_cont - 1] >= (muestra - 1):
                    html = (
                        html
                        + '<tr><td style="text-align: left"><strong>'
                        + colum_name_num
                        + '</strong></td><td style="text-align: left">DATE dd/mm/yyyy (ENCERRADO EN COMILLAS)</td></tr>'
                    )
                else:
                    html = (
                        html
                        + '<tr><td style="text-align: left"><strong>'
                        + colum_name_num
                        + '</strong></td><td style="text-align: left">DATE dd/mm/yyyy </td></tr>'
                    )
            if item_pos == 4:

                print(
                    "-------------------- La columna "
                    + colum_name_num
                    + " es tipo Fecha formato yyyy/mm/dd "
                )
                if comillas_columnas[a_cont - 1] >= (muestra - 1):
                    html = (
                        html
                        + '<tr><td style="text-align: left"><strong>'
                        + colum_name_num
                        + '</strong></td><td style="text-align: left">DATE yyyy/mm/dd (ENCERRADO EN COMILLAS)</td></tr>'
                    )
                else:
                    html = (
                        html
                        + '<tr><td style="text-align: left"><strong>'
                        + colum_name_num
                        + '</strong></td><td style="text-align: left">DATE yyyy/mm/dd</td></tr>'
                    )

            if item_pos == 5:
                print(
                    "-------------------- La columna "
                    + colum_name_num
                    + " es tipo Fecha formato dd-mm-yyyy "
                )
                if comillas_columnas[a_cont - 1] >= (muestra - 1):
                    html = (
                        html
                        + '<tr><td style="text-align: left"><strong>'
                        + colum_name_num
                        + '</strong></td><td style="text-align: left">DATE dd-mm-yyyy (ENCERRADO EN COMILLAS)</td></tr>'
                    )
                else:
                    html = (
                        html
                        + '<tr><td style="text-align: left"><strong>'
                        + colum_name_num
                        + '</strong></td><td style="text-align: left">DATE dd-mm-yyyy </td></tr>'
                    )
            if item_pos == 6:
                print(
                    "-------------------- La columna "
                    + colum_name_num
                    + " es tipo Fecha formato yyyy-mm-dd "
                )
                if comillas_columnas[a_cont - 1] >= (muestra - 1):
                    html = (
                        html
                        + '<tr><td style="text-align: left"><strong>'
                        + colum_name_num
                        + '</strong></td><td style="text-align: left">DATE  yyyy-mm-dd (ENCERRADO EN COMILLAS)</td></tr>'
                    )
                else:
                    html = (
                        html
                        + '<tr><td style="text-align: left"><strong> '
                        + colum_name_num
                        + '</strong></td><td style="text-align: left">DATE  yyyy-mm-dd</td></tr>'
                    )

            if item_pos == 7:
                print(
                    "-------------------- La columna "
                    + colum_name_num
                    + " es tipo  formato str "
                )
                if comillas_columnas[a_cont - 1] >= (muestra - 1):
                    html = (
                        html
                        + '<tr><td style="text-align: left"><strong>'
                        + colum_name_num
                        + '</strong></td><td style="text-align: left">STRING (ENCERRADO EN COMILLAS)</td></tr>'
                    )
                else:
                    html = (
                        html
                        + '<tr><td style="text-align: left"><strong>'
                        + colum_name_num
                        + '</strong></td><td style="text-align: left">STRING</td></tr>'
                    )

            a_cont = a_cont + 1

        html = html + "</table></div>"
        mensaje = ""

    if validado:
        mensaje = "ARCHIVO   " + archivo + ": VALIDO"
        print("--------------- 3.4.8 ESTADO : VALIDO")

        return (
            1,
            h,
            mensaje,
            delimiter_2,
            html,
            html2,
            validado,
            html3,
            contador_lineas_error,
        )
    else:
        mensaje = "ARCHIVO " + archivo + " TIENE ERRORES EN EL LARGO DE FILAS"
        print(
            "--------------- 3.4.7 ESTADO : NO VALIDO, HAY ERRORES EN EL LARGO DE FILAS"
        )
        return (
            0,
            h,
            mensaje,
            delimiter_2,
            html,
            html2,
            validado,
            html3,
            contador_lineas_error,
        )


def CreaMensajeHTML(
    carpeta_archivos,
    archivo,
    estado,
    decoded,
    delimiter,
    total_columnas,
    total_reg,
    mensaje,
    lineas_error,
    html_columnas,
    html_top_sample,
    html_error,
    contador_lineas_error,
):
    correo_gobierno_datos = "gobernanza-datos@desarrollosocial.gob.cl"
    inicio_html = "<!DOCTYPE html><html><head><meta charset='utf-8'><meta name='pdfkit-page-size' content='Legal'/><meta name='pdfkit-orientation' content='Landscape'/><style>table,td {border: 1px solid black;}</style></head><body>"
    fin_html = "<body></html>"
    portada_html = "<p> <img src='https://www.desarrollosocialyfamilia.gob.cl/img/logo-main.jpg' align='left' style='width: 100px;'><img src='http://gobiernodedatos.mideplan.cl/assets/img/logo-gobiernodatos.png' align='left' style='width: 300px;'></p><br></br><br></br><h1 style='text-align: center'>Reporte de Revisión de Datos </h1><h4 style='text-align: center'>Departamento de Análisis de la Información Social</h4>"
    desc_html = html_desc(carpeta_archivos, archivo)
    desc_html = desc_html + html_desc2(
        estado,
        decoded,
        delimiter,
        total_columnas,
        total_reg,
        mensaje,
        lineas_error,
        contador_lineas_error,
    )
    desc_html = desc_html + html_columnas
    desc_html = desc_html + html_top_sample
    if not valida_error:
        desc_html = desc_html + html_error

    division_html = "</br><div align='left' width='80%' ><hr style='border-color:black;'></div></br>"
    pie_html = (
        "En caso de dudas, contactar al equipo de Gobierno de Datos a <a href='mailto: gobernanza-datos@desarrollosocial.gob.cl'> "
        + str(correo_gobierno_datos)
        + "</a></div>"
    )
    html_final = (
        inicio_html
        + portada_html
        + desc_html
        + division_html
        + division_html
        + pie_html
        + fin_html
    )
    return html_final


def Enviarmensaje(estado):
    try:
        est = "ERROR"
        if estado:
            est = "VÁLIDO"

        msg = MIMEMultipart("alternative")
        mes = ""
        msg["Subject"] = (
            "--- "
            + est
            + " --- Reporte Revisión de Datos - '"
            + archivo
            + "' - "
            + str(now.day)
            + " de "
            + str(now.month)
            + " del "
            + str(now.year)
        )
        msg["From"] = me
        msg["To"] = you
        msg["Cc"] = cc
        text = "Hola!\n el proceso ha finalizado correctamente\n"
        # Record the MIME types of both parts - text/plain and text/html.
        part1 = MIMEText(text, "plain")
        part2 = MIMEText(html_final, "html")
        msg.attach(part1)
        msg.attach(part2)
        s = smtplib.SMTP("smtp.mideplan.cl")
        print("--MENSAJE ENVIADO")
        s.sendmail(me, [you, cc, otrocc], msg.as_string())
        s.quit()
        return 1
    except:
        return 0


def ObtenerMetaData(archivo_dir, archivo):
    peso = Obtenerpeso(archivo_dir)
    owner = Obtener_owner(archivo_dir)
    crea, modifica = Obtener_Fechas(archivo_dir)
    Checksum = md5(archivo_dir)
    fecha_informe = str(now.day) + "-" + str(now.month) + "-" + str(now.year)
    print("--------------- 3.3.1 Nombre archivo: " + str(archivo))
    print("--------------- 3.3.2 Creador(Propietario): " + str(owner))
    print("--------------- 3.3.3 Fecha Creacion: " + str(crea))
    print("--------------- 3.3.4 Fecha Modificacion: " + str(modifica))
    print("--------------- 3.3.5 Fecha generación informe: " + str(fecha_informe))
    print("--------------- 3.3.6 Checksum: " + str(Checksum))
    print("--------------- 3.3.7 Tamaño: " + str(peso))
    print("--------------- 3.3.8 Extensión: " + str(extension))
    return peso, owner, crea, modifica, Checksum, fecha_informe


def ValidarCabecera(cabecera, delimiter):
    cabecera3 = cabecera.split("\t" if delimiter == "Tab" else delimiter)
    validar_cabecera = True
    for head in cabecera3:
        if head.strip() == "":
            tipo = 8
            tipo2 = 1
        else:
            tipo = ValidarDato(head)
            tipo2 = ValidarDato(head[0])

        if not tipo == 8 or tipo2 == 1:
            validar_cabecera = False
    return validar_cabecera


def CopiarArchivo(archivo_dir, estado, total_reg_csv):
    if estado:
        shutil.move(archivo_dir, carpeta_archivos_procesados + archivo)
        if total_reg_csv <= 5000000:
            shutil.copy(
                carpeta_archivos_procesados + archivo, dir_gobierno_datos + archivo
            )

    else:
        shutil.move(archivo_dir, carpeta_archivos_error + archivo)


# def DescomprimirArchivo():
def FNum(x):
    f = str(x)
    if x == 1:
        f = "01"
    if x == 2:
        f = "02"
    if x == 3:
        f = "03"
    if x == 4:
        f = "04"
    if x == 5:
        f = "05"
    if x == 6:
        f = "06"
    if x == 7:
        f = "07"
    if x == 8:
        f = "08"
    if x == 9:
        f = "09"
    return f


def DescomprimirArchivosConsoAPS_IPS(carpeta_archivos, archivo):
    archivo_zip = ""
    resultado = False

    dia = 4
    anio = 4
    mes = 4
    clave = "IPS_MINDES_" + archivo.split(".")[0].split("-")[1] + archivo.split("-")[0]

    with rarfile.RarFile(
        carpeta_archivos + archivo, "r", pwd="IPS_MINDES_072022"
    ) as rf:
        rf.extractall(carpeta_archivos)
    # estoy revisando por que no uedo descomprimir el archivo rar 2022-07

    try:
        archivo_zip = zipfile.ZipFile(carpeta_archivos + archivo, "r")
        archivo_zip.extractall(pwd=bytes(clave, "utf-8"), path=carpeta_archivos)
        resultado = True

    except:
        try:
            with rarfile.RarFile(carpeta_archivos + archivo, "r", pwd=clave) as rf:
                rf.extractall(carpeta_archivos)

            resultado = True
        except:
            pass

    if resultado == True:
        time.sleep(30)
        print("---------------- TERMINO CORRECTO DE DESCOMPRIMIR ARCHIVO: " + archivo)
        MoverArchivosProcesados(archivo_dir, archivo)
    else:
        print("---------------- ERROR DESCOMPRIMIR ARCHIVO: " + archivo)
        MoverArchivosErrorDescomprimir(archivo_dir, archivo)

    return resultado


def DescomprimirArchivosAFC(carpeta_archivos, archivo):
    print("trtetret")
    archivo_zip = ""
    resultado = False

    for x in range(0, 50):
        today = datetime.today() + timedelta(days=-x)
        dia = today.day
        anio = today.year
        mes = today.month
        clave = "A" + FNum(dia) + "F" + FNum(mes) + "C" + str(anio)

        try:
            archivo_zip = zipfile.ZipFile(carpeta_archivos + archivo, "r")
            archivo_zip.extractall(pwd=bytes(clave, "utf-8"), path=carpeta_archivos)
            resultado = True

            break
        except:
            try:
                with rarfile.RarFile(carpeta_archivos + archivo, "r", pwd=clave) as rf:
                    rf.extractall(carpeta_archivos)

                resultado = True
                break
            except:
                pass
    if resultado == True:
        time.sleep(30)
        print("---------------- TERMINO CORRECTO DE DESCOMPRIMIR ARCHIVO: " + archivo)
        MoverArchivosProcesados(archivo_dir, archivo)
    else:
        print("---------------- ERROR DESCOMPRIMIR ARCHIVO: " + archivo)
        MoverArchivosErrorDescomprimir(archivo_dir, archivo)

    return resultado


def DescomprimirArchivosAFCSDCPFP(carpeta_archivos, archivo):
    archivo_zip = ""
    resultado = False
    for x in range(0, 50):
        today = datetime.today() + timedelta(days=-x)
        dia = today.day
        anio = today.year
        mes = today.month
        clave = "ips_pfp_" + str(anio) + FNum(mes) + FNum(dia)

        try:

            archivo_zip = zipfile.ZipFile(
                carpeta_archivos + archivo, "r", allowZip64=True
            )
            archivo_zip.extractall(pwd=bytes(clave, "utf-8"), path=carpeta_archivos)
            resultado = True
            archivo_zip.close()
            break
        except:
            try:
                with pyzipper.AESZipFile(carpeta_archivos + archivo) as zf:
                    zf.setpassword(bytes(clave, "utf-8"))
                    zf.extractall(carpeta_archivos)
                resultado = True
            except:
                pass

    if resultado == True:
        print("descomprimido")
        print(clave)
        MoverArchivosProcesados(archivo_dir, archivo)
    else:
        print("---------------- ERROR DESCOMPRIMIR ARCHIVO: " + archivo)
        MoverArchivosErrorDescomprimir(archivo_dir, archivo)

    return resultado


def DescomprimirArchivos(carpeta_archivos, archivo):
    resultado = False
    archivo_zip = ""

    try:
        archivo_zip = zipfile.ZipFile(carpeta_archivos + archivo, "r")
        archivo_zip.extractall(pwd=None, path=carpeta_archivos)
        resultado = True
        archivo_zip.close()
    except:
        try:
            import gzip

            filename = carpeta_archivos + archivo

            f = gzip.open(filename)
            write_file(filename[: filename.rfind(".gz")], f.read())
            f.close()
        except:
            resultado = False
            pass

    if resultado == True:
        archivo_zip.close()
        MoverArchivosProcesados(archivo_dir, archivo)
    else:
        print("---------------- ERROR DESCOMPRIMIR ARCHIVO: " + archivo)
        MoverArchivosErrorDescomprimir(archivo_dir, archivo)

    return resultado


def write_file(filename, data):
    try:
        f = open(filename, "wb")
    except IOError as e:
        print(e.errno, e.message)
    else:
        f.write(data)
        f.close()


def CrearAfil(carpeta_archivos, archivo_dir, archivo):

    with open(
        carpeta_archivos + archivo.replace(".zip", ".csv").replace(".rar", ".csv"),
        "w",
        newline="",
    ) as csvfile:
        spamwriter = csv.writer(
            csvfile, delimiter=";", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )
        with open(
            archivo_dir.replace(".zip", ".txt").replace(".rar", ".txt"),
            encoding="UTF-8",
            errors="ignore",
        ) as file_obj:
            reader_obj = csv.reader(file_obj, delimiter="ψ")
            wp = 0
            for row in reader_obj:
                wp = wp + 1
                if wp == 1:
                    col1 = "numcue"
                    col2 = "dvcue"
                    col3 = "nrrafi"
                    col4 = "dvrafi"
                    col5 = "apepat"
                    col6 = "apemat"
                    col7 = "nomafi"
                    col8 = "sexo"
                    col9 = "fecnac"
                    col10 = "lugnac"
                    col11 = "fecafi"
                    col12 = "fecsus"
                    col13 = "tipafi"
                    col14 = "estado"
                    col15 = "fecact"
                    col16 = "afpactual"
                    col17 = "niveduc"
                    col18 = "ncargas"
                    col19 = "estcivil"
                    col20 = "paisnac"
                    col21 = "aprob"
                    col22 = "cartoweb"
                    col23 = "caueli"
                    spamwriter.writerow(
                        [
                            col1,
                            col2,
                            col3,
                            col4,
                            col5,
                            col6,
                            col7,
                            col8,
                            col9,
                            col10,
                            col11,
                            col12,
                            col13,
                            col14,
                            col15,
                            col16,
                            col17,
                            col18,
                            col19,
                            col20,
                            col21,
                            col22,
                            col23,
                        ]
                    )
                col1 = row[0][0:10].strip()
                col2 = row[0][10:11].strip()
                col3 = row[0][11:21].strip()
                col4 = row[0][21:22].strip()
                col5 = row[0][22:42].strip()
                col6 = row[0][42:62].strip()
                col7 = row[0][62:82].strip()
                col8 = row[0][82:83].strip()
                col9 = row[0][83:91].strip()
                col10 = row[0][91:121].strip()
                col11 = row[0][121:129].strip()
                col12 = row[0][129:137].strip()
                col13 = row[0][137:138].strip()
                col14 = row[0][138:139].strip()
                col15 = row[0][139:147].strip()
                col16 = row[0][147:151].strip()
                col17 = row[0][151:153].strip()
                col18 = row[0][153:155].strip()
                col19 = row[0][155:156].strip()
                col20 = row[0][156:176].strip()
                col21 = row[0][176:178].strip()
                col22 = row[0][178:179].strip()
                col23 = row[0][179:180].strip()
                spamwriter.writerow(
                    [
                        col1,
                        col2,
                        col3,
                        col4,
                        col5,
                        col6,
                        col7,
                        col8,
                        col9,
                        col10,
                        col11,
                        col12,
                        col13,
                        col14,
                        col15,
                        col16,
                        col17,
                        col18,
                        col19,
                        col20,
                        col21,
                        col22,
                        col23,
                    ]
                )
    EliminarArchivo(archivo_dir, archivo)


def CrearAfilMensual(carpeta_archivos, archivo_dir, archivo):

    with open(
        carpeta_archivos + archivo.replace(".zip", ".csv").replace(".rar", ".csv"),
        "w",
        newline="",
    ) as csvfile:
        spamwriter = csv.writer(
            csvfile, delimiter=";", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )
        with open(
            archivo_dir.replace(".zip", ".fc3").replace(".rar", ".fc3"),
            encoding="UTF-8",
            errors="ignore",
        ) as file_obj:
            reader_obj = csv.reader(file_obj, delimiter="ψ")
            wp = 0
            for row in reader_obj:
                wp = wp + 1

                if wp == 1:
                    col1 = "DG_TIPO_REGISTRO"
                    col2 = "DN_RUT_AFILIADO"
                    col3 = "DG_DV_RUT_AFILIADO"
                    col4 = "DG_APELLIDO_PATERNO"
                    col5 = "DG_APELLIDO_MATERNO"
                    col6 = "DG_NOMBRES"
                    col7 = "DN_FECHA_NACIMIENTO"
                    col8 = "DG_SEXO_OR"
                    col9 = "DG_PAIS_NACIMIENTO"
                    col10 = "DN_NIVEL_EDUCACIONAL"
                    col11 = "DN_TOTAL_ANNOS_APROBADOS"
                    col12 = "DN_ESTADO_CIVIL"
                    col13 = "DG_CALLE_TRABAJADOR"
                    col14 = "DG_NUMERO_TRABAJADOR"
                    col15 = "DG_RESTO_DIRECCION"
                    col16 = "DN_COMUNA_TRABAJADOR"
                    col17 = "DG_NUM_TELEFONO_FIJO"
                    col18 = "DG_CORREO_ELECTRONICO"
                    col19 = "DG_NUM_CELULAR"
                    col20 = "DN_CODIGO_POSTAL"
                    col21 = "DN_INST_PREVISIONAL_ACTUAL"
                    col22 = "DN_MODALIDAD_ENV_CARTO"
                    spamwriter.writerow(
                        [
                            col1,
                            col2,
                            col3,
                            col4,
                            col5,
                            col6,
                            col7,
                            col8,
                            col9,
                            col10,
                            col11,
                            col12,
                            col13,
                            col14,
                            col15,
                            col16,
                            col17,
                            col18,
                            col19,
                            col20,
                            col21,
                            col22,
                        ]
                    )
                if wp > 1:
                    col1 = row[0][0:1].strip()
                    col2 = row[0][1:9].strip()
                    col3 = row[0][9:10].strip()
                    col4 = row[0][10:30].strip()
                    col5 = row[0][30:50].strip()
                    col6 = row[0][50:80].strip()
                    col7 = row[0][80:88].strip()
                    col8 = row[0][88:89].strip()
                    col9 = row[0][89:91].strip()
                    col10 = row[0][91:93].strip()
                    col11 = row[0][93:95].strip()
                    col12 = row[0][95:97].strip()
                    col13 = row[0][97:147].strip()
                    col14 = row[0][147:157].strip()
                    col15 = row[0][157:207].strip()
                    col16 = row[0][207:212].strip()
                    col17 = row[0][212:227].strip()
                    col18 = row[0][227:267].strip()
                    col19 = row[0][267:282].strip()
                    col20 = row[0][282:289].strip()
                    col21 = row[0][289:293].strip()
                    col22 = row[0][293:294].strip()
                    spamwriter.writerow(
                        [
                            col1,
                            col2,
                            col3,
                            col4,
                            col5,
                            col6,
                            col7,
                            col8,
                            col9,
                            col10,
                            col11,
                            col12,
                            col13,
                            col14,
                            col15,
                            col16,
                            col17,
                            col18,
                            col19,
                            col20,
                            col21,
                            col22,
                        ]
                    )
    EliminarArchivo(archivo_dir, archivo)


def CrearEmpl(carpeta_archivos, archivo_dir, archivo):

    with open(
        carpeta_archivos + archivo.replace(".zip", ".csv").replace(".rar", ".csv"),
        "w",
        newline="",
    ) as csvfile:
        spamwriter = csv.writer(
            csvfile, delimiter=";", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )
        with open(
            archivo_dir.replace(".zip", ".txt").replace(".rar", ".txt"),
            encoding="UTF-8",
            errors="ignore",
        ) as file_obj:
            reader_obj = csv.reader(file_obj, delimiter="ψ")
            wp = 0
            for row in reader_obj:
                wp = wp + 1
                if wp == 1:
                    col1 = "acteco"
                    col2 = "nrremp"
                    col3 = "dvremp"
                    col4 = "razsocp1"
                    col6 = "nrrepleg"
                    col7 = "dvrepleg"
                    col8 = "repleg"
                    col9 = "fecact"
                    col10 = "estado"
                    spamwriter.writerow(
                        [col1, col2, col3, col4, col6, col7, col8, col9, col10]
                    )
                col1 = row[0][0:6].strip()
                col2 = row[0][6:16].strip()
                col3 = row[0][16:17].strip()
                col4 = row[0][17:67].strip()
                col6 = row[0][67:77].strip()
                col7 = row[0][77:78].strip()
                col8 = row[0][78:128].strip()
                col9 = row[0][128:136].strip()
                col10 = row[0][136:137].strip()
                spamwriter.writerow(
                    [col1, col2, col3, col4, col6, col7, col8, col9, col10]
                )
    EliminarArchivo(archivo_dir, archivo)


def CrearEmplMensual(carpeta_archivos, archivo_dir, archivo):

    with open(
        carpeta_archivos + archivo.replace(".zip", ".csv").replace(".rar", ".csv"),
        "w",
        newline="",
    ) as csvfile:
        spamwriter = csv.writer(
            csvfile, delimiter=";", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )
        with open(
            archivo_dir.replace(".zip", ".fc3").replace(".rar", ".fc3"),
            encoding="UTF-8",
            errors="ignore",
        ) as file_obj:
            reader_obj = csv.reader(file_obj, delimiter="ψ")
            wp = 0
            for row in reader_obj:
                wp = wp + 1
                if wp == 1:
                    col1 = "TIPO_REG"
                    col2 = "RUT_EMPLEA"
                    col3 = "DV_EMPLEA"
                    col4 = "ACTIV_ECON"
                    col5 = "RAZON_SOC"
                    col6 = "NUM_TRAB"
                    col7 = "CALLE_DOM"
                    col8 = "NUM_DOM"
                    col9 = "REST_DOM"
                    col10 = "COMUNA"
                    col11 = "FONO_EMPLEA"
                    col12 = "EMAIL"
                    spamwriter.writerow(
                        [
                            col1,
                            col2,
                            col3,
                            col4,
                            col5,
                            col6,
                            col7,
                            col8,
                            col9,
                            col10,
                            col11,
                            col12,
                        ]
                    )
                if wp > 1:
                    col1 = row[0][0:1].strip()
                    col2 = row[0][1:9].strip()
                    col3 = row[0][9:10].strip()
                    col4 = row[0][10:16].strip()
                    col5 = row[0][16:66].strip()
                    col6 = row[0][66:72].strip()
                    col7 = row[0][72:122].strip()
                    col8 = row[0][122:132].strip()
                    col9 = row[0][132:182].strip()
                    col10 = row[0][182:187].strip()
                    col11 = row[0][187:202].strip()
                    col12 = row[0][202:242].strip()
                    spamwriter.writerow(
                        [
                            col1,
                            col2,
                            col3,
                            col4,
                            col5,
                            col6,
                            col7,
                            col8,
                            col9,
                            col10,
                            col11,
                            col12,
                        ]
                    )

    EliminarArchivo(archivo_dir, archivo)


def CrearGiro(carpeta_archivos, archivo_dir, archivo):

    with open(
        carpeta_archivos + archivo.replace(".zip", ".csv").replace(".rar", ".csv"),
        "w",
        newline="",
    ) as csvfile:
        spamwriter = csv.writer(
            csvfile, delimiter=";", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )
        with open(
            archivo_dir.replace(".zip", ".txt").replace(".rar", ".txt"),
            encoding="UTF-8",
            errors="ignore",
        ) as file_obj:
            reader_obj = csv.reader(file_obj, delimiter="ψ")
            wp = 0
            for row in reader_obj:
                wp = wp + 1
                if wp == 1:
                    col1 = "tipsolic"
                    col2 = "nrosolic"
                    col3 = "nrogiro"
                    col4 = "nrrbenef"
                    col5 = "fecgiro"
                    col6 = "feccaduc"
                    col7 = "estgiro"
                    col8 = "apercibir"
                    col9 = "ctaind"
                    col10 = "fdosol"
                    col11 = "asigfam"
                    col12 = "apercuota"
                    col13 = "mciccuota"
                    col14 = "mfcscuota"
                    col15 = "fecestad"
                    col16 = "modpago"

                    spamwriter.writerow(
                        [
                            col1,
                            col2,
                            col3,
                            col4,
                            col5,
                            col6,
                            col7,
                            col8,
                            col9,
                            col10,
                            col11,
                            col12,
                            col13,
                            col14,
                            col15,
                            col16,
                        ]
                    )
                col1 = row[0][0:2].strip()
                col2 = row[0][2:12].strip()
                col3 = row[0][12:15].strip()
                col4 = row[0][15:25].strip()
                col5 = row[0][25:33].strip()
                col6 = row[0][33:41].strip()
                col7 = row[0][41:43].strip()
                col8 = row[0][43:51].strip()
                col9 = row[0][51:59].strip()
                col10 = row[0][59:67].strip()
                col11 = row[0][67:75].strip()
                col12 = row[0][75:84].strip()
                col13 = row[0][84:93].strip()
                col14 = row[0][93:102].strip()
                col15 = row[0][102:110].strip()
                col16 = row[0][110:112].strip()
                spamwriter.writerow(
                    [
                        col1,
                        col2,
                        col3,
                        col4,
                        col5,
                        col6,
                        col7,
                        col8,
                        col9,
                        col10,
                        col11,
                        col12,
                        col13,
                        col14,
                        col15,
                        col16,
                    ]
                )

    EliminarArchivo(archivo_dir, archivo)


def CrearGiroMensual(carpeta_archivos, archivo_dir, archivo):

    with open(
        carpeta_archivos + archivo.replace(".zip", ".csv").replace(".rar", ".csv"),
        "w",
        newline="",
    ) as csvfile:
        spamwriter = csv.writer(
            csvfile, delimiter=";", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )
        with open(
            archivo_dir.replace(".zip", ".fc3").replace(".rar", ".fc3"),
            encoding="UTF-8",
            errors="ignore",
        ) as file_obj:
            reader_obj = csv.reader(file_obj, delimiter="ψ")
            wp = 0
            for row in reader_obj:
                wp = wp + 1
                if wp == 1:
                    col1 = "DG_TIPO_REGISTRO"
                    col2 = "DN_NUMERO_SOLICITUD"
                    col3 = "DG_TIPO_SOLICITUD"
                    col4 = "DN_NUMERO_CUENTA"
                    col5 = "DG_DV_CUENTA"
                    col6 = "DN_RUN_BENEFICARIO"
                    col7 = "DG_DV_RUN_BENFICIARIO"
                    col8 = "DN_NUM_GIRO_PAGADO"
                    col9 = "DN_TIPO_FINANCIAMIENTO"
                    col10 = "DN_FECHA_GIRO"
                    col11 = "DG_ESTADO_GIRO"
                    col12 = "DN_FECHA_ESTADO_GIRO"
                    col13 = "DN_MONTO_PERCIBIR"
                    col14 = "DN_MONTO_CIC"
                    col15 = "DN_MONTO_FCS"
                    col16 = "DN_MONTO_ASIG_FAMILIAR"
                    col17 = "DN_MONTO_CUOTAS_PERCIBIR"
                    col18 = "DN_MONTO_CUOTAS_CIC"
                    col19 = "DN_MONTO_CUOTAS_FCS"
                    col20 = "DN_MONTO_REMUNERACION"
                    col21 = "DN_MONTO_CUOTAS_REMUNERACION"
                    col22 = "DN_TIPO_BENEFICIARIO"
                    col23 = "DN_FECHA_SUSPENSION"
                    col24 = "DG_GIROS_COBROS_ADIC"
                    spamwriter.writerow(
                        [
                            col1,
                            col2,
                            col3,
                            col4,
                            col5,
                            col6,
                            col7,
                            col8,
                            col9,
                            col10,
                            col11,
                            col12,
                            col13,
                            col14,
                            col15,
                            col16,
                            col17,
                            col18,
                            col19,
                            col20,
                            col21,
                            col22,
                            col23,
                            col24,
                        ]
                    )
                if wp > 1:

                    col1 = row[0][0:1].strip()
                    col2 = row[0][1:11].strip()
                    col3 = row[0][11:13].strip()
                    col4 = row[0][13:23].strip()
                    col5 = row[0][23:24].strip()
                    col6 = row[0][24:32].strip()
                    col7 = row[0][32:33].strip()
                    col8 = row[0][33:35].strip()
                    col9 = row[0][35:37].strip()
                    col10 = row[0][37:45].strip()
                    col11 = row[0][45:47].strip()
                    col12 = row[0][47:55].strip()
                    col13 = row[0][55:64].strip()
                    col14 = row[0][64:73].strip()
                    col15 = row[0][73:82].strip()
                    col16 = row[0][82:88].strip()
                    col17 = row[0][88:97].strip()
                    col18 = row[0][97:106].strip()
                    col19 = row[0][106:115].strip()
                    col20 = row[0][115:124].strip()
                    col21 = row[0][124:133].strip()
                    col22 = row[0][133:135].strip()
                    col23 = row[0][135:143].strip()
                    col24 = row[0][143:144].strip()
                    spamwriter.writerow(
                        [
                            col1,
                            col2,
                            col3,
                            col4,
                            col5,
                            col6,
                            col7,
                            col8,
                            col9,
                            col10,
                            col11,
                            col12,
                            col13,
                            col14,
                            col15,
                            col16,
                            col17,
                            col18,
                            col19,
                            col20,
                            col21,
                            col22,
                            col23,
                            col24,
                        ]
                    )

    EliminarArchivo(archivo_dir, archivo)


def CrearSoli(carpeta_archivos, archivo_dir, archivo):

    with open(
        carpeta_archivos + archivo.replace(".zip", ".csv").replace(".rar", ".csv"),
        "w",
        newline="",
    ) as csvfile:
        spamwriter = csv.writer(
            csvfile, delimiter=";", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )
        with open(
            archivo_dir.replace(".zip", ".txt").replace(".rar", ".txt"),
            encoding="UTF-8",
            errors="ignore",
        ) as file_obj:
            reader_obj = csv.reader(file_obj, delimiter="ψ")
            wp = 0
            for row in reader_obj:
                wp = wp + 1
                if wp == 1:
                    col1 = "tipsolic"
                    col2 = "nrosolic"
                    col3 = "nrrafi"
                    col4 = "numcue"
                    col5 = "nrremp"
                    col6 = "fecrecep"
                    col7 = "fecfinla"
                    col8 = "causal"
                    col9 = "optafcs"
                    col10 = "nrcarfam"
                    col11 = "masigfam"
                    col12 = "actbenef"
                    col13 = "cantgiro"
                    col14 = "poder"
                    col15 = "caa"
                    col16 = "derecho"
                    col17 = "tipcon"
                    col18 = "rentaprom"
                    col19 = "indfcspre"
                    col20 = "plafcspre"
                    col21 = "saldocic"

                    spamwriter.writerow(
                        [
                            col1,
                            col2,
                            col3,
                            col4,
                            col5,
                            col6,
                            col7,
                            col8,
                            col9,
                            col10,
                            col11,
                            col12,
                            col13,
                            col14,
                            col15,
                            col16,
                            col17,
                            col18,
                            col19,
                            col20,
                            col21,
                        ]
                    )
                col1 = row[0][0:2].strip()
                col2 = row[0][2:12].strip()
                col3 = row[0][12:22].strip()
                col4 = row[0][22:32].strip()
                col5 = row[0][32:42].strip()
                col6 = row[0][42:50].strip()
                col7 = row[0][50:58].strip()
                col8 = row[0][58:73].strip()
                col9 = row[0][73:74].strip()
                col10 = row[0][74:76].strip()
                col11 = row[0][76:82].strip()
                col12 = row[0][82:83].strip()
                col13 = row[0][83:86].strip()
                col14 = row[0][86:87].strip()
                col15 = row[0][87:91].strip()
                col16 = row[0][91:92].strip()
                col17 = row[0][92:93].strip()
                col18 = row[0][93:101].strip()
                col19 = row[0][101:102].strip()
                col20 = row[0][102:103].strip()
                col21 = row[0][103:111].strip()
                spamwriter.writerow(
                    [
                        col1,
                        col2,
                        col3,
                        col4,
                        col5,
                        col6,
                        col7,
                        col8,
                        col9,
                        col10,
                        col11,
                        col12,
                        col13,
                        col14,
                        col15,
                        col16,
                        col17,
                        col18,
                        col19,
                        col20,
                        col21,
                    ]
                )
    EliminarArchivo(archivo_dir, archivo)


def CrearSoliMensual(carpeta_archivos, archivo_dir, archivo):

    with open(
        carpeta_archivos + archivo.replace(".zip", ".csv").replace(".rar", ".csv"),
        "w",
        newline="",
    ) as csvfile:
        spamwriter = csv.writer(
            csvfile, delimiter=";", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )
        with open(
            archivo_dir.replace(".zip", ".fc3").replace(".rar", ".fc3"),
            encoding="UTF-8",
            errors="ignore",
        ) as file_obj:
            reader_obj = csv.reader(file_obj, delimiter="ψ")
            wp = 0
            for row in reader_obj:
                wp = wp + 1
                if wp == 1:
                    col1 = "DG_TIPO_REGISTRO"
                    col2 = "DN_NUMERO_SOLICITUD"
                    col3 = "DG_TIPO_SOLICITUD"
                    col4 = "DN_RUT_ASISTENTE_COMERCIAL"
                    col5 = "DG_DV_ASISTENTE_COMERCIAL"
                    col6 = "DN_NUMERO_CUENTA"
                    col7 = "DG_DV_CUENTA"
                    col8 = "DN_RUT_EMPLEADOR"
                    col9 = "DG_DV_EMPLEADOR"
                    col10 = "DN_CAA"
                    col11 = "DN_TIPO_PRESTACION"
                    col12 = "DN_FECHA_SOLICITUD"
                    col13 = "DG_OP_FONDO_CS"
                    col14 = "DG_ACTUALIZA_BENEF"
                    col15 = "DN_NUM_GIROS_PERMITIDOS"
                    col16 = "DG_DER_FONDO_CESANTIA_SOL"
                    col17 = "DN_NUM_CARGAS_FAM"
                    col18 = "DN_MONTO_TOTAL_ASIG_FAM"
                    col19 = "DN_INSTITUCION_PAGADORA"
                    col20 = "DN_TIPO_PENSION"
                    col21 = "DN_FECHA_DEVENGAMIENTO"

                    col22 = "DN_FECHA_FALLECIMIENTO"
                    col23 = "DN_FECHA_TERMINO_LABORAL"
                    col24 = "DN_CAUSAL_TERMINO"
                    col25 = "DG_DER_AFC_CTRATO_INDEFINIDO"
                    col26 = "DG_DER_AFC_CTRATO_PLAZO_FIJO"
                    col27 = "DN_TIPO_CONTRATO_BENEFICIO"
                    col28 = "DN_COMUNA_TRABAJO"
                    col29 = "DG_DER_FCS"
                    col30 = "DN_REMU_PROMEDIO"
                    col31 = "DN_SALDO_CIC"

                    spamwriter.writerow(
                        [
                            col1,
                            col2,
                            col3,
                            col4,
                            col5,
                            col6,
                            col7,
                            col8,
                            col9,
                            col10,
                            col11,
                            col12,
                            col13,
                            col14,
                            col15,
                            col16,
                            col17,
                            col18,
                            col19,
                            col20,
                            col21,
                            col22,
                            col23,
                            col24,
                            col25,
                            col26,
                            col27,
                            col28,
                            col29,
                            col30,
                            col31,
                        ]
                    )
                if wp > 1:

                    col1 = row[0][0:1].strip()
                    col2 = row[0][1:11].strip()
                    col3 = row[0][11:13].strip()
                    col4 = row[0][13:21].strip()
                    col5 = row[0][21:22].strip()
                    col6 = row[0][22:32].strip()
                    col7 = row[0][32:33].strip()
                    col8 = row[0][33:41].strip()
                    col9 = row[0][41:42].strip()
                    col10 = row[0][42:46].strip()
                    col11 = row[0][46:48].strip()
                    col12 = row[0][48:56].strip()
                    col13 = row[0][56:57].strip()
                    col14 = row[0][57:58].strip()
                    col15 = row[0][58:60].strip()
                    col16 = row[0][60:61].strip()
                    col17 = row[0][61:63].strip()
                    col18 = row[0][63:69].strip()
                    col19 = row[0][69:71].strip()
                    col20 = row[0][71:73].strip()
                    col21 = row[0][73:81].strip()
                    col22 = row[0][81:89].strip()
                    col23 = row[0][89:97].strip()
                    col24 = row[0][97:99].strip()
                    col25 = row[0][99:100].strip()
                    col26 = row[0][100:101].strip()
                    col27 = row[0][101:103].strip()
                    col28 = row[0][103:108].strip()
                    col29 = row[0][108:109].strip()
                    col30 = row[0][109:118].strip()
                    col31 = row[0][118:128].strip()
                    spamwriter.writerow(
                        [
                            col1,
                            col2,
                            col3,
                            col4,
                            col5,
                            col6,
                            col7,
                            col8,
                            col9,
                            col10,
                            col11,
                            col12,
                            col13,
                            col14,
                            col15,
                            col16,
                            col17,
                            col18,
                            col19,
                            col20,
                            col21,
                            col22,
                            col23,
                            col24,
                            col25,
                            col26,
                            col27,
                            col28,
                            col29,
                            col30,
                            col31,
                        ]
                    )

    EliminarArchivo(archivo_dir, archivo)


def CrearReim(carpeta_archivos, archivo_dir, archivo):

    with open(
        carpeta_archivos + archivo.replace(".zip", ".csv").replace(".rar", ".csv"),
        "w",
        newline="",
    ) as csvfile:
        spamwriter = csv.writer(
            csvfile, delimiter=";", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )
        with open(
            archivo_dir.replace(".zip", ".txt").replace(".rar", ".txt"),
            encoding="UTF-8",
            errors="ignore",
        ) as file_obj:
            reader_obj = csv.reader(file_obj, delimiter="ψ")
            wp = 0
            for row in reader_obj:
                wp = wp + 1
                if wp == 1:
                    col1 = "numcue"
                    col2 = "perpag"
                    col3 = "nrremp"
                    col4 = "renimp"
                    col5 = "tipo_contrato"
                    col6 = "grupo_acteco"

                    spamwriter.writerow([col1, col2, col3, col4, col5, col6])
                col1 = row[0][0:10].strip()
                col2 = row[0][10:16].strip()
                col3 = row[0][16:26].strip()
                col4 = row[0][26:35].strip()
                col5 = row[0][35:45].strip()
                col6 = row[0][45:47].strip()

                spamwriter.writerow([col1, col2, col3, col4, col5, col6])
    EliminarArchivo(archivo_dir, archivo)


def CrearReimMensual(carpeta_archivos, archivo_dir, archivo):

    with open(
        carpeta_archivos + archivo.replace(".zip", ".csv").replace(".rar", ".csv"),
        "w",
        newline="",
    ) as csvfile:
        spamwriter = csv.writer(
            csvfile, delimiter=";", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )
        with open(
            archivo_dir.replace(".zip", ".fc3").replace(".rar", ".fc3"),
            encoding="UTF-8",
            errors="ignore",
        ) as file_obj:
            reader_obj = csv.reader(file_obj, delimiter="ψ")
            wp = 0
            for row in reader_obj:
                wp = wp + 1
                if wp == 1:
                    col1 = "DG_TIPO_REGISTRO"
                    col2 = "DN_NUM_CUENTA"
                    col3 = "DG_DV_CUENTA"
                    col4 = "DG_RUT_EMPLEADOR"
                    col5 = "DG_DV_RUT_EMPLEADOR"
                    col6 = "DN_MES_DEVENGAMIENTO"
                    col7 = "DN_TIPO_CONTRATO"
                    col8 = "DN_SUBSIDIO_INCAPACIDAD"
                    col9 = "DN_RENTA_IMPONIBLE"

                    spamwriter.writerow(
                        [col1, col2, col3, col4, col5, col6, col7, col8, col9]
                    )

                if wp > 1:
                    col1 = row[0][0:1].strip()
                    col2 = row[0][1:11].strip()
                    col3 = row[0][11:12].strip()
                    col4 = row[0][12:20].strip()
                    col5 = row[0][20:21].strip()
                    col6 = row[0][21:27].strip()
                    col7 = row[0][27:29].strip()
                    col8 = row[0][29:30].strip()
                    col9 = row[0][30:39].strip()
                    spamwriter.writerow(
                        [col1, col2, col3, col4, col5, col6, col7, col8, col9]
                    )

                # if wp==10:
                #   break
    EliminarArchivo(archivo_dir, archivo)


def MoverArchivosProcesados(archivo_dir, archivo):
    try:
        shutil.move(archivo_dir, carpeta_archivos_procesados + archivo)
        print(
            "---------------- SE MOVIO ARCHIVO " + archivo + " A CARPETA DE PROCESADOS "
        )
    except:
        print("---------------- ERROR: NO SE PUEDE MOVER ARCHIVO" + archivo)
        pass


def EliminarArchivo(archivo_dir, archivo):
    try:
        remove(archivo_dir.replace(".zip", ".txt").replace(".rar", ".txt"))
        print(
            "---------------- SE  ELIMINO  "
            + archivo.replace(".zip", ".txt").replace(".rar", ".txt")
        )
    except:
        try:
            remove(archivo_dir.replace(".zip", ".fc3").replace(".rar", ".fc3"))
            print(
                "---------------- SE  ELIMINO  "
                + archivo.replace(".zip", ".fc3").replace(".rar", ".fc3")
            )
        except:
            print("---------------- ERROR: NO SE PUEDE ELIMINAR ARCHIVO" + archivo)
            pass


def MoverArchivosErrorDescomprimir(archivo_dir, archivo):
    try:
        shutil.move(archivo_dir, carpeta_archivos_error_descomprimir + archivo)
        print(
            "---------------- ERROR: SE MOVIO ARCHIVO "
            + archivo
            + " A CARPETA DE ERRORES"
        )
    except:
        print("---------------- ERROR: NO SE PUEDE MOVER ARCHIVO" + archivo)
        pass


def ContarLargoFilaAFC(archivo_dir):
    try:
        largo = -1
        with open(
            archivo_dir.replace(".zip", ".txt").replace(".rar", ".txt"),
            encoding="UTF-8",
            errors="ignore",
        ) as file_obj:
            reader_obj = csv.reader(file_obj, delimiter="ψ")
            p = 0
            for row in reader_obj:
                p = p + 1
                if p == 2:
                    largo = len(row[0])
                    break
    except:

        try:
            largo = -1
            with open(
                archivo_dir.replace(".zip", ".fc3").replace(".rar", ".fc3"),
                encoding="UTF-8",
                errors="ignore",
            ) as file_obj:
                reader_obj = csv.reader(file_obj, delimiter="ψ")
                p = 0
                for row in reader_obj:
                    p = p + 1
                    if p == 2:
                        largo = len(row[0])
                        break
        except:
            print(
                "---------------- NO SE ENCONTRO ARCHIVO "
                + archivo_dir.replace(".zip", ".txt").replace(".rar", ".txt")
            )
    print(largo)
    return largo


########################################## COMIENZA APLICACIÓN #######################################################################################

print("---------- INICIO PROCESO CHECKSUM: " + str(now) + " ----------")
print("----- 1.ANALIZANDO CARPETA por archivos rar o zip...")
contenido = os.listdir(carpeta_archivos)
num_registros = len(contenido)
print("---------- 1.1.SE ENCONTRARON " + str(num_registros) + " registros")
if num_registros > 0:
    print("----- 2.COMINEZA PROCESO DE VALIDACIÓN REGISTROS")
    for archivo in contenido:
        start_time = time.time()
        local_time = time.ctime(start_time)

        validado = True
        arr_archiv = archivo.split(".")
        archi_name_arr = archivo.split("_")
        extension = arr_archiv[len(arr_archiv) - 1]

        name = ""
        contador = 0

        for n in archi_name_arr:
            if contador > 0:
                name = name + n
            contador = contador + 1

        archivo_dir = carpeta_archivos + name
        print(archivo_dir)

        if (
            extension == "rar"
            or extension == "zip"
            or extension == "txt.gz"
            or extension == "gz"
        ):
            print(
                "----- 3.SE PROCESA ARCHIVO: " + archivo + " HORA INICIO:" + local_time
            )
            print("---------- 3.1 LA EXTENSION DEL ARCHIVO ES " + extension)
            print("---------- 3.2 DESCOMPRIMIR ARCHIVOS ")

            if "afil" in archivo:

                DescomprimirArchivosAFC(carpeta_archivos, archivo)
                largo_fila_csv = ContarLargoFilaAFC(archivo_dir)
                if largo_fila_csv == 180:
                    CrearAfil(carpeta_archivos, archivo_dir, archivo)
                elif largo_fila_csv == 294:

                    CrearAfilMensual(carpeta_archivos, archivo_dir, archivo)

                else:
                    print("LARGO AFIL NO CORRESPONDE AL ARCHIVO QUINCENAL NI MENSUAL")
            elif "empl" in archivo:
                DescomprimirArchivosAFC(carpeta_archivos, archivo)
                largo_fila_csv = ContarLargoFilaAFC(archivo_dir)
                if largo_fila_csv == 137:
                    CrearEmpl(carpeta_archivos, archivo_dir, archivo)
                elif largo_fila_csv == 242:

                    CrearEmplMensual(carpeta_archivos, archivo_dir, archivo)

                else:
                    print("LARGO EMPL NO CORRESPONDE AL ARCHIVO QUINCENAL NI MENSUAL")
            elif "giro" in archivo:
                DescomprimirArchivosAFC(carpeta_archivos, archivo)
                largo_fila_csv = ContarLargoFilaAFC(archivo_dir)
                if largo_fila_csv == 112:
                    CrearGiro(carpeta_archivos, archivo_dir, archivo)
                elif largo_fila_csv == 144:
                    CrearGiroMensual(carpeta_archivos, archivo_dir, archivo)

                else:
                    print("LARGO GIRO NO CORRESPONDE AL ARCHIVO QUINCENAL NI MENSUAL")
            elif "soli" in archivo:
                DescomprimirArchivosAFC(carpeta_archivos, archivo)
                largo_fila_csv = ContarLargoFilaAFC(archivo_dir)
                print(largo_fila_csv)
                if largo_fila_csv == 111:
                    CrearSoli(carpeta_archivos, archivo_dir, archivo)
                elif largo_fila_csv == 128:
                    CrearSoliMensual(carpeta_archivos, archivo_dir, archivo)
                else:
                    print("LARGO SOLI NO CORRESPONDE AL ARCHIVO QUINCENAL NI MENSUAL")
            elif "reim" in archivo:
                DescomprimirArchivosAFC(carpeta_archivos, archivo)
                largo_fila_csv = ContarLargoFilaAFC(archivo_dir)
                print(largo_fila_csv)
                if largo_fila_csv == 47:
                    CrearReim(carpeta_archivos, archivo_dir, archivo)
                if largo_fila_csv == 39:
                    CrearReimMensual(carpeta_archivos, archivo_dir, archivo)
                else:
                    print("LARGO REIM NO CORRESPONDE AL ARCHIVO QUINCENAL")
            elif "sdcpfp" in archivo:
                DescomprimirArchivosAFCSDCPFP(carpeta_archivos, archivo)
            elif (
                len(archivo.split("-")) == 2
                and int(archivo.split("-")[0]) > 2010
                and int(archivo.split(".")[0].split("-")[1]) < 13
            ):
                DescomprimirArchivosConsoAPS_IPS(carpeta_archivos, archivo)

            else:
                DescomprimirArchivos(carpeta_archivos, archivo)

            print("---------- 3.4 TERMINO DESCOMPRIMIR ARCHIVO " + archivo)

            fin = time.time()
            local_time_fin = time.ctime(fin)
            print("---- FIN ARCHIVO " + archivo + " HORA FIN: " + local_time_fin)

print("----- 4.ANALIZANDO CARPETA PARA ANALIS...")
contenido2 = os.listdir(carpeta_archivos)
num_registros = len(contenido2)
print("---------- 4.1.SE ENCONTRARON " + str(num_registros) + " registros")
if num_registros > 0:
    print("----- 5.COMINEZA PROCESO DE VALIDACIÓN REGISTROS")
    for archivo in contenido2:
        start_time = time.time()
        local_time = time.ctime(start_time)
        arr_archiv = archivo.split(".")
        extension = arr_archiv[len(arr_archiv) - 1]
        archivo_dir = carpeta_archivos + archivo
        if extension == "csv" or extension == "txt":
            print(
                "----- 6.SE PROCESA ARCHIVO: " + archivo + " HORA INICIO:" + local_time
            )
            print("---------- 6.2 EXTENSIÓN CSV VALIDA")
            print("---------- 6.3 OBTENER METADATA")
            peso, owner, crea, modifica, Checksum, fecha_informe = ObtenerMetaData(
                archivo_dir, archivo
            )
            print("---------- 6.4 FORMATO Y CARACTERISTICAS DEL ARCHIVO")
            total_reg_csv = TotalRegistros(archivo_dir)
            decoded, comillas = DetEnconder(archivo_dir, total_reg_csv)
            cabecera = ObtenerCabecera(archivo_dir)
            delimiter, total_columnas = ObtenerDelimitador(cabecera)
            validar_cabecera = ValidarCabecera(cabecera, delimiter)
            cabecera2 = len(cabecera.split("\t" if delimiter == "Tab" else delimiter))
            (
                estado,
                total_reg,
                mensaje,
                lineas_error,
                html_columnas,
                html_top_sample,
                valida_error,
                html_error,
                contador_lineas_error,
            ) = ValidarLargoColumnas(
                archivo_dir,
                total_columnas,
                validar_cabecera,
                decoded,
                total_reg_csv,
                cabecera.split("\t" if delimiter == "Tab" else delimiter),
                cabecera,
            )
            html_final = CreaMensajeHTML(
                carpeta_archivos,
                archivo,
                estado,
                decoded,
                delimiter,
                total_columnas,
                total_reg,
                mensaje,
                lineas_error,
                html_columnas,
                html_top_sample,
                html_error,
                contador_lineas_error,
            )
            enviado = Enviarmensaje(estado)
            CopiarArchivo(archivo_dir, estado, total_reg_csv)
            fin = time.time()
            local_time_fin = time.ctime(fin)
            print("---- FIN ARCHIVO " + archivo + " HORA FIN: " + local_time_fin)

print("FIN PROCESO COMPLETO")
