# OBTENER EL NOMBRE DE CAMPOS 

from base64 import decode
from codecs import Codec
from ntpath import join
from operator import concat
import requests as req
from flask import Flask, render_template, request, redirect, url_for, flash
from flask_mysqldb import MySQL
from sqlalchemy import create_engine
import pandas as pd
import sqlite3 

# initializations
app = Flask(__name__)

# conexión
conexion = create_engine('mysql+pymysql://root@localhost:3306/indiceanemia')
con = conexion.raw_connection()
cur = con.cursor()

# Agregar datos de provincia y distrito
def agregarDatoGeo(nivel):

    nom_columna = concat("nombre_",nivel)
    
    
    if nivel == 'provincia':
       base_command = ('ALTER TABLE data_indiceanemia ADD {nom_columna} varchar(50) AFTER SHPROVIN')
       sql_command = base_command.format(tabla = nivel, nom_columna = nom_columna)
       cur.execute(sql_command)
       base_command = ('UPDATE data_indiceanemia JOIN db_nom_provDis ON data_indiceanemia.HV024 = db_nom_provDis.ccdd and data_indiceanemia.{tabla} = db_nom_provDis.ccpp SET data_indiceanemia.{nom_columna} = db_nom_provDis.provincia')
       sql_command = base_command.format(tabla = 'SHPROVIN', nom_columna = nom_columna)
       cur.execute(sql_command)

    if nivel == 'distrito':
       base_command = ('ALTER TABLE data_indiceanemia ADD {nom_columna} varchar(50) AFTER SHDISTRI')
       sql_command = base_command.format(tabla = nivel, nom_columna = nom_columna)
       cur.execute(sql_command)
       base_command = ('UPDATE data_indiceanemia JOIN db_nom_provDis ON data_indiceanemia.HV024 = db_nom_provDis.ccdd and data_indiceanemia.SHPROVIN = db_nom_provDis.ccpp and  data_indiceanemia.{tabla} = db_nom_provDis.ccdi SET data_indiceanemia.{nom_columna} = db_nom_provDis.distrito')
       sql_command = base_command.format(tabla = 'SHDISTRI', nom_columna = nom_columna)
       cur.execute(sql_command)

    print ("se agregó:" +nivel)

def div_lima ():
    base_command = ('UPDATE data_indiceanemia SET HV024_NAME = "Lima (city)" WHERE HV024 = 15 AND SHREGION = 1')
    sql_command = base_command.format()
    cur.execute(sql_command)

    base_command = ('UPDATE data_indiceanemia SET HV024 = 151 WHERE HV024_NAME = "Lima (city)"')
    sql_command = base_command.format()
    cur.execute(sql_command)

#agregarDatoGeo('provincia')
agregarDatoGeo('distrito')

