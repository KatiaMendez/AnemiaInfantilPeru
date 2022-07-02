# CREACIÓN DE TABLA CON LOS DATOS DE ENCUESTAS POR AÑO Y MÓDULO
# Esta actividad se realiza solo una vez.
# La carga de la BD global ENDES es requerida al inicio, previo a cualquier otro proceso 
# La creación de la BD de índice se realiza cuando exista las tablas a nivel cabecera

from base64 import decode
from codecs import Codec
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

# Verificamos el índice de anemia
años = ['2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021']
for año in años:
    info_año = pd.read_sql_query('SELECT ANO, Anemia, (SUM(FP)/(SELECT SUM(FP) FROM data_indiceanemia WHERE ANO = {ano} AND HC57 <= 4 AND HV103 = 1 AND Hc1 >= 6 AND Hc1 <= 35 AND HC53 is NOT null))*100 AS ind_anemia FROM data_indiceanemia WHERE ANO = {ano} AND HC57 <= 4 AND HV103 = 1 AND Hc1 >= 6 AND Hc1 <= 35 AND HC53 is NOT null and Anemia = "Sí" GROUP BY ANO, Anemia'.format(ano = año), con=conexion)
    print(info_año)
