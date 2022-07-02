# CREACIÓN DE TABLA PARA EVOLUCIÓN POR REGIONES

from base64 import decode
from codecs import Codec
from ntpath import join
from numpy import empty
import requests as req
from flask import Flask, render_template, request, redirect, url_for, flash
from flask_mysqldb import MySQL
from sqlalchemy import create_engine, null
import pandas as pd
import sqlite3 

# initializations
app = Flask(__name__)

# conexión
conexion = create_engine('mysql+pymysql://root@localhost:3306/indiceanemia')
con = conexion.raw_connection()
cur = con.cursor()

# agregar valores a metas regionales de anemia
def calcula_indice_anemia ():
    info_años = pd.read_sql_query('SELECT DISTINCT ANO FROM data_indiceanemia', con=conexion)
    info_regiones = pd.read_sql_query('SELECT DISTINCT HV024 FROM data_indiceanemia', con=conexion)
   
    for año in info_años['ANO']:

        for region in info_regiones['HV024']:

            ind_reg = pd.read_sql_query("SELECT ANO, HV024, HV024_NAME, (SUM(FP)/(SELECT SUM(FP) FROM data_indiceanemia WHERE ANO = {año} AND HV024 = {region} AND Hc1 >= 6 AND Hc1 <= 35)) AS ind_anemia FROM data_indiceanemia WHERE ANO = {año} AND HV024 = {region} AND Hc1 >= 6 AND Hc1 <= 35 AND ANEMIA = 'Sí' GROUP BY ANO, HV024".format(año = año, region=region), con=conexion)
            ind_reg.to_sql(name='data_indice', con = conexion, if_exists ='append')
        
        ind_anual = pd.read_sql_query("SELECT ANO, (SUM(FP)/(SELECT SUM(FP) FROM data_indiceanemia WHERE ANO = {año} AND Hc1 >= 6 AND Hc1 <= 35)) AS ind_anemia FROM data_indiceanemia WHERE ANO = {año} AND Hc1 >= 6 AND Hc1 <= 35 and anemia = 'Sí' GROUP BY ANO".format(año = año), con=conexion)
        ind_anual.to_sql(name='data_indice', con = conexion, if_exists ='append')
        base_command = ('UPDATE data_indice SET HV024 = 0 WHERE HV024 is null')
        cur.execute(base_command)
        base_command = ('UPDATE data_indice SET HV024_NAME = "Peru" WHERE HV024_NAME is null')
        cur.execute(base_command)

#agregar el índice a la tabla metas regionales, para facilitar las visualizaciones
def agregar_indice_metas():
    base_command = ('UPDATE db_metas_regionales JOIN data_indice ON db_metas_regionales.Nombre_region = data_indice.HV024_NAME and db_metas_regionales.ano = data_indice.ano SET db_metas_regionales.indice_real = data_indice.ind_anemia')
    sql_command = base_command.format()
    cur.execute(sql_command)


calcula_indice_anemia ()
agregar_indice_metas()
