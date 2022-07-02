# CORRECCIONES REALIZADAS LUEGO DEL ANÁLISIS REALIZADO A LAS CORRECCIONES

from codecs import decode
from io import BytesIO
from multiprocessing import connection
from queue import Empty
from unicodedata import name
from zipfile import ZipFile
from os.path import basename
import os
import requests as req
import sqlalchemy
from simpledbf import Dbf5
import string
from flask import Flask, render_template, request, redirect, url_for, flash
from flask_mysqldb import MySQL
from sqlalchemy import create_engine, false
import pandas as pd
import sqlite3 
import pyreadstat

# initializations
app = Flask(__name__)

# conexión
conexion = create_engine('mysql+pymysql://root@localhost:3306/indiceanemia')
con = conexion.raw_connection()
cur = con.cursor()

# consulta de la información por año y por las tablas que requerimos

def correccion_tablas (tabla_ok,tabla_error):
    # Ingresar los datos de la tabla errada (tabla_error) en la tabla correcta (tabla_ok)
    bd_error = pd.read_sql_query('SELECT * FROM `{table_name}`'.format(table_name = tabla_error), con=conexion)
    bd_error.set_index('index', inplace = True)
    bd_error.columns = bd_error.columns.str.strip()
    bd_error.columns = bd_error.columns.str.replace('[\x00]', '')
    columnas_bd_error = bd_error.columns.str.lower()
    info_años_error = bd_error['ano'].unique()
    
    for año_error in info_años_error:
        print("año error: " + año_error)
        #bd_ok = pd.read_sql_query('SELECT * FROM `{table_name}`'.format(table_name = tabla_ok), con=conexion)
        columnas_bd_ok = pd.read_sql_query('SHOW COLUMNS FROM `{table_name}`'.format(table_name = tabla_ok), con=conexion)[['Field']]
        info_años_ok = pd.read_sql_query('SELECT DISTINCT ANO FROM `{table_name}`'.format(table_name = tabla_ok), con=conexion)
        #info_años_ok = info_años_ok.to_numpy()
        #print(info_años_ok)

        if año_error is info_años_ok:
           print("año existe, eliminando registros e ingresando nueva data")
           base_command = ("DELETE FROM `{table_name}` WHERE ANO = {ano}")
           sql_command = base_command.format(table_name = tabla_ok, ano = año_error)
           cur.execute(sql_command)

        for col in columnas_bd_error:
            df_prueba = columnas_bd_ok[columnas_bd_ok['Field'].str.lower() == col]
                            
            if df_prueba.empty:
                print('agrega columna a la base de datos')
                print (type(col))
                if type(col) == "int":
                    data_formatted = "INT"
                else:
                    data_formatted = "VARCHAR(30)"
                
                base_command = ("ALTER TABLE `{table_name}` ADD column `{column_name}` {data_formatted} NULL")
                sql_command = base_command.format(table_name=tabla_ok, column_name=col, data_formatted = data_formatted)
                print(sql_command)
                cur.execute(sql_command)
        
        df_error_año = bd_error[bd_error['ano'] == año_error]
        print("agregando data del:" + año_error)
        df_error_año.to_sql(name=tabla_ok, con = conexion, if_exists ='append')

    # Eliminar la tabla errada y quitar del inventario de tablas
    base_command = ("DROP TABLE `{table_name}`")
    sql_command = base_command.format(table_name=tabla_error)
    cur.execute(sql_command)

    base_command = ('UPDATE inventario_tablas SET tabla = {table_ok} WHERE tabla = {table_error}')
    sql_command = base_command.format(table_ok= tabla_ok, table_error=tabla_error)
    cur.execute(sql_command)

# corrección de campos con espacios en blancos
def correccion_campos (tabla,campo,accion):

    if accion == 'quitar espacios':

       base_command = ("UPDATE `{table_name}` set `{field_name}` = LTRIM(RTRIM({field_name}))")
       sql_command = base_command.format(table_name = tabla, field_name = campo)
       cur.execute(sql_command)
       print("espacios quitados")
    
# corrección de asignación de "0" en el CASEID
def correccion_caseid (tabla):
       
    base_command = ("UPDATE `{table_name}` SET CASEID = concat (SUBSTRING(CASEID, 1, (length(CASEID)-2)), ' ', SUBSTRING(CASEID, length(CASEID), 1)) WHERE substring(CASEID,-2,1) = '0'")
    sql_command = base_command.format(table_name = tabla)
    cur.execute(sql_command)
    print("CEROS quitados")

# Casos que se tuvieron que corregir
# correccion_tablas ('RECH0','RECHO')
# correccion_tablas ('RE223132','REC22312')
# correccion_tablas ('RE223132','R22312')
# correccion_tablas ('REC0111','REC01_11')
# correccion_tablas ('rec84dv','rec84_dv') # El rec84_dv contiene data del 2004 y 2006
# correccion_tablas ('rec84dv','rec84') # El rec84 contiene data del 2007
# correccion_tablas ('programas sociales x hogar','programas_sociales_x_hogar') # información del 2015 y 2018
# correccion_tablas ('rec93dvdisciplina','rec93dv_disciplina') # información del 2015
# correccion_tablas ('rec93dvdisciplina','rec93dv disciplina') # información del 2013, 2014 y 2017


# Correción de campos
correccion_campos ('RECH6','HHID','quitar espacios') # la primera vez se realiza para todos los campos que entrarán en evaluación
correccion_campos ('RECH23','HHID','quitar espacios') # la primera vez se realiza para todos los campos que entrarán en evaluación
correccion_campos ('PS_WAWAWASI','HHID','quitar espacios') # la primera vez se realiza para todos los campos que entrarán en evaluación
correccion_campos ('rech0','HHID','quitar espacios') # la primera vez se realiza para todos los campos que entrarán en evaluación


# Corregir todos los campos 

#df_baseTotal= pd.read_csv('Iniciales\TablasCampos.csv', encoding = "ISO-8859-1", delimiter=';') # también se puede usar la función read_table

#print(df_baseTotal.index)
#for item in df_baseTotal.index:
 #  correccion_campos (df_baseTotal['TABLA'][item],df_baseTotal['CAMPO'][item],'quitar espacios') # la primera vez se realiza para todos los campos que entrarán en evaluación
    
  # if df_baseTotal['CAMPO'][item] == 'CASEID':
   #    correccion_caseid (df_baseTotal['TABLA'][item])

