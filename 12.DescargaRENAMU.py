# DESCARGA DE LA DATA DESDE LA FUENTE DE DATOS DE CONSULTA AMIGABLE MEF
from codecs import decode
from io import BytesIO
from multiprocessing import connection
from queue import Empty
from unicodedata import name
from warnings import catch_warnings
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

conexion = create_engine('mysql+pymysql://root@localhost:3306/renamu')
con = conexion.raw_connection()
cur = con.cursor()

# leer la tabla base de la información recopilada
df_bd_consolidado = pd.read_sql_query('SELECT * FROM db_renamu', con=conexion)

# consulta y descarga de la información para un año específico. Esta se utiliza para cuando se publique nueva información
def consulta_anual (año):
  registros = df_bd_consolidado[df_bd_consolidado['Año'] == int(año)].reset_index(drop=True) 
  print(registros)
  consulta_enc_mod (registros)

# consulta y descarga de la información de todos los años. Esta se utilizó para la carga inicial y cuando hubo reprocesos.
def consulta_total (): 
  registros = df_bd_consolidado
  print(registros)
  consulta_enc_mod (registros)

# consulta que sirve para componer la URL de descarga, es decir, el link donde se encuentra almacenada la data

def consulta_enc_mod (registros):
  for i in registros.index:
    Nro_Encuesta = str(registros.loc[i,'Cod_Encuesta'])
    Nro_Modulo = str(registros.loc[i,'Cod_Modulo'])
    año = str(registros.loc[i,'Año'])
    acceso = str(registros.loc[i,'Tipo de Acceso'])
    file_url = 'http://iinei.inei.gob.pe/iinei/srienaho/descarga/'+acceso+'/'+ Nro_Encuesta +'-Modulo'+ Nro_Modulo +'.zip'
    get_zip(file_url,año)

# Función para descomprimir e ingresar la data de los módulos a la bd
def get_zip(file_url,año):
    url = req.get(file_url)
    myzipfile = ZipFile(BytesIO(url.content))

    for Zip_info in myzipfile.infolist():
      
      if Zip_info.filename[-1] == '/':
          continue

      Zip_info.filename = os.path.basename(Zip_info.filename)
      
      if '.dbf' in Zip_info.filename.lower() or '.dta' in Zip_info.filename.lower():
          myzipfile.extract(Zip_info, '/content/dbfs-dtas')
          ruta = '/content/dbfs-dtas/'+ Zip_info.filename
          if '.dbf' in Zip_info.filename.lower():
              dbf5 = Dbf5(ruta, codec='iso-8859-1')
              df = dbf5.to_dataframe()
          if '.dta' in Zip_info.filename.lower(): # En la web del INEI se ha ingresado algunos archivos .DAT a pesar que están en la ruta de DBFs
              df, meta = pyreadstat.read_dta(ruta) # Esta casuística es solo para el año 2015 y 2021
              # df.columns = df.columns.str.swapcase()
          nombre = 'renamu_'+ Zip_info.filename[0:(len(Zip_info.filename)-4)].lower()
          print("nombre: " + nombre)
          df.columns = df.columns.str.strip()
          df.columns = df.columns.str.replace('[\x00]', '')
          columnas_df = df.columns.str.lower()
          #print(columnas_df)
          df['ANO'] = str(año)
          tablas= pd.read_sql_query('SHOW TABLES',con=conexion)[['Tables_in_renamu']]
          #print(tablas)
          df_comprueba = tablas[tablas['Tables_in_renamu'] == nombre]
          if df_comprueba.empty:
              print("la tabla no existe, creamos!")
          else:
              print("ya está creada, verificamos las columnas y fila")
              columnas_bd = pd.read_sql_query('SHOW COLUMNS FROM `{table_name}`'.format(table_name = nombre), con=conexion)[['Field']]
              info_año = pd.read_sql_query('SELECT ANO FROM `{table_name}` WHERE ANO = {ano}'.format(table_name = nombre, ano = año), con=conexion)
             
              if info_año.empty:
                 print("nuevo año a agregar!")
              else:
                 print("año existe, eliminando registros e ingresando nueva data")
                 base_command = ("DELETE FROM `{table_name}` WHERE ANO = {ano}")
                 sql_command = base_command.format(table_name=nombre, ano = año)
                 cur.execute(sql_command)
                              
              for col in columnas_df:
                  df_prueba = columnas_bd[columnas_bd['Field'].str.lower() == col]
                  if df_prueba.empty:
                      print('agrega columna a la base de datos')
                      print (type(col))
                      if type(col) == "int":
                        data_formatted = "INT"
                      else:
                        data_formatted = "VARCHAR(90)"
                      
                      base_command = ("ALTER TABLE `{table_name}` ADD column `{column_name}` {data_formatted} NULL")
                      sql_command = base_command.format(table_name=nombre, column_name=col, data_formatted = data_formatted)
                      print(sql_command)
                      cur.execute(sql_command)
          df.to_sql(name=nombre, con = conexion, if_exists ='append')

con.close()

#consulta_anual (2020)

# aquí se debe ejecutar el script SLQ

