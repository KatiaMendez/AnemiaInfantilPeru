# CREACIÓN DE ÍNDICES
# Esta actividad nos sirve para poder lograr mayor eficiencia de la base de datos a través de INDEX
# Las columnas que se escogen para crear índices son aquellas que permiten relación con otras tablas
# Se requiere que todas las tablas se encuentren cargadas
# Previo a la ejecución, se necesita cargar la tabla de índices. Ver archivo CargasIniciales.py

from codecs import decode
from io import BytesIO
from multiprocessing import connection
from queue import Empty
from tempfile import tempdir
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
from sqlalchemy import create_engine
import pandas as pd
import sqlite3 

# initializations
app = Flask(__name__)

# conexión
conexion = create_engine('mysql+pymysql://root@localhost:3306/indiceanemia')
con = conexion.raw_connection()
cur = con.cursor()

# leer la tabla base de la información recopilada
df_indices = pd.read_sql_query('SELECT * FROM db_indices', con=conexion)
print (df_indices)

# actualizar el tipo de dato para los índices
for item in df_indices.index:
    nombre = df_indices.loc[item,"Tabla"]
    col = df_indices.loc[item,"Campo"]
    data_formatted = df_indices.loc[item,"Tipo"]
    base_command = ("ALTER TABLE `{table_name}` MODIFY `{column_name}` {data_formatted} NULL")
    sql_command = base_command.format(table_name=nombre, column_name=col, data_formatted = data_formatted)
    cur.execute(sql_command)

# crear los índices
for item in df_indices.index:

    nombre = df_indices.loc[item,"Tabla"]
    col = df_indices.loc[item,"Campo"]
    nom_index = df_indices.loc[item,"Índice"]
    indices = pd.read_sql_query("SHOW INDEX FROM `{tabla_name}` WHERE COLUMN_NAME = '{col_name}'".format(tabla_name = nombre, col_name = col), con=conexion)
  
    if nom_index in indices["Key_name"]:
        print("Índice ya existe: "+nom_index)    
    else:
        base_command = ("CREATE INDEX `{nom_index}` ON `{tabla}`(`{column_name}`)")
        sql_command = base_command.format(tabla=nombre, column_name=col, nom_index = nom_index)
        cur.execute(sql_command)
        print("indice creado: "+nom_index)


