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

def crear_baseTotal():
    # Creamos el df con la información de la tabla endes y lo enviamos a BD
    df_baseTotal= pd.read_csv('Iniciales\DB_Endes.csv', encoding = "ISO-8859-1", delimiter=';') # también se puede usar la función read_table
    df_baseTotal.to_sql(name='db_endes', con = conexion, if_exists ='replace')

def crear_baseIndexes():
    # Creamos el df con la información de la tabla índices y lo enviamos a BD
    df_baseIndices = pd.read_csv('Iniciales\DB_Indices.csv', encoding = "ISO-8859-1", delimiter=';') 
    df_baseIndices.to_sql(name='db_indices', con = conexion, if_exists ='replace')

def crear_baseCampos():
    # Creamos el df con la descripción de los campos y lo enviamos a BD
    df_baseCampos= pd.read_csv('Iniciales\DB_NomCampos.csv', encoding = "ISO-8859-1", delimiter=';') # también se puede usar la función read_table
    df_baseCampos.to_sql(name='db_nom_campos', con = conexion, if_exists ='replace')
    
def crear_baseNomIDs():
    # Creamos el df con el despliegue de los nombres codificados en la información recopilada
    df_baseIds= pd.read_csv('Iniciales\DB_nombreIDs.csv', encoding = "ISO-8859-1", delimiter=';') # también se puede usar la función read_table
    df_baseIds.to_sql(name='db_nombre_ids', con = conexion, if_exists ='replace')

def crear_baseMetas():
    # Creamos el df con el despliegue de las metas regionales que se plantearon en el año 2017
    df_baseMetas= pd.read_csv('Iniciales\MetasRegionalesPeru.csv', encoding = "ISO-8859-1", delimiter=';') # también se puede usar la función read_table
    df_baseMetas.to_sql(name='db_metas_regionales', con = conexion, if_exists ='replace')

def crear_baseRenamu():
    # Creamos el df con el despliegue de las metas regionales que se plantearon en el año 2017
    df_baseMetas= pd.read_csv('Iniciales\DB_RENAMU.csv', encoding = "ISO-8859-1", delimiter=';') # también se puede usar la función read_table
    df_baseMetas.to_sql(name='db_renamu', con = conexion, if_exists ='replace')


# Al incio, previo a cualquier carga
crear_baseTotal() 
crear_baseIndexes()  
crear_baseCampos()
crear_baseNomIDs()
crear_baseMetas()
crear_baseRenamu()