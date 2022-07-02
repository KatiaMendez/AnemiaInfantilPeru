# VERIFICACIÓN DE QUE SE TENGA TODA LA DATA DE LAS TABLAS QUE UTILIZAREMOS PARA NUESTRO ANÁLISIS

from multiprocessing import connection
from queue import Empty
from unicodedata import name
from os.path import basename
import requests as req
import string
from flask import Flask, render_template, request, redirect, url_for, flash
from flask_mysqldb import MySQL
from sqlalchemy import create_engine, false
import pandas as pd
import sqlite3 

# initializations
app = Flask(__name__)

# conexión
conexion = create_engine('mysql+pymysql://root@localhost:3306/indiceanemia')
con = conexion.raw_connection()
cur = con.cursor()

# consulta de la información por año y por las tablas que requerimos

def verificar_data (tablas,años):
    for tabla in tablas:
        for año in años:
            info_año = pd.read_sql_query('SELECT ANO FROM `{table_name}` WHERE ANO = {ano}'.format(table_name = tabla, ano = año), con=conexion)
            if info_año.empty:
                print("No hay información en la tabla: "+tabla+", para el año: "+año)
            

# Se brindan los valores que se quieren verificar que tengan información
tablas = ['RECH1','RECH4','RECH6','RECH0','RECH23','PS_WAWAWASI','REC43','REC95','REC94','REC41','REC21','REC0111','RE223132']
años = ['1996','2000','2004','2005','2006','2007','2008','2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021']
verificar_data (tablas,años)