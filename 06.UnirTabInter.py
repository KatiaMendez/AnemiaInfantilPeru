# Validar las rutas y los scripts

import requests as req
from flask import Flask, render_template, request, redirect, url_for, flash
from flask_mysqldb import MySQL
from sqlalchemy import create_engine
import pandas as pd

# initializations
app = Flask(__name__)

# conexión
conexion = create_engine('mysql+pymysql://root@localhost:3306/indiceanemia')
con = conexion.raw_connection()
cur = con.cursor()

# leer la tabla base de datos recopilados y crear nuevas tablas
# esto se tiene que hacer tabla por tabla y no admite el ";" --- se mantiene para futuras necesidades 
# pero las consultas son ejecutadas directacmente desde mySQL

def crear_tab_intermedias ():
    
    with open('scripts\TablasIntermedias.sql', 'r') as myfile:
        data = myfile.read()
        print(data)
        cur.execute(data)

def crear_tab_final ():
    
    with open('scripts\TablaFinal.sql', 'r') as myfile:
        data = myfile.read()
        print(data)
        cur.execute(data)


crear_tab_intermedias()

