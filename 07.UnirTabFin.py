# Validar las rutas y los scripts

from flask import Flask
from sqlalchemy import create_engine
import pandas as pd

# initializations
app = Flask(__name__)

# conexión
conexion = create_engine('mysql+pymysql://root@localhost:3306/indiceanemia')
con = conexion.raw_connection()
cur = con.cursor()

# consolidación de las tablas intermedias

def crear_tab_final ():
    
    with open('scripts\TablaFinal.sql', 'r') as myfile:
        data = myfile.read()
        print(data)
        cur.execute(data)