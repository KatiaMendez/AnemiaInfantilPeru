# CREACIÓN DE TABLA PARA EVOLUCIÓN POR REGIONES

from flask import Flask
from flask_mysqldb import MySQL
from sqlalchemy import create_engine, null
import pandas as pd

# initializations
app = Flask(__name__)

# conexión
conexion = create_engine('mysql+pymysql://root@localhost:3306/indiceanemia')
con = conexion.raw_connection()
cur = con.cursor()

# Asignar nombre descriptivo a las columnas para un mejor entendimiento y procesamiento
def nombre_columnas (table):
    info_columns = pd.read_sql_query('SHOW COLUMNS FROM `{table_name}`'.format(table_name = table), con=conexion)[['Field']]
    print(info_columns)
    for columna in info_columns['Field']:
        print(columna)
        datos = pd.read_sql_query('SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_NAME="{table_name}" AND COLUMN_NAME="{campo}"'.format(table_name = table, campo = columna), con=conexion).reset_index(drop=True) 
        tip_dato = datos.iloc[0]['DATA_TYPE']
        long_dato = datos.iloc[0]['CHARACTER_MAXIMUM_LENGTH']
        print(tip_dato)
        print(long_dato)
        descripcion = pd.read_sql_query('SELECT Descripcion FROM db_nom_campos WHERE campo="{campo}"'.format(campo = columna), con=conexion).reset_index(drop=True) 
                
        if descripcion.empty:
           print("ingresó al vacío")
           continue
        else:
           descripcion = descripcion.iloc[0]['Descripcion']
           print(descripcion)
           if long_dato is None:
              base_command = ('ALTER TABLE {table_name} CHANGE COLUMN {campo} `{campo_nuevo}` {tipo_dato}') 
              sql_command = base_command.format(table_name=table, campo=columna, campo_nuevo = descripcion, tipo_dato = tip_dato)
              cur.execute(sql_command)

           else:
               base_command = ('ALTER TABLE {table_name} CHANGE COLUMN {campo} `{campo_nuevo}` {tipo_dato} ({long_dato})') 
               sql_command = base_command.format(table_name=table, campo=columna, campo_nuevo = descripcion, tipo_dato = tip_dato, long_dato = long_dato)
               cur.execute(sql_command)

nombre_columnas ('data_indiceanemia')
