# OBTENER EL NOMBRE DE CAMPOS 

from base64 import decode
from codecs import Codec
from ntpath import join
from operator import concat
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

# Verificar 

def add_equivalencias ():

    info_campos = pd.read_sql_query('SELECT DISTINCT CAMPO FROM db_nombre_ids', con=conexion)

    for campo in info_campos['CAMPO']:
        
        base_command = ('DROP TABLE IF EXISTS {tabla}')
        sql_command = base_command.format(tabla = campo)
        cur.execute(sql_command)
        
        base_command = ('CREATE TABLE {tabla} AS SELECT id, Nombre_ID FROM db_nombre_ids where campo = "{tabla}"')
        sql_command = base_command.format(tabla = campo)
        cur.execute(sql_command)

        base_command = ('CREATE INDEX IND_ID ON {tabla}(id)')
        sql_command = base_command.format(tabla = campo)
        cur.execute(sql_command)

        nom_columna = concat(campo,"_name")

        base_command = ('ALTER TABLE data_indiceanemia ADD {nom_columna} varchar(20) AFTER {tabla}')
        sql_command = base_command.format(tabla = campo, nom_columna = nom_columna)
        cur.execute(sql_command)

        base_command = ('UPDATE data_indiceanemia JOIN {tabla} ON data_indiceanemia.{tabla} = {tabla}.id SET data_indiceanemia.{nom_columna} = {tabla}.Nombre_ID')
        sql_command = base_command.format(tabla = campo, nom_columna = nom_columna)
        cur.execute(sql_command)

        #print ("se agregó:" +campo)

# crear campo indicador de anemia
def ind_anemia ():

    base_command = ('ALTER TABLE data_indiceanemia ADD anemia varchar(2) AFTER HC57')
    sql_command = base_command.format()
    cur.execute(sql_command)

    base_command = ('UPDATE data_indiceanemia SET anemia = "Sí" WHERE HC57 = 1 OR HC57 = 2 OR HC57 = 3')
    sql_command = base_command.format()
    cur.execute(sql_command)

    base_command = ('UPDATE data_indiceanemia SET anemia = "No" WHERE HC57 = 4')
    sql_command = base_command.format()
    cur.execute(sql_command)


# crear campo agrupación de edad
def edad_grupo ():

    base_command = ('ALTER TABLE data_indiceanemia ADD edad_meses_grupo varchar(30) AFTER HC1')
    sql_command = base_command.format()
    cur.execute(sql_command)

    base_command = ('UPDATE data_indiceanemia SET edad_meses_grupo = "< 6 " WHERE HC1 < 6')
    sql_command = base_command.format()
    cur.execute(sql_command)

    base_command = ('UPDATE data_indiceanemia SET edad_meses_grupo = "6 a 11" WHERE HC1 < 12 and HC1 >=6')
    sql_command = base_command.format()
    cur.execute(sql_command)
    
    base_command = ('UPDATE data_indiceanemia SET edad_meses_grupo = "12 a 17" WHERE HC1 < 18 and HC1 >=12')
    sql_command = base_command.format()
    cur.execute(sql_command)
    
    base_command = ('UPDATE data_indiceanemia SET edad_meses_grupo = "18 a 23" WHERE HC1 < 24 and HC1 >=18')
    sql_command = base_command.format()
    cur.execute(sql_command)
    
    base_command = ('UPDATE data_indiceanemia SET edad_meses_grupo = "24 a 29" WHERE HC1 < 30 and HC1 >=24')
    sql_command = base_command.format()
    cur.execute(sql_command)
    
    base_command = ('UPDATE data_indiceanemia SET edad_meses_grupo = "30 a 35" WHERE HC1 < 36 and HC1 >=30')
    sql_command = base_command.format()
    cur.execute(sql_command)

    base_command = ('UPDATE data_indiceanemia SET edad_meses_grupo = "36 a 59" WHERE HC1 < 60 and HC1 >=36')
    sql_command = base_command.format()
    cur.execute(sql_command)

def consolidar_CH ():
    
    sql_command = ("ALTER TABLE data_indiceanemia ADD column Consumo_hierro_7d VARCHAR(2) NULL")
    cur.execute(sql_command)
    sql_command = ("UPDATE data_indiceanemia SET Consumo_hierro_7d = 'NS' WHERE S465EA = 8 or S465EB = 8 or S465EC = 8 or S465ED = 8").format()
    cur.execute(sql_command)
    sql_command = ("UPDATE data_indiceanemia SET Consumo_hierro_7d = 'No' WHERE S465EA = 2 or S465EB = 2 or S465EC = 2 or S465ED = 2").format()
    cur.execute(sql_command)
    sql_command = ("UPDATE data_indiceanemia SET Consumo_hierro_7d = 'Sí' WHERE S465EA = 1 or S465EB = 1 or S465EC = 1 or S465ED = 1").format()
    cur.execute(sql_command)

def consolidar_SS ():
    
    #sql_command = ("ALTER TABLE data_indiceanemia ADD column Tiene_seguro_salud VARCHAR(2) NULL")
    #cur.execute(sql_command)
    sql_command = ("UPDATE data_indiceanemia SET Tiene_seguro_salud = 'Sí' WHERE `SH11A` = 1 or `SH11B` = 1 or `SH11C` = 1 or `SH11D` = 1 or `SH11E` = 1" ).format()
    cur.execute(sql_command)
    sql_command = ("UPDATE data_indiceanemia SET Tiene_seguro_salud = 'No' WHERE Tiene_seguro_salud is null and `SH11Z` = 1").format()
    cur.execute(sql_command)
    sql_command = ("UPDATE data_indiceanemia SET Tiene_seguro_salud = 'NS' WHERE Tiene_seguro_salud is null and `SH11Y` = 1").format()
    cur.execute(sql_command)    

#add_equivalencias ()
#ind_anemia ()
edad_grupo ()
consolidar_CH ()
consolidar_SS ()
