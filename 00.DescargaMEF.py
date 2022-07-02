# DESCARGA DE LA DATA DESDE LA FUENTE DE DATOS DE CONSULTA AMIGABLE MEF
import requests as req
from flask import Flask
from flask_mysqldb import MySQL
from sqlalchemy import create_engine, false
import pandas as pd

# conexión
conexion = create_engine('mysql+pymysql://root@localhost:3306/indiceanemia')
con = conexion.raw_connection()
cur = con.cursor()

def descarga_MEF():
    data_pan = pd.DataFrame()
    años = ['2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021']
    for año in años:
        ruta = 'https://apps5.mineco.gob.pe/bingos/pestrategicos/Navegador/Navegador.aspx?_tgt=xls&_uhc=yes&0=&8=0001&1=R&2=99&3=&y='+año+'&ap=0'
        file = req.get(ruta, allow_redirects=True)
        xls = open('ejecPpto'+str(año)+'.html', 'wb')
        xls.write(file.content)
        xls.close()

def carga_bdMEF():
    df_baseMetas= pd.read_csv('Iniciales\EG-PAN.csv', encoding = "ISO-8859-1", delimiter=';') # también se puede usar la función read_table
    df_baseMetas.to_sql(name='db_EG-PAN', con = conexion, if_exists ='replace')
    df_baseMetas= pd.read_csv('Iniciales\EG-VITA-HIERRO.csv', encoding = "ISO-8859-1", delimiter=';') # también se puede usar la función read_table
    df_baseMetas.to_sql(name='db_EG-VITHIERRO', con = conexion, if_exists ='replace')

descarga_MEF()
#carga_bdMEF()