from operator import concat
from numpy import concatenate
import requests
import requests as req
from flask import Flask
from flask_mysqldb import MySQL
from sqlalchemy import create_engine
import pandas as pd
import pyreadstat
from APIS import obtenerAPI_GM

# initializations
app = Flask(__name__)

# conexión
conexion = create_engine('mysql+pymysql://root@localhost:3306/indiceanemia')
con = conexion.raw_connection()
cur = con.cursor()

# creamos nuevas columna para la geolocalización tabla: db_nom_provdis
# base_command = ('ALTER TABLE db_nom_provdis ADD COLUMN lat_prov double (20,16), ADD COLUMN  lnt_prov double(20,16), ADD COLUMN lat_dist double(20,16), ADD COLUMN  lnt_dist double(20,16)')
# cur.execute(base_command)

# hacemos la consulta por provincia

def geolocalizar_provincia():
    df_bd_provincias = pd.read_sql_query('SELECT distinct provincia, departamen FROM db_nom_provdis', con=conexion)
    df_bd_provincias = df_bd_provincias.head()
    for i in df_bd_provincias.index:
        provincia = str(df_bd_provincias.loc[i,'provincia'])
        region  = str(df_bd_provincias.loc[i,'departamen'])
        place_provincia = provincia + ',' + region + ',PERÚ'
        API_Key = obtenerAPI_GM()
        url = 'https://maps.googleapis.com/maps/api/geocode/json?&address='+place_provincia+'%C3%BA&key='+API_Key+'&'
        req = requests.get(url)
        res = req.json()
        try: 
            result = res['results']
            geodata_latitud = result[0]['geometry']['location']['lat']
            geodata_longitud = result[0]['geometry']['location']['lng']
            base_command = ('UPDATE db_nom_provdis SET lat_prov = {geodata_latitud}, lnt_prov = {geodata_longitud} WHERE provincia = "{provincia}" and departamen = "{region}"') 
            sql_command = base_command.format(geodata_latitud = geodata_latitud, geodata_longitud = geodata_longitud, provincia = provincia, region = region)
            cur.execute(sql_command)
        except:
            print('except')
            geodata_latitud = ""
            geodata_longitud = ""


# hacemos la consulta por distrito

def geolocalizar_distrito():

    df_bd_distritos = pd.read_sql_query('SELECT distinct distrito, provincia, departamen FROM db_nom_provdis', con=conexion)

    for i in df_bd_distritos.index:
        provincia = str(df_bd_distritos.loc[i,'provincia'])
        region  = str(df_bd_distritos.loc[i,'departamen'])
        distrito  = str(df_bd_distritos.loc[i,'distrito'])
        place_distrito = distrito + ',' + provincia + ',' + region + ',PERÚ'
        
        url = 'https://maps.googleapis.com/maps/api/geocode/json?&address='+place_distrito+'%C3%BA&key=AIzaSyAY2ThK3p51AIV-9czncihRj7-brwadCZ4&'
        print(url)
        req = requests.get(url)
        #print(req)
        res = req.json()
        #print(res['results'])
        
        try: 
            result = res['results']
            geodata_latitud = result[0]['geometry']['location']['lat']
            geodata_longitud = result[0]['geometry']['location']['lng']
            print(geodata_latitud)
            print(geodata_longitud)
            base_command = ('UPDATE db_nom_provdis SET lat_dist = {geodata_latitud}, lnt_dist = {geodata_longitud} WHERE distrito = "{distrito}" and provincia = "{provincia}" and departamen = "{region}"') # Se consolida porque el INEI consolida ambos resultados
            sql_command = base_command.format(geodata_latitud = geodata_latitud, geodata_longitud = geodata_longitud, distrito= distrito, provincia = provincia, region = region)
            cur.execute(sql_command)

        except:
            print('except')
            geodata_latitud = ""
            geodata_longitud = ""


geolocalizar_provincia()
#geolocalizar_distrito()
