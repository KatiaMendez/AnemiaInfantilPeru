# DESCARGA DE LA DATA DESDE LA FUENTE DE DATOS DEL INEI Y CARGA EN BASE DE DATOS
# Este scrit permite cargar toda la data de las encuestas que se encuentra en la página oficial del INEI (ENDES), 
# previo a ello, se tiene que procesar el ítem 01. CargasIniciales.py en el cual se realiza la carga de 
# todos los módulos en forma de tabla de la siguiente ruta: http://iinei.inei.gob.pe/microdatos/ 
# en la bd: indiceanemia / table: db_endes. 
# Esta actividad solo se debe realizar cuando publiquen actualizaciones de la data o data de un nuevo año. 

from codecs import decode
from io import BytesIO
from zipfile import ZipFile
import os
import requests as req
import sqlalchemy
from simpledbf import Dbf5
import string
from flask import Flask
from flask_mysqldb import MySQL
from sqlalchemy import create_engine
import pandas as pd
import pyreadstat
from datetime import datetime

# initializations
app = Flask(__name__)

# conexión
conexion = create_engine('mysql+pymysql://root@localhost:3306/indiceanemia')
con = conexion.raw_connection()
cur = con.cursor()

# leer la tabla base de la información recopilada
df_bd_consolidado = pd.read_sql_query('SELECT * FROM db_endes', con=conexion)

# consulta y descarga de la información para un año específico. Esta se utiliza para cuando se publique nueva información
# por ende la periodicidad de este proceso es anual. En nuestro caso lo utilizamos para el año 2021
def consulta_anual (año):
  registros = df_bd_consolidado[df_bd_consolidado['Año'] == int(año)].reset_index(drop=True) 
  print(registros)
  consulta_enc_mod (registros)

# consulta y descarga de la información de todos los años. Esta se utilizó para la carga inicial y cuando hubo reprocesos.
# Sirve para que si otras personas requerieren ajustar el código a sus necesidades utilicen esta función para una carga total.
def consulta_total (): 
  registros = df_bd_consolidado.iloc[::-1].reset_index(drop=True) 
  print(registros)
  consulta_enc_mod (registros)

# Consulta  y descarga por año y módulo. Especializada, por si se presentan errores de carga y se requiera reprocesos.
def consulta_forAnual (Modulo, año):
  registros = df_bd_consolidado[(df_bd_consolidado['Módulo'] == Modulo) & (df_bd_consolidado['Año'] == año)].reset_index(drop=True)
  consulta_enc_mod (registros)

# consulta que sirve para componer la URL de descarga, es decir, el link donde se encuentra almacenada la data
# hasta el año 2020, todos los años se contaba con data en DBFs, sin embargo en el 2021 no se tuvo este tipo de data, 
# por lo cual se tuvo que adaptar el código para la lectura de código DTAs (STATA)
def consulta_enc_mod (registros):
  for i in registros.index:
    Nro_Encuesta = str(registros.loc[i,'Cod_Encuesta'])
    Nro_Modulo = str(registros.loc[i,'Cod_Modulo'])
    año = str(registros.loc[i,'Año'])
    acceso = str(registros.loc[i,'Tipo de acceso'])
    file_url = 'http://iinei.inei.gob.pe/iinei/srienaho/descarga/'+acceso+'/'+ Nro_Encuesta +'-Modulo'+ Nro_Modulo +'.zip'
    get_zip(file_url,año)

# Función para descomprimir e ingresar la data de los módulos a la bd
# tomar en cuenta que si se desea incurrir en menor tiempo de procesamiento, 
# se deben ingresar primero los registros más actuales, debido a que tienen más campos

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
          if '.dta' in Zip_info.filename.lower(): # En la web del INEI se tienen algunos .DAT
              df, meta = pyreadstat.read_dta(ruta) 
          nombre = Zip_info.filename[0:(len(Zip_info.filename)-4)].lower()
          df.columns = df.columns.str.strip()
          df.columns = df.columns.str.replace('[\x00]', '')
          columnas_df = df.columns.str.lower()
          df['ano'] = str(año)
          df['fecha_extrae'] = str(datetime.now().date())
          tablas= pd.read_sql_query('SHOW TABLES',con=conexion)[['Tables_in_indiceanemia']]
         
          df_comprueba = tablas[tablas['Tables_in_indiceanemia'] == nombre]
          print(nombre)
          if df_comprueba.empty:
              print("la tabla no existe, creamos!")
          else:
              print("ya está creada, verificamos las columnas y fila")
              columnas_bd = pd.read_sql_query('SHOW COLUMNS FROM `{table_name}`'.format(table_name = nombre), con=conexion)[['Field']]
              info_año = pd.read_sql_query('SELECT ANO FROM `{table_name}` WHERE ANO = {ano}'.format(table_name = nombre, ano = año), con=conexion)
                        
              for col in columnas_df:
                  df_prueba = columnas_bd[columnas_bd['Field'].str.lower() == col]
                  if df_prueba.empty:
                      print('agrega columna a la base de datos')
                      if type(col) == "int":
                        data_formatted = "INT"
                      else:
                        data_formatted = "TEXT"
                      
                      base_command = ("ALTER TABLE `{table_name}` ADD column `{column_name}` {data_formatted} NULL")
                      sql_command = base_command.format(table_name=nombre, column_name=col, data_formatted = data_formatted)
                      cur.execute(sql_command)
              
              if info_año.empty:
                 print("nuevo año a agregar!")
              else:
                 print("año existe, eliminando registros e ingresando nueva data")
                 base_command = ("DELETE FROM `{table_name}` WHERE ANO = {ano}")
                 sql_command = base_command.format(table_name=nombre, ano = año)
                 cur.execute(sql_command)


          df.to_sql(name=nombre, con = conexion, if_exists ='append')

con.close()

# consulta_total()
# consulta_anual(2008)
consulta_anual(2007)


# consulta_forAnual ('Peso y talla - Anemia', 2007)
# consulta_forAnual ('Peso y talla - Anemia', 2008)