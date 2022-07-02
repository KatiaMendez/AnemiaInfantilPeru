# CREACIÓN DE TABLA PARA EVOLUCIÓN POR REGIONES

from flask import Flask
from sqlalchemy import create_engine, null

# initializations
app = Flask(__name__)

# conexión
conexion = create_engine('mysql+pymysql://root@localhost:3306/indiceanemia')
con = conexion.raw_connection()
cur = con.cursor()


# Asignar lo del 2008 al 2007 y retirar los duplicados del 2006 
def act_año ():
    base_command = ('UPDATE data_indiceanemia SET ANO = 2007 WHERE ANO = 2008') # Se consolida porque el INEI consolida ambos resultados
    sql_command = base_command.format()
    cur.execute(sql_command)

    base_command = ('DELETE FROM  data_indiceanemia WHERE ANO = 2006') # Se elimina porque la data está duplicada con la del 2005
    sql_command = base_command.format()
    cur.execute(sql_command)

act_año ()
