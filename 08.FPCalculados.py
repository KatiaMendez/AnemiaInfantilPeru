from flask import Flask
from sqlalchemy import create_engine
import pandas as pd


# initializations
app = Flask(__name__)

# conexión
conexion = create_engine('mysql+pymysql://root@localhost:3306/indiceanemia')
con = conexion.raw_connection()
cur = con.cursor()

# actualización del FP (Factor de ponderación en la tabla data_indiceanemia)

def consolidar_FP ():
    
    info_años = pd.read_sql_query('SELECT DISTINCT ANO FROM all_data', con=conexion)
    sql_command = ("ALTER TABLE data_indiceanemia ADD column FP double NULL")
    cur.execute(sql_command)
    info_años = info_años.to_numpy()
    print(info_años)
    for año in info_años:
        print(año[0])

        if año == 2020:
            sql_command = ("UPDATE data_indiceanemia SET FP = HV005A_RECH6 WHERE ANO = {ano}").format(ano = año[0])
            cur.execute(sql_command)
            continue

        if año == 2015:
            sql_command = ("UPDATE data_indiceanemia SET FP = HV005X WHERE ANO = {ano}").format(ano = año[0])
            cur.execute(sql_command)
            continue

        else: 
            sql_command = ("UPDATE data_indiceanemia SET FP = HV005 WHERE ANO = {ano}").format(ano = año[0])
            cur.execute(sql_command)
            continue

consolidar_FP()           

