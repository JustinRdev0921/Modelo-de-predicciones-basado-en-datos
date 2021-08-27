import mysql.connector
from mysql.connector import Error
import numpy as np
import matplotlib.pyplot as plt
import datetime
from sklearn import linear_model
from sklearn.model_selection import train_test_split

try:
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
        db='meteorologica'
    )
    if conn.is_connected():
        cursor = conn.cursor()

except Error as ex:
    print(ex)
finally:
    if conn.is_connected():
        print('Conexión exitosa')

def obtenerDatosTemperatura(fecha):
    fecha_ini = fecha + ' 00:00:00'
    fecha_fin = fecha + ' 23:59:59'
    sql = "SELECT * FROM temperatura WHERE dat_hour >= '"+fecha_ini+"' AND dat_hour <= '"+fecha_fin+"'"
    cursor.execute(sql)
    temperatura = cursor.fetchall()
    return temperatura

def obtenerDatosHumedad(fecha):
    fecha_ini = fecha + ' 00:00:00'
    fecha_fin = fecha + ' 23:59:59'
    sql = "SELECT * FROM humedad WHERE dat_hour >= '"+fecha_ini+"' AND dat_hour <= '"+fecha_fin+"'"
    cursor.execute(sql)
    temperatura = cursor.fetchall()
    return temperatura

def obtenerDatosPrecipitacion(fecha):
    fecha_ini = fecha + ' 00:00:00'
    fecha_fin = fecha + ' 23:59:59'
    sql = "SELECT * FROM precipitacion WHERE dat_hour >= '"+fecha_ini+"' AND dat_hour <= '"+fecha_fin+"'"
    cursor.execute(sql)
    temperatura = cursor.fetchall()
    return temperatura

def obtenerDatosPresion(fecha):
    fecha_ini = fecha + ' 00:00:00'
    fecha_fin = fecha + ' 23:59:59'
    sql = "SELECT * FROM presion WHERE dat_hour >= '"+fecha_ini+"' AND dat_hour <= '"+fecha_fin+"'"
    cursor.execute(sql)
    temperatura = cursor.fetchall()
    return temperatura

def obtenerDatosRadiacion(fecha):
    fecha_ini = fecha + ' 00:00:00'
    fecha_fin = fecha + ' 23:59:59'
    sql = "SELECT * FROM radiacion WHERE dat_hour >= '"+fecha_ini+"' AND dat_hour <= '"+fecha_fin+"'"
    cursor.execute(sql)
    temperatura = cursor.fetchall()
    return temperatura

def obtenerDatosRocio(fecha):
    fecha_ini = fecha + ' 00:00:00'
    fecha_fin = fecha + ' 23:59:59'
    sql = "SELECT * FROM rocio WHERE dat_hour >= '"+fecha_ini+"' AND dat_hour <= '"+fecha_fin+"'"
    cursor.execute(sql)
    temperatura = cursor.fetchall()
    return temperatura

def obtenerDatosViento(fecha):
    fecha_ini = fecha + ' 00:00:00'
    fecha_fin = fecha + ' 23:59:59'
    sql = "SELECT * FROM viento WHERE dat_hour >= '"+fecha_ini+"' AND dat_hour <= '"+fecha_fin+"'"
    cursor.execute(sql)
    temperatura = cursor.fetchall()
    return temperatura

def obtenerDatosTranspiracion(fecha):
    fecha_ini = fecha + ' 00:00:00'
    fecha_fin = fecha + ' 23:59:59'
    sql = "SELECT * FROM transpiracion WHERE dat_hour >= '"+fecha_ini+"' AND dat_hour <= '"+fecha_fin+"'"
    cursor.execute(sql)
    temperatura = cursor.fetchall()
    return temperatura




def prediccion():
    sql = "SELECT dat_hour FROM temperatura ORDER BY `id` DESC LIMIT 1"
    cursor.execute(sql)
    fecha = cursor.fetchall()
    for i in fecha:
        date = i[0]

    #date = datetime.datetime.utcnow()

    for i in range(7):
        fecha_ini = date - datetime.timedelta(days=7-i)
        fecha_prediccion = fecha_ini + datetime.timedelta(weeks=1)
        fecha_prediccion = fecha_prediccion.strftime('%Y-%m-%d')

        fecha_rad_ini = fecha_ini.strftime('%Y-%m-%d')
        fecha_rad_fin = fecha_ini.strftime('%Y-%m-%d')

        fecha_ini = fecha_ini.strftime('%Y-%m-%d')

        fecha_ini += ' 00:00:00'
        fecha_rad_ini += ' 07:00:00'


        fecha_fin = date - datetime.timedelta(days=7-i-1)
        fecha_fin = fecha_fin.strftime('%Y-%m-%d')
        fecha_fin += ' 00:00:00'
        fecha_rad_fin += ' 18:00:00'

        sql = "SELECT max(temp), min(temp) FROM temperatura WHERE dat_hour >= '" + fecha_ini + "' AND dat_hour < '" + fecha_fin + "'"
        cursor.execute(sql)
        temp = cursor.fetchall()

        temp_max = float(0)
        temp_min = float(0)
        for i in temp:
            temp_max = float(i[0])
            temp_min = float(i[1])

        sql = "SELECT max(lev_hum), min(lev_hum) FROM humedad WHERE dat_hour >= '" + fecha_ini + "' AND dat_hour <= '" + fecha_fin + "'"
        cursor.execute(sql)
        temp = cursor.fetchall()
        hum_max = float(0)
        hum_min = float(0)
        for i in temp:
            hum_max = float(i[0])
            hum_min = float(i[1])

        sql = "SELECT max(presi_nivel), min(presi_nivel) FROM precipitacion WHERE dat_hour >= '" + fecha_ini + "' AND dat_hour <= '" + fecha_fin + "'"
        cursor.execute(sql)
        temp = cursor.fetchall()
        pres_max = float(0)
        pres_min = float(0)
        for i in temp:
            pres_max = float(i[0])
            pres_min = float(i[1])

        sql = "SELECT max(presion_atm), min(presion_atm) FROM presion WHERE dat_hour >= '" + fecha_ini + "' AND dat_hour <= '" + fecha_fin + "'"
        cursor.execute(sql)
        temp = cursor.fetchall()
        pres_atm_max = float(0)
        pres_atm_min = float(0)
        for i in temp:
            pres_atm_max = float(i[0])
            pres_atm_min = float(i[1])

        sql = "SELECT max(rad_nivel), min(rad_nivel) FROM radiacion WHERE dat_hour >= '" + fecha_rad_ini + "' AND dat_hour < '" + fecha_rad_fin + "'"
        cursor.execute(sql)
        temp = cursor.fetchall()
        rad_max = float(0)
        rad_min = float(0)
        for i in temp:
            rad_max = float(i[0])
            rad_min = float(i[1])

        sql = "SELECT max(rocio_nivel), min(rocio_nivel) FROM rocio WHERE dat_hour >= '" + fecha_ini + "' AND dat_hour <= '" + fecha_fin + "'"
        cursor.execute(sql)
        temp = cursor.fetchall()
        rocio_max = float(0)
        rocio_min = float(0)
        for i in temp:
            rocio_max = float(i[0])
            rocio_min = float(i[1])

        sql = "SELECT max(trans), min(trans) FROM transpiracion WHERE dat_hour >= '" + fecha_ini + "' AND dat_hour <= '" + fecha_fin + "'"
        cursor.execute(sql)
        temp = cursor.fetchall()
        trans_max = float(0)
        trans_min = float(0)
        for i in temp:
            trans_max = float(i[0])
            trans_min = float(i[1])

        sql = "SELECT max(viento_vel), min(viento_vel) FROM viento WHERE dat_hour >= '" + fecha_ini + "' AND dat_hour <= '" + fecha_fin + "'"
        cursor.execute(sql)
        temp = cursor.fetchall()
        viento_max = float(0)
        viento_min = float(0)
        for i in temp:
            viento_max = float(i[0])
            viento_min = float(i[1])

        sql = "INSERT INTO prediccion (fecha, humedad_max, humedad_min, precipitacion_max, precipitacion_min, presion_max, " \
              "presion_min, viento_max, viento_min, rocio_max, rocio_min, temperatura_max, temperatura_min, radiacion_max, radiacion_min, " \
              "transpiracion_max, transpiracion_min) " \
              "VALUES('%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
              "%s)"% (fecha_prediccion,hum_max, hum_min, pres_max, pres_min, pres_atm_max, pres_atm_min, viento_max, viento_min, rocio_max, rocio_min, temp_max, temp_min,rad_max,rad_min, trans_max, trans_min)
        cursor.execute(sql)
        conn.commit()



prediccion()




