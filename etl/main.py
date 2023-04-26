import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import mysql.connector
import os

if 'etl' not in os.getcwd():
    os.chdir('etl')
    

# vamos a crearnos una serie de inputs para preguntarle al cliente

idioma = input("¿En qué idioma quieres la información entre los siguientes?: es/en ")
print("---------------------------------------------------------------")

categoria = input("Indica la categoría de la que quieres información (p.ej: generacion) ")
print("---------------------------------------------------------------")

widget = input("Indica el widget del que quieres información (p.ej: evolucion-renovable-no-renovable) ")
print("---------------------------------------------------------------")

año_inicio = int(input("Indica el primer año del que quieres información "))
print("---------------------------------------------------------------")

año_fin = int(input("Indica el último año de que quieres información "))
print("---------------------------------------------------------------")

time_trunc = input("Indica el nivel de detalle deseado entre los siguientes: hour/day/month/year ")
print("---------------------------------------------------------------")

# incluimos también los inputs que necesitaremos para la clase Bbdd_sql para no tener que pregunta más adelante

nombre_bbdd = input("¿Cómo quieres llamar a la base de datos? ")
print("---------------------------------------------------------------")

contraseña = input("Indica la contraseña para conectarte a la base de datos ")
print("---------------------------------------------------------------")



import soporte as sp 


# iniciamos la clase Extraccion_limpieza
api = sp.Extraccion_limpieza(idioma, categoria, widget)

# utilizamos el metodo de "llamada API" para obtener los datos de la API
print(f"Estamos haciendo la llamada a la API para obtener los datos a nivel nacional de la categoría {categoria}, widget {widget}")
df_nacional = api.extraccion_nacional(año_inicio, año_fin, time_trunc)

print("-----------------------------------------")

print(f"Estamos haciendo la llamada a la API para obtener los datos por comunidad autónoma de la categoría {categoria}, widget {widget}")
df_ccaa = api.extraccion_ccaa(año_inicio, año_fin, time_trunc)


# el siguiente paso es limpiar los datos de los dataframes
print("-----------------------------------------")
print(f"Estamos limpiando los datos a nivel nacional")
df_nacional = api.limpieza(df_nacional, f'nacional_{widget}')
print(df_nacional)
print("-----------------------------------------")


print("-----------------------------------------")
print(f"Estamos limpiando los datos a nivel de comunidad autónoma")
df_ccaa = api.limpieza(df_ccaa, f'ccaa_{widget}')
print(df_ccaa)
print("-----------------------------------------")



# iniciamos la clase Bbdd_sql
carga = sp.Bbdd_sql(nombre_bbdd, contraseña)

# usamos el método para crear la base de datos
carga.crear_bbdd()

# usamos el método para crear la base de datos
carga.crear_insertar_tabla(sp.tabla_fechas)

carga.crear_insertar_tabla(sp.tabla_comunidades)

carga.crear_insertar_tabla(sp.tabla_nacional_renov_no_renov)

carga.crear_insertar_tabla(sp.tabla_comun_renov_no_renov)

# proceso finalizado
print("El proceso ya ha termiando, tienes todos tus datos almacenados en tu ordenador")