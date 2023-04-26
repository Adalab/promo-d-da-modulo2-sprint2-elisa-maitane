import requests
import pandas as pd
import numpy as np
import os
import mysql.connector
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"

if 'etl' not in os.getcwd():
    os.chdir('etl')


# creamos la clase de extracción y limpieza
class Extraccion_limpieza():

    def __init__(self, lang, category, widget):
        self.lang = lang
        self.category = category
        self.widget = widget


    def extraccion_nacional(self, start_year, end_year, time_trunc):
        self.start_year = start_year
        self.end_year = end_year
        self.time_trunc = time_trunc

        df_vacio = pd.DataFrame()

        for año in range(start_year, end_year+1):
            url = f'https://apidatos.ree.es/{self.lang}/datos/{self.category}/{self.widget}?start_date={año}-01-01T00:00&end_date={año}-12-31T23:59&time_trunc={time_trunc}'
            response = requests.get(url=url)
                       
            datos_electricidad = response.json()
            df_ren = pd.DataFrame(datos_electricidad['included'][0]['attributes']['values'])
            df_ren['type'] = datos_electricidad['included'][0]['type']
            df_no_ren = pd.DataFrame(datos_electricidad['included'][1]['attributes']['values'])
            df_no_ren['type'] = datos_electricidad['included'][1]['type']
            df_vacio = pd.concat([df_vacio, df_ren, df_no_ren], axis=0)

        df_vacio.to_csv(f'../datos/{self.widget}_nacional.csv')

        return df_vacio


    def extraccion_ccaa(self, start_year, end_year, time_trunc):
        self.start_year = start_year
        self.end_year = end_year
        self.time_trunc = time_trunc    
    
        cod_comunidades = {'Ceuta': 8744,
                        'Melilla': 8745,
                        'Andalucía': 4,
                        'Aragón': 5,
                        'Cantabria': 6,
                        'Castilla - La Mancha': 7,
                        'Castilla y León': 8,
                        'Cataluña': 9,
                        'País Vasco': 10,
                        'Principado de Asturias': 11,
                        'Comunidad de Madrid': 13,
                        'Comunidad Foral de Navarra': 14,
                        'Comunitat Valenciana': 15,
                        'Extremadura': 16,
                        'Galicia': 17,
                        'Illes Balears': 8743,
                        'Canarias': 8742,
                        'Región de Murcia': 21,
                        'La Rioja': 20}
        df_vacio = pd.DataFrame()

        for ccaa, geo_id in cod_comunidades.items():

            for año in range(start_year, end_year+1):
                url = f'https://apidatos.ree.es/{self.lang}/datos/{self.category}/{self.widget}?start_date={año}-01-01T00:00&end_date={año}-12-31T23:59&time_trunc={time_trunc}&geo_trunc=electric_system&geo_limit=ccaa&geo_ids={geo_id}'
                response = requests.get(url=url)
                
                datos_electricidad = response.json()
                try:
                    df_ren = pd.DataFrame(datos_electricidad['included'][0]['attributes']['values'])
                    df_ren['type'] = datos_electricidad['included'][0]['type']
                    df_ren['ccaa'] = ccaa
                    df_vacio = pd.concat([df_vacio, df_ren], axis=0)
                
                    df_no_ren = pd.DataFrame(datos_electricidad['included'][1]['attributes']['values'])
                    df_no_ren['type'] = datos_electricidad['included'][1]['type']
                    df_no_ren['ccaa'] = ccaa
                    df_vacio = pd.concat([df_vacio, df_no_ren], axis=0)
                except:
                    pass

        df_vacio.to_csv(f'../datos/{self.widget}_ccaa.csv')

        return df_vacio
    

    def limpieza(self, df, nombre_archivo, columnas_eliminar = []):
        self.df = df
        self.columnas_eliminar = columnas_eliminar

        df.drop(columns=columnas_eliminar, inplace=True)
        df[['percentage', 'value']] = df[['percentage', 'value']].apply(lambda dato: round(dato, 2))
        df['date'] = df['datetime'].str.split('T', expand=True).get(0)
        df['date'] = pd.to_datetime(df['date'])
        df.to_pickle(f'../datos/{nombre_archivo}.pkl')
        return df
    



# creamos la clase de creación de BBDD y tablas de SQL
class Bbdd_sql:
    
    def __init__(self, nombre_bbdd, contraseña):
        self.nombre_bbdd = nombre_bbdd
        self.contraseña = contraseña
    
    def crear_bbdd(self):
        
        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password=f'{self.contraseña}')
        
        print("Conexión realizada con éxito")
        
        mycursor = mydb.cursor()

        try:
            mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.nombre_bbdd};")
            print(mycursor)
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
            
        
    def crear_insertar_tabla(self, query):
        self.query = query
        
        cnx = mysql.connector.connect(user='root', password=f"{self.contraseña}",
                                        host='127.0.0.1', database=f"{self.nombre_bbdd}")
        
        mycursor = cnx.cursor()
        
        
        try: 
            mycursor.execute(query)
            cnx.commit() 
        
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)



# creammos las variables para las querys que usaremos con el método .crear_insertar_tabla() de la clase Bbdd_sql:
tabla_fechas = """CREATE TABLE IF NOT EXISTS `energia`.`fecha` (
  `idfecha` INT NOT NULL AUTO_INCREMENT,
  `fecha` DATE NOT NULL,
  PRIMARY KEY (`idfecha`))
ENGINE = InnoDB;
"""

tabla_nacional_renov_no_renov = """CREATE TABLE IF NOT EXISTS `energia`.`nacional_renovable_no_renovable` (
  `id_nacional_renovable_no_renovable` INT NOT NULL AUTO_INCREMENT,
  `porcentaje` INT NOT NULL,
  `tipo_energia` VARCHAR(45) NOT NULL,
  `valor` DECIMAL NOT NULL,
  `fecha_idfecha` INT NOT NULL,
  PRIMARY KEY (`id_nacional_renovable_no_renovable`),
  INDEX `fk_nacional_renovable_no_renovable_fecha_idx` (`fecha_idfecha` ASC) VISIBLE,
  CONSTRAINT `fk_nacional_renovable_no_renovable_fecha`
    FOREIGN KEY (`fecha_idfecha`)
    REFERENCES `energia`.`fecha` (`idfecha`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;
"""

tabla_comunidades = """CREATE TABLE IF NOT EXISTS `energia`.`comunidades` (
  `idcomunidades` INT NOT NULL AUTO_INCREMENT,
  `comunidad` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`idcomunidades`))
ENGINE = InnoDB;
"""

tabla_comun_renov_no_renov = """CREATE TABLE IF NOT EXISTS `energia`.`comunidades_renovable_no_renovable` (
  `idcomunidad_renovable_no_renovable` INT NOT NULL AUTO_INCREMENT,
  `porcentaje` INT NOT NULL,
  `tipo_energia` VARCHAR(45) NOT NULL,
  `valor` DECIMAL NOT NULL,
  `fecha_idfecha` INT NOT NULL,
  `comunidades_idcomunidades` INT NOT NULL,
  PRIMARY KEY (`idcomunidad_renovable_no_renovable`),
  INDEX `fk_comunidades_renovable_no_renovable_fecha1_idx` (`fecha_idfecha` ASC) VISIBLE,
  INDEX `fk_comunidades_renovable_no_renovable_comunidades1_idx` (`comunidades_idcomunidades` ASC) VISIBLE,
  CONSTRAINT `fk_comunidades_renovable_no_renovable_fecha1`
    FOREIGN KEY (`fecha_idfecha`)
    REFERENCES `energia`.`fecha` (`idfecha`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_comunidades_renovable_no_renovable_comunidades1`
    FOREIGN KEY (`comunidades_idcomunidades`)
    REFERENCES `energia`.`comunidades` (`idcomunidades`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
    ENGINE = InnoDB;
    """