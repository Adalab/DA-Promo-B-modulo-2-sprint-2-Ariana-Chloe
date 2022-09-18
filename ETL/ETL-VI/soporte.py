from IPython.core.interactiveshell import InteractiveShell 
InteractiveShell.ast_node_interactivity = "all"

import requests
import pandas as pd
import numpy as np
from datetime import datetime
import mysql.connector


class Extraccion_paises:
    def __init__(self, diccionario_paises):
        self.diccionario_paises = diccionario_paises
    
    def extracciones_paises(self):
        # Creamos un df vacío
        df_clima= pd.DataFrame()
        
        for key, value in self.diccionario_paises.items():
            # Extraemos la latitud y la longitud
            lat = value[0]
            lon = value[1]
            pais = key
            producto = 'meteo'

            url = f'http://www.7timer.info/bin/api.pl?lon=-{lon}&lat={lat}&product={producto}&output=json'
            # Llamamos a la Api
            response = requests.get(url)
        
            df = pd.json_normalize(response.json()['dataseries'])
            df['pais'] = key
            df['latitud'] = lat
            df['longitud'] = lon
            # Concatenamos cada df extraído al df vacío inicial
            df_clima = pd.concat([df_clima, df], axis = 0, ignore_index=True)

            if response.status_code == 200:
                print('La peticion se ha realizado correctamente, se ha devuelto el código de estado:',response.status_code,' y como razón del código de estado: ',response.reason)
            elif response.status_code == 402:
                print('No se ha podido autorizar usario, se ha devuelto el código de estado:', response.status_code,' y como razón del código de estado: ',response.reason)
            elif response.status_code == 404:
                print('Algo ha salido mal, el recurso no se ha encontrado,se ha devuelto el código de estado:', response.status_code,' y como razón del código de estado: ',response.reason)
            else:
                print('Algo inesperado ha ocurrido, se ha devuelto el código de estado:', response.status_code,' y como razón del código de estado: ',response.reason)
            
        return df_clima
    
    def desempaquetar_rh(self, df_clima):
        self.df_clima = df_clima

        df_rh = df_clima['rh_profile'].apply(pd.Series)
        df_limpio_rh = pd.DataFrame()

        for col in range(len(df_rh.columns)):
            # Volvemos a aplicar pd.Series a cada fila, ya que convertirá los keys en columnas y los values en filas 
            # y es una de las maneras posibles para extraer su contenido
            desempaque_dicc = df_rh[col].apply(pd.Series)
            
            layer = desempaque_dicc.iloc[0].index[0]
            num = desempaque_dicc.iloc[0].values[0]
            #Construimos el nombre de la columna
            nom = 'rh_'+ layer + '_' + num

            # Creamos una lista que será el contenido de cada columna creada en este loop   
            lista = []
            for fila in range(len(desempaque_dicc.index)):
                
                value = desempaque_dicc.iloc[fila].values[1]
                lista.append(value)

            # Terminamos el loop rellenando nuestro df vacío con el contenido extraído
            df_limpio_rh[nom] = lista
            
        return df_limpio_rh

    def merge_clima_rh(self, df_clima, df_limpio_rh):
        self.df_clima = df_clima
        self.df_limpio_rh = df_limpio_rh
        
        # Queremos sacar los indices en ambos df
        df_clima.reset_index(inplace=True)
        df_limpio_rh.reset_index(inplace=True)

        # Mergeamos por los indices
        df_clima2 = df_clima.merge(df_limpio_rh, on= ['index'])

        # Eliminamos la columna rh_profile
        df_clima2.drop(['rh_profile'], axis=1, inplace=True)
        
        return df_clima2
        
    def desempaquetar_wind(self, df_clima):
        self.df_clima = df_clima

        df_wind = df_clima['wind_profile'].apply(pd.Series)
        df_limpio_wind = pd.DataFrame()

        for col in range(len(df_wind.columns)):
            # Volvemos a aplicar pd.Series a cada fila, ya que convertirá los keys en columnas y los values en filas 
            desempaq_dicc = df_wind[col].apply(pd.Series)
            
            layer = desempaq_dicc.iloc[0,:].index[0]
            num = desempaq_dicc.iloc[0,:].values[0]
            direction = desempaq_dicc.iloc[0,:].index[1]
            speed = desempaq_dicc.iloc[0,:].index[2]
            
            #Construimos el nombre de las columnas
            nom1 = layer + num + '_' + direction
            nom2 = layer + num + '_' + speed

            # Creamos una lista que será el contenido de cada columna creada en este loop   
            lista1 = []
            lista2 = []
            for fila in range(len(desempaq_dicc.index)):

                value_dir = desempaq_dicc.iloc[fila,:].values[1]
                value_sp= desempaq_dicc.iloc[fila,:].values[2]
                
                lista1.append(value_dir)
                lista2.append(value_sp)

            # Terminamos el loop rellenando nuestro df vacío con el contenido extraído
            df_limpio_wind[nom1] = lista1
            df_limpio_wind[nom2] = lista2
        
        return df_limpio_wind

    def merge_clima_wind(self, df_clima, df_limpio_wind):
        self.df_clima = df_clima
        self.df_limpio_wind = df_limpio_wind

        # Queremos sacar los indices en ambos df
        df_limpio_wind.reset_index(inplace=True)
        # Añadimos este paso para que las funciones sean intercambiables de orden y no nos falte o sobre una columna 'index'
        if 'index' not in df_clima.columns:
            df_clima.reset_index(inplace=True)
        
        # Mergeamos por los indices
        df_clima3 = df_clima.merge(df_limpio_wind, on= ['index'])

        # Eliminamos la columna rh_profile
        df_clima3.drop(['index','wind_profile'], axis=1, inplace=True)
        
        return df_clima3


    def merge_attacks_paises_clima(self, df_clima_total):
        self.df_clima_total = df_clima_total

        df_group = df_clima_total.groupby('pais').mean()

        #Ceamos el df de attacks por los paises elegidos
        df_attacks = pd.read_csv('datos/input/attacks_limpieza_completa.csv', index_col = 0)

        df_attacks_limpio = df_attacks[['year', 'type', 'country', 'age', 'species_', 'fecha_limpia', 'fatal',
       'sex']]

        lista_paises = list(df_clima_total['pais'].unique())
        
        df_attacks_paises = df_attacks_limpio[df_attacks_limpio['country'].isin(lista_paises)]

        df_attacks_clima_paises = df_attacks_paises.merge(df_group, left_on=['country'] , right_on=['pais'])
        
        return df_attacks_clima_paises

    def guardar_pkl_csv(self, df_attacks_clima_paises):
        self.df_attacks_clima_paises = df_attacks_clima_paises

        # Añadimos la fecha de hoy para que las siguienes extracciones no se sobreescriban
        fecha_hoy = datetime.now()
        fecha_hoy = datetime.strftime(fecha_hoy, '%Y-%m-%d')
        df_attacks_clima_paises.to_csv(f'datos/output/attacks_clima_paises_{fecha_hoy}.csv')
        df_attacks_clima_paises.to_pickle(f'datos/output/attacks_clima_paises_{fecha_hoy}.pkl')




class Cargar:

    def __init__(self, nombre_bbdd, contraseña):
         
        self.nombre_bbdd = nombre_bbdd
        self.contraseña = contraseña

    # Llamamos nuestra función para crear la BBDD 
    def crear_bbdd(self):

        mydb = mysql.connector.connect(host="localhost",
                                       user="root",
                                       password=f'{self.contraseña}')
                                       
        mycursor = mydb.cursor()
        print("Conexión realizada con éxito")

        try:
            mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.nombre_bbdd};")
            
        except:
            print("La BBDD ya existe")
            
    # Llamamos nuestra función para crear tablas e insertar datos en ellas   
    def crear_insertar_tabla(self, query):
        
        mydb = mysql.connector.connect(host="localhost",
                                       user="root",
                                       password=f'{self.contraseña}', 
                                       database=f"{self.nombre_bbdd}") 
        mycursor = mydb.cursor()
        
        try:
            mycursor.execute(query)
            mydb.commit()
          
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
