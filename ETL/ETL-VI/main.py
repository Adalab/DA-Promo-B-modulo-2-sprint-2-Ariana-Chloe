from IPython.core.interactiveshell import InteractiveShell 
InteractiveShell.ast_node_interactivity = "all"

import requests
import pandas as pd
import numpy as np
from datetime import datetime
import mysql.connector
import soporte as sp


# Definimos nuestro diccionario y creamos una instancia de nuestra clase
diccionario = {'usa':[39.7837304, -100.445882], 
               'australia':[-24.7761086, 134.755], 
               'south africa':[-28.8166236, 24.991639], 
               'new zealand':[-41.5000831, 172.8344077], 
               'papua new guinea':[-5.6816069, 144.2489081]}

api = sp.Extraccion_paises(diccionario)

# Extraemos nuestro df_clima
df_clima = api.extracciones_paises()

# Desempaquetamos rh
df_limpio_rh = api.desempaquetar_rh(df_clima)

# Mergeo 1
df_clima2 = api.merge_clima_rh(df_clima, df_limpio_rh)

# Desempaquetamos wind
df_limpio_wind= api.desempaquetar_wind(df_clima)

# Mergeo 2
df_clima3 = api.merge_clima_wind(df_clima2, df_limpio_wind)

# Cargaremos internamente el df attacks desde el csv de la carpeta input y mergeamos con df clima
df_attacks_climas_paises = api.merge_attacks_paises_clima(df_clima3)

# Guardamos nuestros datos extraídos en la carpeta output
api.guardar_pkl_csv(df_attacks_climas_paises)

# Vamos a cargar en nuestra BBDD los datos limpios. 
carga = sp.Cargar("tiburones", "AlumnaAdalab")

carga.crear_bbdd()

tabla_attacks = ''' 
    CREATE TABLE IF NOT EXISTS `attacks` (
    `id_attacks` INT NOT NULL AUTO_INCREMENT,
    `id_climate` INT NOT NULL,
    `year` INT DEFAULT NULL,
    `type` VARCHAR(50) DEFAULT NULL, 
    `country` VARCHAR(50) DEFAULT NULL,  
    `age` FLOAT DEFAULT NULL,
    `species` VARCHAR(50) DEFAULT NULL,
    `date` VARCHAR (10) DEFAULT NULL,
    `fatal` CHAR (1) DEFAULT NULL,
    `sex` CHAR (1) DEFAULT NULL,
    PRIMARY KEY (`id_attacks`),
    INDEX `fk_attacks_climate` (`id_climate` ASC),
    CONSTRAINT `fk_attacks_climate`
    FOREIGN KEY (`id_climate`)
    REFERENCES `tiburones`.`climate` (`id_climate`)
    )
    
    ENGINE = InnoDB;
    '''

carga.crear_insertar_tabla(tabla_attacks)

tabla_climate = '''
       CREATE TABLE IF NOT EXISTS `climate` (
       `id_climate` INT NOT NULL AUTO_INCREMENT,
       `timepoint` FLOAT DEFAULT NULL, 
       `cloudcover` FLOAT DEFAULT NULL, 
       `highcloud` FLOAT DEFAULT NULL, 
       `midcloud` FLOAT DEFAULT NULL, 
       `lowcloud` FLOAT DEFAULT NULL,
       `temp2m` FLOAT DEFAULT NULL, 
       `lifted_index` FLOAT DEFAULT NULL, 
       `rh2m` FLOAT DEFAULT NULL, 
       `msl_pressure` FLOAT DEFAULT NULL, 
       `prec_amount` FLOAT DEFAULT NULL,
       `snow_depth` FLOAT DEFAULT NULL, 
       `wind_speed` FLOAT DEFAULT NULL, 
       `latitud` FLOAT DEFAULT NULL, 
       `longitud` FLOAT DEFAULT NULL, 
       `rh_layer_950mb` FLOAT DEFAULT NULL,
       `rh_layer_900mb` FLOAT DEFAULT NULL, 
       `rh_layer_850mb` FLOAT DEFAULT NULL, 
       `rh_layer_800mb` FLOAT DEFAULT NULL, 
       `rh_layer_750mb` FLOAT DEFAULT NULL,
       `rh_layer_700mb` FLOAT DEFAULT NULL, 
       `rh_layer_650mb` FLOAT DEFAULT NULL, 
       `rh_layer_600mb` FLOAT DEFAULT NULL, 
       `rh_layer_550mb` FLOAT DEFAULT NULL,
       `rh_layer_500mb` FLOAT DEFAULT NULL,
       `rh_layer_450mb` FLOAT DEFAULT NULL, 
       `rh_layer_400mb` FLOAT DEFAULT NULL, 
       `rh_layer_350mb` FLOAT DEFAULT NULL,
       `rh_layer_300mb` FLOAT DEFAULT NULL,
       `rh_layer_250mb` FLOAT DEFAULT NULL, 
       `rh_layer_200mb` FLOAT DEFAULT NULL,
       `layer950mb_direction` FLOAT DEFAULT NULL, 
       `layer950mb_speed` FLOAT DEFAULT NULL, 
       `layer900mb_direction` FLOAT DEFAULT NULL,
       `layer900mb_speed` FLOAT DEFAULT NULL, 
       `layer850mb_direction` FLOAT DEFAULT NULL, 
       `layer850mb_speed` FLOAT DEFAULT NULL,
       `layer800mb_direction` FLOAT DEFAULT NULL, 
       `layer800mb_speed` FLOAT DEFAULT NULL, 
       `layer750mb_direction` FLOAT DEFAULT NULL,
       `layer750mb_speed` FLOAT DEFAULT NULL, 
       `layer700mb_direction` FLOAT DEFAULT NULL, 
       `layer700mb_speed` FLOAT DEFAULT NULL,
       `layer650mb_direction` FLOAT DEFAULT NULL, 
       `layer650mb_speed` FLOAT DEFAULT NULL,
       `layer600mb_direction` FLOAT DEFAULT NULL,
       `layer600mb_speed` FLOAT DEFAULT NULL,
       `layer550mb_direction` FLOAT DEFAULT NULL, 
       `layer550mb_speed` FLOAT DEFAULT NULL,
       `layer500mb_direction` FLOAT DEFAULT NULL,
       `layer500mb_speed` FLOAT DEFAULT NULL, 
       `layer450mb_direction` FLOAT DEFAULT NULL,
       `layer450mb_speed` FLOAT DEFAULT NULL,
       `layer400mb_direction` FLOAT DEFAULT NULL,
       `layer400mb_speed` FLOAT DEFAULT NULL,
       `layer350mb_direction` FLOAT DEFAULT NULL,
       `layer350mb_speed` FLOAT DEFAULT NULL,
       `layer300mb_direction` FLOAT DEFAULT NULL,
       `layer300mb_speed` FLOAT DEFAULT NULL,
       `layer250mb_direction` FLOAT DEFAULT NULL,
       `layer250mb_speed` FLOAT DEFAULT NULL,
       `layer200mb_direction` FLOAT DEFAULT NULL,
       `layer200mb_speed` FLOAT DEFAULT NULL,
       PRIMARY KEY (`id_climate`))
    
    ENGINE = InnoDB;
       '''

carga.crear_insertar_tabla(tabla_climate)
