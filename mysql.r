library(RMySQL)
library(dplyr)
library(data.table)

setwd('$$$$$') #PONER AQUI EL DIRECTORIO CORRESPONDIENTE
#Se construye el objeto driver
driver <- MySQL()

#Parametros conexión
host <- "127.0.0.1"
port <- 3306
usr<-"$$$$" #PONER EL USUARIO
pass<-"$$$$" #PONER LA CONTRASEÑA DE CADA UNO 
database <- "$$$$" #PONER EL NOMBRE DE LA BD

#Se incia la conexion
mydb <- dbConnect(driver, user = usr, password = pass,
                  dbname=database, host=host,port=port)

#Cargamos los datos
routes_df <- fread("$$$$") #PONER AQUI EL DIRECTORIO CORRESPONDIENTE AL ARCHIVO CSV

#Para que se puedan meter datos en el SQL hay que ejecutar en la BD:
#SET GLOBAL local_infile=1;

#Crea tabla y mete datos 
dbWriteTable(mydb, "Routes", routes_df, append = TRUE, row.names = FALSE)

#Cerrar conexion
dbDisconnect(mydb)
