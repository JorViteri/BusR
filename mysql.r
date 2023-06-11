library(RMySQL)
library(dplyr)
library(data.table)

#Script para cargar los datos en MySQL

setwd('$$$$') #PONER AQUI EL DIRECTORIO CORRESPONDIENTE
#Se construye el objeto driver
driver <- MySQL()

#Parametros conexión
host <- "127.0.0.1"
port <- 3306
usr<-"$$$$" #PONER EL USUARIO
pass<-"$$$$" #PONER LA CONTRASEÑA DE CADA UNO 
database <- "pfm_buses" #PONER EL NOMBRE DE LA BD

#Cargamos los datos
routes_df <- fread("routes.csv")
anual_freq_df <- fread("anual_freq.csv")
busstops_df <- fread("busstops.csv")
municipalities_df <- fread("concellos.csv")
contratos_df <- fread("contratos.csv")
expeditions_df <- fread("expeditions.csv")
lines_df <- fread("lines_grouped.csv")
operators_df <- fread("operators_complete.csv")
services_df <- fread("services_new_id.csv")
week_freq_df <- fread("week_freq.csv")

#Se incia la conexion
mydb <- dbConnect(driver, user=usr, password=pass,
                  dbname=database, host=host, port=port)

#Para que se puedan meter datos en el SQL hay que ejecutar en la BD:
#SET GLOBAL local_infile=1;

#Crea tabla y mete datos 
dbWriteTable(mydb, "Routes", routes_df, append=TRUE, row.names=FALSE)
dbWriteTable(mydb, "Anual_freq", anual_freq_df, append=TRUE, row.names=FALSE)
dbWriteTable(mydb, "Bus_stops", busstops_df, append=TRUE, row.names=FALSE)
dbWriteTable(mydb, "Municipalities", municipalities_df, append=TRUE, row.names=FALSE)
dbWriteTable(mydb, "Contracts", contratos_df, append=TRUE, row.names=FALSE)
dbWriteTable(mydb, "Expeditions", expeditions_df, append=TRUE, row.names=FALSE)
dbWriteTable(mydb, "Bus_lines", lines_df, append=TRUE, row.names=FALSE)
dbWriteTable(mydb, "Operators", operators_df, append=TRUE, row.names=FALSE)
dbWriteTable(mydb, "Services", services_df, append=TRUE, row.names=FALSE)
dbWriteTable(mydb, "Week_freq", week_freq_df, append=TRUE, row.names=FALSE)

#Cerrar conexion
dbDisconnect(mydb)
