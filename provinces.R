library(httr)
library(jsonlite)
library(dplyr)
library(stringr)
library(data.table)

setwd('D:\\IFFE\\TFM\\csv\\') 

#Se cargan los df necesarios 
provincias_df <- fread("prov.csv",encoding = 'Latin-1') %>% rename(province=`NOME PROVINCIA`)
mun_pob_df <- fread("mun_pob.csv")

#Obtenemos los valores únicos de nombres de provincia
provincias_nom_df <- data.frame(province=unique(provincias_df$province))
#Se genera un df de las provincias, cada cual con su id
provincias_nom_df <- provincias_nom_df %>%
                     mutate(id = row_number())
#Se hace merge con el df que tenía los datos de las provincias
provincias_df <- merge(provincias_df,provincias_nom_df,by="province")

#Se renombran columnas para hacer el merge con el df de los concellos
provincias_df <- provincias_df %>%
                 rename(id_province=id) %>%
                 rename(id=`CD CONCELLO`)

#Se hace el merge y nos quedamos con las columnas que nos interesan
mun_pob_prov_df <- merge(provincias_df,mun_pob_df,by='id', all=TRUE) %>%
                  select(c("id","municipality","population","id_province")) %>%
                  unique()

#Hay algunos casos que eran de fuera de Galicia, por lo que les damos un ID especial, 5, que es para ESPAÑA
tmp <- data.frame(id=5,province='Fuera Galicia')
provincias_nom_df <- bind_rows(provincias_nom_df,tmp)

mun_pob_prov_df$id_province[is.na(mun_pob_prov_df$id_province)] <- 5

#Para finalizar escribimos las dos nuevas tablas
write.csv(provincias_nom_df, "D:\\IFFE\\TFM\\csv\\provinces.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

write.csv(mun_pob_prov_df, "D:\\IFFE\\TFM\\csv\\mun_prov.csv",
          row.names=FALSE,fileEncoding = "UTF-8")
