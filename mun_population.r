library(httr)
library(jsonlite)
library(dplyr)
library(stringr)
library(data.table)

setwd('D:\\IFFE\\TFM\\csv\\') 

#Se cargan los df necesarios 
municipalities_df <- fread("concellos.csv")
pob_2022_df <- fread("2022.csv")

#Ponemos a 0 las entradas que no tienen registrada población alguna
#y transformamos los valores a numericos

pob_2022_df$population [pob_2022_df$population=='-'] <- '0'
pob_2022_df <- pob_2022_df %>%
               mutate_at('population', as.numeric)
               
#Comprobamos si hay concellos del servicio de buses no presentes en el de poblacion
errors <- anti_join(municipalities_df,pob_2022_df,by='id')

#Son 12, los cuales se corresponde a áreas fuera de Galicia, y uno llamado CARBALLO2 que
#parece ser una parada de Lugo que está contabilizada por el servicio de buses como un concello
#No son muchas entradas a rellenar y son de diversas comunidades, por lo que se busca la población del 2022 para cada caso y se rellena
#menos para CARBALLO2, que se pone a 0

#Hacemos el merge de los dos df
mun_pob_df <- merge(municipalities_df,pob_2022_df,by='id', all=TRUE) %>%
              select(-c(municipality.y))%>%
              rename(municipality=municipality.x)

mun_pob_df$population [mun_pob_df$municipality=='CARBALLO2'] <- 0
mun_pob_df$population [mun_pob_df$municipality=='HERMISENDE'] <- 248
mun_pob_df$population [mun_pob_df$municipality=='LEÓN'] <- 120951
mun_pob_df$population [mun_pob_df$municipality=='MADRID'] <- 3286662
mun_pob_df$population [mun_pob_df$municipality=='PONFERRADA'] <- 63001
mun_pob_df$population [mun_pob_df$municipality=='PORTUGALETE'] <- 44800
mun_pob_df$population [mun_pob_df$municipality=='PUENTE_DE_DOMINGO_FLÓREZ'] <- 1421
mun_pob_df$population [mun_pob_df$municipality=='SAN_TIRSO_DE_ABRES'] <- 408
mun_pob_df$population [mun_pob_df$municipality=='VEGADEO'] <- 3895
mun_pob_df$population [mun_pob_df$municipality=='VILLADECANES'] <- 209
mun_pob_df$population [mun_pob_df$municipality=='VILLAFRANCA_DEL_BIERZO'] <- 2756

#Guardamos el nuevo df
write.csv(mun_pob_df, "D:\\IFFE\\TFM\\csv\\mun_pob.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

