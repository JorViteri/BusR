library(httr)
library(jsonlite)
library(dplyr)
library(data.table)
library(stringi)

#Con este scipt se cargan los datos del csv de las tarifas y se asocian a los ids de concellos.
setwd('D:\\IFFE\\TFM\\csv\\') 

#Se cargan los df necesarios 
fares_df <- fread("fares.csv")
municipalities_df <- fread("concellos.csv")

#Se ponen en mayusculas los textos del df de tarifas
fares_df$AYUNTAMIENTO_ORIGEN <- toupper(fares_df$AYUNTAMIENTO_ORIGEN)
fares_df$AYUNTAMIENTO_DESTINO <- toupper(fares_df$AYUNTAMIENTO_DESTINO)



#Se eliminan las tildes de los textos del df de tarifas
fares_df$AYUNTAMIENTO_ORIGEN <- stri_replace_all_regex(fares_df$AYUNTAMIENTO_ORIGEN,
                                pattern=c('á','é','í','ó','ú','Á','É','Í','Ó','Ú'),
                                replacement=c('a','e','i','o','u','A','E','I','O','U'),
                                vectorize=FALSE)

fares_df$AYUNTAMIENTO_DESTINO <- stri_replace_all_regex(fares_df$AYUNTAMIENTO_DESTINO,
                                  pattern=c('á','é','í','ó','ú','Á','É','Í','Ó','Ú'),
                                  replacement=c('a','e','i','o','u','A','E','I','O','U'),
                                  vectorize=FALSE)


#Ahora toca hacer el merge en base a substr
#Primero obtenemos los unicos de origen y destino
origin_df = data.frame(municipality=unique(fares_df$AYUNTAMIENTO_ORIGEN))
destination_df = data.frame(municipality=unique(fares_df$AYUNTAMIENTO_DESTINO))

#y comprobamos que sean identicos
identical(origin_df,destination_df)
#Son identicos, por lo que con uno nos deberia llegar

#Aqui tenemos los que coinciden en texto con nuestro df
equals_df <- merge(origin_df,municipalities_df,by="municipality")

#y ahora obtenemos los concellos que no coinciden
missing_df<-anti_join(origin_df,municipalities_df,by="municipality")


#En el municipalidades hay que quitar tildes
municipalities_df$municipality <- stri_replace_all_regex(municipalities_df$municipality,
                                                         pattern=c('á','é','í','ó','ú','Á','É','Í','Ó','Ú'),
                                                         replacement=c('a','e','i','o','u','A','E','I','O','U'),
                                                         vectorize=FALSE)
#En el missing poner guiones bajos en lugar de altos
missing_df$municipality<- stri_replace_all_regex(missing_df$municipality,
                                                 pattern=c('-'),
                                                 replacement=c('_'),
                                                 vectorize=FALSE)

#Cambiar "SAN_CRISTOVO_DE_CEA" a "SAN_CRISTOBO_DE_CEA" en el missing_df
missing_df$municipality[missing_df$municipality == "SAN_CRISTOVO_DE_CEA"] <-"SAN_CRISTOBO_DE_CEA"

#Ahora generamos el df para el bucle
last_df <- data.frame()
#Por cada concello que falta
for(i in 1:nrow(missing_df)){
  #Iteramos los que ya tenemos
  for(j in 1:nrow(municipalities_df)){
    #Miramos si el que teniamos es sub cadena del uno y viceversa
    comp1<- grepl(municipalities_df$municipality[j],missing_df$municipality[i], fixed=TRUE)
    comp2<-grepl(missing_df$municipality[i],municipalities_df$municipality[j], fixed=TRUE)
    
    #Si se cumple cualquiera de los casos
    if(comp1 || comp2){
      #Obtenemos el id del concello
      id <-c(municipalities_df$id[j])
      #Luego el nombre
      municipality <- c(missing_df$municipality[i])
      #Y se añade la entrada al df resultante
      last_df<-bind_rows(last_df,data.frame(id,municipality))
      
    }
  }
}

#Con last_df y equals_df puedo filtar aquellos ids que ya tenia en equals_df, 
#quedandome solo con los nuevos
last_df<-anti_join(last_df,equals_df,by="id")

#Ahora ya los podemos concatenar
fares_mun_df <- unique(bind_rows(equals_df,last_df)) %>% na.omit()

#preparamos el origin_df para hacer comparaciones cambianso los guiones altos por bajos
origin_df$municipality<- stri_replace_all_regex(origin_df$municipality,
                                                pattern=c('-'),
                                                replacement=c('_'),
                                                vectorize=FALSE)
#Y modificando del concello previo
origin_df$municipality[origin_df$municipality == "SAN_CRISTOVO_DE_CEA"] <-"SAN_CRISTOBO_DE_CEA"

error <- anti_join(origin_df,fares_mun_df,by="municipality")
#Hay 0 entradas

error <- anti_join(fares_mun_df,origin_df,by="municipality")
#Hay 0 entradas, está todo correcto

#Se prepara el df para su merge, cambiando el nombre de la columna
fares_mun_df<- fares_mun_df %>%
  rename(AYUNTAMIENTO_ORIGEN=municipality)

#Hacemos el join en base al nombre de concello con el df de las tarifas
fares_df1<- fares_df %>%
  merge(fares_mun_df,by="AYUNTAMIENTO_ORIGEN")

#Lo mismo, pero preparando la union en base a los destinos
fares_mun_df<- fares_mun_df %>%
  rename(AYUNTAMIENTO_DESTINO=AYUNTAMIENTO_ORIGEN)

#Se cambian los nombres para diferenciar los ids
fares_df2<- fares_df1 %>%
  rename(id_origin=id)

#Se hace la union, se modifica el nombre del nuevo id y se eliminan los columnas
#de los nombres, no nos son necesarias
fares_df2<- fares_df2 %>% 
  merge(fares_mun_df,by="AYUNTAMIENTO_DESTINO") %>%
  rename(id_destination=id) %>%
  select(-c("AYUNTAMIENTO_ORIGEN","AYUNTAMIENTO_DESTINO"))

#Escribimos el nuevo df en un csv
write.csv(fares_df2, "D:\\IFFE\\TFM\\csv\\fares_df.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

#Debido a la complejidad de los nombres de las columnas se han cambiando directamente
#en un editor de texto
#Vamos a cargar para cambiar las columnas de lugar y añadir ids
new_fares_df <- fread("D:\\IFFE\\TFM\\csv\\fares_new_names.csv")

#Se acopla la columna de ids de fila y se reorganizan las columnas
new_fares_df <- new_fares_df %>%
  mutate(id = row_number()) %>%
  select(c("id","id_origin","id_destination","journey","bonus"
           ,"fee_in_cash","fee_tmg_until40",
           "fee_tmg_until40with_discount",
           "fee_tmg_more40","fee_tmg_more40with_discount"))


#Convertir las columnas de precios en numericas
new_fares_df$fee_in_cash <- 
  as.numeric(gsub(",",".",gsub("€", "",new_fares_df$fee_in_cash )))

new_fares_df$fee_tmg_until40 <- 
  as.numeric(gsub(",",".",gsub("€", "",new_fares_df$fe<e_tmg_until40)))

new_fares_df$fee_tmg_until40with_discount <- 
  as.numeric(gsub(",",".",gsub("€", "",new_fares_df$fee_tmg_until40with_discount)))

new_fares_df$fee_tmg_more40 <- 
  as.numeric(gsub(",",".",gsub("€", "",new_fares_df$fee_tmg_more40)))

new_fares_df$fee_tmg_more40with_discount <- 
  as.numeric(gsub(",",".",gsub("€", "",new_fares_df$fee_tmg_more40with_discount)))                              

#Hay una entrada Vigo-Vigo que estaba sin valor y queda a NAN-> se pone igual que a A Coruña
#Asignamos a una variable los puntos en los que se cumple que el trayecto es A Coruña - A Coruña
cond= (new_fares_df$id_origin=='15030'&new_fares_df$id_destination=='15030')

#Asignamos los valores, accediendo a estos gracias a la variable cond
new_fares_df$fee_in_cash [is.na(new_fares_df$fee_in_cash)] <-
new_fares_df$fee_in_cash[cond]

new_fares_df$fee_tmg_until40 [is.na(new_fares_df$fee_tmg_until40)] <- 
new_fares_df$fee_tmg_until40[cond]

new_fares_df$fee_tmg_until40with_discount [is.na(new_fares_df$fee_tmg_until40with_discount)] <- 
new_fares_df$fee_tmg_until40with_discount[cond]

new_fares_df$fee_tmg_more40 [is.na(new_fares_df$fee_tmg_more40)] <- 
new_fares_df$fee_tmg_more40[cond]

new_fares_df$fee_tmg_more40with_discount [is.na(new_fares_df$fee_tmg_more40with_discount)] <- 
new_fares_df$fee_tmg_more40with_discount[cond]


#Se escribe el csv definitivo, con los valores de € ya numericos
write.csv(new_fares_df, "D:\\IFFE\\TFM\\csv\\fares_final.csv",
          row.names=FALSE,fileEncoding = "UTF-8")