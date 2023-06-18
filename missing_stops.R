
library(httr)
library(jsonlite)
library(dplyr)
library(stringr)
library(data.table)

#Con este script se obtienen paradas que estaban presentes en los servicios pero no en nuestro df de éstas.

#Constantes
BASE_URL='https://tpgal-ws-externos.xunta.gal/tpgal_ws/rest/'
STOPS_URL='busstops/autocomplete?text=%s&num_results=1000000'


convertir <- function(datos){
  # Esta funcion ayurdará a convertir info del API en dataframes
  data = rawToChar(datos$content)
  Encoding(data) <- 'UTF-8'
  return (fromJSON(enc2utf8(data)))
  
}


#Funcion con la que se obtiene el dataset de paradas de buses asocidas a su concello
get_busstops_mun<-function(data){
  #Inicializamos un dataframe vacío
  busstops_df<-data.frame()
  #Y una lista para los casos de error
  errors <- list(c("0-0"=TRUE))
  #Se crea un df con los valores unicos de inciales
  initials <- data.frame(municipality=unique(data$municipality))
  #Para cada posible inicial
  for(i in 1:nrow(initials)) 
  {
    #Se construye la url de la petición
    mun=initials$municipality[i]
    url = sprintf(paste(BASE_URL,STOPS_URL,sep=''),paste(mun,'_',sep=''))
    #Se ejecuta la petición
    answer <- GET(url)
    #Se comprueba que el mensaje tenga formato json
    if((validate(rawToChar(answer$content)))==FALSE){
      #En caso de error se almacena en "errors...
      errors[[sprintf('%s',mun)]]=TRUE
      #Y se avanza a la siguiente iteracion en j
      next
    }

    answer <- convertir(answer)
    #Se obtiene el df de los resultados
    answer_df <- answer$results 
    #Se comprueba que se tengan resultados
    if(length(answer_df)>0){
      
      #Cogemos las ids correspondientes a las inciales
      tmp_id<- data %>%
        filter(mun==data$municipality) %>%
        select("id")
      #Genramos un df para el conjunto de paradas que se van a sacar
      complete_df <- data.frame()
      #Iteramos para los ids de los concellos
      for(j in 1:nrow(tmp_id)){
        
        tmp_df <- answer_df %>%
          #Filtramos las entradas de tipo 'municipality'...
          filter(type!='municipality') %>%
          #... y aquellas cuyo ID se corresponda al del concello
          filter(startsWith(as.character(id), as.character(tmp_id$id[j])))
        #Si hay filas...
        if(nrow(tmp_df)>0){
          #Asociamos el id del municipio a la parada
          tmp_df$id_municipality=tmp_id$id[j]
          #Y se almacena en el df
          complete_df <- rbind(complete_df,tmp_df)
        }
      }
      
      #Si el df contiene paradas
      if(nrow(complete_df)>0){
        #Se renombran algunas columnas
        complete_df <- complete_df %>% rename("stop_name"="text","id_stop"="id_sitme")
        #Se concatena el df a busstops_df, que contendrá todas
        busstops_df<-bind_rows(busstops_df,complete_df)
      }
    } 
  }
  return(busstops_df)
}

setwd('D:\\IFFE\\TFM\\csv\\') 

#Cargamos los datos
busstops_df <- fread("busstops.csv")
services_df <- fread("services_new_id.csv")
municipalities_df <- fread("concellos.csv")

#Sacamos las paradas de origen a un df
services_origins_df <- services_df %>% 
                       select(id_origin) %>% 
                       rename(id=id_origin) %>%
                       distinct()

#Lo mismo pero con las de destino
services_destinations_df <- services_df %>% 
                             select(id_destination) %>% 
                             rename(id=id_destination) %>%
                             distinct() 
#Y las concatenamos para tener un df de las paradas de los servicios
services_stops_id_df <- rbind(services_origins_df,services_destinations_df ) %>%
                        distinct()

#Sacamos los ids de las paradas que tenemos a un propio df, para mejor manejo
busstops_id_df <- busstops_df %>% select(id)

#Se hace el anti_join para obtener las paradas de los servicios que nos faltan
missing_stops <- anti_join(services_stops_id_df,busstops_id_df)

#Se transforman los ids a char y se cambia el nombre de la columna
missing_stops <- data.frame(as.character(missing_stops$id)) %>%
                                rename(id=as.character.missing_stops.id.)

#En este bucle nos vamos a quedar con los primeros 5 caracteres de cada entrada id
missing_municipalities_df<-data.frame()
for(i in 1:nrow(missing_stops)){
  #Se aplica la función con la que se extrae la parte de identificacion del concello
  tmp <- str_extract(missing_stops$id[i],"^.{5}")
  #Se hace append a un df de resultados
  missing_municipalities_df<- rbind(missing_municipalities_df,data.frame(as.numeric(tmp)))
  
}

#Eliminamos repetidos y cambiamos el nombre de la columna
missing_municipalities_df <- distinct(missing_municipalities_df) %>% 
                          rename(id=as.numeric.tmp.)

#Merge del df al de concellos que ya teniamos en base al ID, de modo que tenemos los nombres
missing_municipalities_df <- merge(municipalities_df,missing_municipalities_df,
                          by='id',all=FALSE)

#Ahora se crea un df para el siguiente bucle....
missing_municipalities_cut_df <- data.frame()

#... En el cual se va a hacer una petición por cada concello que nos falta, aunque sólo
#usando los dos primeros caracteres del nombre. Las paradas correspondientes a cada concello
#las podemos filtrar gracias a los ids
for(i in 1:nrow(missing_municipalities_df)){
  #Se extraen los dos primeros caracteres del concello
  tmp <- str_extract(missing_municipalities_df$municipality[i],"^.{2}")
  #Se crea un df con los caracteres extraidos y su id correspondiente
  tmp_df <- data.frame(missing_municipalities_df$id[i],tmp)
  #Se hace append en un df de resultado
  missing_municipalities_cut_df<- rbind(missing_municipalities_cut_df,tmp_df)
  
}
#Se renombran las columnas
missing_municipalities_cut_df <- missing_municipalities_cut_df %>% 
                              rename(municipality=tmp) %>%
                              rename(id=missing_municipalities_df.id.i.)

#Se extrae un df de la columna de los valores unicos de las iniciales de los concellos
mun_df <- data.frame(municipality=unique(missing_municipalities_cut_df$municipality))

#Se hace peiticion con la que se obtienen las paradas por cada municipio
#La fucion es diferente a la de un script previo, dado que se va a hacer una peticion
#para cada par de letras, pero se comprobaran en la misma todos los posibles concellos.
stops_df <- get_busstops_mun(missing_municipalities_cut) 

#Se guarda el nuevo df de las paradas en un csv
write.csv(stops_df, "D:\\IFFE\\TFM\\csv\\missing_stops.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

#Lo volvemos a cargar, con lo que quitamos el problema de location que era lista
stops_df <- fread("D:\\IFFE\\TFM\\csv\\missing_stops.csv")

#Se  cargan los ids de las paradas que faltaban y de los obtenidos
tmp_ids <- data.frame(id=as.numeric(missing_stops$id)) 

tmps_ids_got <- data.frame(id=stops_df$id)

#Nos quedamos con las paradas que ha sido imposible retornar
errors <- anti_join(tmp_ids,tmps_ids_got,by='id') #Son 19
nrow(errors)
#Estas paradas hay que eliminarlas del csv de servicios, no queda otra

#Las que tenemos recien conseguidas las añadimos al csv de las paradas
busstops_df <- fread("busstops.csv")

busstops_df <- bind_rows(busstops_df,stops_df) %>%
              distinct(id, .keep_all = TRUE)

#Escribimos el nuevo csv de las paradas
write.csv(busstops_df, "D:\\IFFE\\TFM\\csv\\busstops_comb.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

#Ahora vamos a filtrar las entradas con paradas erroneas
services_df <- fread("services_new_id.csv")

#Renombramos las columnas y generamos un dos df, para comparar con los origenes y destinos
errors_origin <- errors %>% rename(id_origin=id) 
errors_destination <- errors %>% rename(id_destination=id) 

#Se genera el df con el anti_join en cada columna
services_df <- services_df %>%
              anti_join(errors_origin,by="id_origin") %>%
              anti_join(errors_destination,by="id_destination")

#Guardamos el nuevo csv de los servicios
write.csv(services_df, "D:\\IFFE\\TFM\\csv\\services_removed_missing.csv",
          row.names=FALSE,fileEncoding = "UTF-8")
