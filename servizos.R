
library(httr)
library(jsonlite)
library(dplyr)
library(stringr)

#Constantes
BASE_URL='https://tpgal-ws-externos.xunta.gal/tpgal_ws/rest/'
STOPS_URL='busstops/autocomplete?text=%s&num_results=1000000'
SERVICES_URL='service/search?origin_id=%s&destination_id=%s&origin_type=municipality&destination_type=municipality'

MUN_SERVICES_URL='service/search?origin_id=%s&destination_id=%s&origin_type=municipality&destination_type=municipality'
BUS_SERVICES_URL='service/search?origin_id=%s&destination_id=%s&origin_type=busstop&destination_type=busstop'


convertir <- function(datos){
  # Esta funcion ayurdará a convertir info del API en dataframes
  data = rawToChar(datos$content)
  Encoding(data) <- 'UTF-8'
  return (fromJSON(enc2utf8(data)))
  
}

format_names<-function(data){
  #Para cada fila del dataframe de entrada(concellos)
  for(i in 1:nrow(data)) 
  {
    concello_nm<-data$concello[i]
    if(concello_nm=="COIROS"){
      concello_nm<-'COIRÓS'
    }
    #Se comprueba si el nombre contiene coma
    if(grepl(",",concello_nm)){
      #En caso afirmativo se invirten los extremos respecto a la coma
      concello_nm_v<-unlist(strsplit(concello_nm, ","))
      concello_nm<-paste(trimws(concello_nm_v[2]),concello_nm_v[1])
    }
    #Se elimina una " (A)"
    data$concello[i] <- str_replace_all(concello_nm,c(" \\(A\\)"=""))
    #Se sustituyen los espacios y guiones por barras bajas, otrosi de poner en mayusculas
    data$concello[i] <- toupper(str_replace_all(data$concello[i],c(" " = "_", "-" = "_")))
  }
  
  data<-data[order(data$concello),]
  return(data)
}

#Funcion con la que se obtiene el dataset de paradas de buses asocidas a su concello
get_busstops<-function(data){
  #Inicializamos un dataframe vacío
  busstops_df<-data.frame()
  #Para cada fila del dataframe de entrada (concellos)
  for(i in 1:nrow(data)) 
  {
    #Se construye la url de la petición
    url = sprintf(paste(BASE_URL,STOPS_URL,sep=''),data$concello[i])
    #Se ejecuta la petición
    answer <- GET(url) %>% convertir()
    #Se obtiene el df de los resultados
    answer_df <- answer$results 
    #Se comprueba que se tengan resultados
    print(data$concello[i]) #TODO Linea de debug
    print(nrow(answer_df)) #TODO Linea de debug
    if(length(answer_df)>0){
      
      answer_df <- answer_df %>%
        #Filtramos las entradas de tipo 'municipality'...
        filter(type!='municipality') %>%
        #... y aquellas cuyo ID se corresponda al del concello
        filter(startsWith(as.character(id), as.character(data$id[i])))
      
      #Comprobamos que todavía queden entradas
      if(nrow(answer_df)>0){
        print(nrow(answer_df))
        #Se renombran algunas columnas
        answer_df <- answer_df %>% rename("parada"="text","parada_id"="id_sitme")
        #Se agrega una columna con el ID del concello
        answer_df["id_concello"] = data$id[i]
        #Se concatena el df al busstops_df
        busstops_df<-rbind(busstops_df,answer_df)
      }
    } 
  }
  return(busstops_df)
}


get_services_municipalities <- function(concellos_df) {
  
  services_df <- data.frame()
  pairs <- list("0"=FALSE) #TODO
  
  for(i in 1:nrow(concellos_df)) 
  {
    origin_id <- concellos_df$id[i]
    
    for(j in 1:nrow(concellos_df)){
      cat("\014")  
      print(sprintf("current i: %s",i))
      print(sprintf("current j: %s",j))
      
      dest_id=concellos_df$id[j]
      
      pairs_key=sprintf("%s-%s",min(origin_id,dest_id),max(origin_id,dest_id))
      
      if ((origin_id!=dest_id)&(is.null(pairs[[pairs_key]]))){
        
        url = sprintf(paste(BASE_URL,SERVICES_URL,sep=''),origin_id,dest_id)

        answer <- GET(url) %>% convertir()
        asnwer_df <- answer$results
        
        if(length(asnwer_df)>0){
          asnwer_df$id_origin <- asnwer_df$origin$id 
          asnwer_df$id_destination <- asnwer_df$destination$id 
          asnwer_df <- asnwer_df %>% select(-c("line_name","operator","contract_name","warnings",
                                               "origin", "destination","special_rates","warnings"))
          
          services_df<-rbind(services_df,asnwer_df)
          
          pairs[[pairs_key]]<-TRUE
        }
      }
    }
  }
  return(services_df)
}


get_services <- function(df,service_url) {
  
  services_df <- data.frame()

  for(i in 1:nrow(df)) 
  {
    origin_id <- df$id[i]
    
    for(j in i:nrow(df)){
      cat("\014")  
      print(sprintf("current i: %s",i))
      print(sprintf("current j: %s",j))
      
      dest_id=df$id[j]
      
      
      if (origin_id!=dest_id){
        
        url = sprintf(paste(BASE_URL,service_url,sep=''),origin_id,dest_id)
        
        answer <- GET(url) %>% convertir()
        asnwer_df <- answer$results
        
        if(length(asnwer_df)>0){
          asnwer_df$id_origin <- asnwer_df$origin$id 
          asnwer_df$id_destination <- asnwer_df$destination$id 
          asnwer_df$time_origin <- asnwer_df$origin$time 
          asnwer_df$time_destination <- asnwer_df$destination$time
          asnwer_df <- asnwer_df %>% select(-c("line_name","operator","contract_name","warnings",
                                               "origin", "destination","special_rates","warnings"))
          
          services_df<-rbind(services_df,asnwer_df)
          
        }
      }
    }
  }
  return(services_df)
}

# Datos
# Lista de concellos
concellos <- GET(sprintf('%smunicipalities',BASE_URL))
concellos <- convertir(concellos)
#Se obtiene el df de los resultados
concellos_df <- concellos$results


concellos_df<- concellos_df %>% 
  #Se renombra la columna "text" a "concello"
  rename("concello"="text") %>% 
  #Se eliminan las entradas de nombre 'DESCOÑECIDO'
  filter(concello!='DESCOÑECIDO')

#Se llama a la funcion que formatea concellos_df
concellos_df<-format_names(concellos_df)

#TODO aqui tengo que hacer la peticion para obtener los servicios por combinacion de concello
#TODO no me traga el DF pairs vacio
#services_municipalities_df <- get_services_municipalities(concellos_df)
services_municipalities_df <- get_services(concellos_df,MUN_SERVICES_URL)



#TODO me falta establecer el directorio
write.csv(services_municipalities_df, "D:\\IFFE\\TFM\\services_municipalities.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

#Con concellos_df se obtienen las servicios que los comunican
busstops_df <- get_services(concellos_df)
#TODO
services_busstops_df <- get_services(concellos_df,MUN_SERVICES_URL)
write.csv(services_busstops_df, "D:\\IFFE\\TFM\\services_busstops.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

#TODO aqui toca hacer la peticion de los servicios para cada parada de bus dentro del mismo concello
#Ni idea de como hacerse, pero parece que no tienen registrado mucho
#TODO creo que me valdria la misma funcion, solo cambiando la url (pasar otro parametro)
services_busstops_df <- get_services_busstops(busstops_df,BUS_SERVICES_URL)

#*****************************************************************#
#Paradas y concellos de Santiago
paradas_concellos_santiago <- GET('https://tpgal-ws-externos.xunta.gal/tpgal_ws/rest/busstops/autocomplete?text=BAÑOS_DE_MOLGAS&num_results=1000000')
paradas_concellos_santiago <- convertir(paradas_concellos_santiago)
paradas_concellos_santiago_df <- paradas_concellos_santiago$results

paradas_santiago_df <- paradas_concellos_santiago_df %>% filter(type!='municipality') %>% filter(startsWith(as.character(id), '15078'))




services <- GET('https://tpgal-ws-externos.xunta.gal/tpgal_ws/rest/service/search?origin_id=36057&destination_id=36003&origin_type=municipality&destination_type=municipality')
services <- convertir(services)
services <- services$results

services$id_origin <- services$origin$id 
services$id_destination <- services$destination$id 

services <- services %>% select(-c("line_name","operator","contract_name","warnings","origin", "destination","special_rates","warnings"))
                               
                               