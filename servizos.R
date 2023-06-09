
library(httr)
library(jsonlite)
library(dplyr)
library(stringr)
library(data.table)

#En este script se obtienen los servicios que hay para cada par de concellos

#Constantes
BASE_URL='https://tpgal-ws-externos.xunta.gal/tpgal_ws/rest/'
STOPS_URL='busstops/autocomplete?text=%s&num_results=1000000'
SERVICES_URL='service/search?origin_id=%s&destination_id=%s&origin_type=municipality&destination_type=municipality'
MUN_SERVICES_URL='service/search?origin_id=%s&destination_id=%s&origin_type=municipality&destination_type=municipality'


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

get_services <- function(df,service_url) {
  
  services_df <- data.frame()
  errors <- list(c("0-0"=TRUE))
  
  #Para cada entrada del df de concellos...
  for(i in 1:nrow(df))
  {
    origin_id <- df$id[i]
    
    #Obtenemos los servicios que tiene para cada concello posterior en el df
    for(j in i:nrow(df)){ 
      cat("\014")  
      print(sprintf("current i: %s",i))
      print(sprintf("current j: %s",j))
      
      dest_id=df$id[j]
      #Se comprueba que origen y destino no sean el mismo
      if (origin_id!=dest_id){
        
        url = sprintf(paste(BASE_URL,service_url,sep=''),origin_id,dest_id)
        
        answer <- GET(url) 
        #Se comprueba que la respuesta sea un json, otro caso es mensaje de error
        if((validate(rawToChar(answer$content)))==FALSE){
          #En caso de error se almacena en "errors...
          errors[[sprintf('%s-%s',i,j)]]=TRUE
          #Y se avanza a la siguiente iteracion en j
          next
        }
        
        #En caso positivo procesamos la respuesta como siempre
        answer <- convertir(answer)
        
        asnwer_df <- answer$results
        
        if(length(asnwer_df)>0){
          #La respuesta json es de forma peculiar, por lo que se generan columnas y
          #se eliminan otras entradas para que se adapte mejor
          asnwer_df$id_origin <- asnwer_df$origin$id 
          asnwer_df$id_destination <- asnwer_df$destination$id 
          asnwer_df$time_origin <- asnwer_df$origin$time 
          asnwer_df$time_destination <- asnwer_df$destination$time
          asnwer_df <- asnwer_df %>% select(-c("line_name","operator","contract_name","warnings",
                                               "origin", "destination","special_rates"))
          
          #Se concatena el df de la respuesta al del resultado
          services_df<-rbind(services_df,asnwer_df)
          
        }
      }
    }
    
    if(i%%20==0){
      #Por seguridad, se escribe un csv cada 20 iteraciones de i
      path=sprintf("D:\\IFFE\\TFM\\services_municipalities_%s.csv",i)
      write.csv(services_df,path,row.names=FALSE,fileEncoding = "UTF-8")

    }
  }
  #Al final se imprime "errors"...
  errors
  #y se retorna el df de los servicios
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

#Peticion para obtener los servicios por combinacion de concello
services_municipalities_df <- get_services(concellos_df,MUN_SERVICES_URL)

#Quitamos filas repetidas
final_services_df <- services_municipalities_df %>% distinct()

#Y las guardamos en un CSV
write.csv(final_services_df, "D:\\IFFE\\TFM\\csv\\services_municipalities_complete.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

#Ahora vamos a generar tablas a partir de los valores de las columnas de frecuencias
final_services_df <- fread( "D:\\IFFE\\TFM\\csv\\services_municipalities_complete.csv")

#Frecuencias semanales
week_frequency <- unique(final_services_df$week_frequency)
week_frequency_df <- data.frame(week_frequency)
#Se le genera la columna de ids en base al num de fila
week_frequency_df <- week_frequency_df %>% 
                     mutate(id = row_number()) %>% 
                     select(c("id","week_frequency"))

#Frecuencias anuales
anual_frequency<- unique(final_services_df$anual_frequency)
anual_frequency_df <- data.frame(anual_frequency)
#Se le genera la columna de ids en base al num de fila
anual_frequency_df <- anual_frequency_df %>% 
                      mutate(id = row_number()) %>% 
                      select(c("id","anual_frequency"))

#Se escribe un CSV para cada tablas de las frecuencias
write.csv(week_frequency_df, "D:\\IFFE\\TFM\\csv\\week_freq.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

write.csv(anual_frequency_df, "D:\\IFFE\\TFM\\csv\\anual_freq.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

#Ahora vamos a añadir los ids de los valores de frecuencias a la tabla de los servicios
final_services_df <- merge(x=final_services_df,y=week_frequency_df,by='week_frequency',all.x=TRUE)
final_services_df <- final_services_df %>% 
                     rename(id=id.x,weekly_id=id.y)

final_services_df <- merge(x=final_services_df,y=anual_frequency_df,by='anual_frequency',all.x=TRUE)
final_services_df <- final_services_df %>% 
                     rename(id=id.x,anual_id=id.y)

final_services_df <- final_services_df %>% 
                     select(-c('week_frequency','anual_frequency'))

#Se guarda el csv actual, dado que va a ser preciso hacer nuevas iteraciones dado que salen operadores y lineas desconocidas
write.csv(final_services_df, "D:\\IFFE\\TFM\\csv\\services_municipalities_freq.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

