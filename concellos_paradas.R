
library(httr)
library(jsonlite)
library(dplyr)
library(stringr)

#Con este script se obtienen todos los concellos y las paradas

#Constantes
BASE_URL='https://tpgal-ws-externos.xunta.gal/tpgal_ws/rest/'
STOPS_URL='busstops/autocomplete?text=%s&num_results=1000000'


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
    concello_nm<-data$municipality[i]
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
    data$municipality[i] <- str_replace_all(concello_nm,c(" \\(A\\)"=""))
    #Se sustituyen los espacios y guiones por barras bajas, otrosi de poner en mayusculas
    data$municipality[i] <- toupper(str_replace_all(data$municipality[i],c(" " = "_", "-" = "_")))
  }
  
  data<-data[order(data$municipality),]
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
    url = sprintf(paste(BASE_URL,STOPS_URL,sep=''),data$municipality[i])
    #Se ejecuta la petición
    answer <- GET(url) %>% convertir()
    #Se obtiene el df de los resultados
    answer_df <- answer$results 
    #Se comprueba que se tengan resultados
    if(length(answer_df)>0){
      
      answer_df <- answer_df %>%
        #Filtramos las entradas de tipo 'municipality'...
        filter(type!='municipality') %>%
        #... y aquellas cuyo ID se corresponda al del concello
        filter(startsWith(as.character(id), as.character(data$id[i])))
      
      #Comprobamos que todavía queden entradas
      if(nrow(answer_df)>0){
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

# Datos
# Lista de concellos
concellos <- GET(sprintf('%smunicipalities',BASE_URL))
concellos <- convertir(concellos)
#Se obtiene el df de los resultados
concellos_df <- concellos$results


concellos_df<- concellos_df %>% 
  #Se renombra la columna "text" a "concello"
  rename("municipality"="text") %>% 
  #Se eliminan las entradas de nombre 'DESCOÑECIDO'
  filter(municipality!='DESCOÑECIDO')

#Se llama a la funcion que formatea concellos_df
concellos_df<-format_names(concellos_df)

#Con concellos_df se obtienen las paradas
busstops_df <- get_busstops(concellos_df)

#Se eliminan las paradas repetidas en base a sus coordenadas
busstops_df <- busstops_df[!duplicated(busstops_df$location), ]

#Tambien eliminamos los duplicados en base al id(que hay)
busstops_df <- busstops_df %>% distinct(id, .keep_all = TRUE)

#A este punto tengo todas las paradas disponibles con sus concellos

#Escribimos las paradas en un csv
busstops_df <- busstops_df %>%
  rename(
    stop_name=parada,
    id_stop=parada_id,
    id_municipality=id_concello
  )
write.csv(busstops_df, "D:\\IFFE\\TFM\\csv\\busstops.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

#Escribimos los concellos en un csv
write.csv(concellos_df, "D:\\IFFE\\TFM\\csv\\concellos.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

