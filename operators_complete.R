
library(httr)
library(jsonlite)
library(dplyr)
library(stringr)
library(data.table)

#Con este script se comprueba que no tenemos, de la consulta previa, todos los operadores que salen para los servicios.
#Se una nueva peticion para obtener los operadores que faltaban.

# URL DE LA API OPERADORES
URL_ALL_OPERATORS <- "https://tpgal-ws-externos.xunta.gal/tpgal_ws/rest/operators/autocomplete?text=L&numresults=10&show_all=true"

# Convertir el texto en un dataframe
convertir <- function(datos){
  # Esta funcion ayurdará a convertir info del API en dataframes
  data = rawToChar(datos$content)
  Encoding(data) <- 'UTF-8'
  return (fromJSON(enc2utf8(data)))
  
}

#Se carga el dataframe de los servicios y de los operadores
services_df <- fread( "D:\\IFFE\\TFM\\csv\\servies_lines_contracts_operators.csv")
operators_df <- fread( "D:\\IFFE\\TFM\\csv\\operators_base.csv")

#Comprobamos que los operadores de los servicios no coinciden conn los que teniamos
service_operators<- unique(services_df$operator_id)
service_operators_df <- data.frame(service_operators) %>% rename(id=service_operators) #98
identical(service_operators_df, operators_df)

#Comprobamos los ids que nos faltaban (son todos)
missing_id_df <- anti_join(service_operators_df,operators_df)
nrow(missing_id_df)

# Peticion que recupera todos los operadores
all_operators <- GET(URL_ALL_OPERATORS)
all_operators <- convertir(all_operators)
#Se obtiene el df de los resultados
all_operators_df <- all_operators$results

#Ajustamos el df para poder concatenarlo con el que ya teniamos
all_operators_df$address <- NA
all_operators_df$phone <- NA
all_operators_df$email <- NA
all_operators_df<- all_operators_df %>% 
  #Se renombra la columna "text" a "concello"
  rename("operator/s"="text")

comb_operators_df <- rbind(operators_df, all_operators_df)
#Filtramos filas repetidas
comb_operators_df <- comb_operators_df %>% distinct()

#Vamos a comprobar si queda alguno de los operadores de services fuera
comb_operators_id_df <- unique(comb_operators_df$id)
comb_operators_id_df <- data.frame(comb_operators_id_df) %>% rename(id=comb_operators_id_df)

missing_id_df <- anti_join(service_operators_df,comb_operators_id_df)
nrow(missing_id_df) #0
#Ya estan los operadores correctos

#Vamos a escribir el df para uso futuro con los servicios
write.csv(comb_operators_df, "D:\\IFFE\\TFM\\csv\\operators_complete.csv",
          row.names=FALSE,fileEncoding = "UTF-8")