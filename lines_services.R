library(httr)
library(jsonlite)
library(dplyr)
library(stringr)
library(data.table)


#URL de la API DE LINEAS
URL_LINEAS<-"https://tpgal-ws-externos.xunta.gal/tpgal_ws/rest/lines/search?page_number=%s&page_size=30000"

convertir <- function(datos){
  # Esta funcion ayurdará a convertir info del API en dataframes
  data = rawToChar(datos$content)
  Encoding(data) <- 'UTF-8'
  return (fromJSON(enc2utf8(data)))
  
}

#Del script previo no se habían cargado todas las lineas
#Vamos a lanzar una peticion en bucle con el que obtener muchas mas
#Obtenemos todas las lineas
lineas_df <- data.frame()
for(i in 1:4){
  url <- sprintf(URL_LINEAS,i)
  print(url)
  tmp <- GET(url)
  tmp <- convertir(tmp)
  tmp_df <- tmp$results
  tmp_df <- tmp_df %>% select(c("id","line_name"))
  lineas_df<-rbind(lineas_df,tmp_df)
}

#Guardamos el nuevo df de lineas en un csv
write.csv(lineas_df, "D:\\IFFE\\TFM\\csv\\lines.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

lineas_df <- fread("D:\\IFFE\\TFM\\csv\\lines.csv")

#Vamos a comprobar si nos faltan algunas lineas respecto a los servicios
services_df <- fread("D:\\IFFE\\TFM\\csv\\services_municipalities_freq.csv")

services_lines_id <-unique(services_df$line_id)
services_lines_id <- data.frame(services_lines_id) %>% rename(id=services_lines_id) 

lines_id <- unique(lineas_df$id)
lines_id <- data.frame(lines_id) %>% rename(id=lines_id) 
missing_lines_id_df <- anti_join(services_lines_id,lines_id) 
#Faltan todavía las lineas de los servicios...


#Vamos a pillar las lineas que no tenemos y meterlas en el df de las lineas, dado que no parece haber otra forma
services_lines_df <- services_df %>% 
                    distinct(line_id, route_name, line_code) %>%
                    rename(
                      id=line_id,
                      line_name=route_name
                    )

#Aqui añadimos las nuevas entradas en el df de lineas
lineas_df<-rbind(lineas_df,services_lines_df, fill=TRUE)
#Nos aseguramos de quitar duplicados
lineas_df <- unique(lineas_df)

#Y las entradas de mismo id y diferente nombre se combinan
lines_group_df <- lineas_df %>% 
                  group_by(id) %>%
                  mutate(line_name = paste0(line_name, collapse = " | ")) %>%
                  distinct()
                  
#Escribimos el nuevo df de lineas en un csv
write.csv(lines_group_df, "D:\\IFFE\\TFM\\csv\\lines_grouped.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

#Se hace el merge con la tabla de servicios
lines_group_df <- lines_group_df %>% rename(line_id=id)
final_services_df <- merge(x=services_df,y=lines_group_df,by='line_id',all.x=TRUE)
#Eliminamos el campo de line_name y reordenamos las columnas
final_services_df <- final_services_df %>%
                     select(-c("line_name")) %>% 
                     unique() %>%
                     select(c("id","line_id","route_name","route_code",
                              "expedition_name","expedition_code","operator_id",
                              "on_demand","school_integration","contract_code",
                              "id_origin","id_destination","time_origin",
                              "time_destination","weekly_id","anual_id"))


#Se guarda el nuevo df de los servicios en un csv
write.csv(final_services_df, "D:\\IFFE\\TFM\\csv\\services_lines.csv",
          row.names=FALSE,fileEncoding = "UTF-8")
