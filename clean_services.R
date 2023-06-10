library(httr)
library(jsonlite)
library(dplyr)
library(stringr)
library(data.table)

# Convertir el texto en un dataframe
convertir <- function(datos){
  # Esta funcion ayurdará a convertir info del API en dataframes
  data = rawToChar(datos$content)
  Encoding(data) <- 'UTF-8'
  return (fromJSON(enc2utf8(data)))
  
}


services_df <- fread("D:\\IFFE\\TFM\\csv\\servies_lines_contracts_operators.csv")

#Falta hacer algo mas de limpieza en la tabla de servicios
#Puede ser interesante cargar los codigos de linea en la tabla de lineas 
lines_df <- fread("D:\\IFFE\\TFM\\csv\\lines_comb.csv")


route_names_df <- data.frame(unique(services_df$route_name)) %>% rename(name=unique.services_df.route_name.)
expedition_names_df <- data.frame(unique(services_df$expedition_name)) %>% rename(name=unique.services_df.expedition_name.)

tmp <- anti_join(expedition_names_df,route_names_df) 
#Son iguales, podemos descartar las dos dado que los tenemos como nombres de lineas
#Los codidgos de linea los vamos a guardar en la tabla de lineas, aunque queden casos a null

route_names_df <- data.frame(unique(services_df$route_name)) %>% rename(name=unique.services_df.route_name.)

tmp <- data.frame(unique(services_df$line_id, services_df$line_code))

tmp <- services_df %>% 
       select(c("line_id","line_code")) %>% 
       unique()

tmp2 <- merge(lines_df, tmp,all = TRUE)

write.csv(tmp2, "D:\\IFFE\\TFM\\csv\\lines_codes.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

#####Del services sobran: line_code, route_name, expedition_name
services_df <- services_df %>% select (-c("line_code"))

expeditions_df <- services_df %>%
                  select(c("expedition_name","expedition_code")) %>% 
                  unique() %>%
                  mutate(id = row_number()) %>%
                  select(c("id","expedition_name","expedition_code"))

write.csv(expeditions_df, "D:\\IFFE\\TFM\\csv\\expeditions.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

#####Del services sobran: line_code, route_name, expedition_name

routes_df <- services_df %>%
  select(c("route_name","route_code")) %>% 
  unique() %>%
  mutate(id = row_number()) %>%
  select(c("id","route_name","route_code"))

write.csv(routes_df, "D:\\IFFE\\TFM\\csv\\routes.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

#Toca quitar las columnas y unir con las nuevas tablas
services_expeditions_df <- merge(expeditions_df, services_df, by=c("expedition_code","expedition_name")) %>% 
                           rename(expedition_id=id.x,id=id.y) %>%
                           select(-c("expedition_name","expedition_code"))

services_expeditions_routes_df <- merge(routes_df, services_expeditions_df, by=c("route_code","route_name")) %>% 
  rename(route_id=id.x,id=id.y) %>%
  select(-c("route_code","route_name"))


services_df <- services_expeditions_routes_df %>% select(c("id","line_id","route_id","expedition_id","operator_id"
                        ,"contract_id","on_demand","school_integration","id_origin","id_destination","time_origin"
                        ,"time_destination","weekly_id","anual_id"))

write.csv(services_df, "D:\\IFFE\\TFM\\csv\\services_clear.csv",
          row.names=FALSE,fileEncoding = "UTF-8")
