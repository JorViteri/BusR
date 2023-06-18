library(httr)
library(jsonlite)
library(dplyr)
library(stringr)
library(data.table)

#Este script finaliza el services_df generando los df de las rutas y expediciones


services_df <- fread("D:\\IFFE\\TFM\\csv\\servies_lines_contracts_operators.csv")

#Se comienza cargando unos dataframes correspondientes a los nombres de rutas y servicios
route_names_df <- data.frame(unique(services_df$route_name)) %>% rename(name=unique.services_df.route_name.)
expedition_names_df <- data.frame(unique(services_df$expedition_name)) %>% rename(name=unique.services_df.expedition_name.)

#Se habia apreciado que el estos valores son similares y se comprueban
identical(expedition_names_df,route_names_df)
#Son iguales

#Se procede a crear el df de las expediciones
expeditions_df <- services_df %>%
                  select(c("expedition_name","expedition_code")) %>% 
                  unique() %>%
                  mutate(id = row_number()) %>%
                  select(c("id","expedition_name","expedition_code"))

write.csv(expeditions_df, "D:\\IFFE\\TFM\\csv\\expeditions.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

#Se procede a crear el df de las rutas
routes_df <- services_df %>%
  select(c("route_name","route_code")) %>% 
  unique() %>%
  mutate(id = row_number()) %>%
  select(c("id","route_name","route_code"))

write.csv(routes_df, "D:\\IFFE\\TFM\\csv\\routes.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

#Toca quitar las columnas y unir con las nuevas tablas
services_expeditions_df <- merge(expeditions_df, services_df, by=c("expedition_code","expedition_name")) %>% 
                           rename(id_expedition=id.x,id=id.y) %>%
                           select(-c("expedition_name","expedition_code"))

services_expeditions_routes_df <- merge(routes_df, services_expeditions_df, by=c("route_code","route_name")) %>% 
                                  rename(id_route=id.x,id=id.y) %>%
                                  select(-c("route_code","route_name"))

#Vamos a añadir una columna de ids por posición para identificar cada fila
#De paso se modifca el nombre de algunas columnas y el orden de estas
services_df <- services_expeditions_routes_df %>% 
               rename(
                 id_service=id,
                 id_line=line_id,
                 id_operator=operator_id,
               )%>%
               mutate(id = row_number()) %>%
               select(c("id","id_service","id_line","id_route","id_expedition","id_operator",
               "id_contract","on_demand","school_integration","id_origin",
               "id_destination","time_origin","time_destination","weekly_id","anual_id"))

write.csv(services_df, "D:\\IFFE\\TFM\\csv\\services_new_id.csv",
          row.names=FALSE,fileEncoding = "UTF-8")
