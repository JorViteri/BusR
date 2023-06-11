library(httr)
library(jsonlite)
library(dplyr)
library(stringr)
library(data.table)

#Con este script se genera el df de los contratos y se actualiza el de servicios para que referencie
#al id de estos

# URL DE LA API OPERADORES-CONTRATOS
URL_CONTRATOS<-"https://tpgal-ws-externos.xunta.gal/tpgal_ws/rest/operators/contracts"


# Convertir el texto en un dataframe
convertir <- function(datos){
  # Esta funcion ayurdará a convertir info del API en dataframes
  data = rawToChar(datos$content)
  Encoding(data) <- 'UTF-8'
  return (fromJSON(enc2utf8(data)))
  
}


#Es necesario tener bien los contratos y los operadores, cada cual deberia ser una tabla 
#Las tablas no deberian estar relacionadas directamente, si no a los servicios
services_df <- fread("D:\\IFFE\\TFM\\csv\\services_lines.csv")
operators_df <- fread("D:\\IFFE\\TFM\\csv\\operators_complete.csv")


# Contratos
contratos <- GET(URL_CONTRATOS)
contratos <- convertir(contratos)
#Se obtiene el df de los resultados
contratos_df <- contratos$results

#Codigos de contratos en el df recien cargado
tmp1 <- data.frame(unique(contratos_df$code)) %>% 
        rename(id_contract=unique.contratos_df.code.)

#Codigos de contratos en el df de servicios
tmp2 <- data.frame(unique(services_df$contract_code)) %>% 
        rename(id_contract=unique.services_df.contract_code.)


tmp3 <- anti_join(tmp2,tmp1) #Tenemos todos los contratos, coinciden

#Vamos a generar unos ids para el dataframe de contratos y quitamos a las columnas de operador
contratos_df <- contratos_df %>%
                mutate(id = row_number()) %>%
                select(-c("operator")) %>%
                rename(contract_nm=name) %>%
                select(c("id","code","contract_nm","consolidated"))

#Se escrive el df para su futuro uso
write.csv(contratos_df, "D:\\IFFE\\TFM\\csv\\contratos.csv",
          row.names=FALSE,fileEncoding = "UTF-8")

#Vamos a actualizar el df de servicios para que tenga los IDs de contratos, no los codigos

services_contracts_df <- services_df %>%
                        rename(code=contract_code) %>%
                        merge(y=contratos_df,by='code',all.x=TRUE) %>%
                        select(-c("code","contract_nm","consolidated")) %>%
                        rename(id_contract=id.y,id=id.x)

#Guardamos el nuevo csv de servicios
write.csv(services_contracts_df, "D:\\IFFE\\TFM\\csv\\servies_lines_contracts_operators.csv",
          row.names=FALSE,fileEncoding = "UTF-8")
  