library(httr)
library(jsonlite)
library(dplyr)
library(stringr)

#URL de la API DE LINEAS
URL_LINEAS<-"https://tpgal-ws-externos.xunta.gal/tpgal_ws/rest/lines"
# URL DE LA API OPERADORES-CONTRATOS
URL_CONTRATOS<-"https://tpgal-ws-externos.xunta.gal/tpgal_ws/rest/operators/contracts"


# Convertir el texto en un dataframe
#data_contratos <- fromJSON(content)
convertir <- function(datos){
  # Esta funcion ayurdará a convertir info del API en dataframes
  data = rawToChar(datos$content)
  Encoding(data) <- 'UTF-8'
  return (fromJSON(enc2utf8(data)))
  
}


# Líneas
lineas <- GET(URL_LINEAS)
lineas <- convertir(lineas)
#Se obtiene el df de los resultados
lineas_df <- lineas$results
lineas_df <- lineas_df %>% select(c("id","line_name")) %>% unique()



# Contratos
contratos <- GET(URL_CONTRATOS)
contratos <- convertir(contratos)
#Se obtiene el df de los resultados
contratos_df <- contratos$results

#Mantenemos el id del operador asociado a cada contrato
contratos_df$operator_id = contratos_df$operator$id

#Pasamos los datos de operador a su propio df
operators_df <- contratos_df[,'operator']
#Modificamos el nombre de columa
operators_df<- operators_df %>% 
  rename("operator/s"="text")

#Y eliminamos los duplicados por id
operators_df<- operators_df[!duplicated(operators_df$id), ]

#Ahora se eliminan los datos de operador en el df de contratos
contratos_df <- contratos_df %>% select(-c("operator"))
#Se añade una columna de ids para cada entrada de contratos
contratos_df <- contratos_df %>% mutate(id = row_number())
#Se renombra la columna de name
contratos_df <- contratos_df %>% 
  rename("contract"="name")

#Se esriben los operadores para trabajar mas comodamente
write.csv(operators_df, "D:\\IFFE\\TFM\\csv\\operators_base.csv",
          row.names=FALSE,fileEncoding = "UTF-8")


