library(bigrquery)
library(dplyr)
library(ggplot2)
library(knitr)

######### FUNCION FECHA ###########
fecha <- function(input_date){
  year <- substr(input_date,1,4);
  month <- substr(input_date,5,6);
  day <- substr(input_date,7,8);
  out_date <- paste(year,month,day,sep = "-")
  return(out_date)}


######### CONEXION BIGQUERY ###########
# BigQuery project Id - NH
BQ_PROJECT_ID <- "useful-art-91609";
#BigQuery NH DataSet for Google Analytics Premium
#BQ_DATASET_ID <- NH_pred;

#Fechas de entrada y salida de Bigquery
fec_inicial <- 20160801
fec_final <- 20170131

### Extracción de los datos para el html de las variables booleanas ###
query_generalb <- function(cd,tipo){
  if(tipo=="cd"){
    query <- paste("
                 SELECT
                 cd,
                 ROUND(MAX(frecuencia),2) as max,
                 ROUND(MIN(frecuencia),2) as min,
                 ROUND(AVG(frecuencia),2) as avg
                 FROM(
                 SELECT
                 date,
                 cd,
                 Count(cookie_id) as frecuencia,  
                 from(
                 SELECT 
                 date,
                 fullVisitorId as cookie_id,
                 MAX(IF(",cd,",hits.customDimensions.value,NULL)) within record AS cd
                 FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                 GROUP BY 1,2)
                 GROUP BY 1
                 ")
  }else if(tipo=="var_nativa"){
    query <- paste("
                 SELECT
                   cd,
                   ROUND(MAX(frecuencia),2) as max,
                   ROUND(MIN(frecuencia),2) as min,
                   ROUND(AVG(frecuencia),2) as avg
                   FROM(
                   SELECT
                   date,
                   cd,
                   Count(cookie_id) as frecuencia,  
                   from(
                   SELECT 
                   date,
                   fullVisitorId as cookie_id,
                   ",cd," AS cd
                   FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('20161222')))
                   GROUP BY 1,2)
                   GROUP BY 1
                   ")
  }
  data <- query_exec(query=query, project = BQ_PROJECT_ID,max_pages = Inf)
  data
}
query_generalb_total <- function(cd,tipo){
  if(tipo=="cd"){
    query <- paste("
                 SELECT
                 cd,
                 sum(frecuencia)
                 FROM(
                 SELECT
                 date,
                 cd,
                 Count(cookie_id) as frecuencia,  
                 from(
                 SELECT 
                 date,
                 fullVisitorId as cookie_id,
                 MAX(IF(",cd,",hits.customDimensions.value,NULL)) within record AS cd
                 FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                 GROUP BY 1,2)
                 GROUP BY 1
                 ")
  }else if(tipo=="var_nativa"){
    query <- paste("
                 SELECT
                   cd,
                   sum(frecuencia)
                   FROM(
                   SELECT
                   date,
                   cd,
                   Count(cookie_id) as frecuencia,  
                   from(
                   SELECT 
                   date,
                   fullVisitorId as cookie_id,
                   ",cd," AS cd
                   FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                   GROUP BY 1,2)
                   GROUP BY 1
                   ")
  }
  data <- query_exec(query=query, project = BQ_PROJECT_ID,max_pages = Inf)
  data
}
query_evolb <- function(cd,tipo){
  if(tipo=="cd"){
    query <- paste("            
                 SELECT
                 date,
                 cd as cd,
                 Count(cd) as frecuencia                 
                 FROM (
                 SELECT 
                 date,
                 fullVisitorId as cookie_id,
                 MAX(IF(",cd,",hits.customDimensions.value,NULL)) within record AS cd
                 FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                 GROUP BY 1,2
                 ")
  }else if(tipo=="var_nativa"){
    query <- paste("            
                 SELECT
                 date,
                 cd as cd,
                 Count(cd) as frecuencia                 
                 FROM (
                 SELECT 
                 date,
                 fullVisitorId as cookie_id,
                 ",cd," AS cd
                 FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                 GROUP BY 1,2
                 ")
  }
  data <- query_exec(query=query, project = BQ_PROJECT_ID,max_pages = Inf)
  data
}
query_general_20b <- function(cd,tipo){
  if(tipo=="cd"){
    query <- paste("                 
                 SELECT date,
                 cookie_id,
                 count(cd) as freq,
                 FROM(
                 SELECT 
                 date,
                 fullVisitorId as cookie_id,
                 MAX(IF(",cd,",hits.customDimensions.value,NULL)) within hits AS cd
                 FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"'))
                 where hits.type = 'PAGE')
                 group by 1,2
                 order by freq desc
                 limit 20
                 ")
  }else if(tipo=="var_nativa"){
    query <- paste("                 
                 SELECT date,
                   cookie_id,
                   count(cd) as freq,
                   FROM(
                   SELECT 
                   date,
                   fullVisitorId as cookie_id,
                   ",cd," AS cd
                   FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"'))
                   where hits.type = 'PAGE')
                   group by 1,2
                   order by freq desc
                   limit 20
                   ")
  }
  data <- query_exec(query=query, project = BQ_PROJECT_ID,max_pages = Inf)
  data
}

### Fin del programa de las queries ###