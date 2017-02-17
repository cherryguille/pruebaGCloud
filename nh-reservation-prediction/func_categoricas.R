
#### Funcion de computo de las variables categorícas ####

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
BQ_PROJECT_ID <- "useful-art-91609"
#BigQuery NH DataSet for Google Analytics Premium
BQ_DATASET_ID <- "NH_pred";

#Fechas de entrada y salida de Bigquery
fec_inicial <- 20160801
fec_final <- 20170131
### Extracción de los datos para el html de las variables categoricas ###

query_evol <- function(cd,tipo){
  if(tipo=="cd"){
    query <- paste("SELECT
                 date,
                 count(cd)
                 FROM(
                 SELECT
                 date,
                 fullVisitorId as cookie_id,
                 max(IF(",cd,",hits.customDimensions.value,NULL)) WITHIN record AS cd
                 FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                 GROUP BY 1",sep="")
  }else if(tipo=="var_nativa"){
    query <- paste("SELECT
                 date,
                   count(cd)
                   FROM(
                   SELECT
                   date,
                   fullVisitorId as cookie_id,
                   ",cd," AS cd
                   FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                   GROUP BY 1",sep="")
  }

  data <- query_exec(query=query, project = BQ_PROJECT_ID,max_pages = Inf)
  data
}
query_hist <- function(cd,tipo){
  if(tipo=="cd"){
    query <- paste("SELECT
                 cd,
                 Count(cd) as cuenta,
                 FROM(
                 SELECT
                 date,
                 fullVisitorId as cookie_id,
                 max(IF(",cd,",hits.customDimensions.value,NULL)) WITHIN record AS cd
                 FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                 GROUP BY 1
                 order by cuenta desc
                 limit 10
                 ")
  }else if(tipo=="var_nativa"){
    query <- paste("SELECT
                   cd,
                   Count(cd) as cuenta,
                   FROM(
                   SELECT
                   date,
                   fullVisitorId as cookie_id,
                   ",cd," AS cd
                   FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                   GROUP BY 1
                   order by cuenta desc
                   limit 10
                   ")
  }
  data <- query_exec(query=query, project = BQ_PROJECT_ID,max_pages = Inf)
  data
}
query_general <- function(cd,tipo){
  if(tipo=="cd"){
    query <- paste("
                 SELECT
                 ROUND(MAX(frecuencia),2) as max,
                 ROUND(MIN(frecuencia),2) as min,
                 ROUND(AVG(frecuencia),2) as avg,
                 FROM(
                 SELECT
                 date,
                 cookie_id,
                 SUM(frecuencia) as frecuencia
                 FROM(
                 SELECT
                 date,
                 cookie_id,
                 cd,
                 Count(cd) as frecuencia
                 FROM(
                 SELECT
                 date,
                 fullVisitorId as cookie_id,
                 max(IF(",cd,",hits.customDimensions.value,NULL)) WITHIN record AS cd
                 FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                 GROUP BY 1,2,3)
                 GROUP BY 1,2)
                 ")
  }else if(tipo=="var_nativa"){
    query <- paste("
                 SELECT
                   ROUND(MAX(frecuencia),2) as max,
                   ROUND(MIN(frecuencia),2) as min,
                   ROUND(AVG(frecuencia),2) as avg,
                   FROM(
                   SELECT
                   date,
                   cookie_id,
                   SUM(frecuencia) as frecuencia
                   FROM(
                   SELECT
                   date,
                   cookie_id,
                   cd,
                   Count(cd) as frecuencia
                   FROM(
                   SELECT
                   date,
                   fullVisitorId as cookie_id,
                   ",cd," AS cd
                   FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                   GROUP BY 1,2,3)
                   GROUP BY 1,2)
                   ")
  }
  data <- query_exec(query=query, project = BQ_PROJECT_ID,max_pages = Inf)
  data
}
query_general_percent <- function(cd,tipo){
  if(tipo=="cd"){
    query <- paste("
                 SELECT
                 p_cont25,
                 p_cont50,
                 p_cont75
                 FROM(
                 SELECT
                 PERCENTILE_CONT(0.01) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont25,
                 PERCENTILE_CONT(0.5) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont50,
                 PERCENTILE_CONT(0.75) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont75,
                 FROM(
                 SELECT
                 date,
                 cookie_id,
                 1 as var_aux,
                 SUM(frecuencia) as frecuencia
                 FROM(
                 SELECT
                 date,
                 cookie_id,
                 cd,
                 Count(cd) as frecuencia
                 FROM(
                 SELECT
                 date,
                 fullVisitorId as cookie_id,
                 MAX(IF(",cd,",hits.customDimensions.value,NULL)) WITHIN record AS cd
                 FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                 GROUP BY 1,2,3)
                 GROUP BY 1,2,3))
                 GROUP BY 1,2,3
                 ")
  }else if(tipo=="var_nativa"){
    query <- paste("
                 SELECT
                   p_cont25,
                   p_cont50,
                   p_cont75
                   FROM(
                   SELECT
                   PERCENTILE_CONT(0.01) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont25,
                   PERCENTILE_CONT(0.5) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont50,
                   PERCENTILE_CONT(0.75) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont75,
                   FROM(
                   SELECT
                   date,
                   cookie_id,
                   1 as var_aux,
                   SUM(frecuencia) as frecuencia
                   FROM(
                   SELECT
                   date,
                   cookie_id,
                   cd,
                   Count(cd) as frecuencia
                   FROM(
                   SELECT
                   date,
                   fullVisitorId as cookie_id,
                   ",cd," AS cd
                   FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                   GROUP BY 1,2,3)
                   GROUP BY 1,2,3))
                   GROUP BY 1,2,3
                   ")
  }
  data <- query_exec(query=query, project = BQ_PROJECT_ID,max_pages = Inf)
  data
}
query_cookie <- function(cd,tipo){
  if(tipo=="cd"){
    query <- paste("
                 SELECT
                 ROUND(MAX(frecuencia),2) as max,
                 ROUND(MIN(frecuencia),2) as min,
                 ROUND(AVG(frecuencia),2) as avg,
                 FROM(
                 SELECT
                 cookie_id,
                 SUM(frecuencia) as frecuencia
                 FROM(
                 SELECT
                 date,
                 cookie_id,
                 cd,
                 Count(cd) as frecuencia
                 FROM(
                 SELECT
                 date,
                 fullVisitorId as cookie_id,
                 MAX(IF(",cd,",hits.customDimensions.value,NULL)) WITHIN record AS cd
                 FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                 GROUP BY 1,2,3)
                 GROUP BY 1)
                 ")
  }else if(tipo=="var_nativa"){
    query <- paste("
                 SELECT
                   ROUND(MAX(frecuencia),2) as max,
                   ROUND(MIN(frecuencia),2) as min,
                   ROUND(AVG(frecuencia),2) as avg,
                   FROM(
                   SELECT
                   cookie_id,
                   SUM(frecuencia) as frecuencia
                   FROM(
                   SELECT
                   date,
                   cookie_id,
                   cd,
                   Count(cd) as frecuencia
                   FROM(
                   SELECT
                   date,
                   fullVisitorId as cookie_id,
                   ",cd," AS cd
                   FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                   GROUP BY 1,2,3)
                   GROUP BY 1)
                   ")
  }
  data <- query_exec(query=query, project = BQ_PROJECT_ID,max_pages = Inf)
  data
}
query_cookie_percent <- function(cd,tipo){
  if(tipo=="cd"){
    query <- paste("
                 SELECT
                 p_cont25,
                 p_cont50,
                 p_cont75
                 FROM(
                 SELECT
                 PERCENTILE_CONT(0.01) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont25,
                 PERCENTILE_CONT(0.5) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont50,
                 PERCENTILE_CONT(0.75) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont75,
                 FROM(
                 SELECT
                 cookie_id,
                 1 as var_aux,
                 SUM(frecuencia) as frecuencia
                 FROM(
                 SELECT
                 date,
                 cookie_id,
                 cd,
                 Count(cd) as frecuencia
                 FROM(
                 SELECT
                 date,
                 fullVisitorId as cookie_id,
                 MAX(IF(",cd,",hits.customDimensions.value,NULL)) WITHIN record AS cd
                 FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                 GROUP BY 1,2,3)
                 GROUP BY 1,2
                 ))
                 GROUP BY 1,2,3
                 ")
  }else if(tipo=="var_nativa"){
    query <- paste("
                 SELECT
                   p_cont25,
                   p_cont50,
                   p_cont75
                   FROM(
                   SELECT
                   PERCENTILE_CONT(0.01) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont25,
                   PERCENTILE_CONT(0.5) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont50,
                   PERCENTILE_CONT(0.75) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont75,
                   FROM(
                   SELECT
                   cookie_id,
                   1 as var_aux,
                   SUM(frecuencia) as frecuencia
                   FROM(
                   SELECT
                   date,
                   cookie_id,
                   cd,
                   Count(cd) as frecuencia
                   FROM(
                   SELECT
                   date,
                   fullVisitorId as cookie_id,
                   ",cd," AS cd
                   FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                   GROUP BY 1,2,3)
                   GROUP BY 1,2
                   ))
                   GROUP BY 1,2,3
                   ")
  }
  data <- query_exec(query=query, project = BQ_PROJECT_ID,max_pages = Inf)
  data
}
query_date <- function(cd,tipo){
  if(tipo=="cd"){
    query <- paste("
                 SELECT
                 ROUND(MAX(frecuencia),2) as max,
                 ROUND(MIN(frecuencia),2) as min,
                 ROUND(AVG(frecuencia),2) as avg,
                 FROM(
                 SELECT
                 date,
                 SUM(frecuencia) as frecuencia
                 FROM(
                 SELECT
                 date,
                 cookie_id,
                 cd,
                 Count(cd) as frecuencia
                 FROM(
                 SELECT
                 date,
                 fullVisitorId as cookie_id,
                 MAX(IF(",cd,",hits.customDimensions.value,NULL)) WITHIN record AS cd
                 FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                 GROUP BY 1,2,3)
                 GROUP BY 1)
                 ")
  }else if(tipo=="var_nativa"){
    query <- paste("
                 SELECT
                   ROUND(MAX(frecuencia),2) as max,
                   ROUND(MIN(frecuencia),2) as min,
                   ROUND(AVG(frecuencia),2) as avg,
                   FROM(
                   SELECT
                   date,
                   SUM(frecuencia) as frecuencia
                   FROM(
                   SELECT
                   date,
                   cookie_id,
                   cd,
                   Count(cd) as frecuencia
                   FROM(
                   SELECT
                   date,
                   fullVisitorId as cookie_id,
                   ",cd," AS cd
                   FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                   GROUP BY 1,2,3)
                   GROUP BY 1)
                   ")
  }
  data <- query_exec(query=query, project = BQ_PROJECT_ID,max_pages = Inf)
  data
}
query_date_percent <- function(cd,tipo){
  if(tipo=="cd"){
    query <- paste("
                 SELECT
                 p_cont25,
                 p_cont50,
                 p_cont75
                 FROM(
                 SELECT
                 PERCENTILE_CONT(0.01) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont25,
                 PERCENTILE_CONT(0.5) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont50,
                 PERCENTILE_CONT(0.75) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont75,
                 FROM(
                 SELECT
                 date,
                 1 as var_aux,
                 SUM(frecuencia) as frecuencia
                 FROM(
                 SELECT
                 date,
                 cookie_id,
                 cd,
                 Count(cd) as frecuencia
                 FROM(
                 SELECT
                  date,
                  fullVisitorId as cookie_id,
                  MAX(IF(",cd,",hits.customDimensions.value,NULL)) WITHIN record AS cd
                  FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                 GROUP BY 1,2,3)
                 GROUP BY 1,2
                 ))
                 GROUP BY 1,2,3
                 ")
  }else if(tipo=="var_nativa"){
    query <- paste("
                 SELECT
                   p_cont25,
                   p_cont50,
                   p_cont75
                   FROM(
                   SELECT
                   PERCENTILE_CONT(0.01) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont25,
                   PERCENTILE_CONT(0.5) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont50,
                   PERCENTILE_CONT(0.75) OVER (PARTITION BY var_aux ORDER BY frecuencia ASC) as p_cont75,
                   FROM(
                   SELECT
                   date,
                   1 as var_aux,
                   SUM(frecuencia) as frecuencia
                   FROM(
                   SELECT
                   date,
                   cookie_id,
                   cd,
                   Count(cd) as frecuencia
                   FROM(
                   SELECT
                   date,
                   fullVisitorId as cookie_id,
                   ",cd," AS cd
                   FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                   GROUP BY 1,2,3)
                   GROUP BY 1,2
                   ))
                   GROUP BY 1,2,3
                   ")
  }
  data <- query_exec(query=query, project = BQ_PROJECT_ID,max_pages = Inf)
  data
}
query_general_20 <- function(cd,tipo){
  if(tipo=="cd"){
    query <- paste("
                 SELECT
                 date,
                 cookie_id,
                 cd,
                 Count(cd) as frecuencia
                 FROM (
                 SELECT
                 date,
                 fullVisitorId as cookie_id,
                 //MAX(IF(",cd,",hits.customDimensions.value,NULL)) WITHIN record AS cd
                 MAX(IF(",cd,",hits.customDimensions.value,NULL)) WITHIN record AS cd
                 FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                 GROUP BY 1,2,3
                 order by frecuencia desc, cookie_id desc, date asc
                 limit 20
                 ")
  }else if(tipo=="var_nativa"){
    query <- paste("
                 SELECT
                   date,
                   cookie_id,
                   cd,
                   Count(cd) as frecuencia
                   FROM (
                   SELECT
                   date,
                   fullVisitorId as cookie_id,
                   ",cd," AS cd
                   FROM TABLE_DATE_RANGE([94438972.ga_sessions_],TIMESTAMP('",fec_inicial,"'),TIMESTAMP('",fec_final,"')))
                   GROUP BY 1,2,3
                   order by frecuencia desc, cookie_id desc, date asc
                   limit 20
                   ")
  }
  data <- query_exec(query=query, project = BQ_PROJECT_ID,max_pages = Inf)
  data
}

### Fin del programa de las queries ###