library(bigrquery)
library(dplyr)
library(ggplot2)
library(knitr)
library(parallel)
library(doSNOW)
source("func_categoricas.R")
source("func_cuantitativas.R")
source("func_booleanas.R")


asignacion_variables <- function(query,tabla){
  tabla_salida <- c()
  if((query=="query_evol")||(query=="query_evol_q")||(query=="query_evolb")){
    print("Entra en query_evol")
    evolutivo <<- tabla
  }else if((query=="query_hist")||(query=="query_hist_q")){
    print("Entra en query_list")
    histograma <<- tabla
  }else if((query=="query_general")||(query=="query_general_q")||(query=="query_generalb")){
    print("Entra en query_general")
    general <<- tabla
  }else if((query=="query_general_percent")||(query=="query_general_qpercent")){
    print("Entra en query_general_percent")
    general_percent <<- tabla
  }else if((query=="query_cookie")||(query=="query_cookie_q")){
    print("Entra en query_cookie")
    cookie <<- tabla
  }else if((query=="query_cookie_percent")||(query=="query_cookie_qpercent")){
    print("Entra en query_cookie_percent")
    cookie_percent <<- tabla
  }else if((query=="query_date")||(query=="query_date_q")){
    print("Entra en query_date")
    date_table <<- tabla
  }else if((query=="query_date_percent")||(query=="query_date_qpercent")){
    print("Entra en query_date_percent")
    date_percent <<- tabla
  }else if((query=="query_general_20")||(query=="query_general_20_q")||(query=="query_general_20b")){
    print("Entra en query_general_20")
    preview <<- tabla
  }else if(query=="query_generalb_total"){
    print("Entra en query_generalb_total")
    generalb_total <<- tabla
  }
  
}

inicializacion_variables <- function(){
  cadenas_cual <<- c("query_evol","query_hist","query_general","query_general_percent","query_cookie","query_cookie_percent","query_date","query_date_percent","query_general_20")
  #cadenas_cuan <<- c("query_evol_q","query_evol_qago","query_evol_qsep","query_evol_qoct","query_evol_qnov","query_evol_qdic","query_evol_qene","query_hist_q","query_general_q","query_general_qpercent","query_cookie_q","query_cookie_qpercent","query_date_q","query_date_qpercent","query_general_20_q")
  cadenas_cuan <<- c("query_evol_q","query_hist_q","query_general_q","query_general_qpercent","query_cookie_q","query_cookie_qpercent","query_date_q","query_date_qpercent","query_general_20_q")
  cadenas_bool <<- c("query_generalb","query_generalb_total","query_evolb","query_general_20b")
  cl <<- makeCluster(c("localhost","localhost","localhost","localhost","localhost","localhost","localhost","localhost","localhost","localhost","localhost","localhost","localhost","localhost","localhost"), type = "SOCK")
  registerDoSNOW(cl)
  
  output_query <<- c()
  histograma <<- c()
  evolutivo <<- c()
  preview <<- c()
  evolutivo <<- c()
  general <<- c()
  general_percent <<- c()
  cookie <<- c()
  cookie_percent <<- c()
  date_table <<- c()
  date_percent <<- c()
  generalb_total <<- c()
}

ejec_query <- function(cd,tipo,query){
  if(query=="query_evol"){
    query_evol(cd,tipo)
  }else if(query=="query_hist"){
    query_hist(cd,tipo)
  }else if(query=="query_general"){
    query_general(cd,tipo)
  }else if(query=="query_general_percent"){
    query_general_percent(cd,tipo)
  }else if(query=="query_cookie"){
    query_cookie(cd,tipo)
  }else if(query=="query_cookie_percent"){
    query_cookie_percent(cd,tipo)
  }else if(query=="query_date"){
    query_date(cd,tipo)
  }else if(query=="query_date_percent"){
    query_date_percent(cd,tipo)
  }else if(query=="query_general_20"){
    query_general_20(cd,tipo)
  }else if(query=="query_evol_qago"){
    query_evol_qago(cd,tipo)
  }else if(query=="query_evol_qsep"){
    query_evol_qsep(cd,tipo)
  }else if(query=="query_evol_qoct"){
    query_evol_qoct(cd,tipo)
  }else if(query=="query_evol_qnov"){
    query_evol_qnov(cd,tipo)
  }else if(query=="query_evol_qdic"){
    query_evol_qdic(cd,tipo)
  }else if(query=="query_evol_qene"){
    query_evol_qene(cd,tipo)
  }else if(query=="query_hist_q"){
    query_hist_q(cd,tipo)
  }else if(query=="query_general_q"){
    query_general_q(cd,tipo)
  }else if(query=="query_general_qpercent"){
    query_general_qpercent(cd,tipo)
  }else if(query=="query_cookie_q"){
    query_cookie_q(cd,tipo)
  }else if(query=="query_cookie_qpercent"){
    query_cookie_qpercent(cd,tipo)
  }else if(query=="query_date_q"){
    query_date_q(cd,tipo)
  }else if(query=="query_date_qpercent"){
    query_date_qpercent(cd,tipo)
  }else if(query=="query_general_20_q"){
    query_general_20_q(cd,tipo)
  }else if(query=="query_evol_q"){
    query_evol_q(cd,tipo)
  }else if(query=="query_generalb"){
    query_generalb(cd,tipo)
  }else if(query=="query_generalb_total"){
    query_generalb_total(cd,tipo)
  }else if(query=="query_evolb"){
    query_evolb(cd,tipo)
  }else if(query=="query_general_20b"){
    query_general_20b(cd,tipo)
  }else{
    print("Fallo")
  }
}


########## Fin programa de funciones generales