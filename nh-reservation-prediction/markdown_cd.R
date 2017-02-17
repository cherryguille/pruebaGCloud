###############################################
# Auditoría de variables modelo predictivo NH #      
###############################################
# Custom dimensions                           #
###############################################
library("dplyr")
library("googlesheets")
source("func_categoricas.R")
source("func_cuantitativas.R")
source("func_booleanas.R")
source("funcs.R")

####### Autentificamos con la hoja de Drive #################

# Nos identificamos en la cuenta de Drive

gs_auth()
sheet<- gs_title("Variables Modelo Predictivo NH")

############################ CUSTOM DIMENSIONs ##################################
table <- data.frame(gs_read(sheet,ws = "CustomDimension"))
table <- data.frame(table[1:130,1:4])

# Dividimos las variables en tres tipos:
table_cuan <- subset(table,table$Tipo == 'Cuantitativa')
table_cual <- subset(table,table$Tipo == 'Cualitativa')
table_bool <- subset(table,table$Tipo == 'Booleana')
table_cuan_t <- subset(table,table$Tipo == 'Cuantitativa T')
tipo <- "cd"

######################## Empezamos con las variables de tipo cualitativas. #####################################
# Ejecucion de las queries de forma paralela
inicializacion_variables()
for(j in 1:NROW(cadenas_cual)){
  print(paste0("Query-",cadenas_cual[j]))
  system.time(
    output_query <- foreach(i = 1:NROW(table_cual), .packages = "bigrquery")%dopar%{
      variable_beauty <- paste("Variable",table_cual[i,3],sep = " ")
      variable <- paste0("hits.customDimensions.index=",table_cual[i,3])
      tabla_1 <- ejec_query(variable,tipo,cadenas_cual[j])
      tabla_1
    }
  )
  ############# Asignacion de variables ############
  asignacion_variables(cadenas_cual[j],output_query)
}
stopCluster(cl)
for (i in 1:NROW(table_cual)){
  variable <- paste0("hits.customDimensions.index=",table_cual[i,3])
  variable_beauty <- paste("Custom dimension",table_cual[i,3],sep = " ")
  tabla_hist <-data.frame(histograma[i])
  variables_top <- data.frame(top_n(tabla_hist,10,cuenta))
  tabla_evol <- data.frame(evolutivo[i])
  previews <- data.frame(preview[i]) 
  
  tabla_general <- t(data.frame(general[i]))
  tabla_general_percent <- t(data.frame(general_percent[i]))
  tabla_cookie <- t(data.frame(cookie[i]))
  tabla_cookie_percent <- t(data.frame(cookie_percent[i]))
  tabla_date <- t(data.frame(date_table[i]))
  tabla_date_percent <- t(data.frame(date_percent[i]))
  
  #Tabla inicial de variables
  var_table_gen    <- rbind(tabla_general,tabla_general_percent)
  var_table_cookie <- rbind(tabla_cookie,tabla_cookie_percent)
  var_table_date  <- rbind(tabla_date,tabla_date_percent)
  var_table_h <- cbind(var_table_gen,var_table_cookie,var_table_date)
  
  rownames(var_table_h) <- c(paste("Máximo",variable_beauty,sep =" "),paste("Mínimo",variable_beauty,sep =" "),paste("Promedio",variable_beauty,sep =" "),"Percentil 25","Percentil 50","Percentil 75")
  colnames(var_table_h) <- c("Valores cookie y fecha","Valores por cookie","Valores por fecha")
  kable(var_table_h, format = "markdown")
  
  #Gráfica de la evolución de la variable
  
  colnames(tabla_evol) <- c("date","variable");
  
  tabla_evol$dia <- as.integer(substr(tabla_evol$date,7,8));
  tabla_evol$annomes <- as.factor(as.integer(substr(tabla_evol$date,0,6)));
  
  gr_evol <- ggplot(tabla_evol,aes(x = dia,y = variable)) 
  gr_evol + geom_line(colour="#003A70",size=0.8) + facet_wrap(~annomes) + xlab("Día del mes") + ylab("Frecuencia de la variable") + ggtitle("Evolución de la variable en función del día")
  
  ## Histograma
  vector_top <- variables_top$cuenta
  gr_evol_hist <- ggplot(variables_top,aes(x = cd, y = cuenta))
  gr_evol_hist + geom_area(colour="#003A70",fill="#003A70",aes(x=reorder(cd,-cuenta), y=cuenta),size=5) + xlab(paste("Valores de",variable_beauty,sep = " ")) + ylab(paste("Número de veces",variable_beauty,sep= " ")) + ggtitle(paste("Histograma de la variable",variable_beauty,sep= " ")) + scale_x_discrete(labels=abbreviate)
  
  # Preview de variables
  kable(previews, format = "markdown")
  
  input <- paste("entregableNH_categorica.Rmd")
  output <- paste0("htmls/Categoricas/variable_",table_cual[i,3],".html")
  rmarkdown::render(input = input, output_file = output)
}

####################### Seguimos con las variables cuantitativas. #######################################
# Ejecucion de las queries de forma paralela
inicializacion_variables()
for(j in 1:NROW(cadenas_cuan)){
  print(paste0("Query-",cadenas_cuan[j]))
  system.time(
    output_query <- foreach(i = 1:NROW(table_cuan), .packages = "bigrquery")%dopar%{
      variable_beauty <- paste("Variable",table_cuan[i,3],sep = " ")
      variable <- paste0("hits.customDimensions.index=",table_cuan[i,3])
      tabla_1 <- ejec_query(variable,tipo,cadenas_cuan[j])
      tabla_1
    }
  )
  ############# Asignacion de variables ############
  asignacion_variables(cadenas_cuan[j],output_query)
}
stopCluster(cl)
for (i in 18:NROW(table_cuan)){
   #Saltarse cinco variables que falla la creación del informe ya que los campos obtenidos son Null debido a que al caster no se converte a integer correctamente
  if((i==9)||(i==10)||(i==15)||(i==16)||(i==17)){i<-i+1}
  variable <- paste0("hits.customDimensions.index=",table_cuan[i,3])
  variable_beauty <- paste("Custom dimension",table_cuan[i,3],sep = " ")

  # Asignacion de  variables
  var_table_20_q <- data.frame(preview[i])
  tabla_evol_q <- data.frame(evolutivo[i])
  data_evol_q <- data.frame(tabla_evol_q)
  data_evol_q <- data_evol_q %>% mutate(cd_avg = mean(cd))
  tabla_hist_q <- data.frame(histograma[i])
  
  datos_general_q <- t(data.frame(general[i]))
  datos_general_qpercent <- t(data.frame(general_percent[i]))
  datos_cookie_q <- t(data.frame(cookie[i]))
  datos_cookie_qpercent <- t(data.frame(cookie_percent[i]))
  datos_date_q <- t(data.frame(date_table[i]))
  datos_date_qpercent <- t(data.frame(date_percent[i]))
  
  
  var_table_general_q    <- rbind(datos_general_q,datos_general_qpercent)
  var_table_cookie_q <- rbind(datos_cookie_q,datos_cookie_qpercent)
  var_table_date_q  <- rbind(datos_date_q,datos_date_qpercent)
  var_table_q <- cbind(var_table_general_q,var_table_cookie_q,var_table_date_q)
  rownames(var_table_q) <- c("Máximo", "Mínimo", "Promedio","Percentil 25","Percentil 50","Percentil 75")
  colnames(var_table_q) <- c("Valores cookie y fecha", "Valores por cookie", "Valores por fecha")
  
  kable(var_table_q, format = "markdown")
  kable(var_table_20_q, format = "markdown")
  
  #Gráfica de la evolución de la variable
  
  #tabla_evol_q <- rbind(data_evol_ago,data_evol_sep,data_evol_oct,data_evol_nov,data_evol_dic,data_evol_ene)
  tabla_evol_q <- data_evol_q
  colnames(tabla_evol_q) <- c("date","cd","cd_avg");
  tabla_evol_q$dia <- as.integer(substr(tabla_evol_q$date,7,8))
  tabla_evol_q$annomes <- as.factor(as.integer(substr(tabla_evol_q$date,0,6)))
  
  
  gr_evol <- ggplot(tabla_evol_q,aes(x = dia, y = cd))
  gr_evol + geom_hline(data=tabla_evol_q, aes(yintercept=cd_avg), colour="red",size=0.4) +  facet_wrap(~annomes) + geom_line(colour="#003A70",size=0.8) + facet_wrap(~annomes) + xlab("Día del mes") + ylab(variable_beauty) + ggtitle(paste("Evolución de",variable_beauty,"en función del mes",sep=" "))
  
  ## Histograma
  
  gr_evol_hist <- ggplot(tabla_hist_q,aes(x = cd))
  gr_evol_hist + geom_area(colour="#003A70",fill="#003A70",aes(x=reorder(cd,-cuenta), y=cuenta),size=5) + xlab(paste("Valores de",variable_beauty,sep = " ")) + ylab(paste("Número de veces",variable_beauty,sep= " ")) + ggtitle(paste("Histograma de la variable",variable_beauty,sep= " ")) + scale_x_discrete(labels=abbreviate)
  
  input <- paste("entregableNH_cuanti.Rmd")
  output <- paste0("htmls/Cuantitativas/variable_",table_cuan[i,3],".html")
  rmarkdown::render(input = input, output_file = output)
}

####################### Seguimos con las variables booleanas. #################################
# Ejecucion de las queries de forma paralela
inicializacion_variables()
for(j in 1:NROW(cadenas_bool)){
  print(paste0("Query-",cadenas_bool[j]))
  system.time(
    output_query <- foreach(i = 1:NROW(table_bool), .packages = "bigrquery")%dopar%{
      variable_beauty <- paste("Variable",table_bool[i,3],sep = " ")
      variable <- paste0("hits.customDimensions.index=",table_bool[i,3])
      tabla_1 <- ejec_query(variable,tipo,cadenas_bool[j])
      tabla_1
    }
  )
  ############# Asignacion de variables ############
  asignacion_variables(cadenas_bool[j],output_query)
}
stopCluster(cl)
for (i in 1:NROW(table_bool)){
  variable <- paste0("hits.customDimensions.index=",table_bool[i,3])
  variable_beauty <- paste("Custom dimension",table_bool[i,3],sep = " ")

  # Asignamos variables.
  evol_b <- data.frame(evolutivo[i])
  tabla_generalb <- data.frame(general[i])
  tabla_previewb <- data.frame(preview[i])
  evol_b_total <- data.frame(generalb_total[i])
  
  #evol_b_total <- data.frame(5622447,2469400)
  #colnames(evol_b_total) <- c("New Cookie", "Returning Cookie")
  colnames(tabla_generalb) <- c("Tipo de cookie", "Máximo", "Mínimo", "Promedio")
  tabla_generalb <- data.frame(tabla_generalb)
  kable(tabla_generalb, format = "markdown")
  colnames(tabla_previewb) <- c("fecha", "Cookie_id","Frecuencia de la cd")
  tabla_previewb <- data.frame(tabla_previewb)
  kable(tabla_previewb, format = "markdown")
  colnames(evol_b_total) <- c("Valor de variable","Veces que se repite")
  kable(evol_b_total)
  
  #Evolucion
  colnames(evol_b) <- c("date","b_cookie_state","count_cookie");
  
  evol_b$dia <- as.integer(substr(evol_b$date,7,8));
  evol_b$annomes <- as.factor(as.integer(substr(evol_b$date,0,6)))
  
  evol_b_final <- data.frame(evol_b)
  gr_evol <- ggplot(evol_b_final,aes(x = dia,y = count_cookie))
  gr_evol +  geom_line(aes(group = b_cookie_state)) + facet_wrap(~annomes) + xlab("Día del mes") + ylab("Número de cookies") + ggtitle("Evolución tipo de cd booleano")
  
  input <- paste("entregableNH_bool.Rmd")
  output <- paste0("htmls/Booleanas/variable_",table_bool[i,3],".html")
  rmarkdown::render(input = input, output_file = output)
}
