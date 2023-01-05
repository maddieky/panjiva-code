library(lubridate)
library(tidyverse)
library(DBI)
library(seasonal)
library(frb)
library(policyPlot)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('../functions/functions_panjiva.R')
source('../functions/functions_ports.R')

import_us <- dplyr::tbl(conn_panjiva, 'panjivausimport')

teu_port_one = import_us %>%
  filter(portofunlading == "New York/Newark Area, Newark, New Jersey") %>%
  mutate(portofunlading = case_when(portofunlading == "New York/Newark Area, Newark, New Jersey" ~
                                      "NY")) %>%
  monthly_var() %>%
  teu_data() %>%
  spread(., portofunlading, total_teu) %>%
  as.data.frame()

teu_port_two = import_us %>%
  filter(concountry == 'United States', 
         portofunlading == "Port of Tacoma, Tacoma, Washington" | 
           portofunlading == "Port of Seattle, Seattle, Washington" |
           portofunlading == "Houston, Houston, Texas" ) %>%
  mutate(portofunlading = case_when(portofunlading == "Port of Tacoma, Tacoma, Washington" | 
                                      portofunlading == "Port of Seattle, Seattle, Washington" ~ 
                                      "SEATTLE",
                                    portofunlading == "Houston, Houston, Texas" ~
                                      "HOUSTON")) %>%
  monthly_var() %>%
  teu_data() %>%
  spread(., portofunlading, total_teu) %>%
  as.data.frame() %>% collect()
  
 teu_port_three = import_us %>%
  filter(concountry == 'United States' | is.null(concountry), 
         portofunlading == 'The Port of Los Angeles, Los Angeles, California' |
           portofunlading == 'Port of Long Beach, Long Beach, California' |
           portofunlading == "Georgia Ports Authority, Savannah, Georgia") %>%
  mutate(portofunlading = case_when(portofunlading == 'The Port of Los Angeles, Los Angeles, California' ~
                                      "LA",
                                    portofunlading == 'Port of Long Beach, Long Beach, California' ~
                                      "LB",
                                    portofunlading == "Georgia Ports Authority, Savannah, Georgia" ~
                                      "SAVANNAH")) %>%
  monthly_var() %>%
  teu_data() %>%
  spread(., portofunlading, total_teu) %>%
  as.data.frame() %>% collect()

six_ports = teu_port_two %>% 
  left_join(., teu_port_three) %>%
  left_join(., teu_port_one)
  
six_ports$panjiva_six = rowSums(six_ports[, c("HOUSTON", "SEATTLE", "LA", "LB", "SAVANNAH", "NY")], na.rm = TRUE)

ports <- fame2df(c(port_la = "LAIL", port_lb = "LBIL",
                   port_seattle = "saiif", port_ny = "NYI",
                   port_savannah = "gaif", port_houston = "houil"),
                 db = "industry", 
                 start = NULL, 
                 end = NULL)

ports_all <- ports
ports_all$all_ports = rowSums(ports_all[, c("port_la", "port_lb", "port_seattle", "port_ny", "port_savannah", "port_houston")], na.rm = TRUE)

ports_all$x.date = as.Date(ports_all$date, "%Y/%m/%d")

ports_all = ports_all %>%
  filter(x.date >= "2008-01-31") %>%
  mutate(mon = month(date),
         yr = year(date)) %>%
  mutate(month = as.Date(paste0(yr,"-",mon,"-01")))

all_teu_port <- import_us %>%
  monthly_var() %>%
  teu_data2() %>%
  as.data.frame() 

all_teu_port$total_teu = ts(all_teu_port$total_teu, start = c(2007, 1), frequency = 12)

levels(all_teu_port$total_teu)[levels(all_teu_port$total_teu) == "total_teu"] <- "All ports (Panjiva)"
levels(ports_all$all_ports)[levels(ports_all$all_ports) == "all_ports"] <- "Big ports (Official port statistics)"
levels(six_ports$panjiva_six)[levels(six_ports$panjiva_six) == "panjiva_six"] <- "Big ports (Panjiva)"

data_for_chart <- merge(merge(six_ports, all_teu_port, by = "month"), ports_all, by = "month") %>%
  select(month, all_ports, total_teu, panjiva_six) %>%
  mutate(all_ports = all_ports/1000,
         total_teu = total_teu/1000,
         panjiva_six = panjiva_six/1000)  %>%
  filter(month < "2021-08-01")
  
setwd('../../data_for_paper')
write.csv(data_for_chart, 'data_teu_ports_all.csv')