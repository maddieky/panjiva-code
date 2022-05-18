library(lubridate)
library(tidyverse)
library(DBI)
library(policyPlot)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source("../functions/functions_panjiva.R")
source('../functions/functions_ports.R')

import_us <- dplyr::tbl(conn_panjiva, 'panjivausimport')

teu_port_four = import_us %>%
  filter(concountry == 'United States' | concountry == 'None', 
         portofunlading == 'The Port of Los Angeles, Los Angeles, California' |
           portofunlading == 'Port of Long Beach, Long Beach, California' |
           portofunlading == "New York/Newark Area, Newark, New Jersey" |
           portofunlading == "Georgia Ports Authority, Savannah, Georgia") %>%
  mutate(portofunlading = case_when(portofunlading == 'The Port of Los Angeles, Los Angeles, California' ~
                                      "LA",
                                    portofunlading == "New York/Newark Area, Newark, New Jersey" ~
                                      "NY",
                                    portofunlading == 'The Port of Los Angeles, Los Angeles, California' ~
                                      "LA",
                                    portofunlading == 'Port of Long Beach, Long Beach, California' ~
                                      "LB",
                                    portofunlading == "Georgia Ports Authority, Savannah, Georgia" ~
                                      "SAVANNAH")) %>%
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
  as.data.frame()

six_ports = left_join(teu_port_two, teu_port_four)

haver_teu <- fame2df(c(PORT_LA = "LAIL", PORT_LB = "LBIL", 
					   PORT_SEATTLE = "saiif", PORT_NY = "NYI", 
					   PORT_SAVANNAH = "gaif", PORT_HOUSTON = "houil"), 
                     db = "industry", 
                     start = NULL, 
                     end = NULL) %>%
  filter(date >= "2008-01-01") %>%
  mutate(year = year(date), month = month(date)) %>%
  mutate(month = as.Date(paste0(year, "-", month, "-01")))

in_thousands <- function(col) {
  col/1000
}

chart_data<- merge(haver_teu, six_ports, by="month") %>% 
  filter(month >= '2018-01-01') %>% 
  filter(month < '2021-08-01') %>%
  mutate_if(is.numeric, in_thousands) %>%
  select(-date, -year)

setwd('../../data_for_paper')
write.csv(chart_data, 'data_ports_teu.csv')
