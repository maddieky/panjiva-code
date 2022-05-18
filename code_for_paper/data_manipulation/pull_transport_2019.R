library(tidyverse)
library(DBI)
library(lubridate)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('../functions/functions_hstrade.R')

import_dta <- dplyr::tbl(conn_hstrade, 'imp_detl') 
export_dta <- dplyr::tbl(conn_hstrade, 'exp_detl')

# Variables of interest
#   Total imports: gen_val_mo (imp), all_val_mo (exp) 
#   Air: air_val_mo
#   Vessel: ves_val_mo
#   Other: gen_val_mo - ves_val_mo - air_val_yr

# You can make sure it matches up to Census data online (see Exhibit 4a): 
#  https://www.census.gov/foreign-trade/Press-Release/ft920_index.html 

import_data <- import_dta %>%
  mutate(oth_val_mo = gen_val_mo - ves_val_mo - air_val_mo) %>%
  mutate(date = as.Date(ym_partition), "%Y-%m") %>%
  filter(year_n == 2019) %>% 
  summarize(tot_value = sum(gen_val_mo), ves_value = sum(ves_val_mo), 
            air_value = sum(air_val_mo), oth_value = sum(oth_val_mo)) %>% 
  mutate(ves_pct = ves_value/tot_value*100, air_pct = air_value/tot_value*100, oth_pct = oth_value/tot_value*100) %>%
  select(air_pct, ves_pct, oth_pct) %>%
  collect() %>%
  gather() %>%
  mutate(type = "Imports")

export_data <- export_dta %>%
  mutate(oth_val_mo = all_val_mo - ves_val_mo - air_val_mo) %>%
  mutate(date = as.Date(ym_partition), "%Y-%m") %>%
  filter(year_n == 2019) %>% 
  summarize(tot_value = sum(all_val_mo), ves_value = sum(ves_val_mo),
            air_value = sum(air_val_mo), oth_value = sum(oth_val_mo)) %>% 
  mutate(ves_pct = ves_value/tot_value*100, air_pct = air_value/tot_value*100, 
         oth_pct = oth_value/tot_value*100) %>%
  select(air_pct, ves_pct, oth_pct) %>%
  collect() %>%
  gather() %>%
  mutate(type = "Exports") 

data_combined <- rbind(import_data, export_data) 

setwd('../../data_for_paper')
write.csv(data_combined, 'data_transport_2019.csv')

