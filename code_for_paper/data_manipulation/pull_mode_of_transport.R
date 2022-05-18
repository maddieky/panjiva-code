library(tidyverse)
library(DBI)
library(lubridate)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('../functions/functions_hstrade.R')

import_dta <- dplyr::tbl(conn_hstrade, 'imp_detl') 

# Variables of interest
#   Total imports: gen_val_mo (imp), all_val_mo (exp)
#   Air: air_val_mo
#   Vessel: ves_val_mo
#   Other: gen_val_mo - ves_val_mo - air_val_yr

# You can make sure it matches up to Census data online (see Exhibit 4a): https://www.census.gov/foreign-trade/Press-Release/ft920_index.html 
monthly_import_data <- import_dta %>%
  mutate(oth_val_mo = gen_val_mo - ves_val_mo - air_val_mo) %>%
  mutate(date = as.Date(ym_partition), "%Y-%m") %>%
  group_by(date) %>% 
  summarize(ves_value = sum(ves_val_mo), air_value = sum(air_val_mo), oth_value = sum(oth_val_mo)) %>%
  select(date, ves_value, air_value, oth_value) %>%
  mutate(ves_value = ves_value/1000000000, air_value = air_value/1000000000, oth_value = oth_value/1000000000) %>%
  arrange(date) %>% 
  collect() %>%
  mutate(date = as.Date(paste0(date,"01"), "%Y%m%d")) %>%
  filter(date <= '2021-07-01') 

setwd('../../data_for_paper')
write.csv(monthly_import_data, 'data_mode_of_transport_import.csv')


