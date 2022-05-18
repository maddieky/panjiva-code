library(lubridate)
library(tidyverse)
library(DBI)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source("../functions/functions_panjiva.R")

import_us <- dplyr::tbl(conn_panjiva, 'panjivausimport')

data_all <- import_us %>% 
  filter(concountry == "United States" | concountry == "None") %>% 
  mutate(year = (sql("cast(year(arrivaldate) as string)"))) %>%
  mutate(month = (sql("cast(month(arrivaldate) as string)"))) %>%
  mutate(date = paste0(year, "-", month, "-01")) %>%
  filter(year == "2021" | year == "2020" | year == "2019" | year == "2018" | year == "2017" | year == "2016") %>%
  mutate(quarter = (sql("cast(quarter(arrivaldate) as string)"))) %>%
  mutate(qdate = paste0(year, "-", quarter)) %>%
  mutate(l_shpt = 1) %>% 
  group_by(qdate, shpmtorigin, conpanjivaid, shppanjivaid) %>%
  summarize(shpt = sum(l_shpt), vol_teu = sum(volumeteu), weight_tot = sum(weightkg)) %>% 
  collect() 

setwd('../../intermediate_dta_files')
write.csv(data_all, "full_imports_for_decomp.csv")
