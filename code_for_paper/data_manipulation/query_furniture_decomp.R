library(lubridate)
library(tidyverse)
library(DBI)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source("../functions/functions_panjiva.R")

import_us <- dplyr::tbl(conn_panjiva, 'panjivausimport')

data_record_mon_ex <- import_us %>% 
  mutate(year = (sql("cast(year(arrivaldate) as string)"))) %>%
  mutate(month = (sql("cast(month(arrivaldate) as string)"))) %>%
  mutate(quarter = (sql("cast(quarter(arrivaldate) as string)"))) %>%
  mutate(date = paste0(year, "-", month, "-01")) %>%
  filter(year == "2021" | year == "2020" | year == "2019" | year == "2018" | year == "2017" | year == "2016") %>%
  filter(concountry == "United States" | is.null(concountry))

import_us_hs <- dplyr::tbl(conn_panjiva, 'panjivausimphscode')

data_hs <- import_us_hs %>%
  filter(hscode %LIKE% "%94%")

data_furniture_mon <- inner_join(data_hs, data_record_mon_ex, by = "panjivarecordid") %>% 
  collect() %>% 
  mutate(date = as.Date(date)) %>% 
  filter(str_detect(hscode, "^94|\\s{1,}94")) %>%
  mutate(qdate = paste0(year, "-", quarter)) %>%
  mutate(l_shpt = 1) %>%
  mutate(conpanjivaid = as.integer(conpanjivaid), shppanjivaid = as.integer(shppanjivaid)) %>%
  group_by(qdate, shpmtorigin, conpanjivaid, shppanjivaid) %>%
  summarize(shpt = sum(l_shpt, na.rm = TRUE), vol_teu = sum(volumeteu, na.rm = TRUE), weight_tot = sum(weightkg, na.rm = TRUE)) %>% 
  collect() 

setwd('../../intermediate_dta_files')
write.csv(data_furniture_mon, "furniture_imports_for_decomp.csv")