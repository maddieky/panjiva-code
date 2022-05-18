library(DBI)
library(quantmod)
library(tidyverse)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source("../functions/functions_panjiva.R")

import_us <- dplyr::tbl(conn_panjiva, 'panjivausimport')

data_us_import <- import_us %>%
  filter(shpmtorigin == "India") %>%
  filter(concountry == "United States" | concountry == "None") %>%
  mutate(year = (sql("cast(year(arrivaldate) as string)")),
         month = (sql("cast(month(arrivaldate) as string)")),
         day = (sql("cast(day(arrivaldate) as string)"))) %>%
  mutate(date = paste0(year, "-", month, "-", day)) %>% 
  select(panjivarecordid, date, year, month, day) %>% 
  filter(year == "2020") %>%
  filter(month >= "2" & month <= "7") %>%
  collect() %>%
  mutate(date = as.Date(date, "%Y-%m-%d")) %>%
  group_by(date) %>%
  summarize(daily_shpt = n()) %>%
  mutate(daily_shpt_wma = SMA(daily_shpt, n=7)) %>%
  filter(date >= "2020-03-01") %>%
  mutate(daily_shpt_wma_ind = 100*(daily_shpt_wma/daily_shpt_wma[1]))

export_in <- dplyr::tbl(conn_panjiva, 'panjivainexport')
data_in_export <- export_in %>%
  filter(concountry == "United States") %>%
  mutate(year = (sql("cast(year(departuredate) as string)")),
         month = (sql("cast(month(departuredate) as string)")),
         day = (sql("cast(day(departuredate) as string)"))) %>%
  mutate(date = paste0(year, "-", month, "-", day)) %>% 
  select(panjivarecordid, date, year, month, day) %>% 
  filter(year == "2020") %>%
  filter(month >= "2" & month <= "7") %>%
  collect() %>%
  mutate(date = as.Date(date, "%Y-%m-%d")) %>%
  group_by(date) %>%
  summarize(daily_shpt = n())  %>%
  mutate(daily_shpt_wma = SMA(daily_shpt, n=7)) %>%
  filter(date >= "2020-03-01") %>%
  mutate(daily_shpt_wma_ind = 100*(daily_shpt_wma/daily_shpt_wma[1]))

setwd('../../data_for_paper')
write.csv(data_us_import, 'data_intramonth_us_import.csv')
write.csv(data_in_export, 'data_intramonth_in_export.csv')
