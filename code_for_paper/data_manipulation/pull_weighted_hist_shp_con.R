library(lubridate)
library(tidyverse)
library(DBI)
library(quantmod)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('../functions/functions_panjiva.R')

import_us <- dplyr::tbl(conn_panjiva, 'panjivausimport')

data_consignees_teu <- import_us %>%
  filter(!is.na(shppanjivaid) & !is.na(conpanjivaid) & !is.na(volumeteu)) %>%
  filter(concountry == "United States" | is.null(concountry)) %>%
  select(shppanjivaid, conpanjivaid, arrivaldate, volumeteu) %>%
  mutate(year = (sql("cast(year(arrivaldate) as string)"))) %>%
  mutate(date = paste0(year, "-01-01")) %>% 
  filter(date == '2019-01-01') %>%
  group_by(shppanjivaid) %>%
  summarize(consignees = n_distinct(conpanjivaid), teu = sum(volumeteu)) %>%
  arrange(shppanjivaid) %>%
  mutate(teu = as.numeric(teu), consignees = as.numeric(consignees)) %>%
  collect() %>%
  mutate(bin = case_when(consignees == 1 ~ '1', 
                         consignees > 1 & consignees <= 4 ~'2',
                         consignees > 4 & consignees <= 9 ~'3',
                         consignees > 9 & consignees <= 24 ~ '4',
                         consignees > 24 ~ '5'))  

data_shippers_teu <- import_us %>%
  filter(!is.na(shppanjivaid) & !is.na(conpanjivaid) & !is.na(volumeteu)) %>%
  filter(concountry == "United States" | is.null(concountry)) %>% 
  select(shppanjivaid, conpanjivaid, arrivaldate, volumeteu) %>%
  mutate(year = (sql("cast(year(arrivaldate) as string)"))) %>%
  mutate(date = paste0(year, "-01-01")) %>% 
  filter(date == '2019-01-01') %>%
  group_by(conpanjivaid) %>%
  summarize(shippers = n_distinct(shppanjivaid), teu = sum(volumeteu)) %>%
  arrange(conpanjivaid) %>%
  mutate(teu = as.numeric(teu), shippers = as.numeric(shippers)) %>%
  collect() %>%
  mutate(bin = case_when(shippers == 1 ~ '1', 
                         shippers > 1 & shippers <= 4 ~'2',
                         shippers > 4 & shippers <= 9 ~'3',
                         shippers > 9 & shippers <= 24 ~ '4',
                         shippers > 24 ~ '5'))  

bar_data_con <- data_consignees_teu %>%
  group_by(bin) %>%
  summarize(teu = sum(teu), shippers = n()) %>%
  mutate(teu_total = sum(teu), shippers_total = sum(shippers)) %>%
  mutate(teu = teu/teu_total*100, shippers = shippers/shippers_total*100) %>%
  select(bin, teu, shippers) %>%
  pivot_longer(!bin, names_to = "type", values_to = "percent") %>%
  mutate(type = as.factor(type))

bar_data_shp <- data_shippers_teu %>%
  group_by(bin) %>%
  summarize(teu = sum(teu), consignees = n()) %>%
  mutate(teu_total = sum(teu), consignees_total = sum(consignees)) %>%
  mutate(teu = teu/teu_total*100, consignees = consignees/consignees_total*100) %>%
  select(bin, teu, consignees) %>%
  pivot_longer(!bin, names_to = "type", values_to = "percent") %>%
  mutate(type = as.factor(type))

setwd('../../data_for_paper')
write.csv(bar_data_con, 'data_weighted_hist_con_shp.csv')
write.csv(bar_data_shp, 'data_weighted_hist_shp_con.csv')