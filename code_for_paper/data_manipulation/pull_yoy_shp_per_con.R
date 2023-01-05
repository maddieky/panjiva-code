library(tidyverse)
library(DBI)
library(lubridate)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('../functions/functions_panjiva.R')

import_us <- dplyr::tbl(conn_panjiva, 'panjivausimport')

data_shp_per_con <- import_us %>%
  filter(!is.na(shppanjivaid)) %>%
  filter(!is.na(conpanjivaid)) %>%
  filter(concountry == "United States" | is.null(concountry)) %>% 
  dplyr::select(shppanjivaid, conpanjivaid, arrivaldate) %>%
  mutate(year = (sql("cast(year(arrivaldate) as string)")),
         month = (sql("cast(month(arrivaldate) as string)"))) %>%
  mutate(date = paste0(year, "-", month, "-01")) %>% 
  distinct(date, shppanjivaid, conpanjivaid) %>% 
  group_by(date, conpanjivaid) %>% 
  summarize(num_of_ship = n()) %>% 
  group_by(date) %>% 
  summarize(mon_avg_shp = mean(num_of_ship)) %>%
  arrange(date) %>%
  collect() %>%
  mutate(date = as.Date(date)) %>%
  filter(date >= "2008-01-01") %>% 
  filter(date < "2021-09-01") %>%
  group_by(month = as.numeric(month(date))) %>%
  arrange(date) %>%
  mutate(year = year(date)) %>%
  ungroup()

# Get change from same month in 2019
data_shp_per_con_2020 <- data_shp_per_con %>%
  filter(year == "2019" | year == "2020") %>%
  arrange(month) %>%
  mutate(yoy_mon_avg_shp = (mon_avg_shp/lag(mon_avg_shp, 1))-1) %>%
  filter(year == "2020")

data_shp_per_con_2021 <- data_shp_per_con %>%
  filter(year == "2019" | year == "2021") %>%
  arrange(month) %>%
  mutate(yoy_mon_avg_shp = (mon_avg_shp/lag(mon_avg_shp, 1))-1) %>%
  filter(year == "2021")

data_shp_per_con_chart <- rbind(data_shp_per_con_2020, data_shp_per_con_2021) %>%
  dplyr::select(-year,-month)

setwd('../../data_for_paper')
write.csv(data_shp_per_con_chart, 'data_shp_per_con.csv')