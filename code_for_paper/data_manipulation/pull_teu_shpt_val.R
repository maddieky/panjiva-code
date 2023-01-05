library(lubridate)
library(tidyverse)
library(DBI)
library(seasonal)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('../functions/functions_hstrade.R')
source('../functions/functions_panjiva.R')
source('../functions/functions_teu_shpt_val.R')

import_us <- dplyr::tbl(conn_panjiva, 'panjivausimport')
imp_detl <- dplyr::tbl(conn_hstrade, 'imp_detl')

teu_agg_mon = import_us %>% 
  filter(concountry == "United States" | is.null(concountry)) %>% 
  month_var() %>% 
  group_by(date) %>% 
  teu_data_mon() %>%
  select(date, total_teu) %>% 
  mutate(total_teu = as.double(total_teu)) %>%
  arrange(date)

shpt_agg_mon = import_us %>%
  filter(concountry == "United States" | is.null(concountry)) %>% 
  month_var() %>%
  group_by(date) %>% 
  shpt_data_mon() %>%
  select(date, total_shpt) %>% 
  mutate(total_shpt = as.double(total_shpt)) %>%
  arrange(date)

value_agg_mon <- imp_detl %>% 
  fix_ym_partition() %>%
  group_by(date) %>%
  value_data_mon() %>%
  select(date, tot_value, cnt_value, ves_value, cnt_weight, ves_weight) %>% 
  mutate(tot_value = as.double(tot_value), cnt_value = as.double(cnt_value), ves_value = as.double(ves_value),
         cnt_weight = as.double(cnt_weight), ves_weight = as.double(ves_weight)) %>%
  arrange(date)  %>% collect()

value_agg_mon_no_mxcn <- imp_detl %>% 
  filter(cty_code != "1220" & cty_code != "2010") %>%
  fix_ym_partition() %>%
  group_by(date) %>%
  value_data_mon() %>%
  mutate(tot_value_no_cnmx = as.double(tot_value)) %>%
  select(date, tot_value_no_cnmx) %>% 
  arrange(date)  %>% collect()

data_mon <- left_join(left_join(left_join(teu_agg_mon, shpt_agg_mon, by="date"), value_agg_mon, by="date"), value_agg_mon_no_mxcn, by = "date") %>%
  filter(date <= as.Date('2021-07-01')) %>%
  mutate(total_shpt = ts(total_shpt, start = "2009", frequency = 12)) %>%
  mutate(total_shpt = final(seas(total_shpt))) %>%
  mutate(total_teu = ts(total_teu, start = "2009", frequency = 12)) %>%
  mutate(total_teu = final(seas(total_teu))) %>%
  mutate(cnt_value = ts(cnt_value, start = "2009", frequency = 12)) %>%
  mutate(cnt_value = final(seas(cnt_value))) %>%
  mutate(tot_value = ts(tot_value, start = "2009", frequency = 12)) %>%
  mutate(tot_value = final(seas(tot_value))) %>%
  mutate(ves_value = ts(ves_value, start = "2009", frequency = 12)) %>%
  mutate(ves_value = final(seas(ves_value))) %>%
  mutate(cnt_weight = ts(cnt_weight, start = "2009", frequency = 12)) %>%
  mutate(cnt_weight = final(seas(cnt_weight))) %>%
  mutate(ves_weight = ts(ves_weight, start = "2009", frequency = 12)) %>%
  mutate(ves_weight = final(seas(ves_weight))) %>%
  mutate(tot_value_no_cnmx = ts(tot_value_no_cnmx, start = "2009", frequency = 12)) %>%
  mutate(tot_value_no_cnmx = final(seas(tot_value_no_cnmx))) %>%
  index_dplr_mon(., "tot") %>%
  index_dplr_mon(., "cnt") %>%
  index_dplr_mon(., "ves")

setwd('../../data_for_paper')
write.csv(data_mon, 'data_teu_shpt_val.csv')