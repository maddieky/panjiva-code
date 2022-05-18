library(tidyverse)
library(DBI)
library(lubridate)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('../functions/functions_panjiva.R')

import_us <- dplyr::tbl(conn_panjiva, 'panjivausimport')
crossref <- dplyr::tbl(conn_panjiva, 'panjivacompanycrossref')

crosswalk <- crossref %>%
  select(companyid, identifiervalue) %>%
  mutate(identifiervalue = as.integer(identifiervalue)) 

imp_data <- import_us %>%
  filter(concountry == "United States" | concountry == "None") %>% 
  select(arrivaldate, volumeteu, conpanjivaid, conname) %>%
  mutate(year = (sql("cast(year(arrivaldate) as string)")),
         month = (sql("cast(month(arrivaldate) as string)"))) 

data_walmart <- left_join(imp_data, crosswalk, by = c("conpanjivaid" = "identifiervalue")) %>%
  filter(companyid == 313055) %>% # (NYSE:WMT) Walmart Inc.
  collect()

teu_shpt_by_month <- data_walmart %>%
  group_by(year, month) %>%
  summarize(total_teu = sum(volumeteu, na.rm = TRUE),
            total_shpt = sum(n(), na.rm=TRUE)) %>%
  mutate(date = paste0(year, "-", month, "-01")) %>% 
  mutate(date = as.Date(date)) %>%
  ungroup() %>%
  select(-year,-month) %>%
  arrange(date) %>%
  filter(date < "2021-08-01")

setwd('../../data_for_paper')
write.csv(teu_shpt_by_month, 'data_walmart_redaction.csv')