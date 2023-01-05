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
  filter(concountry == "United States" | is.null(concountry)) %>% 
  select(arrivaldate, volumeteu, conpanjivaid, conname, conoriginalformat) %>%
  mutate(year = (sql("cast(year(arrivaldate) as string)")),
         month = (sql("cast(month(arrivaldate) as string)"))) 

# Pull Walmart data using companyID
data_walmart_companyid <- left_join(imp_data, crosswalk, by = c("conpanjivaid" = "identifiervalue")) %>%
  filter(companyid == 313055) %>% # (NYSE:WMT) Walmart Inc.
  collect() 

# Using conname: replace very low occurrences strings with "other"
replace_names_list <- list('"Walmart Inc."', '"Walmart Inc. 601 N"', '"Walmart Inc. Edi Food 310"',
                           '"Walmart Inc.110522"', '"Walmart Incwalmart Inc."')

teu_shpt_by_month_companyid <- data_walmart_companyid %>%
  mutate(conname = paste0('"',conname,'"')) %>%
  mutate(conname = replace(conname, conname %in% replace_names_list, "Other")) %>%
  group_by(year, month, conname) %>%
  summarize(total_teu_companyid = sum(volumeteu, na.rm = TRUE),
            total_shpt_companyid = sum(n(), na.rm=TRUE)) %>%
  mutate(date = paste0(year, "-", month, "-01")) %>% 
  mutate(date = as.Date(date)) %>%
  ungroup() %>%
  select(-year,-month) %>%
  arrange(date) %>%
  filter(date < "2021-08-01")

# Fill in missing data with 0 so stacked area chart will be prettier
date <- unique(teu_shpt_by_month_companyid$date)
conname <- unique(teu_shpt_by_month_companyid$conname)
combinations <- expand.grid(date = date, conname = conname)

teu_shpt_by_month_companyid <- full_join(teu_shpt_by_month_companyid, combinations,
                                         by = c("date" = "date", "conname" = "conname")) %>%
  mutate(total_shpt_companyid = ifelse(is.na(total_shpt_companyid), 0, total_shpt_companyid )) %>%
  arrange(date, conname) 

setwd('../../data_for_paper')
write.csv(teu_shpt_by_month, 'data_walmart_redaction.csv')