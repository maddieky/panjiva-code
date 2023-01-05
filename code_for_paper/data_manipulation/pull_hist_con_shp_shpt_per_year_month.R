library(lubridate)
library(tidyverse)
library(DBI)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source("../functions/functions_panjiva.R")

import_us <- dplyr::tbl(conn_panjiva, 'panjivausimport')

df <- NULL

# For each year, we are looking for how many months of the year each shipper-consignee pair interacted
#   IF that pair also occurred in the previous year
for (i in 2019:2019) {
  x = toString(i)
  y = toString(i -1)
  
  data_record <- import_us %>% 
    filter(concountry == "United States" | is.null(concountry)) %>%  
    select(arrivaldate, panjivarecordid, shppanjivaid, conpanjivaid, volumeteu) %>%
    mutate(year = (sql("cast(year(arrivaldate) as string)"))) %>%
    mutate(month = (sql("cast(month(arrivaldate) as string)"))) %>%
    filter(year == x | year == y) %>%
    filter(shppanjivaid != 0 & conpanjivaid != 0)  
  
  # keep one occurrence of each shipper/consignee pair per year, month
  #   then group by year and sum up how many occurrences of each shipper/consignee pair there are per year
  #   arrange by year (e.g. 2020, then 2021)
  data <- data_record %>%
    select(year, month, shppanjivaid, conpanjivaid, volumeteu) %>%
    collect() %>%
    group_by(year, month, shppanjivaid, conpanjivaid) %>%
    summarize(teu = sum(volumeteu)) %>%
    ungroup() %>% 
    group_by(year, conpanjivaid, shppanjivaid) %>%
    summarize(count = n(), teu= sum(teu)) %>%
    arrange(year)
  
  # then find any duplicated pairs and mark the SECOND occurrence (eg the 2021 occurrence).
  #   Then only keep the 2021 observations that are marked
  data_dropped <- data %>%
    group_by(conpanjivaid, shppanjivaid) %>%
    mutate(Index = 1:n()) %>%
    ungroup() %>%
    filter(year == x) %>%
    filter(Index == 2) %>%
    select(year, count, teu)
  
  # bind this year of data to overall dataset
  df <- rbind(df, data_dropped) %>%
    mutate(count = as.double(count)) 
}

df_grouped <- df %>%
  group_by(count) %>%
  summarize(teu = sum(teu, na.rm=TRUE), n = n()) 

totals <- df_grouped %>%
  summarize(teu = sum(teu), n = sum(n)) 

df_chart <- df_grouped %>%
  mutate(teu = teu/totals$teu,
         n = n/totals$n)

setwd('../../data_for_paper')
write.csv(df_chart, 'data_hist_con_shp_shpt_per_year_month.csv')