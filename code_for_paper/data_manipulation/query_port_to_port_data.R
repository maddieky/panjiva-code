library(tidyverse)
library(DBI)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source("../functions/functions_panjiva.R")

import_us <- dplyr::tbl(conn_panjiva, 'panjivausimport')

data <- import_us %>%
  filter(concountry == "United States" | is.null(concountry)) %>%
  select(arrivaldate, portofunlading, portoflading, vessel, vesselimo, vesselvoyageid, volumeteu, weightkg) %>%
  mutate(year = (sql("cast(year(arrivaldate) as string)")),
         month = (sql("cast(month(arrivaldate) as string)")),
         day = (sql("cast(day(arrivaldate) as string)"))) %>%
  mutate(date = paste0(year, "-", month, "-", day)) %>%
  mutate(year = as.numeric(year)) %>%
  filter(year >= 2012) %>%
  group_by(portofunlading, portoflading, vessel, vesselimo, vesselvoyageid, date) %>%
  summarize(volumeteu = sum(volumeteu), weightkg = sum(weightkg), shipments = n()) %>%
  collect() %>%
  mutate(date = as.Date(date)) %>%
  arrange(date)

setwd('../../intermediate_dta_files')
write.csv(data, "port_analysis_data_daily_from_2012.csv")