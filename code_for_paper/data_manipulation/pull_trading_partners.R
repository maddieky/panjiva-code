library(lubridate)
library(tidyverse)
library(DBI)
library(quantmod)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('../functions/functions_panjiva.R')

import_us <- dplyr::tbl(conn_panjiva, 'panjivausimport')
export_us <- dplyr::tbl(conn_panjiva, 'panjivausexport')

# Note: Census data taken from this press release: 
#  https://www.census.gov/foreign-trade/Press-Release/edb/2018/text.pdf

# Export data
export_2018 = export_us %>%
  mutate(year = (sql("cast(year(shpmtdate) as string)"))) %>%
  filter(year == "2018") %>%
  filter(!is.na(shppanjivaid) & !is.na(shpmtdestination) & !is.na(volumeteu)) %>%
  collect() %>%
  as.data.frame()

trading_partners = export_2018 %>%
  group_by(shppanjivaid) %>%
  summarise(count = n_distinct(shpmtdestination)) 

trading_partners$categories = cut(trading_partners$count, breaks = c(0, 1,4,9, 24, 49, 200))

export_2018 = merge(x = export_2018, y = trading_partners, by = "shppanjivaid", all = TRUE)

cat_na = export_2018 %>%
  filter(is.na(categories))

summary = export_2018 %>%  
  group_by(categories) %>%
  summarize(total_teu = volumeteu) %>%
  mutate(shipments = 1) %>%
  group_by(categories) %>%
  summarize(teu = sum(total_teu), shpt = sum(shipments))

teu_count <- sum(summary$teu)
shpt_count <- sum(summary$shpt)

census_shr = c(4.9, 5.6, 6.5, 13.1, 19.6, 50.2)

to_plot = cbind(summary, teu_count, shpt_count)

teu_shr = (to_plot$teu/to_plot$teu_count)*100
shpt_shr = (to_plot$shpt/to_plot$shpt_count)*100

to_plot_wcensus = cbind(to_plot, teu_shr, shpt_shr, census_shr)

fig_export <- melt(to_plot_wcensus, id = c("categories"))
fig_export <- fig_export[25:42,]

# Import data
import_2018 = import_us %>%
  filter(concountry == "United States" | concountry == "None") %>% 
  mutate(year = (sql("cast(year(arrivaldate) as string)"))) %>%
  filter(year == "2018") %>%
  filter(!is.na(conpanjivaid) & !is.na(shpmtorigin) & !is.na(volumeteu)) %>%
  collect() %>%
  as.data.frame()

trading_partners_impo = import_2018 %>%
  collect() %>%
  group_by(conpanjivaid) %>%
  summarise(count = n_distinct(shpmtorigin)) 

trading_partners_impo$categories = cut(trading_partners_impo$count, breaks = c(0, 1,4,9, 24, 49, 200))

import_2018 = merge(x = import_2018, y = trading_partners_impo, by = "conpanjivaid", all = TRUE)

cat_na_imp =import_2018 %>%
  filter(is.na(categories))

summary_imp = import_2018 %>%  
  group_by(categories) %>%
  summarize(total_teu = volumeteu) %>%
  mutate(shipments = 1) %>%
  group_by(categories) %>%
  summarize(teu = sum(total_teu), shpt = sum(shipments))

teu_count_imp <- sum(summary_imp$teu)
shpt_count_imp <- sum(summary_imp$shpt)

census_shr_imp = c(4.6, 9.8, 9.4, 18.5, 36.0, 21.6)

to_plot_imp = cbind(summary_imp, teu_count_imp, shpt_count_imp)

teu_shr_imp = (to_plot_imp$teu/to_plot_imp$teu_count_imp)*100
shpt_shr_imp = (to_plot_imp$shpt/to_plot_imp$shpt_count_imp)*100

to_plot_wcensus_imp = cbind(to_plot_imp, teu_shr_imp, shpt_shr_imp, census_shr_imp)

fig_import <- melt(to_plot_wcensus_imp, id = c("categories"))
fig_import <- fig_import[25:42,]

setwd('../../data_for_paper')
write.csv(fig_export, 'data_trading_partners_export.csv')
write.csv(fig_import, 'data_trading_partners_import.csv')
