library(tidyverse)
library(DBI)
library(lubridate)
library(xtable)
library(readxl)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('../code_for_paper/functions_hstrade.R')
source('../code_for_paper/chart_settings.R')

# creating table object that is actually a connection to hadoop table
import_dta <- dplyr::tbl(conn_hstrade, 'imp_detl') 
export_dta <- dplyr::tbl(conn_hstrade, 'exp_detl')

# find top trading partners
# imports: China, Mexico, Canada, Japan, Germany
import_partners <- import_dta %>%
  filter(year_n == 2019) %>% 
  group_by(cty_code) %>%
  summarize(imp_value = sum(gen_val_mo)) %>%
  arrange(desc(imp_value)) %>%
  collect()

# exports: Canada, Mexico, China, Japan, United Kingdom
export_partners <- export_dta %>%
  filter(year_n == 2019) %>% 
  group_by(cty_code) %>%
  summarize(exp_value = sum(all_val_mo)) %>%
  arrange(desc(exp_value)) %>%
  collect()

total_partners <- left_join(import_partners, export_partners, by = "cty_code") %>%
  mutate(tot_value = imp_value + exp_value) %>%
  arrange(desc(tot_value))


# Variables of interest
#   Total imports: gen_val_mo (imp), all_val_mo (exp) ### double check - is this right?
#   Air: air_val_mo
#   Vessel: ves_val_mo
#   Other: gen_val_mo - ves_val_mo - air_val_yr

# 2010 - Mexico, 1220 - Canada, 5700 - China, 5880 - Japan, 4280 - Germany 

# You can make sure it matches up to Census data online (see Exhibit 4a): https://www.census.gov/foreign-trade/Press-Release/ft920_index.html 
import_data <- import_dta %>%
  mutate(oth_val_mo = gen_val_mo - ves_val_mo - air_val_mo) %>%
  mutate(date = as.Date(ym_partition), "%Y-%m") %>%
  filter(year_n == 2019) %>% 
  filter(cty_code == "4280" | cty_code == "5880" |
           cty_code == "5700" | cty_code == "1220" | cty_code == "2010") %>%
  group_by(cty_code) %>%
  summarize(tot_value = sum(gen_val_mo), ves_value = sum(ves_val_mo), 
            air_value = sum(air_val_mo), oth_value = sum(oth_val_mo)) %>% 
  mutate(ves_pct = ves_value/tot_value*100, air_pct = air_value/tot_value*100, 
         oth_pct = oth_value/tot_value*100) %>%
  dplyr::select(cty_code, tot_value, ves_pct, air_pct, oth_pct) %>%
  collect() %>%
  mutate(cty_code = as.double(cty_code)) %>%
  mutate(country = case_when(cty_code == "2010" ~ "1",
                             cty_code == "1220" ~ "2",
                             cty_code == "5700" ~ "3",
                             cty_code == "5880" ~ "4",
                             cty_code == "4280" ~ "5")) %>%
  mutate(country = as.double(country)) %>%
  dplyr::select(-cty_code) %>%
  mutate(tot_value = tot_value/1000000000) %>% 
  arrange(country)

export_data <- export_dta %>%
  mutate(oth_val_mo = all_val_mo - ves_val_mo - air_val_mo) %>%
  mutate(date = as.Date(ym_partition), "%Y-%m") %>%
  filter(year_n == 2019) %>% 
  filter(cty_code == "2010" | cty_code == "1220" |
           cty_code == "5700" | cty_code == "5880" | cty_code == "4280") %>%
  group_by(cty_code) %>%
  summarize(tot_value = sum(all_val_mo), ves_value = sum(ves_val_mo), 
            air_value = sum(air_val_mo), oth_value = sum(oth_val_mo)) %>% 
  mutate(ves_pct = ves_value/tot_value*100, air_pct = air_value/tot_value*100, 
         oth_pct = oth_value/tot_value*100) %>%
  dplyr::select(cty_code, tot_value, ves_pct, air_pct, oth_pct) %>%
  collect() %>%
  mutate(cty_code = as.double(cty_code)) %>%
  mutate(country = case_when(cty_code == "2010" ~ "1",
                             cty_code == "1220" ~ "2",
                             cty_code == "5700" ~ "3",
                             cty_code == "5880" ~ "4",
                             cty_code == "4280" ~ "5")) %>%
  mutate(country = as.double(country)) %>%
  dplyr::select(-cty_code) %>%
  mutate(tot_value = tot_value/1000000000) %>% 
  arrange(country)

# Make table
column1 <- c(sprintf("%.2f",import_data$ves_pct))
column2 <- c(sprintf("%.2f",import_data$air_pct))
column3 <- c(sprintf("%.2f",import_data$oth_pct))
column4 <- c(sprintf("%.2f",export_data$ves_pct))
column5 <- c(sprintf("%.2f",export_data$air_pct))
column6 <- c(sprintf("%.2f",export_data$oth_pct))
column6a <- c(sprintf("%.2f",export_data$tot_value+import_data$tot_value))

table <- cbind(column1, column2, column3, column4, column5, column6, column6a) 

rownames(table) <- c('\\makecell[l]{Mexico}', '\\makecell[l]{Canada}', '\\makecell[l]{China}', '\\makecell[l]{Japan}', '\\makecell[l]{Germany}')

# Write the tex file --------------------
cat(paste(
  "\\documentclass{article}\n", 
  "\\usepackage[utf8]{inputenc}\n", 
  "\\usepackage{makecell}\n", 
  "\\begin{document}\n",
  "\\begin{table}[ht]\n",
  "\\caption{Trade Shares by Mode of Transport, 2019}\n",
  "\\centering \n",
  "\\begin{tabular}{c c c c c c c c}\n", 
  "\\hline \n", 
  "\\hline \n", 
  "& \\multicolumn{3}{c}{Imports} & \\multicolumn{3}{c}{Exports} & \\multicolumn{1}{c}{Total Value*} \\\\ \n",
  "& \\makecell{Vessel} & \\makecell{Air} & \\makecell{Other} & \\makecell{Vessel} & \\makecell{Air} & \\makecell{Other} & \\makecell{} \\\\",
  
  print.xtable(table, only.contents=TRUE, sanitize.text.function = function(x){x}),
  
  "\\multicolumn{8}{l}{\\footnotesize Source: U.S. Census. Includes top 5 U.S. trading partners by value. *In billions.} \\\\",
  "\\hline \n",
  "\\end{tabular} \n",
  "\\label{table:nonlin} \n",
  "\\end{table},",
  "\\end{document}",
  print("")),
  
  file = paste0("tab_transport_2019_by_country_level.tex")
)


# Run the tex file --------------------
# Run twice because latex is weird sometimes
system(paste0("pdflatex '","tab_transport_2019_by_country_level.tex'"))
system(paste0("pdflatex '","tab_transport_2019_by_country_level.tex'"))

system(paste0("pdftocairo -png -singlefile '","tab_transport_2019_by_country_level.pdf' '","tab_transport_2019_by_country_level'"))
rdn_summary <- as.raster(readPNG("tab_transport_2019_by_country_level.png"))
