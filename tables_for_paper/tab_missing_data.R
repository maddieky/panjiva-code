library(tidyverse)
library(DBI)
library(xtable)
library(grDevices)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('../code_for_paper/functions_panjiva.R')

# Create hadoop connection
import_us <- dplyr::tbl(conn_panjiva, 'panjivausimport')
dataimp <- import_us %>% select(arrivaldate, shpmtorigin, panjivarecordid)
datahs <- dplyr::tbl(conn_panjiva, 'panjivausimphscode')

# missing shippers
shipper_na_yr <- import_us %>%
  year_date() %>%
  filter(is.na(shppanjivaid)) %>%
  summarize(missing_shippers= n()) %>%
  select(year, missing_shippers) %>% 
  arrange(year)  %>% collect()

# missing consignees
consignee_na_yr <- import_us %>%
  year_date() %>%
  filter(is.na(conpanjivaid)) %>%
  summarize(missing_consignees= n()) %>%
  select(year, missing_consignees) %>% 
  arrange(year)  %>% collect()

# missing source country
country_na_yr <- import_us %>%
  year_date() %>%
  filter(shpmtorigin == "None") %>%
  summarize(missing_country= n()) %>%
  select(year, missing_country) %>% 
  arrange(year)  %>% collect()

# missing hs code
hscode_na_yr<- left_join(dataimp, datahs, by = "panjivarecordid") %>%
  year_date() %>%
  filter(hscode == "None") %>%
  summarize(missing_hscode= n()) %>%
  select(year, missing_hscode) %>% 
  arrange(year)  %>% collect()

# missing teu
teu_na_yr <- import_us %>%
  year_date() %>%
  filter(is.na(volumeteu)) %>%
  summarize(missing_teu= n()) %>%
  select(year, missing_teu) %>% 
  arrange(year)  %>% collect()

# missing value
value_na_yr <- import_us %>%
  year_date() %>%
  filter(is.na(valueofgoodsusd)) %>%
  summarize(missing_value= n()) %>%
  select(year, missing_value) %>% 
  arrange(year)  %>% collect()

total_yr <- import_us %>%
  year_date() %>%
  summarize(total_yr= n()) %>%
  select(year, total_yr) %>% 
  arrange(year)  %>% collect()

yearly <- Reduce(function(x,y) merge(x,y,by="year",all=TRUE),list(shipper_na_yr, consignee_na_yr, country_na_yr, hscode_na_yr, teu_na_yr, value_na_yr, total_yr))

#### Make table --------------------
tbl_data <- yearly %>%
  mutate(missing_shippers_pct = sprintf("%.1f", missing_shippers/total_yr*100),
         missing_consignees_pct = sprintf("%.1f",missing_consignees/total_yr*100),
         missing_country_pct = sprintf("%.1f",missing_country/total_yr*100),
         missing_hscode_pct = sprintf("%.1f",missing_hscode/total_yr*100),
         missing_teu_pct = sprintf("%.1f",missing_teu/total_yr*100),
         missing_value_pct = sprintf("%.1f",missing_value/total_yr*100))

### Assign your columns ----------------
column1 <- c(tbl_data$missing_shippers_pct)
column2 <- c(tbl_data$missing_consignees_pct)
column4 <- c(tbl_data$missing_hscode_pct)
column5 <- c(tbl_data$missing_teu_pct)
column6 <- c(tbl_data$missing_value_pct)
table <- cbind(column1, column2, column4, column5, column6) 
# Note that the percentage of missing data for shipment origin country and weight are < 1 %

rownames(table) <- c('2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', 
                     '2015', '2016', '2017', '2018', '2019', '2020', '2021')

# Write the tex file --------------------

cat(paste(
  "\\documentclass{article}\n", 
  "\\usepackage[utf8]{inputenc}\n", 
  "\\begin{document}\n",
  "\\begin{table}[ht]\n",
  "\\caption{Missing U.S. Import Data by Variable, in Percent}\n",
  "\\centering \n",
  "\\begin{tabular}{c c c c c c}\n", 
  "\\hline \n",
  "\\hline \n", 
  "& Shipper ID & Consignee ID & HS Code & TEU & Value\\\\ \n",
  
  print.xtable(table, only.contents=TRUE, sanitize.text.function = function(x){x}),
  
  "\\multicolumn{6}{l}{\\footnotesize Source: Panjiva.} \\\\",
  "\\hline \n",
  "\\end{tabular} \n",
  "\\label{table:nonlin} \n",
  "\\end{table},",
  "\\end{document}",
  print("")),
  
  file = paste0("tab_missing_data.tex")
)


# Run the tex file --------------------
system(paste0("pdflatex '","tab_missing_data.tex'"))
system(paste0("pdflatex '","tab_missing_data.tex'"))

system(paste0("pdftocairo -png -singlefile '","tab_missing_data.pdf' '","tab_missing_data'"))
rdn_summary <- as.raster(readPNG("tab_missing_data.png"))