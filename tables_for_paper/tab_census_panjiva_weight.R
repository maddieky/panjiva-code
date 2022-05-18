library(tidyverse)
library(DBI)
library(xtable)
library(readxl)
library(lubridate)
library(fame)
library(grDevices)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('../code_for_paper/functions_panjiva.R')

# create hadoop connection
import_us <- dplyr::tbl(conn_panjiva, 'panjivausimport')

# Get Panjiva weight
panjiva_weight_four <- import_us %>% 
  mutate(year = (sql("cast(year(arrivaldate) as string)"))) %>%
  filter(concountry == "United States" | concountry == "None") %>% 
  group_by(portofunlading, year) %>%
  summarize(weight_panjiva= sum(weightkg)) %>%
  mutate(port = case_when(portofunlading == 'The Port of Los Angeles, Los Angeles, California' ~
                            "Los Angeles, CA",
                          portofunlading == "New York/Newark Area, Newark, New Jersey" ~
                            "Newark, NJ",
                          portofunlading == 'The Port of Los Angeles, Los Angeles, California' ~
                            "Los Angeles, CA",
                          portofunlading == 'Port of Long Beach, Long Beach, California' ~
                            "Long Beach, CA",
                          portofunlading == "Georgia Ports Authority, Savannah, Georgia" ~
                            "Savannah, GA")) %>% 
  group_by(year, port) %>%
  summarize(weight_panjiva = sum(weight_panjiva)) %>%
  arrange(port, year) %>% collect()

panjiva_weight_two <- import_us %>% 
  mutate(year = (sql("cast(year(arrivaldate) as string)"))) %>%
  filter(concountry == "United States") %>% 
  group_by(portofunlading, year) %>%
  summarize(weight_panjiva= sum(weightkg)) %>%
  mutate(port = case_when(portofunlading == "Port of Tacoma, Tacoma, Washington" | 
                            portofunlading == "Port of Seattle, Seattle, Washington" ~ 
                            "Seattle/Tacoma, WA",
                          portofunlading == "Houston, Houston, Texas" ~
                            "Houston, TX")) %>% 
  group_by(year, port) %>%
  summarize(weight_panjiva = sum(weight_panjiva)) %>%
  arrange(port, year) %>% collect()

panjiva_weight <- rbind(panjiva_weight_four, panjiva_weight_two)

total_panjiva_weight_ex_two <- import_us %>%
  filter(concountry == "United States" | concountry == "None") %>%
  filter(portofunlading != "Port of Tacoma, Tacoma, Washington" &
           portofunlading != "Port of Seattle, Seattle, Washington" &
         portofunlading != "Houston, Houston, Texas") %>%
  mutate(year = (sql("cast(year(arrivaldate) as string)"))) %>%
  group_by(year) %>%
  summarize(total_weight_panjiva_ex_two= sum(weightkg)) %>%
  select(year, total_weight_panjiva_ex_two) %>% collect()
  
total_panjiva_weight_two <- import_us %>%
    filter(concountry == "United States") %>% 
    filter(portofunlading == "Port of Tacoma, Tacoma, Washington" | 
             portofunlading == "Port of Seattle, Seattle, Washington" |
             portofunlading == "Houston, Houston, Texas") %>%
  mutate(year = (sql("cast(year(arrivaldate) as string)"))) %>%
    group_by(year) %>%
    summarize(total_weight_panjiva_two= sum(weightkg)) %>%
    select(year, total_weight_panjiva_two) %>% collect()

total <- total_panjiva_weight_ex_two$total_weight_panjiva_ex_two+total_panjiva_weight_two$total_weight_panjiva_two
total_panjiva_weight <- as.data.frame(cbind(total_panjiva_weight_two$year, total))
colnames(total_panjiva_weight) <- c('year','total_panjiva_weight')
  
# Get Census weight
census_weight <- NULL
total_census_weight <- NULL

for (i in 2011:2019) {
  # We downloaded xls files from Census, Exhibit 4a ("U.S. General Imports - U.S. Port of Unlading 
  #   and Method of Transportation") for 2011-2019 into data_for_paper/censusdata
  append_this <- read_excel(paste0("../data_for_paper/censusdata/",i, ".xls"), range = "B9:H1000", col_names = FALSE) %>% 
    select(1, 7) %>%
    rename(port = 1, weight_census=2) %>%
    mutate(weight_census = as.double(weight_census) * 1000000) %>%
    filter(port == 'Houston, TX' | port == 'Newark, NJ' | 
	       port == 'Long Beach, CA' | port == 'Seattle, WA' | 
		   port == 'Tacoma, WA' | port == 'Savannah, GA' | 
		   port == 'Los Angeles, CA') %>%
    arrange(desc(weight_census)) %>% 
    mutate(year = i) %>%
    mutate(year = as.character(year)) %>%
    mutate(port = case_when(port == 'Los Angeles, CA' ~
                              "Los Angeles, CA",
                            port == "Tacoma, WA" | 
                              port == "Seattle, WA" ~ 
                              "Seattle/Tacoma, WA",
                            port == "Newark, NJ" ~
                              "Newark, NJ",
                            port == 'Los Angeles, CA' ~
                              "Los Angeles, CA",
                            port == 'Long Beach, CA' ~
                              "Long Beach, CA",
                            port == "Savannah, GA" ~
                              "Savannah, GA",
                            port == "Houston, TX" ~
                              "Houston, TX")) %>%
    group_by(port, year) %>% summarize(weight_census = sum(weight_census)) %>%
    collect()
  
  census_weight<-bind_rows(census_weight, append_this)
  
  append_this_total <- read_excel(paste0("../data_for_paper/censusdata/",i,".xls"), range = "B9:H448", col_names = FALSE) %>% 
    select(1, 7) %>%
    rename(port = 1, total_weight_census=2) %>%
    mutate(total_weight_census = as.double(total_weight_census) * 1000000) %>%
    filter(port == 'Total') %>% 
    select(total_weight_census) %>% 
    mutate(year = i) %>% 
    mutate(year = as.character(year)) %>% collect()
  
  total_census_weight<-bind_rows(total_census_weight, append_this_total)
}

weight_comparison  <- left_join(left_join(left_join(census_weight, panjiva_weight, by = c("port"="port", "year"="year")), total_panjiva_weight), total_census_weight) %>%
  mutate(weight_share_panjiva = weight_panjiva / total_weight_panjiva) %>%
  mutate(weight_share_census = weight_census / total_weight_census) %>%
  mutate(year = as.Date(paste0("01-01-", year), format = "%m-%d-%Y"))

# Get Panjiva TEU
panjiva_teu_four = import_us %>%
  filter(concountry == "United States" | concountry == "None") %>% 
  filter(portofunlading == 'The Port of Los Angeles, Los Angeles, California' |
           portofunlading == 'Port of Long Beach, Long Beach, California' |
           portofunlading == "New York/Newark Area, Newark, New Jersey" |
           portofunlading == "Georgia Ports Authority, Savannah, Georgia") %>%
  mutate(port = case_when(portofunlading == 'The Port of Los Angeles, Los Angeles, California' ~
                            "Los Angeles, CA",
                          portofunlading == "New York/Newark Area, Newark, New Jersey" ~
                            "Newark, NJ",
                          portofunlading == 'The Port of Los Angeles, Los Angeles, California' ~
                            "Los Angeles, CA",
                          portofunlading == 'Port of Long Beach, Long Beach, California' ~
                            "Long Beach, CA",
                          portofunlading == "Georgia Ports Authority, Savannah, Georgia" ~
                            "Savannah, GA")) %>%
  mutate(year = (sql("cast(year(arrivaldate) as string)"))) %>%
  group_by(port, year) %>% 
  summarize(teu_panjiva= sum(volumeteu)) %>%
  arrange(year) %>% collect() %>%  
  mutate(year = paste0(year,"-01-01")) %>%
  mutate(year = as.Date(year, "%Y-%m-%d")) %>%
  as.data.frame() 

panjiva_teu_two = import_us %>%
  filter(concountry == "United States") %>% 
  filter(portofunlading == "Port of Tacoma, Tacoma, Washington" | 
           portofunlading == "Port of Seattle, Seattle, Washington" |
           portofunlading == "Houston, Houston, Texas" ) %>%
  mutate(port = case_when(portofunlading == "Port of Tacoma, Tacoma, Washington" | 
                            portofunlading == "Port of Seattle, Seattle, Washington" ~ 
                            "Seattle/Tacoma, WA",
                          portofunlading == "Houston, Houston, Texas" ~
                            "Houston, TX")) %>%
  mutate(year = (sql("cast(year(arrivaldate) as string)"))) %>%
  group_by(port, year) %>% 
  summarize(teu_panjiva= sum(volumeteu)) %>%
  arrange(year) %>% collect() %>%  
  mutate(year = paste0(year,"-01-01")) %>%
  mutate(year = as.Date(year, "%Y-%m-%d")) %>%
  as.data.frame() 

panjiva_teu <- rbind(panjiva_teu_four, panjiva_teu_two)

total_panjiva_teu <- import_us %>%
  filter(concountry == "United States" | concountry == "None") %>% 
  mutate(year = (sql("cast(year(arrivaldate) as string)"))) %>%
  filter(!is.na(portofunlading)) %>%
  group_by(year) %>% 
  summarize(total_teu_panjiva= sum(volumeteu)) %>%
  arrange(year) %>% collect() %>%
  mutate(year = paste0(year,"-01-01")) %>%
  mutate(year = as.Date(year, "%Y-%m-%d")) %>%
  as.data.frame() 

# Get Haver TEU
haver_teu = fame2df(c(seattle = "SAIIF",
                      la = "LAIL",
                      ny = "NYI",
                      lb = "LBIL",
                      savannah = "GAIF",
                      houston = "HOUIL"), 
                    db="industry", start = "2008-01-01") %>% 
  mutate(year = as.Date(paste0("01-", month(date),"-", year(date)), "%d-%m-%Y")) %>%
  select(-date) %>%
  mutate(year = year(year)) %>%
  pivot_longer(., cols = c(-year), names_to = "port", values_to = "haver_teu") %>%
  group_by(year, port) %>%
  summarize(teu_haver = sum(haver_teu)) %>%
  mutate(port = case_when(port == "seattle" ~ 
                            "Seattle/Tacoma, WA",
                          port == "ny" ~
                            "Newark, NJ",
                          port== 'la' ~
                            "Los Angeles, CA",
                          port == 'lb' ~
                            "Long Beach, CA",
                          port == "savannah" ~
                            "Savannah, GA",
                          port == "houston" ~
                            "Houston, TX")) %>%
  mutate(year = as.Date(paste0(as.character(year),"-01-01"), "%Y-%m-%d")) %>%
  as.data.frame() 

teu_comparison  <- left_join(left_join(haver_teu, panjiva_teu, by = c("port"="port", "year"="year")), total_panjiva_teu) %>%
  mutate(teu_share_panjiva = teu_panjiva / total_teu_panjiva) %>%
  mutate(year = as.Date(paste0("01-01-", year), format = "%m-%d-%Y"))

# Make table
table_data_weight <- weight_comparison %>%
  filter(year == "2019-01-01") %>%
  select(-c(total_weight_panjiva, total_weight_census, year)) 

table_data_teu <- teu_comparison %>%
  filter(year == "2019-01-01") %>%
  select(-c(total_teu_panjiva, year)) 

column1 <- c(prettyNum(sprintf("%.0f",table_data_weight$weight_panjiva/1000000), big.mark=","))
column2 <- c(prettyNum(sprintf("%.0f",table_data_weight$weight_census/1000000), big.mark=","))
column3 <- c(sprintf("%.2f",table_data_weight$weight_share_panjiva))
column4 <- c(sprintf("%.2f",table_data_weight$weight_share_census))
column5 <- c(sprintf("%.2f",table_data_teu$teu_panjiva/1000000))
column6 <- c(sprintf("%.2f",table_data_teu$teu_haver/1000000))


table <- cbind(column1, column2, column3, column4, column5, column6) 

rownames(table) <- c('\\makecell[l]{Houston, TX}', '\\makecell[l]{Los Angeles, CA}', '\\makecell[l]{Long Beach, CA}', '\\makecell[l]{Newark, NJ}', '\\makecell[l]{Savannah, GA}', '\\makecell[l]{Seattle/Tacoma, WA}')

# Write tex file
setwd('../tables_for_paper')

cat(paste(
  "\\documentclass{article}\n", 
  "\\usepackage[utf8]{inputenc}\n", 
  "\\usepackage{makecell}\n", 
  "\\begin{document}\n",
  "\\begin{table}[ht]\n",
  "\\caption{Comparison of Panjiva and Official Statistics, 2019}\n",
  "\\centering \n",
  "\\begin{tabular}{c c c c c c c}\n",
  "\\hline \n",
  "\\hline \n", 
  "& \\makecell{Panjiva\\\\Weight*} & \\makecell{Census\\\\Weight*} & \\makecell{Panjiva\\\\Weight\\\\Share} & \\makecell{Census\\\\Weight\\\\Share} & \\makecell{Panjiva\\\\TEUs*} & \\makecell{Official\\\\Port\\\\TEUs*} \\\\ \n",
  
  print.xtable(table, only.contents=TRUE, sanitize.text.function = function(x){x}),
  
  "\\multicolumn{7}{l}{\\footnotesize Source: Panjiva, Census, Haver. *In millions.} \\\\",
  "\\hline \n",
  "\\end{tabular} \n",
  "\\label{table:nonlin} \n",
  "\\end{table},",
  "\\end{document}",
  print("")),
  
  file = paste0("tab_census_panjiva_weight.tex")
)


# Run tex file
# we run it twice because LaTex is weird sometimes
system(paste0("pdflatex '","tab_census_panjiva_weight.tex'"))
system(paste0("pdflatex '","tab_census_panjiva_weight.tex'"))

system(paste0("pdftocairo -png -singlefile '","tab_census_panjiva_weight.pdf' '","tab_census_panjiva_weight'"))
rdn_summary <- as.raster(readPNG("tab_census_panjiva_weight.png"))
