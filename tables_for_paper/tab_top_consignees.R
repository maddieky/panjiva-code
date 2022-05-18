library(tidyverse)
library(DBI)
library(lubridate)
library(xtable)
library(readxl)
library(grDevices)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('../code_for_paper/functions_panjiva.R')

import_us <- dplyr::tbl(conn_panjiva, 'panjivausimport')

import_2020 = import_us %>%
 mutate(year = (sql("cast(year(arrivaldate) as string)"))) %>%
 filter(year == "2020") %>%
 filter(!is.na(conpanjivaid) & !is.na(volumeteu)) %>%
 select(conpanjivaid, conname) %>%
 collect() %>%
 as.data.frame()

top_consignees = import_us %>%
  mutate(year = (sql("cast(year(arrivaldate) as string)"))) %>%
  filter(year == "2020") %>%
  filter(!is.na(conpanjivaid) & !is.na(volumeteu)) %>%
  filter(concountry == "United States" | concountry == "None") %>% 
  group_by(conpanjivaid) %>%
  mutate(shipments = 1) %>%
  summarize(shpt = sum(shipments), teu_count = sum(volumeteu)) %>%
  arrange(desc(teu_count)) %>%
  collect() %>%
  as.data.frame() 

total_teu <- sum(top_consignees$teu_count)
total_shp <- sum(top_consignees$shpt)

top_consignees = top_consignees %>%
  mutate(teu_shr = top_consignees$teu_count/total_teu * 100) %>%
  mutate(shp_shr = top_consignees$shpt/total_shp * 100)

top_10con = top_consignees %>%
  top_n(10, teu_count)

top_10con = top_10con %>%
  mutate(merge_flag = 1)

merge = left_join(top_10con, import_2020, by = "conpanjivaid")

merge_distinct = merge %>%
  distinct(conpanjivaid, .keep_all = TRUE) 
  
  final_merge = merge_distinct %>%
  arrange(desc(teu_count))

final_merge = final_merge[, c("conname", "teu_count", "teu_shr", "shp_shr")]

# Make table 
column2 <- c(prettyNum(sprintf("%.0f",final_merge$teu_count), big.mark=","))
column3 <- c(sprintf("%.2f",final_merge$teu_shr))
column4 <- c(sprintf("%.2f",final_merge$shp_shr))
table <- cbind(column2, column3, column4) 
rownames(table) <- final_merge$conname

# Write the tex file 
cat(paste(
  "\\documentclass{article}\n", 
  "\\usepackage[utf8]{inputenc}\n", 
  "\\usepackage{makecell}\n", 
  "\\begin{document}\n",
  "\\begin{table}[ht]\n",
  "\\caption{Top consignees by total TEU}\n",
  "\\centering \n",
  "\\begin{tabular}{c c c c}\n", 
  "\\hline \n", 
  "\\hline \n", 
  "\\makecell{Consignee name} & \\makecell{Total TEU} & \\makecell{TEU (\\%)} & \\makecell{Shipments (\\%)} \\\\",
  
  print.xtable(table, only.contents=TRUE, sanitize.text.function = function(x){x}),
  
   "\\multicolumn{4}{l}{\\footnotesize Source: Panjiva.} \\\\",
   "\\hline \n",
   "\\end{tabular} \n",
   "\\label{table:nonlin} \n",
   "\\end{table},",
   "\\end{document}",
   print("")),
  
  file = paste0("tab_top_consignees.tex")
)

# Run the tex file 
# we run it twice because LaTex is weird sometimes
system(paste0("pdflatex '","tab_top_consignees.tex'"))
system(paste0("pdflatex '","tab_top_consignees.tex'"))

system(paste0("pdftocairo -png -singlefile '","tab_top_consignees.pdf' '","tab_top_consignees'"))
rdn_summary <- as.raster(readPNG("tab_top_consignees.png"))






