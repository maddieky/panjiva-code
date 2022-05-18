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
  filter(!is.na(shppanjivaid) & !is.na(volumeteu)) %>%
  select(shppanjivaid, shpmtorigin, shpname) %>%
  collect() %>%
  as.data.frame()

top_shippers = import_us %>%
  mutate(year = (sql("cast(year(arrivaldate) as string)"))) %>%
  filter(year == "2020") %>%
  filter(!is.na(shppanjivaid) & !is.na(volumeteu)) %>%
  filter(concountry == "United States" | concountry == "None") %>% 
  group_by(shppanjivaid) %>%
  mutate(shipments = 1) %>%
  summarize(shpt = sum(shipments), teu_count = sum(volumeteu)) %>%
  arrange(desc(teu_count)) %>%
  collect() %>%
  as.data.frame() 

total_teu <- sum(top_shippers$teu_count)
total_shp <- sum(top_shippers$shpt)

top_shippers = top_shippers %>%
  mutate(teu_shr = top_shippers$teu_count/total_teu * 100) %>%
  mutate(shp_shr = top_shippers$shpt/total_shp * 100)

top_10shp = top_shippers %>%
  top_n(10, teu_count)

top_10shp = top_10shp %>%
  mutate(merge_flag = 1)

merge = left_join(top_10shp, import_2020, by = "shppanjivaid")

merge_distinct = merge %>%
  distinct(shppanjivaid, .keep_all = TRUE) 

final_merge = merge_distinct %>%
  arrange(desc(teu_count))

final_merge = final_merge[, c("shpname", "shpmtorigin", "teu_count", "teu_shr", "shp_shr")]

# Make table
column1 <- c(final_merge$shpmtorigin)
column2 <- c(prettyNum(sprintf("%.0f",final_merge$teu_count), big.mark=","))
column3 <- c(sprintf("%.2f",final_merge$teu_shr))
column4 <- c(sprintf("%.2f",final_merge$shp_shr))
table <- cbind(column1, column2, column3, column4) 
rownames(table) <- final_merge$shpname

# Write tex file
cat(paste(
  "\\documentclass{article}\n", 
  "\\usepackage[utf8]{inputenc}\n", 
  "\\usepackage{makecell}\n", 
  "\\begin{document}\n",
  "\\begin{table}[ht]\n",
  "\\caption{Top shippers by total TEU}\n",
  "\\centering \n",
  "\\begin{tabular}{c c c c c}\n", 
  "\\hline \n",
  "\\hline \n", 
  "\\makecell{Shipper name} & \\makecell{Country} & \\makecell{Total TEU} & \\makecell{TEU (\\%)} & \\makecell{Shipments (\\%)} \\\\",
  
  print.xtable(table, only.contents=TRUE, sanitize.text.function = function(x){x}),
  
  "\\multicolumn{5}{l}{\\footnotesize Source: Panjiva.} \\\\",
  "\\hline \n",
  "\\end{tabular} \n",
  "\\label{table:nonlin} \n",
  "\\end{table},",
  "\\end{document}",
  print("")),
  
  file = paste0("tab_top_shippers.tex")
)

# Run the tex file
# we run it twice because LaTex is weird sometimes
system(paste0("pdflatex '","tab_top_shippers.tex'"))
system(paste0("pdflatex '","tab_top_shippers.tex'"))

system(paste0("pdftocairo -png -singlefile '","tab_top_shippers.pdf' '","tab_top_shippers'"))
rdn_summary <- as.raster(readPNG("tab_top_shippers.png"))