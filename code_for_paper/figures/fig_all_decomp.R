library(zoo)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('chart_settings.R')

# Read data
decomp_all = read.csv("../../data_for_paper/data_decomp_for_R_allimports.csv")
decomp_all$quarter <- yq(decomp_all$qdate)

decomp_all_line = read.csv("../../data_for_paper/data_decomp_line_for_R_allimports.csv")
decomp_all_line$quarter <- yq(decomp_all_line$qdate)

decomp_all$quarter <- as.yearqtr(decomp_all$quarter, format = "%Y-%m-%d")
decomp_all_line$quarter <- as.yearqtr(decomp_all_line$quarter, format = "%Y-%m-%d")

# Make chart
allimports <- ggplot() +
  geom_bar(data = decomp_all, aes(x= quarter, y = pct_chg, fill = interaction(type, val)), 
           position = "stack", stat = "identity", width = 0.15) +
  geom_line(data=decomp_all_line, aes(x = quarter, y = im_line, color = "Total change")) +
  geom_point(data = decomp_all_line, aes(x=quarter, y=im_line, color = "Total change"), size = 1.5) + 
  geom_hline(yintercept=0, size =.5, color = "black") +
  labs(x = "",
       y = "",
       subtitle = "Relative to Average Change in 2017-2019") + 
  themePanjiva +
  scale_y_continuous(limits = c(-15, 35), breaks = seq(-15, 25, by = 10)) +
  scale_x_yearqtr(breaks = seq(from = min(decomp_all$quarter), max(decomp_all$quarter), by = 0.25), 
                  format = "%Y-Q%q") +
  legendTheme +
  scale_fill_manual(values= c(approved_colors[["grey"]], approved_colors[["lightblue"]], approved_colors[["blue"]], approved_colors[["red"]], approved_colors[["black"]]), 
                    labels = c("Redacted", "Intensive Margin", "Add/Drop Shipper or Country Margin", "Entry/Exit of Consignees Margin",  "")) +
  scale_color_manual(values=c("Total change" = approved_colors[["black"]]), breaks = c("Total change"), labels = c("Total change")) +
  theme(legend.position = c(0.3, 0.85), 
        legend.key.size = unit(0.3, "cm"),
        legend.spacing.y = unit(-0.18, "cm")) 

# Export chart
ggsave("../../charts_for_paper/fig_all_decomp.pdf", plot = allimports, width = 5, height = 3)


