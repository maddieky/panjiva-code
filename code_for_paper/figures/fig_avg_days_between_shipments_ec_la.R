library(zoo)
library(gridExtra)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('chart_settings.R')

# Read data
avg_shipments_la = read.csv("../../data_for_paper/data_lalb_numdays.csv")
avg_shipments_ec = read.csv("../../data_for_paper/data_ec_numdays.csv")

avg_shipments_la$month = as.Date(as.yearmon(avg_shipments_la$monthvar,format="%Ym%m"))
avg_shipments_ec$month = as.Date(as.yearmon(avg_shipments_ec$monthvar,format="%Ym%m"))

# Make chart
colors <- c("Trend (2013-2017)" = approved_colors[["black"]], "Los Angeles/ Long Beach" = approved_colors[["lightblue"]])
  
la_shipments <- ggplot() +
  geom_line(data = avg_shipments_la, aes(x=month, y=med_days_2013_2017, color = "Trend (2013-2017)"), size = 0.2) + 
  geom_line(data = avg_shipments_la, aes(x=month, y=med_days, color = "Los Angeles/ Long Beach"), size = 0.6) + 
  labs(x = "",
       y = "",
       color = "", 
       title = "", 
       subtitle = "Number of days") +
  themePanjiva +
  scale_y_continuous(limits = c(40, 60), breaks = seq(36, 60, by = 4)) +
  scale_x_date(limits = c(as.Date("2018-01-01"),as.Date("2021-08-01"))) +
  legendTheme +
  scale_color_manual(values=colors, breaks = c("", "")) +
  theme(legend.position = c(0.2, 0.9), legend.key.size = unit(0.2, "cm"),
        plot.title = element_text(size=10,hjust=0.5)) 

colors <- c("Trend (2013-2017)" = approved_colors[["black"]], "Major East Coast Ports" = approved_colors[["green"]])

ec_shipments <- ggplot() +
  geom_line(data = avg_shipments_ec, aes(x=month, y=med_days_2013_2017, color = "Trend (2013-2017)"), size = 0.2) + 
  geom_line(data = avg_shipments_ec, aes(x=month, y=weighted_med_days, color = "Major East Coast Ports"), size = 0.6) + 
  labs(x = "",
       y = "",
       color = "Legend",
       title =  "",
       subtitle = "Number of days") +
  themePanjiva +
  scale_y_continuous(limits = c(56, 80), breaks = seq(56, 80, by = 4)) +
  scale_x_date(limits = c(as.Date("2018-01-01"),as.Date("2021-08-01"))) +
  legendTheme +
  scale_color_manual(values=colors, breaks = c("", "")) +
  theme(legend.position = c(0.2, 0.9), legend.key.size = unit(0.2, "cm"),
        plot.title = element_text(size=10,hjust=0.5)) 

# Export chart
setwd('../../charts_for_paper')
ggsave("avg_days_port_visit_LA.pdf", plot = la_shipments, width = 3.5, height = 3)
ggsave("avg_days_port_visit_EC.pdf", plot = ec_shipments, width = 3.5, height = 3)

combined <- grid.arrange(la_shipments, ec_shipments, nrow = 1)
ggsave("fig_combined_LA_EC.pdf", plot = combined, width = 5.5, height = 3)