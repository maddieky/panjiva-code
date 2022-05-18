dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('chart_settings.R')

# Read data
data_mon <- read.csv('../../data_for_paper/data_teu_shpt_val.csv') %>%
  mutate(date = as.Date(date))

# Make charts
colors <- c("TEUs (Panjiva)" = approved_colors[["blue"]], "Shipments (Panjiva)" = approved_colors[["red"]], "Total Import Value (Census)" = approved_colors[["green"]])
fig_import_mon <- ggplot(data_mon, aes(x=date)) +
 geom_line(aes(y=total_teu, color = "TEUs (Panjiva)")) + 
 geom_line(aes(y=total_shpt, color = "Shipments (Panjiva)")) + 
 geom_line(aes(y=tot_value, color = "Total Import Value (Census)")) +
  labs(x = "",
       y = "",
       color = "Legend", 
       subtitle = "Index, 2009 = 100") +
  scale_color_manual(values=colors, breaks = c("TEUs (Panjiva)","Shipments (Panjiva)","Total Import Value (Census)")) +
  themePanjiva + 
  scalesPanjiva +
  legendTheme +
  theme(legend.position = c(0.3, 0.8)) + 
  scale_x_date(breaks = seq(as.Date("2009-01-01"), as.Date("2021-01-01"), by = "2 years"), date_labels = "%Y") +
  scale_y_continuous(limits = c(90, 240), breaks = seq(100, 240, by = 20))


colors <- c("TEUs (Panjiva)" = approved_colors[["blue"]], "Shipments (Panjiva)" = approved_colors[["red"]], "Containerized Vessel Value (Census)" = approved_colors[["green"]])
fig_import_mon_cnt <- ggplot(data_mon, aes(x=date)) +
  geom_line(aes(y=total_teu, color = "TEUs (Panjiva)")) + 
  geom_line(aes(y=total_shpt, color = "Shipments (Panjiva)")) + 
  geom_line(aes(y=cnt_value, color = "Containerized Vessel Value (Census)")) +
  labs(x = "",
       y = "",
       color = "Legend", 
       subtitle = "Index, 2009 = 100") +
  scale_color_manual(values=colors, breaks = c("TEUs (Panjiva)","Shipments (Panjiva)","Containerized Vessel Value (Census)")) +
  themePanjiva + 
  scalesPanjiva +
  legendTheme +
  theme(legend.position = c(0.3, 0.8)) + 
  scale_x_date(breaks = seq(as.Date("2009-01-01"), as.Date("2021-01-01"), by = "2 years"), date_labels = "%Y") +
  scale_y_continuous(limits = c(80, 240), breaks = seq(100, 260, by = 20))

# Export chart
setwd('../../charts_for_paper')
ggsave("fig_teu_shpt_val_monthly.pdf", plot = fig_import_mon, width = 5, height = 3)
ggsave("fig_teu_shpt_val_monthly_cnt.pdf", plot = fig_import_mon_cnt, width = 5, height = 3)