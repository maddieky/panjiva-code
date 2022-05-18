dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('chart_settings.R')

# Read data
data_in_export <- read.csv('../../data_for_paper/data_intramonth_in_export.csv') %>%
  mutate(date = as.Date(date))
data_us_import <- read.csv('../../data_for_paper/data_intramonth_us_import.csv') %>%
  mutate(date = as.Date(date))

# Make charts
colors <- c("India exports to the U.S." = approved_colors[["grey"]], "U.S. imports from India" = approved_colors[["blue"]])

fig_intramonth_combined <- ggplot() +
  geom_line(data = data_in_export, aes(x=date, y=daily_shpt_wma_ind, color = "India exports to the U.S."), size = 0.8) + 
  geom_line(data = data_us_import, aes(x=date, y=daily_shpt_wma_ind, color = "U.S. imports from India"), size = 0.5) + 
  geom_vline(aes(xintercept = as.numeric(as.Date("2020-03-24"))), linetype = 4) +
  labs(x = "",
       y = "",
       color = "Legend", 
       subtitle = "Index, Mar. 1, 2020 = 100") + 
  themePanjiva +
  scalesPanjiva +
  legendTheme +
  scale_x_date(breaks = seq(as.Date("2020-03-01"), as.Date("2020-08-01"), by = "1 month"), date_labels = "%b") +
  scale_color_manual(values=colors, breaks = c("U.S. imports from India", "India exports to the U.S.")) +
  scale_y_continuous(breaks = seq(0, 140, by = 20), limits = c(-5, 140)) +
  theme(legend.position = c(0.6, 0.8), legend.key.size = unit(0.25, "cm"))

fig_intramonth_combined <- fig_intramonth_combined +
  annotate("text", x = as.Date("2020-03-09"), y = 35, label = "March 24, 2020", size = 2.3, family = "Palatino")

# Export chart
setwd('../../charts_for_paper')
ggsave("fig_intramonth_combined.pdf", plot = fig_intramonth_combined, width = 5, height = 3)
