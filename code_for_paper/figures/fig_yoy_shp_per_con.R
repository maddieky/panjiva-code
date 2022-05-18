dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('chart_settings.R')

# Read data
data_shp_per_con_chart <- read.csv('../../data_for_paper/data_shp_per_con.csv') %>%
  mutate(date = as.Date(date)) %>%
  mutate(pct = yoy_mon_avg_shp *100)

# Make chart
fig_shp_per_con <- ggplot(data_shp_per_con_chart, aes(x=date)) +
  geom_hline(yintercept = 0, size = 0.3) +
  geom_line(aes(y=pct), color = approved_colors['blue'], size = approved_lines['blue']) + 
  themePanjiva +
  scalesPanjiva +
  scale_x_date(breaks = seq(as.Date("2020-01-01"), as.Date("2021-09-01"), by = "6 months"), date_labels = "%b. %Y") +
  scale_y_continuous(limits = c (-50, 30), breaks = seq(-40, 30, by = 20)) +
  labs(x = "",
       y = "",
       color = "Legend", 
       subtitle = "Percent change from same month in 2019")

# Export charts
setwd('../../charts_for_paper')
ggsave("fig_shp_per_con.pdf", plot = fig_shp_per_con, width = 5, height = 3)
