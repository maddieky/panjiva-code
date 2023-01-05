library(ggseas)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('chart_settings.R')

# Read data
monthly_import_data <- read.csv('../../data_for_paper/data_mode_of_transport_import.csv') %>%
  mutate(date = as.Date(date))
monthly_export_data <- read.csv('../../data_for_paper/data_mode_of_transport_export.csv') %>%
  mutate(date = as.Date(date))

# Make chart
list = c("import", "export") 
colors <- c("Vessel" = approved_colors[["blue"]], "Air" = approved_colors[["red"]], "Other" = approved_colors[["green"]])

fig_monthly_import_sa <- ggplot(monthly_import_data, aes(x=date)) +
  stat_seas(aes(y=ves_value, color = "Vessel"), start=c(2009,1), frequency = 12) +
  stat_seas(aes(y=air_value, color = "Air"), start=c(2009,1), frequency = 12) +
  stat_seas(aes(y=oth_value, color = "Other"), start=c(2009,1), frequency = 12) +
  labs(x = "",
       y = "",
       color = "Legend",
       subtitle = "Billions of US dollars, monthly") +
  themePanjiva + 
  scalesPanjiva + 
  legendTheme +
  scale_color_manual(values=colors, breaks = c("Vessel","Air","Other")) +
  scale_y_continuous(breaks = pretty_breaks(5), limits = c(20, 110)) +
  scale_x_date(breaks = seq(as.Date("2009-01-01"), as.Date("2021-01-01"), by = "3 years"), date_labels = "%Y") +
  theme(legend.position = c(0.75, 0.15), legend.key.size = unit(0.2, "cm"))

# Export charts
setwd('../../charts_for_paper')
ggsave("fig_transport_imp_monthly_sa.pdf", plot = fig_monthly_import_sa, width = 5, height = 3)