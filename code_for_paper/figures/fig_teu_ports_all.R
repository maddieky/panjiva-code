dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('chart_settings.R')

# Read data
data_for_chart <- read.csv('../../data_for_paper/data_teu_ports_all.csv') %>%
  mutate(month = as.Date(month))

# Make chart         
colors <- c("Panjiva: All ports" = approved_colors[['blue']], "Panjiva: Major ports*" = approved_colors[['green']], "Official port statistics: Major ports*" = approved_colors[['red']])

ports_plot <- ggplot(data_for_chart, aes(x = month)) +
  geom_line(aes(y = all_ports, color = "Official port statistics: Major ports*"), size = approved_lines[['blue']]) +
  geom_line(aes(y = total_teu, color = "Panjiva: All ports"), size = approved_lines[['green']]) +
  geom_line(aes(y = panjiva_six, color = "Panjiva: Major ports*"), size = approved_lines[['red']]) + 
  labs(x = "",
       y = "",
       subtitle = "TEUs, thousands")+
  themePanjiva +
  scalesPanjiva +
  legendTheme +
  theme(legend.title = element_blank()) +
  theme(legend.position = c(0.3,0.9)) +
  scale_y_continuous(limits = c(0,3500), breaks = pretty_breaks(7)) +
  scale_color_manual(values=c("Panjiva: All ports" = approved_colors[["blue"]], "Panjiva: Major ports*" = approved_colors[["green"]], "Official port statistics: Major ports*" = approved_colors[["red"]]), 
                     breaks = c("Panjiva: All ports", "Panjiva: Major ports*", "Official port statistics: Major ports*")) +
  scale_x_date(breaks = seq(as.Date("2009-01-01"), as.Date("2021-01-01"), by = "2 years"), date_labels = "%Y")

# Export chart
setwd('../../charts_for_paper')
ggsave("fig_teu_port_level_all.pdf", plot = ports_plot, width = 5, height = 3)
