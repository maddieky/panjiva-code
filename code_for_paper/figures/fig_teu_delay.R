dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('chart_settings.R')

# Read data
df = read.csv("../../data_for_paper/panjiva_delay.csv")

to_plot <- df %>%
  filter(portofunlading %in% c("Georgia Ports Authority, Savannah, Georgia", 
                               "New York/Newark Area, Newark, New Jersey",
                               "Port of Long Beach, Long Beach, California",
                               "The Port of Los Angeles, Los Angeles, California"))

# Make chart
group.colors <- c("Georgia Ports Authority, Savannah, Georgia" = approved_colors[["blue"]], 
                  "New York/Newark Area, Newark, New Jersey" = approved_colors[["grey"]],
                  "Port of Long Beach, Long Beach, California" = approved_colors[["red"]],
                  "The Port of Los Angeles, Los Angeles, California" = approved_colors[["green"]])

teu_delay <- ggplot(to_plot, aes(x= date_cutoff, y = avg_delay_percent, group = portofunlading, color = portofunlading)) +
  geom_line() + 
  labs(x = "Days following the end of the month",
       y = "",
       color = "", 
       title = "", 
       subtitle = "Percent of final TEU estimate for the month") +
  geom_vline(aes(xintercept = 28), linetype = 4) +
  geom_text(aes(x=22, label="Census advance \n trade release", y=90, hjust = 0), colour="black", family = "Palatino", size = 2) +
  geom_vline(aes(xintercept = 20), linetype = 4) +
  geom_text(aes(x=14, label="Avg. port-level \n data release*", y=70, hjust = 0), colour="black", family = "Palatino", size = 2) +
  themePanjiva +
  scale_y_continuous(limits = c(30, 100), breaks = seq(30, 100, by = 10)) +
  scale_x_continuous(limits = c(-14, 30), breaks = seq(-14, 30, by = 2)) +
  scale_colour_manual(values=group.colors) +
  legendTheme +
  theme(legend.position = c(0.5, 0.25), legend.key.size = unit(0.05, "cm"),legend.text = element_text(size = 6))

# Export chart
setwd('../../charts_for_paper')
ggsave("fig_teu_delay.pdf", plot = teu_delay, width = 6, height = 3)

