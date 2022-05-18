dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('chart_settings.R')

# Read data
data_combined <- read.csv('../../data_for_paper/data_transport_2019.csv') %>%
  mutate(key = as.factor(key),
         type = as.factor(type))

data_combined$key <- factor(data_combined$key, levels = c("oth_pct", "air_pct", "ves_pct"))
data_combined$type <- factor(data_combined$type, levels = c("Imports", "Exports"))

# Make chart
chart_combined <- ggplot(data = data_combined, aes(x=key, y=value, fill=type)) +
  geom_bar(stat = "identity", position = position_dodge()) +
  themePanjiva +
  scale_fill_manual(values=c(approved_colors[["blue"]],approved_colors[["lightblue"]])) + 
  labs(x = "",
       y = "",
       subtitle = "Share of total, percent") +
  theme(legend.title = element_blank()) + 
  theme(legend.position = c(0.9,0.2),
        legend.key.size =  unit(0.4, 'cm')) +
  theme(plot.caption =element_text(hjust=0)) +
  scale_x_discrete(labels = c("Other transport", "Air", "Vessel")) +
  coord_flip() +
  guides(fill = guide_legend(reverse = TRUE))

# Export chart
setwd('../../charts_for_paper')
ggsave("fig_transport_2019_combined.pdf", plot = chart_combined, width = 5, height = 3)