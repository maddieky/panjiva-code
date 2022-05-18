dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('chart_settings.R')

# Read data
df_chart <- read.csv('../../data_for_paper/data_hist_con_shp_shpt_per_year_month.csv') %>%
  select(-X) %>%
  pivot_longer(!count, names_to = "type", values_to = "percent") %>%
  mutate(type = as.factor(type))

levels(df_chart$type)[levels(df_chart$type) == "teu"] <- "Percent of total TEU"
levels(df_chart$type)[levels(df_chart$type) == "n"] <- "Percent of total long-term shipper-consignee pairs*"

# Make chart
hist_con_shp_shpt_per_year <- ggplot(df_chart, aes(fill = type, y=percent, x=count)) +
  geom_bar(stat="identity", position="dodge") +
  scale_x_continuous(breaks=pretty_breaks(6)) + #limits=c(0,15), 
  scale_y_continuous(labels = function(x) format(x*100, scientific = FALSE), sec.axis = dup_axis(name = NULL, labels = NULL)) +
  scale_fill_manual("",values=c("Percent of total TEU" = approved_colors[['lightblue']],"Percent of total long-term shipper-consignee pairs*" = approved_colors[['blue']])) +
  theme(legend.position = c(0.4,0.8)) +
  themePanjiva +
  legendTheme +
  labs(x = "Number of calendar months with at least one transaction",
       y = "",
       subtitle = "Percent")

# Export
setwd('../../charts_for_paper')
ggsave("fig_hist_con_shp_per_year_month.pdf", plot = hist_con_shp_shpt_per_year, width = 5, height = 3)