library(ggpubr)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('chart_settings.R')

# Read data
bar_data_shp <- read.csv('../../data_for_paper/data_weighted_hist_shp_con.csv') %>%
  mutate(type = as.factor(type))
bar_data_con <- read.csv('../../data_for_paper/data_weighted_hist_con_shp.csv') %>%
  mutate(type = as.factor(type))

levels(bar_data_shp$type)[levels(bar_data_shp$type) == "teu"] <- "Percent of total TEU"
levels(bar_data_shp$type)[levels(bar_data_shp$type) == "consignees"] <- "Percent of total U.S. consignees"

levels(bar_data_con$type)[levels(bar_data_con$type) == "teu"] <- "Percent of total TEU"
levels(bar_data_con$type)[levels(bar_data_con$type) == "shippers"] <- "Percent of total foreign shippers"

levels(bar_data_shp$type)[levels(bar_data_shp$type) == "Total U.S. consignees"] <- "Percent of total U.S. consignees"
levels(bar_data_con$type)[levels(bar_data_con$type) == "Total foreign shippers"] <- "Percent of total foreign shippers"

# Make chart
hist_weighted_shp_1 <- ggplot(bar_data_shp, aes(fill=type, y=percent,x=factor(bin))) +
  geom_bar(position = "dodge", stat = "identity") + 
  scale_fill_manual("", values=c("Percent of total TEU" = approved_colors[["lightblue"]], "Percent of total U.S. consignees" = approved_colors[["blue"]])) +
  scale_x_discrete(labels = c("1", "2-4", "5-9", "10-24", "25+")) +
  labs(x = "Number of foreign shippers",
       y = "",
       title = "", 
       subtitle = "Percent",
       caption = "") +
  theme(legend.position = c(0.6,0.8),
        legend.key.size = unit(0.5, "cm"),
        legend.text = element_text(size=8),
        legend.key=element_blank(),
        legend.background=element_blank()) +
  themePanjiva  +
  scale_y_continuous(limits = c(0,80))


hist_weighted_con_1 <- ggplot(bar_data_con, aes(fill=type, y=percent,x=factor(bin))) +
  geom_bar(position = "dodge", stat = "identity") + 
  scale_fill_manual("", values=c("Percent of total TEU" = approved_colors[["lightblue"]], "Percent of total foreign shippers" = approved_colors[["blue"]])) +
  scale_x_discrete(labels = c("1", "2-4", "5-9", "10-24", "25+")) +
  labs(x = "Number of U.S. consignees",
       y = "",
       title = "", 
       subtitle = "Percent",
       caption = "") +
  theme(legend.position = c(0.6,0.8),
        legend.key.size = unit(0.5, "cm"),
        legend.text = element_text(size=8),
        legend.background = element_blank(),
        legend.key=element_blank()) +
  themePanjiva +
  scale_y_continuous(limits = c(0,80))

charts_list= list(hist_weighted_shp_1, hist_weighted_con_1)
combined_figure <- ggarrange(plotlist = charts_list, nrow=1, ncol=2, common.legend = FALSE) #%>%

setwd('../../charts_for_paper')
ggsave("hist_weighted_combined.pdf", plot = combined_figure, width = 6, height = 3)
