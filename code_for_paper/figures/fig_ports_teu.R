library(ggpubr)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('chart_settings.R')

# Read data
chart_data <- read.csv('../../data_for_paper/data_ports_teu.csv') %>%
  mutate(month = as.Date(month))

# Make chart
colors <- c("Panjiva" = approved_colors[['red']], "Official port statistics" = approved_colors[['blue']])
portnames <- c("HOUSTON", "NY", "SAVANNAH", "LB", "LA", "SEATTLE")
titles <- c("HOUSTON" = "Houston", "NY" = "Newark", "SAVANNAH" = "Savannah",
            "LB" = "Long Beach", "LA" = "Los Angeles", "SEATTLE" = "Seattle and Tacoma")

for(i in portnames) {
  chart<-ggplot(chart_data, aes(x=month)) + 
    geom_line(aes_string(y=i, color = shQuote("Panjiva")))+ 
    geom_line(aes_string(y=paste0("PORT_",(i)), color= shQuote("Official port statistics")))+
    labs(x = "",
         y = "",
         color = "Legend",
         title = titles[i],
         subtitle = "Thousands") + 
    scale_y_continuous(n.breaks = 5) +
    themePanjiva +
    scalesPanjiva  +
    theme(plot.title = element_text(size = 9),
          plot.subtitle = element_text(size = 6),
          axis.text.x = element_text(size = 6),
          axis.text.y = element_text(size=5),
          axis.ticks.length=unit(-0.10, "cm")) +
    legendTheme +
    scale_color_manual(values=colors) 
  assign(paste0("chart_",i,"_level"), chart)
}

# Export chart
charts_levels = list(chart_LA_level, chart_LB_level, chart_SEATTLE_level, chart_NY_level, chart_SAVANNAH_level, chart_HOUSTON_level)
figure1 <- ggarrange(plotlist = charts_levels, nrow=2, ncol=3, common.legend = TRUE, legend = "bottom") 

setwd('../../charts_for_paper')
ggsave("fig_teu_port_level.pdf", plot = figure1, width = 5.5, height = 4)