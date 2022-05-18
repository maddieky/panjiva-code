dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('chart_settings.R')

# Read data
teu_shpt_by_month <- read.csv('../../data_for_paper/data_walmart_redaction.csv') %>%
  mutate(date = as.Date(date))

# Make chart
fig_walmart_redaction_shpt <- ggplot(teu_shpt_by_month, aes(x=date)) +
  geom_line(aes(y=total_shpt), color = approved_colors[['blue']], size = 0.5) + 
  themePanjiva + 
  scalesPanjiva +
  scale_x_date(breaks=pretty_breaks(6)) +
  scale_x_date(breaks = seq(as.Date("2009-01-01"), as.Date("2021-01-01"), by = "3 years"), date_labels = "%Y") +
  scale_y_continuous(limit = c(0, 20000)) +
  labs(x = "",
       y = "",
       color = "Legend", 
       subtitle = "Shipments, monthly") 

fig_walmart_redaction_shpt <- fig_walmart_redaction_shpt + 
  annotate("text", x = as.Date("2010-01-08"), y = 2000, label = "Mostly redacted", size = 2.5, family= "Palatino") +
  annotate("text", x = as.Date("2015-06-08"), y = 2000, label = "Mostly redacted", size = 2.5, family= "Palatino") +
  annotate("text", x = as.Date("2012-12-08"), y = 17000, label = "Not fully \nredacted", size = 2.5, family= "Palatino") +
  annotate("text", x = as.Date("2007-11-08"), y = 19000, label = "Not fully \nredacted", size = 2.5, family= "Palatino") +
  annotate("text", x = as.Date("2019-06-08"), y = 8000, label = "Not fully \nredacted", size = 2.5, family= "Palatino") 

# Export chart
setwd('../../charts_for_paper')
ggsave("fig_walmart_redaction_shpt.pdf", plot = fig_walmart_redaction_shpt, dpi=700, width = 5, height = 3)
