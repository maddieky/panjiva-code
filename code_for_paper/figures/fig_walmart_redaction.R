dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('chart_settings.R')

# Read data
teu_shpt_by_month <- read.csv('../../data_for_paper/data_walmart_redaction.csv') %>%
  mutate(date = as.Date(date))

teu_shpt_by_month$conname <- factor(teu_shpt_by_month$conname, levels = c('"Walmart"', '"Wal Mart Stores, Inc."',
                                                                          '"Walmart Inc. Bentonville"', "Other"))
# Make chart
fig_walmart_redaction_shpt <- ggplot(teu_shpt_by_month, aes(x=date, y=total_shpt_companyid,fill = conname)) +
  geom_area() +
  scale_fill_manual("",values=c('"Walmart"' = approved_colors[["blue"]],'"Wal Mart Stores, Inc."' = approved_colors[["lightblue"]],
                                 '"Walmart Inc. Bentonville"' = approved_colors[["grey"]], "Other" = approved_colors[["black"]])) +
  themePanjiva + 
  scalesPanjiva +
  legendTheme +
  scale_x_date(breaks = seq(as.Date("2009-01-01"), as.Date("2021-01-01"), by = "3 years"), date_labels = "%Y") +
  scale_y_continuous(limit = c(0, 20000)) +
  labs(x = "",
       y = "",
       color = "Legend", 
       subtitle = "Shipments, monthly") +
  theme(legend.position = c(0.8, 0.85))

fig_walmart_redaction_shpt <- fig_walmart_redaction_shpt + 
  annotate("text", x = as.Date("2010-01-08"), y = 2000, label = "Mostly redacted", size = 2.5, family= "Palatino") +
  annotate("text", x = as.Date("2015-06-08"), y = 2000, label = "Mostly redacted", size = 2.5, family= "Palatino") +
  annotate("text", x = as.Date("2012-12-08"), y = 17000, label = "Not fully \nredacted", size = 2.5, family= "Palatino") +
  annotate("text", x = as.Date("2007-11-08"), y = 19000, label = "Not fully \nredacted", size = 2.5, family= "Palatino") +
  annotate("text", x = as.Date("2019-12-08"), y = 8000, label = "Not fully \nredacted", size = 2.5, family= "Palatino") 

  # Export chart
setwd('../../charts_for_paper')
ggsave("fig_walmart_redaction_shpt.pdf", plot = fig_walmart_redaction_shpt, dpi=700, width = 5, height = 3)