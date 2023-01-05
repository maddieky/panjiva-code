library(zoo)

dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('chart_settings.R')

# Read data
route = read.csv("../../data_for_paper/data_lalb_reroutes.csv") %>%
  pivot_wider(., names_from = "portofunlading_new", values_from = "TEU_share_diff")

route$qdate <- paste(c("2020-Q1", "2020-Q2", "2020-Q3", "2020-Q4", "2021-Q1", "2021-Q2"))
route$qdate = as.yearqtr(route$qdate, format = "%Y-Q%q")

# Make chart
colors <- c("Ports of Seattle-Tacoma" = approved_colors[["blue"]], "Port of Oakland" = approved_colors[["red"]], "Avg. of Major East Coast Ports" = approved_colors[["green"]])

inbound_route <- ggplot() +
  geom_line(data = route, aes(x=qdate, y=seattle, color = "Ports of Seattle-Tacoma"), size = 0.6) + 
  geom_point(data = route, aes(x=qdate, y=seattle, color = "Ports of Seattle-Tacoma"), size = 0.8) + 
  geom_line(data = route, aes(x=qdate, y=oakland, color = "Port of Oakland"), size = 0.6) + 
  geom_point(data = route, aes(x=qdate, y=oakland, color = "Port of Oakland"), size = 0.8) + 
  geom_line(data = route, aes(x=qdate, y=ec, color = "Avg. of Major East Coast Ports"), size = 0.6) + 
  geom_point(data = route, aes(x=qdate, y=ec, color = "Avg. of Major East Coast Ports"), size = 0.8) + 
  geom_hline(yintercept=0, size =.5, color = "black") +
  labs(x = "",
       y = "",
       color = "Legend", 
       subtitle = "Percent of inbound LA-LB TEUs") +
  themePanjiva +
  scale_y_continuous(limits = c(-3, 8), breaks = seq(-2, 10, by = 2)) +
  scale_x_yearqtr(format = "%Y-Q%q", n=6) +
    legendTheme +
  scale_color_manual(values=colors, breaks = c("Ports of Seattle-Tacoma", "Port of Oakland", "Avg. of Major East Coast Ports")) +
  theme(legend.position = c(0.3, 0.85), legend.key.size = unit(0.3, "cm"))

# Export chart
ggsave("../../charts_for_paper/fig_inbound_route_la_lb.pdf", plot = inbound_route, width = 5, height = 3)