############ GUIDELINES FOR PANJIVA CHARTS ############## 

# Only capitalize the first word of titles/labels
# Use serif font
# Export charts with width = 5, height = 3 (as a general rule)

# Call this file at the beg. of your chart code by including these lines:
#   source('chart_settings.R')

##########################################################
library(lubridate)
library(tidyverse)
library(ggplot2)
library(scales)

### SCALES - Use this for line charts
# This will add a mirrored scale on the right side
scalesPanjiva <- scale_y_continuous(breaks=pretty_breaks(6), 
                                    sec.axis = dup_axis(name = NULL, labels = NULL)) 

### THEME - Use this in all charts to create white background
themePanjiva <- 
  theme(
    axis.ticks.length = unit(-0.2, "cm"),
    plot.margin       = unit(c(0.2, 0.5, 0, 0), "cm"), 
    axis.text         = element_text(size = 8),
    axis.title        = element_text(size = 8, face = "plain"),
    plot.title        = element_text(size = 11, face = 'plain'),
    plot.caption      = element_text(size = 9, hjust = 0),
    plot.subtitle     = element_text(size = 9, hjust = 1),
    axis.text.x       = element_text(margin = margin(t = 10), color = "black", size = 9),
    axis.text.y       = element_text(margin = margin(r = 9.5), color = "black", size = 9),
    axis.text.y.right = element_text(margin = margin(l = 9.5), size = 9, hjust = 0),
    axis.ticks        = element_line(size = 0.3, color = "black"),
    axis.ticks.x      = element_line(size = 0.3),
    axis.line.x       = element_line(size = 0.3),
    axis.ticks.y      = element_line(size = 0.3),
    text = element_text(family="Palatino"),
    panel.grid.major = element_blank(), 
    panel.grid.minor = element_blank(),
    panel.background = element_blank())  

### LEGEND SETTINGS 
# This removes the legend title and makes the 
#  background behind the legend symbols white
legendTheme = theme(legend.title = element_blank(),
                    legend.key.size =  unit(0.15, "in"),
                    legend.key.width = unit(.65, "cm"),
                    legend.text = element_text(size = 8),
                    legend.background = element_blank(),
                    legend.key=element_blank())

# COLORS - Use these colors in charts
#  Call them in your code like this: approved_colors[["blue"]]
#  Use blue, red, green, and black for line charts
#  Use blue, lightblue for bar charts
approved_colors <- c("blue" = rgb(0, 118/255, 171/255), 
                     "red" = rgb(237/255, 24/255, 73/255), 
                     "black" = rgb(0,0,0),
                     "green" = rgb(99/255, 240/255, 63/255),
                     "lightblue" = rgb(119/255, 169/255, 206/255),
                     "grey" = rgb(147/255, 149/255, 152/255))

approved_lines <- c("blue" = 0.5, 
                    "red" = 0.8, 
                    "black" = 0.2,
                    "green" = 0.5)