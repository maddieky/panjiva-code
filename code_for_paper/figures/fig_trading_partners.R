dir <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(dir)
source('chart_settings.R')

# Read data
fig_export <- read.csv('../../data_for_paper/data_trading_partners_export.csv') %>%
  mutate(variable = as.factor(variable),
         categories = as.factor(categories)) %>%
  arrange(desc(variable))
fig_import <- read.csv('../../data_for_paper/data_trading_partners_import.csv') %>%
  mutate(variable = as.factor(variable),
         categories = as.factor(categories))

levels(fig_export$variable)[levels(fig_export$variable) == "teu_shr"] <- "TEUs"
levels(fig_export$variable)[levels(fig_export$variable) == "shpt_shr"] <- "Shipments"
levels(fig_export$variable)[levels(fig_export$variable) == "census_shr"] <- "Known export value (Census)"
fig_export$variable <- factor(fig_export$variable, levels = c("TEUs", "Shipments", "Known export value (Census)"))
fig_export$categories <- factor(fig_export$categories, levels = c("(0,1]", "(1,4]", "(4,9]", "(9,24]", "(24,49]", "(49,200]"))

colors_exports <- c("TEUs" = approved_colors[["lightblue"]], "Shipments" = approved_colors[["blue"]], "Known export value (Census)" = approved_colors[["grey"]])

levels(fig_import$variable)[levels(fig_import$variable) == "teu_shr_imp"] <- "TEUs"
levels(fig_import$variable)[levels(fig_import$variable) == "shpt_shr_imp"] <- "Shipments"
levels(fig_import$variable)[levels(fig_import$variable) == "census_shr_imp"] <- "Known import value (Census)"
fig_import$variable <- factor(fig_import$variable, levels = c("TEUs", "Shipments", "Known import value (Census)"))
fig_import$categories <- factor(fig_import$categories, levels = c("(0,1]", "(1,4]", "(4,9]", "(9,24]", "(24,49]", "(49,200]"))

colors_imports <- c("TEUs" = approved_colors[["lightblue"]], "Shipments" = approved_colors[["blue"]], "Known import value (Census)" = approved_colors[["grey"]])

# Make charts
fig_export_trading_countries <- ggplot(fig_export, aes(fill = variable, x = categories, y = value)) +
  geom_bar(position = "dodge", stat = "identity") +
  scale_fill_manual("", values=colors_exports, breaks = c("TEUs", "Shipments", "Known export value (Census)")) +
  xlab("Number of partner countries") + 
  ylab("") +
  labs(subtitle = "Percent") + 
  themePanjiva +
  legendTheme +
  theme(legend.title = element_blank()) +
  scale_x_discrete(labels = c("1", "2-4", "5-9", "10-24", "25-49", "50-200")) +
  theme(legend.position = c(0.3,0.8))  

fig_import_trading_countries <- ggplot(fig_import, aes(fill = variable, x = categories, y = value)) +
  geom_bar(position = "dodge", stat = "identity") +
  scale_fill_manual("", values=c(approved_colors[["lightblue"]], approved_colors[["blue"]], approved_colors[['grey']])) +
  xlab("Number of partner countries") + 
  ylab("") +
  labs(subtitle = "Percent") +
  themePanjiva +
  legendTheme +
  theme(legend.title = element_blank()) +
  scale_x_discrete(labels = c("1", "2-4", "5-9", "10-24", "25-49", "50-200")) +
  theme(legend.position = c(0.25,0.8)) 

# Export charts
setwd('../../charts_for_paper')
ggsave("fig_trading_partners_imports.pdf", plot = fig_import_trading_countries, width = 5, height = 3)
ggsave("fig_trading_partners_exports.pdf", plot = fig_export_trading_countries, width = 5, height = 3)