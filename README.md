## Panjiva Data Methods

Code used to create the tables and figures in our FEDS note: "Bill of Lading Data in International Trade Research with an Application to the Covid-19 Pandemic" (Flaaen et al.).

# Data Manipulation

All files used to query/manipulate data from our Panjiva Hadoop database are located in code_for_paper/data_manipulation or code_for_paper/stata:

* Figs 1 + 2: pull_teu_shpt_val.R
* Fig 3: pull_transport_2019.R
* Fig 4: pull_mode_of_transport.R
* Fig 5: pull_walmart_redaction.R
* Fig 6: pull_weighted_hist_shp_con.R 
* Fig 7: pull_hist_con_shp_shpt_per_year.R
* Fig 8: pull_yoy_shp_per_con.R 
* Fig 9: pull_intramonth.R 
* Fig 10a: query_furniture_decomp.R, pull_furniture_decomp_step1.do, pull_furniture_decomp_step2.do
* Fig 10b: query_all_decomp.R, pull_all_decomp_step1.do, pull_all_decomp_step2.do 
* Figs 11+12: query_port_to_port_data.R, step1_vessel_clean.do, step2_vessel_clean.do, port_analysis.do
* Fig 13: pull_ports_teu.R 
* Fig 14: pull_teu_ports_all.R 
* Fig 15: 
* Figs 16 + 17: pull_trading_partners.R 

Each file outputs a csv of data that can be used as the input for the charting scripts listed in the next section.

# Charting

All charts are created using [ggplot2](https://ggplot2.tidyverse.org/). All files used to create charts are in code_for_paper/figures:

* Figs 1 + 2: fig_teu_shpt_val.R
* Fig 3: fig_transport_2019.R
* Fig 4: fig_mode_of_transport.R
* Fig 5: fig_walmart_redaction.R
* Fig 6: fig_weighted_hist_shp_con.R 
* Fig 7: fig_hist_con_shp_shpt_per_year.R
* Fig 8: fig_yoy_shp_per_con.R 
* Fig 9: fig_intramonth.R 
* Fig 10a: fig_furn_decomp.R
* Fig 10b: fig_all_decomp.R
* Figs 11: fig_avg_days_between_shipments_ec_la.R
* Fig 12: fig_inbound_reroute_la_lb.R
* Fig 13: fig_ports_teu.R
* Fig 14: fig_teu_ports_all.R 
* Fig 15: fig_teu_delay.R
* Figs 16 + 17: fig_trading_partners.R 

# Tables

All files used to create tables are in tables_for_paper:

* Tab 2: tab_transport_2019_by_country_levels.R 
* Tab 3: tab_missing_data.R 
* Tab 4: tab_top_consignees.R 
* Tab 5: tab_top_shippers.R 
* Tab 6: tab_census_panjiva_weight.R 
