monthly_var = function(df) {
  w = df %>%
    mutate(year = (sql("cast(year(arrivaldate) as string)")), 
           mon = (sql("cast(month(arrivaldate) as string)"))) %>%
    mutate(month = paste0(year, "/", mon))
  return(w)
}

index_dplr = function(df, col_names) {
  w = df %>%
    mutate(yrdate = substring(month, 1, 4)) %>%
    mutate(mondate = case_when(substring(month,6) == '1' ~ "-01-01",
                               substring(month,6) == '2' ~ "-02-01",
                               substring(month,6) == '3' ~ "-03-01",
                               substring(month,6) == '4' ~ "-04-01",
                               substring(month,6) == '5' ~ "-05-01",
                               substring(month,6) == '6' ~ "-06-01",
                               substring(month,6) == '7' ~ "-07-01",
                               substring(month,6) == '8' ~ "-08-01",
                               substring(month,6) == '9' ~ "-09-01",
                               substring(month,6) == '10' ~ "-10-01",
                               substring(month,6) == '11' ~ "-11-01",
                               substring(month,6) == '12' ~ "-12-01")) %>%
    mutate(month = paste0(yrdate,mondate)) %>%
    mutate(month = as.Date(month, '%Y-%m-%d')) %>%
    select(-c(yrdate, mondate))
  return(w)
}

teu_data = function(df) {
  w = df  %>%
    group_by(portofunlading, month) %>% 
    summarize(total_teu= sum(volumeteu)) %>%
    arrange(month) %>% collect() %>% 
    index_dplr(.,'teu') %>%
    arrange(portofunlading, month)
  return(w)
}

teu_data2 = function(df) {
  w = df  %>%
    filter(!is.na(portofunlading)) %>%
    group_by(month) %>%
    summarize(total_teu= sum(volumeteu)) %>%
    arrange(month) %>% collect() %>%
    index_dplr(.,'teu') %>%
    arrange(month)
  return(w)
}

