index_series = function(series_col){
  w = 100*(series_col/series_col[1])
  return(w)
}

index_dplr = function(df,col_names){ 
  w = df %>% 
    mutate(date = paste0(date,"-01-01")) %>%
    mutate(date = as.Date(date, '%Y-%d-%m')) %>%
    filter(date > as.Date('2008-01-01')) %>%
    mutate_at(vars(matches(col_names)), list(index_series)) %>% 
    filter(date < as.Date('2021-01-01'))
  return(w)
}

index_dplr_mon = function(df, col_names) { 
  w = df %>% 
    mutate_at(vars(matches(col_names)), list(index_series)) 
  return(w)
}

month_var = function(df) {
  w = df %>%
    mutate(year = (sql("cast(year(arrivaldate) as string)")), 
           month = (sql("cast(month(arrivaldate) as string)"))) %>%
    mutate(date = paste0(year, "-", month))
  return(w)
}

fix_ym_partition = function(df) {
  w = df %>%
    mutate(year = sql("left(cast(ym_partition as string), 4)")) %>%
    mutate(month = sql("right(cast(ym_partition as string), 2)")) %>%
    mutate(date=paste0(year, "-", month))
  return(w)
}

shpt_data_mon = function(df) {
  w = df  %>%
    summarize(total_shpt= n()) %>%
    arrange(date) %>% collect() %>%
    mutate(date = paste0(date, "-01")) %>%
    mutate(date = as.Date(date, '%Y-%m-%d')) %>%
    filter(date > as.Date('2008-12-01'))
  return(w)
}

teu_data_mon = function(df) {
  w = df  %>%
    summarize(total_teu= sum(volumeteu)) %>%
    arrange(date) %>% collect() %>%
    mutate(date = paste0(date, "-01")) %>%
    mutate(date = as.Date(date, '%Y-%m-%d')) %>%
    filter(date > as.Date('2008-12-01')) 
  return(w)
}

value_data_mon = function(df) {
  w = df  %>%
    summarize(tot_value = sum(gen_val_mo), cnt_value = sum(cnt_val_mo), ves_value = sum(ves_val_mo),
              ves_weight = sum(ves_wgt_mo), cnt_weight = sum(cnt_wgt_mo)) %>%
    arrange(date) %>% collect() %>%
    mutate(date = paste0(date, "-01")) %>%
    mutate(date = as.Date(date, '%Y-%m-%d')) %>%
    filter(date > as.Date('2008-12-01'))
  return(w)
}